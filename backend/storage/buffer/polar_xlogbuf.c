/*-------------------------------------------------------------------------
 *
 * polar_xlogbuf.c
 *	  xlog buffer manager.
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/polar_xlogbuf.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "storage/polar_xlogbuf.h"
#include "storage/shmem.h"
#include "utils/guc.h"

extern int polar_xlog_page_buffers;

polar_xlog_buffer_ctl_t polar_xlog_buffer_ctl = NULL;

void
polar_init_xlog_buffer(void)
{
	bool 	found,
			foundDesc,
			foundBlock,
			foundLock;

	Size		block_count = (Size)(polar_xlog_page_buffers * 1024 / (XLOG_BLCKSZ / 1024));

	polar_xlog_buffer_ctl = (polar_xlog_buffer_ctl_t)
			ShmemInitStruct(
					"XLog Block Buffer ctl", 
					MAXALIGN(sizeof(polar_xlog_buffer_ctl_data_t)), 
					&found);

	polar_xlog_buffer_ctl->buffer_descriptors = (polar_xlog_buffer_desc_padded*)
			ShmemInitStruct(
					"XLog Block Buffer Descriptor",
					block_count * sizeof(polar_xlog_buffer_desc_padded),
					&foundDesc);
	polar_xlog_buffer_ctl->buffers = (char *)
			ShmemInitStruct(
					"XLog Block Buffer",
					block_count * XLOG_BLCKSZ,
					&foundBlock);
	 polar_xlog_buffer_ctl->buffer_lock_array = (LWLockMinimallyPadded *)
			ShmemInitStruct(
					"XLog Block Buffer Lock",
					block_count * sizeof(LWLockMinimallyPadded),
					&foundLock);
	if (!IsUnderPostmaster)
	{
		int i;

		Assert(!found && !foundDesc && !foundBlock && !foundLock);

		LWLockRegisterTranche(LWTRANCHE_XLOG_BUFFER_CONTENT, "xlog_buffer_content");

		for (i = 0; i < block_count; ++i)
		{
			polar_xlog_buffer_desc* buf = polar_get_xlog_buffer_desc(i);

			buf->buf_id = i;
			buf->start_lsn = InvalidXLogRecPtr;
			buf->end_lsn = InvalidXLogRecPtr;
			LWLockInitialize(polar_xlog_buffer_desc_get_lock(buf), LWTRANCHE_XLOG_BUFFER_CONTENT);
		}
	}
	else
		Assert(found && foundDesc && foundBlock && foundLock);
}

Size
polar_xlog_buffer_shmem_size(void)
{
	Size	size = 0;
	Size	block_count = (Size)(polar_xlog_page_buffers * 1024 / (XLOG_BLCKSZ / 1024));

	/* size of xlog buffer ctl */
	size = add_size(size, MAXALIGN(sizeof(polar_xlog_buffer_ctl_data_t)));
	/* size of xlog block buffer descriptor */
	size = add_size(size, mul_size(block_count, sizeof(polar_xlog_buffer_desc_padded)));
	/* to allow aligning the above */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of xlog buffer blocks */
	size = add_size(size, mul_size(block_count, XLOG_BLCKSZ));

	/* size of xlog buffer lock */
	size = add_size(size, mul_size(block_count, sizeof(LWLockMinimallyPadded)));
	/* to allow aligning the above */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	return size;
}

/*
 * POLAR: Lookup the buffer page using lsn.
 *
 * buf_id will be set when buffer matched or current buffer should be evicted.
 * When buffer should be evicted, buf_id will be set the id of evicted buffer, and because requested page
 * is not matched, so return false.
 *
 * If buffer is matched, return true, otherwise return false.
 */
bool
polar_xlog_buffer_lookup(XLogRecPtr lsn, int len, bool doEvict, bool doCount, int *buf_id)
{
	static uint64 total_count = 0;
	static uint64 hit_count = 0;
	static uint64 evict_count = 0;
	static uint64 direct_io_count = 0;

	polar_xlog_buffer_desc  *buf;

	Assert(polar_xlog_offset_aligned(lsn));
	Assert(len >= 0 && len <= XLOG_BLCKSZ);

	/* Log hit ratio every 1000w page lookup */
	if (total_count % 10000000 == 0 && total_count > 0)
    {
		ereport(LOG, (errmsg("XLog Buffer Hit Ratio: hit_count=%ld, evict=%ld, direct_io=%ld, total_count=%ld, "
					"hit_ratio=%f, evict_ratio=%f, direct_io_ratio=%f",
					hit_count, evict_count, direct_io_count, total_count,
					hit_count * 1.0 /total_count, evict_count * 1.0 /total_count, direct_io_count * 1.0 /total_count)));
    }

	if (doCount)
		total_count++;

	*buf_id = polar_get_xlog_buffer_id(lsn);
	Assert(*buf_id >= 0);

	buf = polar_get_xlog_buffer_desc(*buf_id);

	/* Obtain shared lock to check meta info */
	polar_xlog_buffer_lock(buf->buf_id, LW_SHARED);

	if (buf->start_lsn == lsn && buf->end_lsn >= (lsn + len - 1))
	{
		if (doCount)
			hit_count++;
		return true;
	}
	else if (!doEvict || !polar_xlog_buffer_should_evict(buf, lsn, len))
	{
		polar_xlog_buffer_unlock(buf->buf_id);
		*buf_id = -1;
		if (doCount)
			direct_io_count++;
		return false;
	}

	polar_xlog_buffer_unlock(buf->buf_id);

	polar_xlog_buffer_lock(buf->buf_id, LW_EXCLUSIVE);

	buf->start_lsn = lsn;
	buf->end_lsn = lsn + len - 1;
	if (doCount)
		evict_count++;
	return false;
}

void
polar_xlog_buffer_lock(int buf_id, LWLockMode mode)
{
	LWLockAcquire(polar_xlog_buffer_desc_get_lock(polar_get_xlog_buffer_desc(buf_id)), mode);
}

void
polar_xlog_buffer_unlock(int buf_id)
{
	LWLockRelease(polar_xlog_buffer_desc_get_lock(polar_get_xlog_buffer_desc(buf_id)));
}

/*
 * POLAR: Update xlog buffer page meta.
 *
 * It will update the end_lsn of the page containing this lsn to lsn.
 */
void
polar_xlog_buffer_update(XLogRecPtr lsn)
{
	XLogRecPtr		page_off = lsn - (lsn % XLOG_BLCKSZ);
	int			buf_id = polar_get_xlog_buffer_id(lsn);
	polar_xlog_buffer_desc		*buf = NULL;

	Assert(buf_id >= 0);

	buf = polar_get_xlog_buffer_desc(buf_id);
	polar_xlog_buffer_lock(buf_id, LW_EXCLUSIVE);
	/* 
	 * Ensure page buffer the expected one. 
	 * While stream replication broken, xlog page may be read by twophase
	 * related logic. After startup replay all xlog at local storage, it will
	 * invalidation xlog buffer data related to last record. In some situation,
	 * this invalidation operation may enlarge xlog buffer size at page without
	 * more data filled, so it may cause zero data being read by other twophase
	 * related operation which will print ERROR log when cannot read record.
	 * So we add the check here to ensure updated lsn not larger than original
	 * buffer meta info, because this update func is only used while meet invalid
	 * xlog record.
	 */
	if (buf->start_lsn == page_off && buf->end_lsn > lsn)
		buf->end_lsn = lsn;
	polar_xlog_buffer_unlock(buf_id);
}

/*
 * POLAR: Remove page from xlog buffer.
 *
 * If buffer is io-progress, it do nothing.
 */
void
polar_xlog_buffer_remove(XLogRecPtr lsn)
{
	XLogRecPtr		page_off = lsn - (lsn % XLOG_BLCKSZ);
	int			buf_id = polar_get_xlog_buffer_id(lsn);
	polar_xlog_buffer_desc		*buf = NULL;

	Assert(buf_id >= 0);

	buf = polar_get_xlog_buffer_desc(buf_id);
	polar_xlog_buffer_lock(buf_id, LW_EXCLUSIVE);

	/* Ensure page buffer the expected one */
	if (buf->start_lsn == page_off)
	{
		buf->start_lsn = InvalidXLogRecPtr;
		buf->end_lsn = InvalidXLogRecPtr;
	}
	polar_xlog_buffer_unlock(buf_id);
}

/*
 * POLAR: Judge whether evict the buffer page from desc or not.
 *
 * Caller should hold the desc lock.
 *
 * For now, if requested page is older than buffed page, it won't evict buffer.
 */
bool
polar_xlog_buffer_should_evict(polar_xlog_buffer_desc *buf, XLogRecPtr lsn, int len)
{
	/* If buffer valid size is smaller than current data, evict it.  */
	if (buf->end_lsn >= lsn + len - 1)
		return false;

	return true;
}

/*
 * POLAR: Reset all xlog buffer.
 */
void
polar_xlog_buffer_reset_all_buffer(void)
{
    Size    block_count = (Size)(polar_xlog_page_buffers * 1024 / (XLOG_BLCKSZ / 1024));
    int buf_id = 0;
    polar_xlog_buffer_desc  *buf = NULL;

    if (!polar_enable_xlog_buffer)
        return;

    for (buf_id = 0; buf_id < block_count; ++buf_id)
    {
        buf = polar_get_xlog_buffer_desc(buf_id);
        polar_xlog_buffer_lock(buf_id, LW_EXCLUSIVE);
        buf->start_lsn = InvalidXLogRecPtr;
        buf->end_lsn = InvalidXLogRecPtr;
        polar_xlog_buffer_unlock(buf_id);
    }
}

