/*-------------------------------------------------------------------------
 *
 * polar_flushlist.c
 *	  routines for managing the buffer pool's flushlist.
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 *
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
 *	  src/backend/storage/buffer/polar_flushlist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/polar_logindex_redo.h"
#include "storage/polar_flushlist.h"
#include "storage/polar_bufmgr.h"
#include "utils/guc.h"

#define buffer_not_in_flush_list(buf) \
    (buf->flush_prev == POLAR_FLUSHNEXT_NOT_IN_LIST && buf->flush_next == POLAR_FLUSHNEXT_NOT_IN_LIST)
#define current_pos_is_unavailable() \
	(polar_flush_list_ctl->current_pos == POLAR_FLUSHNEXT_NOT_IN_LIST)

FlushListControl *polar_flush_list_ctl = NULL;

extern XLogRecPtr GetXLogInsertRecPtr();
extern bool polar_in_replica_mode(void);

static void remove_one_buffer(BufferDesc *buf);
static void append_one_buffer(BufferDesc *buf);

/*
 * polar_flush_list_ctl_shmem_size
 *
 * Estimate the size of shared memory used by the flushlist-related structure.
 */
Size
polar_flush_list_ctl_shmem_size(void)
{
	Size        size = 0;

	if (!polar_flush_list_enabled())
		return size;

	/* Size of the shared flushlist control block */
	size = add_size(size, MAXALIGN(sizeof(FlushListControl)));

	return size;
}

/*
 * polar_init_flush_list_ctl -- initialize the flushlist control
 */
void
polar_init_flush_list_ctl(bool init)
{
	bool found;

	if (!polar_flush_list_enabled())
		return;

	/* Get or create the shared memory for flushlist control block */
	polar_flush_list_ctl = (FlushListControl *)
						   ShmemInitStruct("Flushlist Control Status",
										   sizeof(FlushListControl), &found);

	if (!found)
	{
		/* Only done once, usually in postmaster */
		Assert(init);

		pg_atomic_init_u32(&polar_flush_list_ctl->count, 0);

		SpinLockInit(&polar_flush_list_ctl->flushlist_lock);

		polar_flush_list_ctl->first_flush_buffer = POLAR_FLUSHNEXT_END_OF_LIST;
		polar_flush_list_ctl->last_flush_buffer  = POLAR_FLUSHNEXT_END_OF_LIST;
		polar_flush_list_ctl->current_pos = POLAR_FLUSHNEXT_NOT_IN_LIST;
		polar_flush_list_ctl->latest_flush_count = 0;

		pg_atomic_init_u64(&polar_flush_list_ctl->insert, 0);
		pg_atomic_init_u64(&polar_flush_list_ctl->remove, 0);
		pg_atomic_init_u64(&polar_flush_list_ctl->find, 0);
		pg_atomic_init_u64(&polar_flush_list_ctl->batch_read, 0);
		pg_atomic_init_u64(&polar_flush_list_ctl->cbuf, 0);
	}
	else
		Assert(!init);
}

/*
 * polar_get_batch_flush_buffer
 *
 * Get a batch of buffers from flushlist and do not remove it, FlushBuffer will
 * remove them from flushlist.
 */
int
polar_get_batch_flush_buffer(int *batch_buf)
{
	int         i = 0;
	int         buffer_id;
	int         flush_count;

	Assert(polar_flush_list_enabled());

	SpinLockAcquire(&polar_flush_list_ctl->flushlist_lock);
	if (polar_flush_list_is_empty())
	{
		SpinLockRelease(&polar_flush_list_ctl->flushlist_lock);
		return i;
	}

	flush_count = polar_flush_list_ctl->latest_flush_count;

	if (current_pos_is_unavailable())
		polar_flush_list_ctl->current_pos =
			polar_flush_list_ctl->first_flush_buffer;

	buffer_id = polar_flush_list_ctl->current_pos;

	for (i = 0; i < polar_bgwriter_batch_size_flushlist; i++)
	{
		Assert(buffer_id != POLAR_FLUSHNEXT_NOT_IN_LIST);

		if (buffer_id == POLAR_FLUSHNEXT_END_OF_LIST)
			break;

		batch_buf[i] = buffer_id;
		buffer_id = GetBufferDescriptor(buffer_id)->flush_next;
	}

	/*
	 * If latest flush count greater than polar_bgwriter_max_batch_size,
	 * revert it to first buffer.
	 */
	if (buffer_id == POLAR_FLUSHNEXT_END_OF_LIST ||
		(flush_count + i) > polar_bgwriter_max_batch_size)
	{
		polar_flush_list_ctl->current_pos = polar_flush_list_ctl->first_flush_buffer;
		polar_flush_list_ctl->latest_flush_count = 0;
	}
	else
	{
		polar_flush_list_ctl->current_pos = buffer_id;
		polar_flush_list_ctl->latest_flush_count += i;
	}

	SpinLockRelease(&polar_flush_list_ctl->flushlist_lock);

	pg_atomic_fetch_add_u64(&polar_flush_list_ctl->batch_read, 1);

	return i;
}

/*
 * polar_remove_buffer_from_flush_list
 *
 * If the buffer has been flushed, remove it from flushlist.
 */
void
polar_remove_buffer_from_flush_list(BufferDesc *buf)
{
	if (!polar_flush_list_enabled())
		return;

	SpinLockAcquire(&polar_flush_list_ctl->flushlist_lock);
	polar_buffer_set_oldest_lsn(buf, InvalidXLogRecPtr);
	remove_one_buffer(buf);
	SpinLockRelease(&polar_flush_list_ctl->flushlist_lock);

	pg_atomic_fetch_sub_u32(&polar_flush_list_ctl->count, 1);
	pg_atomic_fetch_add_u64(&polar_flush_list_ctl->remove, 1);
}

/*
 * polar_put_buffer_to_flush_list
 *
 * When buffer is modified for the first time, add it to flushlist. If lsn is
 * invalid, we will set a fake lsn. Caller should have required the buffer
 * content exclusive lock already.
 */
void
polar_put_buffer_to_flush_list(BufferDesc *buf,
							   XLogRecPtr lsn)
{
	SpinLockAcquire(&polar_flush_list_ctl->flushlist_lock);

	/* The buffer must be not in flushlist */
	Assert(buffer_not_in_flush_list(buf));

	/* Allocate the current insert lsn as a fake oldest lsn */
	if (XLogRecPtrIsInvalid(lsn))
	{
		polar_buffer_set_fake_oldest_lsn(buf);
	}
	else
		polar_buffer_set_oldest_lsn(buf, lsn);

	append_one_buffer(buf);
	SpinLockRelease(&polar_flush_list_ctl->flushlist_lock);

	/* Outside the spin lock to update statistic info. */
	pg_atomic_fetch_add_u32(&polar_flush_list_ctl->count, 1);
	pg_atomic_fetch_add_u64(&polar_flush_list_ctl->insert, 1);
}

/*
 * polar_adjust_position_in_flush_list
 *
 * Adjust the position of buffer to keep the flushlist order, only set a fake
 * oldest lsn.
 */
void
polar_adjust_position_in_flush_list(BufferDesc *buf)
{
	SpinLockAcquire(&polar_flush_list_ctl->flushlist_lock);

	/* Buffer must be in flushlist */
	Assert(!buffer_not_in_flush_list(buf));

	polar_buffer_set_fake_oldest_lsn(buf);

	/*
	 * If it is the last one, its oldest lsn is the greatest, so do not need
	 * to adjust its position.
	 */
	if (buf->flush_next != POLAR_FLUSHNEXT_END_OF_LIST)
	{
		Assert(buf->flush_next != POLAR_FLUSHNEXT_NOT_IN_LIST);

		/* Not the tail, remove and append it into flushlist */
		remove_one_buffer(buf);
		append_one_buffer(buf);
	}

	SpinLockRelease(&polar_flush_list_ctl->flushlist_lock);
	pg_atomic_fetch_add_u64(&polar_flush_list_ctl->cbuf, 1);
}

/*
 * Remove one buffer from flushlist, caller should already acquired the
 * flushlist lock.
 */
static void
remove_one_buffer(BufferDesc *buf)
{
	int         prev_flush_id;
	int         next_flush_id;
	BufferDesc *prev_buf;
	BufferDesc *next_buf;

	/* The buffer must be in flushlist */
	Assert(!buffer_not_in_flush_list(buf));

	/* Flushlist must be not empty */
	Assert(!polar_flush_list_is_empty());

	prev_flush_id = buf->flush_prev;
	next_flush_id = buf->flush_next;

	if (polar_enable_debug)
		POLAR_LOG_BUFFER_DESC_WITH_FLUSHLIST(buf);

	if (prev_flush_id == POLAR_FLUSHNEXT_END_OF_LIST &&
		next_flush_id == POLAR_FLUSHNEXT_END_OF_LIST)
	{
		/* Only this buffer in flushlist */
		polar_flush_list_ctl->first_flush_buffer = POLAR_FLUSHNEXT_END_OF_LIST;
		polar_flush_list_ctl->last_flush_buffer = POLAR_FLUSHNEXT_END_OF_LIST;
	}
	else if (prev_flush_id == POLAR_FLUSHNEXT_END_OF_LIST &&
		next_flush_id != POLAR_FLUSHNEXT_END_OF_LIST)
	{
		/* First one, and has next buffer */
		next_buf = GetBufferDescriptor(next_flush_id);
		next_buf->flush_prev = prev_flush_id;
		polar_flush_list_ctl->first_flush_buffer = next_flush_id;
	}
	else if (prev_flush_id != POLAR_FLUSHNEXT_END_OF_LIST &&
		next_flush_id == POLAR_FLUSHNEXT_END_OF_LIST)
	{
		/* Last one, and has prev buffer */
		prev_buf = GetBufferDescriptor(prev_flush_id);
		prev_buf->flush_next = next_flush_id;
		polar_flush_list_ctl->last_flush_buffer = prev_flush_id;
	}
	else
	{
		/* Middle */
		next_buf = GetBufferDescriptor(next_flush_id);
		prev_buf = GetBufferDescriptor(prev_flush_id);
		prev_buf->flush_next = next_flush_id;
		next_buf->flush_prev = prev_flush_id;
	}

	if (buf->buf_id == polar_flush_list_ctl->current_pos)
	{
		if (next_flush_id == POLAR_FLUSHNEXT_END_OF_LIST)
			polar_flush_list_ctl->current_pos = polar_flush_list_ctl->first_flush_buffer;
		else
			polar_flush_list_ctl->current_pos = next_flush_id;
	}

	/* Remove buffer from flushlist */
	buf->flush_next = POLAR_FLUSHNEXT_NOT_IN_LIST;
	buf->flush_prev = POLAR_FLUSHNEXT_NOT_IN_LIST;
}

/*
 * Append one buffer to flushlist, caller should already acquired the
 * flushlist lock.
 */
static void
append_one_buffer(BufferDesc *buf)
{
	if (unlikely(polar_enable_debug))
		POLAR_LOG_BUFFER_DESC_WITH_FLUSHLIST(buf);

	if (unlikely(polar_flush_list_is_empty()))
	{
		buf->flush_next = POLAR_FLUSHNEXT_END_OF_LIST;
		buf->flush_prev = POLAR_FLUSHNEXT_END_OF_LIST;

		polar_flush_list_ctl->first_flush_buffer = buf->buf_id;
		polar_flush_list_ctl->last_flush_buffer = buf->buf_id;
	}
	else
	{
		BufferDesc *tail = GetBufferDescriptor(polar_flush_list_ctl->last_flush_buffer);

		if (unlikely(tail->oldest_lsn > buf->oldest_lsn))
			elog(PANIC, "Append buffer with a small oldest lsn than last buffer in flush list.");

		Assert(tail->flush_next == POLAR_FLUSHNEXT_END_OF_LIST);

		buf->flush_prev = tail->buf_id;
		buf->flush_next = tail->flush_next;
		tail->flush_next = buf->buf_id;

		/* Append at the tail */
		polar_flush_list_ctl->last_flush_buffer = buf->buf_id;
	}
}