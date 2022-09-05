/*-------------------------------------------------------------------------
 *
 * polar_copybuf.c
 *	  Basic copy buffer manager.
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
 *	  src/backend/storage/buffer/polar_copybuf.c
 *
 *-------------------------------------------------------------------------
 */
#include "polar_flashback/polar_flashback_log.h"
#include "storage/polar_copybuf.h"
#include "storage/polar_bufmgr.h"
#include "storage/polar_flushlist.h"
#include "utils/guc.h"

CopyBufferControl    *polar_copy_buffer_ctl;
CopyBufferDescPadded *polar_copy_buffer_descriptors;
char                 *polar_copy_buffer_blocks;

extern XLogRecPtr GetXLogInsertRecPtr(void);
extern void XLogFlush(XLogRecPtr record);
extern bool polar_in_replica_mode(void);
extern XLogRecPtr polar_get_consistent_lsn(void);
extern XLogRecPtr polar_get_oldest_applied_lsn(void);

static void init_copy_buffer_ctl(bool init);
static bool copy_buffer_alloc(BufferDesc *buf, XLogRecPtr oldest_apply_lsn);
static bool start_copy_buffer_io(BufferDesc *buf, bool for_input);

/*
 * Initialize shared copy buffer pool
 *
 * This is called once during shared-memory initialization (either in the
 * postmaster, or in a standalone backend).
 */
void
polar_init_copy_buffer_pool(void)
{
	bool		found_copy_bufs,
				found_copy_descs;

	if (!polar_copy_buffer_enabled())
		return;

	/* Align descriptors to a cacheline boundary. */
	polar_copy_buffer_descriptors = (CopyBufferDescPadded *)
									ShmemInitStruct("Copy Buffer Descriptors",
											polar_copy_buffers * sizeof(CopyBufferDescPadded),
											&found_copy_descs);

	polar_copy_buffer_blocks = (char *)
							   ShmemInitStruct("Copy Buffer Blocks",
											   polar_enable_buffer_alignment ?
											   	POLAR_BUFFER_EXTEND_SIZE(polar_copy_buffers * (Size) BLCKSZ) :
												polar_copy_buffers * (Size) BLCKSZ,
											   &found_copy_bufs);
	if (polar_enable_buffer_alignment)
		polar_copy_buffer_blocks = (char *) POLAR_BUFFER_ALIGN(polar_copy_buffer_blocks);

	if (found_copy_bufs || found_copy_descs)
	{
		/* should find all of these, or none of them */
		Assert(found_copy_descs && found_copy_bufs);
	}
	else
	{
		int			i;

		/*
		 * Initialize all the copy buffer headers.
		 */
		for (i = 0; i < polar_copy_buffers; i++)
		{
			CopyBufferDesc *cbuf = polar_get_copy_buffer_descriptor(i);

			CLEAR_BUFFERTAG(cbuf->tag);
			cbuf->buf_id = i;
			cbuf->oldest_lsn = InvalidXLogRecPtr;
			cbuf->origin_buffer = 0;
			pg_atomic_init_u32(&cbuf->pass_count, 0);
			cbuf->is_flushed = false;
			cbuf->state = POLAR_COPY_BUFFER_UNUSED;

			/*
			 * Initially link all the buffers together as unused
			 */
			cbuf->free_next = i + 1;
		}

		/* Correct last entry of linked list */
		polar_get_copy_buffer_descriptor(polar_copy_buffers - 1)->free_next = FREENEXT_END_OF_LIST;

		/* Initialize control structure */
		init_copy_buffer_ctl(!found_copy_descs);
	}
}

static void
init_copy_buffer_ctl(bool init)
{
	bool        found;

	polar_copy_buffer_ctl = (CopyBufferControl *)
							ShmemInitStruct("Copy Buffer Status",
											sizeof(CopyBufferControl), &found);

	if (!found)
	{
		/* Only done once, usually in postmaster */
		Assert(init);

		SpinLockInit(&polar_copy_buffer_ctl->copy_buffer_ctl_lock);

		polar_copy_buffer_ctl->first_free_buffer = 0;
		polar_copy_buffer_ctl->last_free_buffer = polar_copy_buffers - 1;

		pg_atomic_init_u64(&polar_copy_buffer_ctl->flushed_count, 0);
		pg_atomic_init_u64(&polar_copy_buffer_ctl->release_count, 0);
		pg_atomic_init_u64(&polar_copy_buffer_ctl->copied_count, 0);
		pg_atomic_init_u64(&polar_copy_buffer_ctl->unavailable_count, 0);
		pg_atomic_init_u64(&polar_copy_buffer_ctl->full_count, 0);
	}
	else
		Assert(!init);
}

/*
 * polar_copy_buffer_shmem_size
 *
 * Compute the size of shared memory for the copy buffer pool including
 * data pages, buffer descriptors, etc.
 */
Size
polar_copy_buffer_shmem_size(void)
{
	Size        size = 0;

	if (!polar_copy_buffer_enabled())
		return size;

	/* size of copy buffer descriptors, polar_copy_buffers unit is BLOCKS */
	size = add_size(size, mul_size(polar_copy_buffers, sizeof(CopyBufferDescPadded)));
	/* to allow aligning buffer descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* POLAR: extra alignment padding for data I/O buffers */
	if (polar_enable_buffer_alignment)
		size = POLAR_BUFFER_EXTEND_SIZE(size);
	/* size of data pages */
	size = add_size(size, mul_size(polar_copy_buffers, BLCKSZ));

	/* size of copy buffer control */
	size = add_size(size, MAXALIGN(sizeof(CopyBufferControl)));

	return size;
}

/*
 * Allocate a copy buffer and copy the buffer data into it.
 * Caller should pin and share-lock this buffer.
 */
void
polar_buffer_copy_if_needed(BufferDesc *buf, XLogRecPtr oldest_apply_lsn)
{
	Assert(polar_copy_buffer_enabled());

	if (copy_buffer_alloc(buf, oldest_apply_lsn))
	{
		pg_atomic_add_fetch_u64(&polar_copy_buffer_ctl->copied_count, 1);
	}
	else
	{
		pg_atomic_add_fetch_u64(&polar_copy_buffer_ctl->unavailable_count, 1);
	}
}

/* Check if the buffer is satisfied to allocate a copy buffer. */
bool
polar_buffer_copy_is_satisfied(BufferDesc *buf,
							   XLogRecPtr oldest_apply_lsn,
							   bool check_io)
{
	uint32     buf_state;
	uint32     lsn_lag_with_cons_lsn;
	XLogRecPtr oldest_lsn;
	bool       is_dirty;

	if (!polar_copy_buffer_enabled())
		return false;

	if (!polar_enable_shared_storage_mode ||
		polar_in_replica_mode() ||
		XLogRecPtrIsInvalid(oldest_apply_lsn) ||
		buf->tag.forkNum == FSM_FORKNUM ||
		buf->tag.forkNum == INIT_FORKNUM ||
		polar_donot_control_vm_buffer(buf) ||
		BufferGetLSN(buf) <= oldest_apply_lsn)
		return false;

	buf_state = LockBufHdr(buf);

	/* Already has a copy buffer */
	if (buf->copy_buffer)
	{
		UnlockBufHdr(buf, buf_state);
		return false;
	}

	UnlockBufHdr(buf, buf_state);

	oldest_lsn = polar_buffer_get_oldest_lsn(buf);
	is_dirty = buf_state & BM_DIRTY;

	if (XLogRecPtrIsInvalid(oldest_lsn))
	{
		elog(WARNING,
			 "The oldest lsn of buffer [%u,%u,%u], %u, %d) to copy is invalid, dirty: %d, page flags: %d",
			 buf->tag.rnode.spcNode,
			 buf->tag.rnode.dbNode,
			 buf->tag.rnode.relNode,
			 buf->tag.blockNum,
			 buf->tag.forkNum,
			 is_dirty, ((PageHeader)BufferGetPage(buf->buf_id + 1))->pd_flags);

		/* If buffer oldest lsn is invalid, it must be not in the flushlist */
		//Assert(buf->flush_next == POLAR_FLUSHNEXT_NOT_IN_LIST);
		//Assert(buf->flush_prev == POLAR_FLUSHNEXT_NOT_IN_LIST);
		return false;
	}

	if (!(buf_state & BM_PERMANENT) ||
			(check_io && (buf_state & BM_IO_IN_PROGRESS)))
		return false;

	lsn_lag_with_cons_lsn = oldest_lsn - polar_get_consistent_lsn();

	if ((buf->recently_modified_count > polar_buffer_copy_min_modified_count &&
		lsn_lag_with_cons_lsn <
			polar_buffer_copy_lsn_lag_with_cons_lsn * 1024 * 1024) ||
		lsn_lag_with_cons_lsn <
			polar_buffer_copy_lsn_lag_with_cons_lsn * 1024 * 1024 / 2)
		return true;

	return false;
}

static bool
copy_buffer_alloc(BufferDesc *buf, XLogRecPtr oldest_apply_lsn)
{
	CopyBufferDesc *cbuf;
	uint32         buf_state;
	XLogRecPtr     consistent_lsn;

	/* Before copy, lock the buffer using io_in_progress lock like FlushBuffer */
	if (!start_copy_buffer_io(buf, true))
		return false;

	/* Double check */
	if (!polar_buffer_copy_is_satisfied(buf, oldest_apply_lsn, false))
	{
		TerminateBufferIO(buf, false, 0);
		return false;
	}

	consistent_lsn = polar_get_consistent_lsn();

	if (polar_enable_debug)
	{
		elog(DEBUG1,
			 "Copy page ([%u,%u,%u], %u), page latest lsn %X/%X, page oldest lsn %X/%X, consistent lsn %X/%X",
			 buf->tag.rnode.spcNode,
			 buf->tag.rnode.dbNode,
			 buf->tag.rnode.relNode,
			 buf->tag.blockNum,
			 (uint32) (BufferGetLSN(buf) >> 32),
			 (uint32) (BufferGetLSN(buf)),
			 (uint32) (polar_buffer_get_oldest_lsn(buf) >> 32),
			 (uint32) polar_buffer_get_oldest_lsn(buf),
			 (uint32) (consistent_lsn >> 32),
			 (uint32) consistent_lsn);
	}

	/* Acquire the spinlock to remove element from the freelist */
	SpinLockAcquire(&polar_copy_buffer_ctl->copy_buffer_ctl_lock);

	if (polar_copy_buffer_ctl->first_free_buffer < 0)
	{
		static TimestampTz last_log_time = 0;

		SpinLockRelease(&polar_copy_buffer_ctl->copy_buffer_ctl_lock);
		TerminateBufferIO(buf, false, 0);
		pg_atomic_fetch_add_u64(&polar_copy_buffer_ctl->full_count, 1);

		/* Do not log frequently, every 1s log one */
		if (TimestampDifferenceExceeds(last_log_time, GetCurrentTimestamp(), 1000))
		{
			elog(WARNING, "Copy buffer pool is full, pool size is %d",
				 polar_copy_buffers);
			last_log_time = GetCurrentTimestamp();
		}

		return false;
	}

	cbuf = polar_get_copy_buffer_descriptor(polar_copy_buffer_ctl->first_free_buffer);

	Assert(cbuf->free_next != FREENEXT_NOT_IN_LIST);

	/* Unconditionally remove buffer from freelist */
	polar_copy_buffer_ctl->first_free_buffer = cbuf->free_next;
	cbuf->free_next = FREENEXT_NOT_IN_LIST;

	/*
	 * Release the lock so someone else can access the freelist while
	 * we check out this buffer.
	 */
	SpinLockRelease(&polar_copy_buffer_ctl->copy_buffer_ctl_lock);

	Assert(cbuf->state == POLAR_COPY_BUFFER_UNUSED);
	Assert(!XLogRecPtrIsInvalid(buf->oldest_lsn));

	memcpy(CopyBufHdrGetBlock(cbuf), BufHdrGetBlock(buf), BLCKSZ);

	SET_COPYBUFFERTAG(cbuf->tag, buf->tag);
	pg_atomic_write_u64((pg_atomic_uint64 *) &cbuf->oldest_lsn,
						buf->oldest_lsn);
	pg_atomic_write_u32((pg_atomic_uint32 *) &cbuf->origin_buffer,
					   (uint32) BufferDescriptorGetBuffer(buf));
	cbuf->state = POLAR_COPY_BUFFER_USED;

	buf_state = LockBufHdr(buf);
	buf->copy_buffer = cbuf;
	UnlockBufHdr(buf, buf_state);

	TerminateBufferIO(buf, false, 0);

	return true;
}

/*
 * polar_free_copy_buffer - free a copy buffer and put it into the freelist
 *
 * Caller have acquired the buffer's io_in_progress lock.
 */
void
polar_free_copy_buffer(BufferDesc *buf)
{
	uint32         buf_state;
	CopyBufferDesc *cbuf = buf->copy_buffer;

	if (cbuf)
	{
		if (polar_enable_debug)
		{
			elog(DEBUG1,
				 "Free copy buffer ([%u,%u,%u], %u), cbuf oldest lsn %X/%X, latest lsn %X/%X, is_flushed %d",
				 buf->tag.rnode.spcNode,
				 buf->tag.rnode.dbNode,
				 buf->tag.rnode.relNode,
				 buf->tag.blockNum,
				 (uint32) (cbuf->oldest_lsn >> 32),
				 (uint32) cbuf->oldest_lsn,
				 (uint32) (polar_copy_buffer_get_lsn(cbuf) >> 32),
				 (uint32) polar_copy_buffer_get_lsn(cbuf),
				 cbuf->is_flushed);
		}

		/* First reset it */
		buf_state = LockBufHdr(buf);
		buf->copy_buffer = NULL;
		buf->polar_flags &= ~POLAR_BUF_FIRST_TOUCHED_AFTER_COPY;
		UnlockBufHdr(buf, buf_state);

		pg_atomic_fetch_add_u64(&polar_copy_buffer_ctl->release_count, 1);
		if (cbuf->is_flushed)
			pg_atomic_fetch_add_u64(&polar_copy_buffer_ctl->flushed_count, 1);

		CLEAR_BUFFERTAG(cbuf->tag);
		pg_atomic_write_u32((pg_atomic_uint32 *) &cbuf->origin_buffer, 0);
		pg_atomic_write_u32(&cbuf->pass_count, 0);
		pg_atomic_write_u64((pg_atomic_uint64 *) &cbuf->oldest_lsn, InvalidXLogRecPtr);
		cbuf->state = POLAR_COPY_BUFFER_UNUSED;
		cbuf->is_flushed = false;

		/* Then put into the freelist */
		SpinLockAcquire(&polar_copy_buffer_ctl->copy_buffer_ctl_lock);

		Assert(cbuf->free_next == FREENEXT_NOT_IN_LIST);

		cbuf->free_next = polar_copy_buffer_ctl->first_free_buffer;
		if (cbuf->free_next < 0)
			polar_copy_buffer_ctl->last_free_buffer = cbuf->buf_id;
		polar_copy_buffer_ctl->first_free_buffer = cbuf->buf_id;
		SpinLockRelease(&polar_copy_buffer_ctl->copy_buffer_ctl_lock);
	}

	return;
}

/* Like FlushBuffer, flush a copy buffer. */
void
polar_flush_copy_buffer(BufferDesc *buf, SMgrRelation reln)
{
	XLogRecPtr     latest_lsn;
	XLogRecPtr     oldest_apply_lsn;
	Block          buf_block;
	uint32         buf_state;
	CopyBufferDesc *cbuf;

	Assert(polar_copy_buffer_enabled());

	/*
	 * Acquire the buffer's io_in_progress lock.  If start_copy_buffer_io
	 * returns false, then someone else flushed the buffer before we could,
	 * so we need not do anything.
	 */
	if (!start_copy_buffer_io(buf, false))
		return;

	cbuf = buf->copy_buffer;
	Assert(cbuf);

	/*
	 * Double check. The copy buffer may have been replaced by others,
	 * its latest lsn may be greater than oldest apply lsn.
	 */
	oldest_apply_lsn = polar_get_oldest_applied_lsn();
	if (!polar_buffer_can_be_flushed(buf, oldest_apply_lsn, true))
	{
		TerminateBufferIO(buf, false, 0);
		return;
	}

	/* Find smgr relation for buffer */
	if (reln == NULL)
		reln = smgropen(buf->tag.rnode, InvalidBackendId);

	/* We already have the IO lock, don't need the header lock */
	latest_lsn = polar_copy_buffer_get_lsn(cbuf);
	if (unlikely(polar_enable_debug))
	{
		elog(LOG,
			 "Flush copy buffer ([%u,%u,%u], %u), copy buffer oldest lsn %X/%X, latest lsn %X/%X, oldest apply lsn %X/%X",
			 buf->tag.rnode.spcNode,
			 buf->tag.rnode.dbNode,
			 buf->tag.rnode.relNode,
			 buf->tag.blockNum,
			 (uint32) (cbuf->oldest_lsn >> 32),
			 (uint32) cbuf->oldest_lsn,
			 (uint32) (latest_lsn >> 32),
			 (uint32) latest_lsn,
			 (uint32) (oldest_apply_lsn >> 32),
			 (uint32) oldest_apply_lsn);
	}

	/*
	 * Force XLOG flush up to buffer's LSN.  This implements the basic WAL
	 * rule that log updates must hit disk before any of the data-file changes
	 * they describe do.
	 *
	 * However, this rule does not apply to unlogged relations, which will be
	 * lost after a crash anyway.  Most unlogged relation pages do not bear
	 * LSNs since we never emit WAL records for them, and therefore flushing
	 * up through the buffer LSN would be useless, but harmless.  However,
	 * GiST indexes use LSNs internally to track page-splits, and therefore
	 * unlogged GiST pages bear "fake" LSNs generated by
	 * GetFakeLSNForUnloggedRel.  It is unlikely but possible that the fake
	 * LSN counter could advance past the WAL insertion point; and if it did
	 * happen, attempting to flush WAL through that location would fail, with
	 * disastrous system-wide consequences.  To make sure that can't happen,
	 * skip the flush if the buffer isn't permanent.
	 */
	buf_state = LockBufHdr(buf);
	UnlockBufHdr(buf, buf_state);
	if (buf_state & BM_PERMANENT)
	{
		XLogFlush(latest_lsn);
		polar_flush_buf_flog_rec(buf, flog_instance, false);
	}

	/*
	 * Now it's safe to write copy buffer to disk. Note that no one else should
	 * have been able to write it while we were busy with log flushing because
	 * we have the io_in_progress lock.
	 */
	buf_block = CopyBufHdrGetBlock(cbuf);

	PageEncryptInplace((Page) buf_block, buf->tag.forkNum, buf->tag.blockNum);

	/*
	 * For copy buffer, only call the PageSetChecksumInplace instead of
	 * PageSetChecksumCopy for buffer, no other process can be modifying
	 * this copy buffer.
	 */
	PageSetChecksumInplace((Page) buf_block, buf->tag.blockNum);

	/* Write the copy buffer */
	smgrwrite(reln,
			  buf->tag.forkNum,
			  buf->tag.blockNum,
			  (char *) buf_block,
			  false);

	/* The oldest_lsn should be cleared once its copy buffer is flushed. */
	buf_state = LockBufHdr(buf);
	cbuf->is_flushed = true;
	UnlockBufHdr(buf, buf_state);

	polar_free_copy_buffer(buf);

	/*
	 * For copy buffer, do not mark the original buffer as clean and
	 * end the io_in_progress state.
	 */
	TerminateBufferIO(buf, false, 0);
}

/*
 * Before copy the buffer, call this function to mark the buffer as I/O busy.
 * Avoid other backend to copy it.
 */
static bool
start_copy_buffer_io(BufferDesc *buf, bool for_input)
{
	return polar_start_buffer_io_extend(buf, for_input, true);
}

XLogRecPtr
polar_copy_buffers_get_oldest_lsn(void)
{
	int            i;
	CopyBufferDesc *cbuf;
	XLogRecPtr     cur_lsn;
	XLogRecPtr     res = InvalidXLogRecPtr;

	if (!polar_copy_buffer_enabled())
		return res;

	for (i = 0; i < polar_copy_buffers; i++)
	{
		cbuf = polar_get_copy_buffer_descriptor(i);

		/* Use atomic read */
		cur_lsn = pg_atomic_read_u64((pg_atomic_uint64 *) &cbuf->oldest_lsn);

		if (XLogRecPtrIsInvalid(cur_lsn))
			continue;

		if (XLogRecPtrIsInvalid(res) || res > cur_lsn)
			res = cur_lsn;
	}

	return res;
}
