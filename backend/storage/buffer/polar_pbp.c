/*-------------------------------------------------------------------------
 *
 * polar_pbp.c
 *	  polardb persisted buffer pool(PBP) manager.
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
 *	  src/backend/storage/buffer/polar_pbp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash_xlog.h"
#include "access/polar_log.h"
#include "catalog/pg_control.h"
#include "miscadmin.h"
#include "storage/polar_copybuf.h"
#include "storage/polar_flushlist.h"
#include "storage/polar_pbp.h"
#include "storage/polar_shmem.h"

/* used for separate buffer pool checking */
polar_buffer_pool_ctl_t *polar_buffer_pool_ctl = NULL;
bool	polar_buffer_pool_is_inited = false;

static XLogRecPtr *polar_buffers_lsn_array = NULL;

static bool polar_need_reset_all(void);
static void polar_reuse_buffer_pool(void);
static void polar_init_buffer_pool_ctl(void);
static void polar_reset_buffer(BufferDesc* buf);
static void	polar_reuse_buffer(BufferDesc* buf, uint32 buf_state, int *num_dirty);
static bool	should_check_buffer(XLogReaderState *record);

/*
 * try to reused shared buffer pool
 *
 * This is called once during polar_vfs process initialization (either in the
 * postmaster, or in a standalone backend).
 */
void
polar_try_reuse_buffer_pool(void)
{
	bool	need_reset_all;

	if (IsUnderPostmaster)
		return;

	need_reset_all = polar_need_reset_all();
	if (need_reset_all)
	{
		elog(LOG,
			 "server cannot reuse buffer pool, start to reset it, pid=%d",
			 MyProcPid);
		polar_reset_buffer_pool();
	}
	else
		polar_reuse_buffer_pool();

	polar_init_buffer_pool_ctl();

	/* Make sure that buffer pool is inited when ReadBuffer_common */
	polar_buffer_pool_is_inited = true;
}

static bool
polar_need_reset_all(void)
{
	bool need_reset_all = true;

	/* If it's first time to access this memory, reset buffer pool ctl */
	if (!polar_shmem_reused)
	{
		MemSet(polar_buffer_pool_ctl, 0, sizeof(polar_buffer_pool_ctl_padded));
		elog(LOG,
			 "server cannot reuse buffer pool due to polar_shmem_reused is false, start to reset buffer pool");
		return need_reset_all;
	}

	Assert(polar_enable_shared_storage_mode);

	/* If verify buffer pool fail, just reset all buffer pool */
	if ((polar_is_standby() && polar_enable_standby_pbp)
		|| (polar_is_master() && polar_enable_master_pbp))
		need_reset_all = !polar_verify_buffer_pool_ctl();

	return need_reset_all;
}

static void
polar_reuse_buffer_pool(void)
{
	int		i = 0;
	int		num_reused = 0;
	int 	num_reused_dirty = 0;

	elog(LOG,
		 "server can reuse buffer pool, start to check it, pid=%d, flush_lsn:%ld, checkpoint_lsn:%ld",
		 MyProcPid,
		 polar_buffer_pool_ctl->last_flush_lsn,
		 polar_buffer_pool_ctl->last_checkpoint_lsn);

	/* We need to rebuild StrategyControl */
	polar_strategy_set_first_free_buffer(FREENEXT_END_OF_LIST);

	/* Loop all buffers, victim invalid buffer */
	for (i = 0; i < NBuffers; i++)
	{
		uint32		buf_state = 0;
		BufferDesc *buf = GetBufferDescriptor(i);

		if (buf->buf_id != i)
			elog(PANIC,
				 "buf_id(%d) is invalid, expected(%d) when reuse buffer pool",
				 buf->buf_id, i);

		/*
		 * NOTES: don't use LockBufHdr to get buf_state, maybe
		 * buf_header is damaged
		 */
		buf_state = pg_atomic_read_u32(&buf->state);

		if (polar_buffer_can_be_reused(buf, buf_state))
		{
			polar_reuse_buffer(buf, buf_state, &num_reused_dirty);
			num_reused++;
		}
		else
			polar_reset_buffer(buf);

		/* Reset lock anymore for safety */
		LWLockInitialize(BufferDescriptorGetContentLock(buf),
						 LWTRANCHE_BUFFER_CONTENT);

		LWLockInitialize(BufferDescriptorGetIOLock(buf),
						 LWTRANCHE_BUFFER_IO_IN_PROGRESS);
	}

	elog(LOG,
		 "reuse buffer pool successfully, total_buffers: %d, reused: %d, reused ratio: %.2f, reused dirty: %d",
		 NBuffers, num_reused, (num_reused * 1.0f) / NBuffers, num_reused_dirty);
}

/*
 * POLAR: we cannot reuse buffer in the following cases:
 * 1. buffer with invalid latest lsn
 * 2. buffer has POLAR_BUF_UNRECOVERABLE flag
 * 3. buffer has unexpected flags, eg: IO_PROGRESS, IO_ERROR...
 * 4. buffer does not has BM_VALID and BM_TAG_VALID and BM_PERMANENT
 * 5. buffer lsn is ahead of flush_lsn, maybe xlog does not flush to disk
 */
bool
polar_buffer_can_be_reused(BufferDesc *buf, uint32 buf_state)
{
	XLogRecPtr	buf_lsn;

	/* Only postmaster calls this function, so don't need to lock first */
	buf_lsn = BufferGetLSN(buf);

	if(XLogRecPtrIsInvalid(buf_lsn))
		return false;

	if (buf->polar_flags & POLAR_BUF_UNRECOVERABLE)
		return false;

	if (buf_lsn > polar_buffer_pool_ctl->last_flush_lsn)
		return false;

	if ((buf_state & BUF_FLAG_MASK & ~(BM_VALID | BM_TAG_VALID | BM_PERMANENT |
		BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED)) != 0)
		return false;

#define FLAGS_MUST_BE_SET (BM_VALID | BM_TAG_VALID | BM_PERMANENT)

	if ((buf_state & FLAGS_MUST_BE_SET) != FLAGS_MUST_BE_SET)
		return false;

	return true;
}

static void
polar_reset_buffer(BufferDesc* buf)
{
	CLEAR_BUFFERTAG(buf->tag);

	pg_atomic_init_u32(&buf->state, 0);
	pg_atomic_init_u32(&buf->polar_redo_state, 0);

	/* POLAR */
	buf->flush_next = POLAR_FLUSHNEXT_NOT_IN_LIST;
	buf->flush_prev = POLAR_FLUSHNEXT_NOT_IN_LIST;
	buf->oldest_lsn = InvalidXLogRecPtr;
	buf->copy_buffer = NULL;
	buf->recently_modified_count = 0;
	buf->polar_flags = 0;

	buf->wait_backend_pid = 0;

	buf->freeNext = FREENEXT_NOT_IN_LIST;
	StrategyFreeBuffer(buf);
}

static void
polar_reuse_buffer(BufferDesc* buf, uint32 buf_state, int *num_dirty)
{
	uint32	new_hash = 0;				/* hash value for new_tag */
	int		buf_id = 0;
	ControlFileData *control_file = polar_get_control_file();

	Assert(buf_state & (BM_VALID | BM_TAG_VALID | BM_PERMANENT));
	Assert(buf->freeNext == FREENEXT_NOT_IN_LIST);

	/* Determine its hash code */
	new_hash = BufTableHashCode(&buf->tag);
	/* It's no need to lock partition, only one postmaster can do it */
	buf_id = BufTableInsert(&buf->tag, new_hash, buf->buf_id);
	if (buf_id > 0)
		elog(PANIC, "BufTableInsert failed when reuse buffer pool, buffer is already in hashtable");

	buf->flush_next = POLAR_FLUSHNEXT_NOT_IN_LIST;
	buf->flush_prev = POLAR_FLUSHNEXT_NOT_IN_LIST;

	/*
	 * If buffer has oldest lsn(it maybe has copy buffer), we always
	 * use checkpoint_lsn as its oldest_lsn.
	 */
	if (!XLogRecPtrIsInvalid(polar_buffer_get_oldest_lsn(buf)))
	{
		Assert(buf_state & BM_DIRTY);
		Assert(!XLogRecPtrIsInvalid(control_file->checkPoint));
		polar_put_buffer_to_flush_list(buf, control_file->checkPointCopy.redo);
		++(*num_dirty);
	}

	buf->copy_buffer = NULL;
	buf->recently_modified_count = 0;
	/* Mark buffer is reused */
	buf->polar_flags = POLAR_BUF_REUSED;

	pg_atomic_init_u32(&buf->polar_redo_state, 0);

	buf->wait_backend_pid = 0;

	/* Clear BM_JUST_DIRTIED and BM_CHECKPOINT_NEEDED, unused anymore */
	buf_state &= (BM_VALID | BM_TAG_VALID | BM_PERMANENT | BM_DIRTY);

	/* Just like UnlockBufHdr, but call UnlockBufHdr here is ugly */
	pg_write_barrier();
	pg_atomic_write_u32(&buf->state, buf_state);
}

static void
polar_init_buffer_pool_ctl(void)
{
	ControlFileData *control_file = polar_get_control_file();

	SpinLockInit(&polar_buffer_pool_ctl->lock);
	polar_buffer_pool_ctl->version = POLAR_BUFFER_POOL_CTL_VERSION;
	polar_buffer_pool_ctl->header_magic = POLAR_BUFFER_POOL_MAGIC;
	polar_buffer_pool_ctl->system_identifier = control_file->system_identifier;
	polar_buffer_pool_ctl->last_checkpoint_lsn = control_file->checkPoint;
	polar_buffer_pool_ctl->state = 0;
	polar_buffer_pool_ctl_set_node_type(polar_node_type());

	/*
	 * Set the error state to true, if we successfully reuse this buffer pool,
	 * reset it to false.
	 */
	polar_buffer_pool_ctl_set_error_state();

	polar_buffer_pool_ctl->nbuffers = NBuffers;
	polar_buffer_pool_ctl->buf_desc_size = sizeof(BufferDescPadded);
	polar_buffer_pool_ctl->buffer_pool_ctl_shmem_size = sizeof(polar_buffer_pool_ctl_padded);
	polar_buffer_pool_ctl->buffer_pool_shmem_size = BufferShmemSize();
	polar_buffer_pool_ctl->copy_buffer_shmem_size = polar_copy_buffer_shmem_size();
	polar_buffer_pool_ctl->flush_list_ctl_shmem_size = polar_flush_list_ctl_shmem_size();
	polar_buffer_pool_ctl->strategy_shmem_size = StrategyShmemSize();
	polar_buffer_pool_ctl->tail_magic = POLAR_BUFFER_POOL_MAGIC;
}

/*
 * Init structure for checking buffer pool whether there are unexpected
 * reused buffer or not.
 */
void
polar_check_buffer_pool_consistency_init(void)
{
	int 		i = 0;

	if (!polar_enable_persisted_buffer_pool)
		return;

	/* Only check buffer for startup replay process */
	if (!AmStartupProcess())
		return;

	if (reachedConsistency)
		return;

	if (polar_in_replica_mode())
		return;

	polar_buffers_lsn_array = palloc0(NBuffers * sizeof(XLogRecPtr));

	for (i = 0; i < NBuffers; i++)
	{
		BufferDesc *buf = GetBufferDescriptor(i);
		uint32		buf_state = 0;

		buf_state = LockBufHdr(buf);
		if (buf->polar_flags & POLAR_BUF_REUSED)
			polar_buffers_lsn_array[i] = BufferGetLSN(buf);
		UnlockBufHdr(buf, buf_state);
	}
}

/*
 * check buffer pool has unexpected reused buffer or not,
 * we call this function just enter recovery consistency status
 */
void
polar_check_buffer_pool_consistency(void)
{
	int 		i = 0;

	if (!polar_enable_persisted_buffer_pool)
		return;

	/* Only check buffer for startup replay process */
	if (!AmStartupProcess())
		return;

	if (reachedConsistency)
		return;

	if (polar_in_replica_mode())
		return;

	for (i = 0; i < NBuffers; i++)
	{
		BufferDesc *buf = GetBufferDescriptor(i);
		uint32		buf_state = 0;

		buf_state = LockBufHdr(buf);
		/*
		 * Maybe origin buffer has been evicted from this buffer id, but don't
		 * worry, POLAR_BUF_REUSED flag will clear, we cannot go to here
		 * we treat reused buffer is invalid as following cases:
		 * 1. is MAIN_FORK
		 * 2. has POLAR_BUF_REUSED flag
		 * 3. page lsn isn't equal to the reused lsn(maybe older or ahead)
		 */
		if ((buf->polar_flags & POLAR_BUF_REUSED) &&
			polar_buffers_lsn_array[i] != BufferGetLSN(buf) &&
			buf->tag.forkNum == MAIN_FORKNUM)
		{
			UnlockBufHdr(buf, buf_state);
			POLAR_LOG_BUFFER_TAG_INFO(&buf->tag);
			elog(PANIC, "the reused buffer is unexpected left in buffer pool");
		}

		/* Clear vm/fsm/dirty hint buffer flag */
		buf->polar_flags &= ~POLAR_BUF_REUSED;
		UnlockBufHdr(buf, buf_state);
	}

	if (polar_buffers_lsn_array != NULL)
	{
		pfree(polar_buffers_lsn_array);
		polar_buffers_lsn_array = NULL;
	}

	/*
	 * If there are any errors before this point, the error state will not be
	 * reset to false, the persisted buffer pool will not be reuse next time.
	 */
	polar_buffer_pool_ctl_reset_error_state();
	elog(LOG, "check buffer pool consistency successfully");
}

/*
 * check reused buffer is valid or not
 */
void
polar_redo_check_reused_buffer(XLogReaderState *record, Buffer buffer)
{
	BufferDesc *buf = GetBufferDescriptor(buffer - 1);
	XLogRecPtr	lsn = record->EndRecPtr;

	if (polar_buffers_lsn_array == NULL || !BufferIsValid(buffer))
		return;

	/* Only check buffer for startup replay process */
	if (!AmStartupProcess() || reachedConsistency || polar_in_replica_mode())
		return;

	Assert(BufferIsValid(buffer));

	/* It's no need to lock, nobody can modify POLAR_BUF_REUSED flag */
	if ((buf->polar_flags & POLAR_BUF_REUSED) && should_check_buffer(record))
	{
		Assert(!XLogRecPtrIsInvalid(polar_buffers_lsn_array[buffer - 1]));

		/*
		 * Maybe origin buffer has been evicted from this buffer id, but don't
		 * worry, POLAR_BUF_REUSED flag will clear, we cannot go to here.
		 */
		if (polar_buffers_lsn_array[buffer - 1] < lsn)
		{
			POLAR_LOG_XLOG_RECORD_INFO(record);
			POLAR_LOG_BUFFER_TAG_INFO(&buf->tag);
			elog(PANIC,
				 "reused buffer(flags:%d, lsn:%ld) is not the latest version(lsn:%ld) in buffer pool",
				 buf->polar_flags,
				 polar_buffers_lsn_array[buffer - 1],
				 lsn);
		}
	}
}

/*
 * For some wal types, they register buffer, but do not set its lsn,
 * if replay this wal, it will read the buffer that lsn is less than
 * the wal lsn. For this wal, we just do not check it.
 */
static bool
should_check_buffer(XLogReaderState *record)
{
	uint8	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	if (info == XLOG_HASH_DELETE ||
		info == XLOG_HASH_SQUEEZE_PAGE ||
		info == XLOG_HASH_MOVE_PAGE_CONTENTS)
		return false;
	return true;
}

/*
 * when call LockBuffer for X lock, we mark buffer unrecoverable, maybe
 * backend dirties page meanwhile
 */
void
polar_mark_buffer_unrecoverable_flag(BufferDesc *buf_desc)
{
	/* Only buffer lock with LW_EXCLUSIVE should set POLAR_BUF_UNRECOVERABLE flag */
	Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf_desc),
							  LW_EXCLUSIVE));

	/*
	 * We always set/get unrecoverable flag under the content lock, so do not
	 * need the header lock.
	 */
	buf_desc->polar_flags |= POLAR_BUF_UNRECOVERABLE;
}

/*
 * when call LockBuffer for unlock, we clear unrecoverable flag
 */
void
polar_clear_buffer_unrecoverable_flag(BufferDesc *buf_desc)
{
	if (buf_desc->polar_flags & POLAR_BUF_UNRECOVERABLE)
	{
		/*
	 	 * We always set/get unrecoverable flag under the content lock, so do not
	 	 * need the header lock.
	 	 */
		buf_desc->polar_flags &= ~POLAR_BUF_UNRECOVERABLE;
	}
}

bool
polar_verify_buffer_pool_ctl(void)
{
	ControlFileData *control_file = polar_get_control_file();
	bool	valid = false;
	PolarNodeType ctl_node_type = polar_buffer_pool_ctl_get_node_type();

	if (polar_in_replica_mode())
		elog(LOG, "replica cannot reuse buffer pool, reset buffer pool");
	else if (ctl_node_type != polar_local_node_type)
		elog(LOG, "Node type changed from %d to %d, reset buffer pool",
			 ctl_node_type, polar_local_node_type);
	else if (polar_buffer_pool_ctl_get_error_state())
		elog(LOG, "some errors occurred in the last reuse, reset buffer pool");
	else if (polar_buffer_pool_ctl->version != POLAR_BUFFER_POOL_CTL_VERSION)
		elog(LOG, "PBP control struct version is changed, reset buffer pool");
	else if (polar_buffer_pool_ctl->header_magic != POLAR_BUFFER_POOL_MAGIC)
		elog(LOG, "head_magic is inconsistent, reset buffer pool");
	else if (polar_buffer_pool_ctl->system_identifier !=
		control_file->system_identifier)
		elog(LOG, "system_identifier is inconsistent, reset buffer pool");
	else if (polar_buffer_pool_ctl->last_checkpoint_lsn !=
		control_file->checkPoint)
		elog(LOG, "last_checkpoint_lsn is inconsistent, reset buffer pool");
	else if (polar_buffer_pool_ctl->nbuffers != NBuffers)
		elog(LOG, "nbuffers is inconsistent, reset buffer pool");
	else if (polar_buffer_pool_ctl->buf_desc_size != sizeof(BufferDescPadded))
		elog(LOG, "buf_desc_size is inconsistent, reset buffer pool");
	else if (polar_buffer_pool_ctl->buffer_pool_ctl_shmem_size !=
		sizeof(polar_buffer_pool_ctl_padded))
		elog(LOG, "buffer_pool_ctl_shmem_size is inconsistent, reset buffer pool");
	else if (polar_buffer_pool_ctl->buffer_pool_shmem_size != BufferShmemSize())
		elog(LOG, "buffer_pool_shmem_size is inconsistent, reset buffer pool");
	else if (polar_buffer_pool_ctl->copy_buffer_shmem_size !=
		polar_copy_buffer_shmem_size())
		elog(LOG, "copy_buffer_shmem_size is inconsistent, reset buffer pool");
	else if (polar_buffer_pool_ctl->flush_list_ctl_shmem_size !=
		polar_flush_list_ctl_shmem_size())
		elog(LOG, "flush_list_ctl_shmem_size is inconsistent, reset buffer pool");
	else if (polar_buffer_pool_ctl->strategy_shmem_size != StrategyShmemSize())
		elog(LOG, "strategy_shmem_size is inconsistent, reset buffer pool");
	else if (polar_buffer_pool_ctl->tail_magic != POLAR_BUFFER_POOL_MAGIC)
		elog(LOG, "tail_magic is inconsistent, reset buffer pool");
	else
		valid = true;

	return valid;
}
