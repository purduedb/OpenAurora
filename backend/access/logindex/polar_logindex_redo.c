/*-------------------------------------------------------------------------
 *
 * polar_logindex_redo.c
 *   Implementation of parse xlog states and replay.
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
 * IDENTIFICATION
 *    src/backend/access/logindex/polar_logindex_redo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/commit_ts.h"
#include "access/heapam_xlog.h"
#include "access/multixact.h"
#include "access/polar_csnlog.h"
#include "access/polar_logindex_redo.h"
#include "access/rmgr.h"
#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "executor/instrument.h"
#include "pg_trace.h"
#include "polar_flashback/polar_flashback_log.h"
#include "postmaster/bgwriter.h"
#include "postmaster/startup.h"
#include "miscadmin.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/polar_bufmgr.h"
#include "storage/procarray.h"
#include "utils/memutils.h"
#include "utils/polar_backtrace.h"
#include "utils/polar_bitpos.h"
#include "utils/resowner_private.h"

#define PG_RMGR(symname,name,redo,polar_idx_save, polar_idx_parse, polar_idx_redo, desc,identify,startup,cleanup,mask) \
	{name, polar_idx_save, polar_idx_parse, polar_idx_redo},

extern int                  polar_logindex_bloom_blocks;
extern int                  polar_rel_size_cache_blocks;
polar_logindex_redo_ctl_t   polar_logindex_redo_instance = NULL;

/* POLAR: Flag set when replaying and marking buffer dirty */
polar_logindex_bg_proc_t polar_bg_replaying_process = POLAR_NOT_LOGINDEX_BG_PROC;

/* Define the ratio of memory configured by polar_logindex_mem_size used for wal logindex and fullpage logindex */
#define WAL_LOGINDEX_MEM_RATIO          (0.75)
#define FULLPAGE_LOGINDEX_MEM_RATIO     (0.25)

#define WAL_LOGINDEX_DIR                "pg_logindex"
#define FULLPAGE_LOGINDEX_DIR           "polar_fullpage"
#define RELATION_SIZE_CACHE_DIR         "polar_rel_size_cache"

typedef enum
{
	BUF_NEED_REPLAY,
	BUF_IS_REPLAYING,
	BUF_IS_REPLAYED,
	BUF_IS_FLUSHED,
	BUF_IS_TRUNCATED,
} buf_replay_stat_t;

/*
 * The backend process can have only one page iterator for wal logindex snapshot or fullpage logindex snapshot.
 * When backend process receive cancel query signal the memory allocated for
 * page iterator must be released.
 * This static variable is used to track allocated memory for page iterator and release
 * memory when receive signal.
 */
static log_index_page_iter_t wal_page_iter = NULL;
static log_index_page_iter_t fullpage_page_iter = NULL;

static BufferDesc *polar_replaying_buffer = NULL;
static log_index_page_iter_t iter_context = NULL;
static MemoryContext polar_redo_context = NULL;

static const log_index_redo_t polar_idx_redo[RM_MAX_ID + 1] =
{
#include "access/rmgrlist.h"
};

static void polar_only_replay_exists_buffer(polar_logindex_redo_ctl_t instance, XLogReaderState *state, log_index_lsn_t *log_index_page);
static void polar_logindex_apply_one_page(polar_logindex_redo_ctl_t instance, BufferTag *tag, Buffer *buffer, log_index_page_iter_t iter);
static XLogRecPtr polar_logindex_apply_page_from(polar_logindex_redo_ctl_t instance, XLogRecPtr start_lsn, BufferTag *tag, Buffer *buffer, polar_page_lock_t page_lock);
static bool polar_evict_buffer(Buffer buffer);
static void polar_extend_block_if_not_exist(BufferTag* tag);

static void
polar_xlog_log(int level, XLogReaderState *record, const char *func)
{
	RmgrId      rmid = XLogRecGetRmid(record);
	uint8       info = XLogRecGetInfo(record);
	XLogRecPtr  lsn = record->ReadRecPtr;
	const char *id;

	id = RmgrTable[rmid].rm_identify(info);

	if (id == NULL)
		ereport(level, (errmsg("%s lsn=%X/%X UNKNOWN (%X)", func,
							   (uint32)(lsn >> 32),
							   (uint32)lsn,
							   info & ~XLR_INFO_MASK),
						errhidestmt(true),
						errhidecontext(true)));
	else
		ereport(level, (errmsg("%s lsn=%X/%X %s/%s", func,
							   (uint32)(lsn >> 32),
							   (uint32)lsn, RmgrTable[rmid].rm_name, id),
						errhidestmt(true),
						errhidecontext(true)));
}

/*
 * Call this function when we abort transaction.
 * If transaction is aborted, the buffer replaying is stopped,
 * so we have to clear its POLAR_REDO_REPLAYING flag.
 */
static void
polar_logindex_abort_replaying_buffer(void)
{
	if (polar_replaying_buffer != NULL)
	{
		uint32 redo_state = polar_lock_redo_state(polar_replaying_buffer);
		redo_state &= (~POLAR_REDO_REPLAYING);
		polar_unlock_redo_state(polar_replaying_buffer, redo_state);

		elog(LOG, "Abort replaying buf_id=%d, " POLAR_LOG_BUFFER_TAG_FORMAT, polar_replaying_buffer->buf_id,
			 POLAR_LOG_BUFFER_TAG(&polar_replaying_buffer->tag));
		polar_replaying_buffer = NULL;
	}
}

/*
 * For the block file in tag, extend it to tag->blockNum blocks.
 * TODO: blocks are extended one by one, which can be optimized in the future. 
 */
static void
polar_extend_block_if_not_exist(BufferTag* tag) 
{
	SMgrRelation smgr;
	BlockNumber nblocks;
	static char extendBuf[BLCKSZ];
	static bool extendBufIsInit = false;

	smgr = smgropen(tag->rnode, InvalidBackendId);
	smgrcreate(smgr, tag->forkNum, true);
	nblocks = smgrnblocks(smgr, tag->forkNum);

	if (!extendBufIsInit)
	{
		MemSet(extendBuf, 0, BLCKSZ);
		extendBufIsInit = true;
	}

	while (tag->blockNum >= nblocks)
	{
		smgrextend(smgr, tag->forkNum, nblocks, extendBuf, false);
		nblocks++;
	}
}

void
polar_logindex_apply_one_record(polar_logindex_redo_ctl_t instance, XLogReaderState *state, BufferTag *tag, Buffer *buffer)
{
	if (polar_idx_redo[state->decoded_record->xl_rmid].rm_polar_idx_redo == NULL)
	{
		POLAR_LOG_CONSISTENT_LSN();
		POLAR_LOG_XLOG_RECORD_INFO(state);
		POLAR_LOG_BUFFER_TAG_INFO(tag);
		elog(PANIC, "rmid = %d has not polar_idx_redo function",
			 state->decoded_record->xl_rmid);
	}

	ereport(polar_trace_logindex(DEBUG3), (errmsg("%s %d %X/%X, " POLAR_LOG_BUFFER_TAG_FORMAT, __func__,
												  *buffer,
												  (uint32)(state->ReadRecPtr >> 32),
												  (uint32)state->ReadRecPtr,
												  POLAR_LOG_BUFFER_TAG(tag)),
										   errhidestmt(true),
										   errhidecontext(true)));

	polar_idx_redo[state->decoded_record->xl_rmid].rm_polar_idx_redo(instance, state, tag, buffer);
}

XLogRecord *
polar_logindex_read_xlog(XLogReaderState *state, XLogRecPtr lsn)
{
	XLogRecord *record = NULL;
	char        *errormsg = NULL;
	int i = 1;

    XLogBeginRead(state, lsn);
    while (record == NULL)
	{
		record = XLogReadRecord(state, &errormsg);

		if (record == NULL)
		{
			if (i % 100000 == 0)
				elog(WARNING, "Failed to read record which lsn=%X/%X",
					 (uint32)(lsn >> 32), (uint32)lsn);

			i++;
			pg_usleep(1000);
		}
	}

	return record;
}

static Buffer
polar_read_vm_buffer(BufferTag *heap_tag)
{
	Buffer buf = InvalidBuffer;
	Relation rel = CreateFakeRelcacheEntry(heap_tag->rnode);

	visibilitymap_pin(rel, heap_tag->blockNum, &buf);

	FreeFakeRelcacheEntry(rel);

	return buf;
}

//Updated
Buffer
polar_logindex_outdate_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state, BufferTag *heap_tag,
							 bool get_cleanup_lock, polar_page_lock_t *page_lock, bool vm_parse)
{
	uint32      page_hash;
	LWLock      *partition_lock;    /* buffer partition lock for it */
	int         buf_id;
	Buffer      buffer = InvalidBuffer;
	BufferTag   vm_tag;
	BufferTag   *tag;

	if (!vm_parse)
		tag = heap_tag;
	else
	{
		vm_tag.rnode = heap_tag->rnode;
		vm_tag.forkNum = VISIBILITYMAP_FORKNUM;
		vm_tag.blockNum = HEAPBLK_TO_MAPBLOCK(heap_tag->blockNum);
		tag = &vm_tag;
	}

    POLAR_LOGINDEX_MINI_TRANS_ADD_LSN(instance->wal_logindex_snapshot,
                                      instance->mini_trans, *page_lock, state, tag);
    return InvalidBuffer;

	page_hash = BufTableHashCode(tag);
	partition_lock = BufMappingPartitionLock(page_hash);

	/* Check whether the block is already in the buffer pool */
	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(tag, page_hash);

	/* Found the buffer */
	if (buf_id >= 0)
	{
		uint32 redo_state;
		BufferDesc *buf_desc = GetBufferDescriptor(buf_id);

		Assert(0);
//		polar_pin_buffer(buf_desc, NULL);
		LWLockRelease(partition_lock);

		redo_state = polar_lock_redo_state(buf_desc);

		/*
		 * 1. A backend process is reading this buffer from storage and it will redo for this buffer.
		 * 2. The mini transaction lock is acquired by the startup process.
		 * 3. We add lsn to logindex in startup process with the acquired mini transaction lock.
		 * 4. When backend process compete buffer read and start to redo for buffer, it will redo to this lsn
		 */
		if (!(redo_state & POLAR_REDO_READ_IO_END) ||
				((redo_state & POLAR_REDO_REPLAYING) && !get_cleanup_lock))
		{
			redo_state |= POLAR_REDO_OUTDATE;
			polar_unlock_redo_state(buf_desc, redo_state);

			ReleaseBuffer(BufferDescriptorGetBuffer(buf_desc));
			POLAR_LOGINDEX_MINI_TRANS_ADD_LSN(instance->wal_logindex_snapshot,
											  instance->mini_trans, *page_lock, state, tag);
		}
		else
		{
			polar_unlock_redo_state(buf_desc, redo_state);
			polar_logindex_mini_trans_unlock(instance->mini_trans, *page_lock);

			if (!vm_parse)
				buffer = XLogReadBufferExtended(tag->rnode, tag->forkNum, tag->blockNum, RBM_NORMAL_NO_LOG);
			else
				buffer = polar_read_vm_buffer(heap_tag);

			ReleaseBuffer(BufferDescriptorGetBuffer(buf_desc));

			//TODO
			if (BufferIsValid(buffer))
			{
				if (get_cleanup_lock)
				    Assert(0);
//					polar_lock_buffer_for_cleanup_ext(buffer, false);
				else
				    Assert(0);
//					polar_lock_buffer_ext(buffer, BUFFER_LOCK_EXCLUSIVE, false);
			}
			else
			{
				POLAR_LOG_CONSISTENT_LSN();
				POLAR_LOG_XLOG_RECORD_INFO(state);
				POLAR_LOG_BUFFER_TAG_INFO(tag);
				elog(FATAL, "Failed to read page");
			}

			*page_lock = polar_logindex_mini_trans_lock(instance->mini_trans, tag, LW_EXCLUSIVE, NULL);
			POLAR_LOGINDEX_MINI_TRANS_ADD_LSN(instance->wal_logindex_snapshot,
											  instance->mini_trans, *page_lock, state, tag);

			/* Force to get buffer descriptor again to avoid different buffer descriptor when buffer is evicted before */
			buf_desc = GetBufferDescriptor(buffer - 1);
			redo_state = polar_lock_redo_state(buf_desc);
			redo_state |= POLAR_REDO_OUTDATE;
			polar_unlock_redo_state(buf_desc, redo_state);
		}

		if (unlikely(polar_trace_logindex_messages <= DEBUG4))
		{
			XLogRecPtr page_lsn = InvalidXLogRecPtr;

			if (!BufferIsInvalid(buffer))
			{
				Page page = BufferGetPage(buffer);
				page_lsn = PageGetLSN(page);
			}

			ereport(LOG, (errmsg("%s buf_id=%d page_lsn=%X/%X, xlog=%X/%X, " POLAR_LOG_BUFFER_TAG_FORMAT, __func__,
								 buf_id,
								 (uint32)(page_lsn >> 32),
								 (uint32) page_lsn,
								 (uint32)(state->ReadRecPtr >> 32),
								 (uint32)state->ReadRecPtr,
								 POLAR_LOG_BUFFER_TAG(tag)),
						  errhidestmt(true),
						  errhidecontext(true)));
			polar_xlog_log(LOG, state, __func__);
		}
	}
	else
	{
		LWLockRelease(partition_lock);

		/*
		 * Polar: extend blocks if not exist in standby mode;
		 *
		 * The semantic of parsing WAL is that the data of the WAL can be
		 * queried after parsing. However, some queries depend on the number of
		 * blocks in the storage (e.g. select * from <table>). The data in the
		 * buffer won't be queried if its block is not extended in the storage.
		 */
		if (POLAR_IN_PARALLEL_REPLAY_STANDBY_MODE(polar_logindex_redo_instance))
			polar_extend_block_if_not_exist(tag);

		POLAR_LOGINDEX_MINI_TRANS_ADD_LSN(instance->wal_logindex_snapshot,
										  instance->mini_trans, *page_lock, state, tag);
	}

	return buffer;
}

Buffer
polar_logindex_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state, BufferTag *tag,
					 bool get_cleanup_lock, polar_page_lock_t *page_lock)
{
	Buffer      buffer;

	buffer = polar_logindex_outdate_parse(instance, state, tag, get_cleanup_lock, page_lock, false);

	return buffer;
}

static void
polar_logindex_parse_tag(polar_logindex_redo_ctl_t instance, XLogReaderState *state, BufferTag *tag,
						 bool get_cleanup_lock)
{
	polar_page_lock_t   page_lock;
	Buffer              buffer;

	page_lock = polar_logindex_mini_trans_lock(instance->mini_trans, tag, LW_EXCLUSIVE, NULL);
	buffer = polar_logindex_parse(instance, state, tag, get_cleanup_lock, &page_lock);

	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	polar_logindex_mini_trans_unlock(instance->mini_trans, page_lock);
}

void
polar_logindex_save_block(polar_logindex_redo_ctl_t instance, XLogReaderState *state, uint8 block_id)
{
	BufferTag tag;
	DecodedBkpBlock *blk;

	Assert(block_id <= XLR_MAX_BLOCK_ID);

	blk = &state->blocks[block_id];

	Assert(blk->in_use);

	INIT_BUFFERTAG(tag, blk->rnode, blk->forknum, blk->blkno);

	if (unlikely(XLogRecGetRmid(state) == RM_XLOG_ID &&
				 (XLogRecGetInfo(state) & ~XLR_INFO_MASK) == XLOG_FPSI))
	{
		if (instance->fullpage_logindex_snapshot)
			POLAR_LOG_INDEX_ADD_LSN(instance->fullpage_logindex_snapshot, state, &tag);
	}
	else
		POLAR_LOG_INDEX_ADD_LSN(instance->wal_logindex_snapshot, state, &tag);
}

void
polar_logindex_redo_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state, uint8 block_id)
{
	BufferTag tag;

	POLAR_GET_LOG_TAG(state, tag, block_id);
	polar_logindex_parse_tag(instance, state, &tag, false);
}

void
polar_logindex_cleanup_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state, uint8 block_id)
{
	BufferTag tag;

	POLAR_GET_LOG_TAG(state, tag, block_id);
	polar_logindex_parse_tag(instance, state, &tag, true);
}

void
polar_logindex_save_lsn(polar_logindex_redo_ctl_t instance, XLogReaderState *state)
{
	RmgrId rmid = XLogRecGetRmid(state);

	if (polar_idx_redo[rmid].rm_polar_idx_save != NULL)
		polar_idx_redo[rmid].rm_polar_idx_save(instance, state);
	else
	{
		POLAR_LOG_CONSISTENT_LSN();
		POLAR_LOG_XLOG_RECORD_INFO(state);
		elog(PANIC, "rm_polar_idx_save is not set for rmid=%d", rmid);
	}
}

//Updated
bool
polar_enable_logindex_parse(void)
{
	/*
	 * If it's replica mode and logindex is enabled, we parse XLOG and save it to logindex.
	 * If it's standby parallel replay mode and logindex is enabled, we also parse XLOG and
	 * save it to logindex.
	 *
	 * During recovery we read XLOG from checkpoint, master node can drop or truncate table
	 * after this checkpoint. We can't read these removed data blocks, so it will be PANIC
	 * if we read data block and replay XLOG.
	 * In master node it will create table if it does not exist, but we can not do this in
	 * replica mode.
	 */
//	return (polar_in_replica_mode() && polar_logindex_redo_instance) ||
//		POLAR_IN_PARALLEL_REPLAY_STANDBY_MODE(polar_logindex_redo_instance);
    return true;
}

XLogRecPtr
polar_logindex_redo_parse_start_lsn(polar_logindex_redo_ctl_t instance)
{
	if (!instance)
		return InvalidXLogRecPtr;

	return polar_logindex_check_valid_start_lsn(instance->wal_logindex_snapshot);
}

bool
polar_logindex_parse_xlog(polar_logindex_redo_ctl_t instance, RmgrId rmid, XLogReaderState *state, XLogRecPtr redo_start_lsn, XLogRecPtr *mini_trans_lsn)
{
	bool redo = false;
//	static bool parse_valid = false;
//
//	if (unlikely(!parse_valid))
//	{
//		XLogRecPtr start_lsn = polar_logindex_redo_parse_start_lsn(instance);
//
//		if (!XLogRecPtrIsInvalid(start_lsn))
//			parse_valid = true;
//
//		if (!parse_valid)
//			return redo;
//
//		elog(LOG, "wal logindex parse from %lX", state->ReadRecPtr);
//	}

	Assert(mini_trans_lsn != NULL);
	*mini_trans_lsn = InvalidXLogRecPtr;

	/*
	 * Polar: In standby mode, parallel replay can be triggered only when
	 * consistency reached, temporarily!
	 */
//	if (polar_in_replica_mode() || (POLAR_IN_PARALLEL_REPLAY_STANDBY_MODE(polar_logindex_redo_instance) && reachedConsistency))
    if (true)
	{
		if (polar_idx_redo[rmid].rm_polar_idx_parse != NULL)
		{

//			if (unlikely(polar_trace_logindex_messages <= DEBUG4))
//				polar_xlog_log(LOG, state, __func__);

			polar_logindex_mini_trans_start(instance->mini_trans, state->EndRecPtr);
			redo = polar_idx_redo[rmid].rm_polar_idx_parse(instance, state);
			/*
			 * We can not end mini transaction here because XLogCtl->lastReplayedEndRecPtr is not updated.
			 * If we end mini transaction here, and one backend start to do buffer replay,
			 * it can not acquire mini transaction lock and  replay to XLogCtl->lastReplayedEndRecPtr,
			 * so the current record which was parsed and saved to logindex will be lost.
			 * We will end mini transaction after startup process update XLogCtl->lastReplayedEndRecPtr.
			 */
			*mini_trans_lsn = state->EndRecPtr;
		}
	}
	/* POLAR: create and save logindex in master and standby. */
//	else
//	{
//		if (polar_idx_redo[rmid].rm_polar_idx_save != NULL)
//			polar_idx_redo[rmid].rm_polar_idx_save(instance, state);
//	}

	/*
	 * POLAR: If current record lsn is smaller than redo start lsn, then we only parse xlog and create logindex.
	 * Sometimes, currRecPtr will be the start position of xlog block which would make the following condition
	 * unexpectedly failed. So we should use ReadRecPtr.
	 */
	if (state->ReadRecPtr < redo_start_lsn)
		redo = true;

	return redo;
}

/*
 * POLAR: Apply the WAL record from start_lsn to end_lsn - 1.
 */
XLogRecPtr
polar_logindex_apply_page(polar_logindex_redo_ctl_t instance, XLogRecPtr start_lsn, XLogRecPtr end_lsn,
						  BufferTag *tag, Buffer *buffer)
{
	Page page;

	Assert(BufferIsValid(*buffer));
	page = BufferGetPage(*buffer);

	if (!PageIsNew(page))
		start_lsn = Max(start_lsn, PageGetLSN(page));

	if (unlikely(end_lsn <= start_lsn))
	{
		/*
		 * 1. When do online promote, the page could be flushed after replayed, so end_lsn may be smaller than start_lsn
		 * 2. The new created RO node, which received primary consistent lsn but rw didn't have new ro node's replayed lsn,
		 * and then end_lsn may be smaller than start_lsn
		 * 3. The end_lsn is the start point of last replayed record, while start_lsn is set by consistent lsn , which is the end point of last replayed record,
		 * then end_lsn is smaller than start_lsn.
		 */
		if (end_lsn < start_lsn && polar_in_replica_mode())
		{
			ereport(LOG, (errmsg("Try to replay page "POLAR_LOG_BUFFER_TAG_FORMAT" which end_lsn=%lX is smaller than start_lsn=%lX, page_lsn=%lX",
								 POLAR_LOG_BUFFER_TAG(tag), end_lsn, start_lsn, PageGetLSN(page)), errhidestmt(true), errhidecontext(true)));
		}

		return start_lsn;
	}

	ereport(polar_trace_logindex(DEBUG3), (errmsg("%s %d %X/%X %X/%X, " POLAR_LOG_BUFFER_TAG_FORMAT, __func__,
												  *buffer,
												  (uint32)(start_lsn >> 32),
												  (uint32)start_lsn,
												  (uint32)(end_lsn >> 32),
												  (uint32)end_lsn,
												  POLAR_LOG_BUFFER_TAG(tag)),
										   errhidestmt(true),
										   errhidecontext(true)));

	Assert(wal_page_iter == NULL);

	/*
	 * Logindex record the start position of XLOG and we search LSN between [start_lsn, end_lsn].
	 * And end_lsn points to the end position of the last xlog, so we should subtract 1 here .
	 */
	wal_page_iter = polar_logindex_create_page_iterator(instance->wal_logindex_snapshot,
														tag, start_lsn, end_lsn - 1, polar_get_bg_redo_state(instance) == POLAR_BG_ONLINE_PROMOTE);

	if (unlikely(polar_logindex_page_iterator_state(wal_page_iter) != ITERATE_STATE_FINISHED))
	{
		elog(PANIC, "Failed to iterate data for " POLAR_LOG_BUFFER_TAG_FORMAT ", which start_lsn=%X/%X and end_lsn=%X/%X",
			 POLAR_LOG_BUFFER_TAG(tag),
			 (uint32)(start_lsn >> 32), (uint32)start_lsn,
			 (uint32)(end_lsn >> 32), (uint32)end_lsn);
	}

	polar_logindex_apply_one_page(instance, tag, buffer, wal_page_iter);

	polar_logindex_release_page_iterator(wal_page_iter);
	wal_page_iter = NULL;

	return end_lsn;
}

static XLogRecPtr
polar_logindex_apply_page_from(polar_logindex_redo_ctl_t instance, XLogRecPtr start_lsn, BufferTag *tag, Buffer *buffer, polar_page_lock_t page_lock)
{
	XLogRecPtr end_lsn;
	Page page;

	Assert(BufferIsValid(*buffer));
	page = BufferGetPage(*buffer);

	/* If block was truncated before, then we change the start lsn to the lsn when truncate block */
	if (instance->rel_size_cache)
	{
		bool valid;
		XLogRecPtr lsn_changed;

		LWLockAcquire(POLAR_REL_SIZE_CACHE_LOCK(instance->rel_size_cache), LW_SHARED);
		valid = polar_check_rel_block_valid_and_lsn(instance->rel_size_cache,  start_lsn, tag, &lsn_changed);
		LWLockRelease(POLAR_REL_SIZE_CACHE_LOCK(instance->rel_size_cache));

		if (!valid)
		{
			ereport(polar_trace_logindex(DEBUG1), (errmsg("%s change start_lsn from %lX to %lX for page_lsn=%lX "POLAR_LOG_BUFFER_TAG_FORMAT,
														  __func__, start_lsn, lsn_changed, PageGetLSN(page), POLAR_LOG_BUFFER_TAG(tag)), errhidestmt(true), errhidecontext(true)));
			start_lsn = lsn_changed;
		}
	}

	/*
	 * If this buffer replaying is protected by mini transaction page_lock and replaying lsn is added to logindex
	 * then we replay to the record which is currently replaying.
	 * Otherwise we replay to the last record which is successfully replayed.
	 * If we replay to the currently replaying record without mini transaction page_lock, we may get inconsistent
	 * date structure in memory.
	 */
	if (page_lock != POLAR_INVALID_PAGE_LOCK &&
			polar_logindex_mini_trans_get_page_added(instance->mini_trans, page_lock))
		end_lsn = polar_get_replay_end_rec_ptr(NULL);
	else
		end_lsn = GetXLogReplayRecPtr(NULL);

	return polar_logindex_apply_page(instance, start_lsn, end_lsn, tag, buffer);
}

/*
 * Search xlog base on the buffer tag and replay these xlog record for the buffer.
 * Return true if page lsn changed after replay
 */
bool
polar_logindex_lock_apply_page_from(polar_logindex_redo_ctl_t instance, XLogRecPtr start_lsn, BufferTag *tag, Buffer *buffer)
{
	BufferDesc *buf_hdr;
	polar_page_lock_t page_lock;
	uint32 redo_state;
	char *page;
	XLogRecPtr origin_lsn;
	MemoryContext         oldcontext;

	Assert(BufferIsValid(*buffer));

	/*
	 * Record the buffer that is replaying.If we abort transaction in
	 * this backend process, we need to clear POLAR_REDO_REPLAYING from
	 * buffer redo state
	 */
	polar_replaying_buffer = buf_hdr = GetBufferDescriptor(*buffer - 1);

	/*
	 * Prevent interrupts while replaying to avoid inconsistent buffer redo state
	 */
	HOLD_INTERRUPTS();

	oldcontext = MemoryContextSwitchTo(polar_logindex_memory_context());

	/*
	 * We should finish reading data from storage and then replay xlog for page,
	 * so we set redo_state to be POLAR_REDO_READ_IO_END | POLAR_REDO_REPLAYING.
	 */
	redo_state = polar_lock_redo_state(buf_hdr);
	redo_state |= (POLAR_REDO_READ_IO_END | POLAR_REDO_REPLAYING);
	redo_state &= (~POLAR_REDO_OUTDATE);
	polar_unlock_redo_state(buf_hdr, redo_state);

	page = BufferGetPage(*buffer);
	origin_lsn = PageGetLSN(page);

	do
	{
		page_lock = polar_logindex_mini_trans_cond_lock(instance->mini_trans, tag, LW_EXCLUSIVE, NULL);

		start_lsn = polar_logindex_apply_page_from(instance, start_lsn, tag, buffer, page_lock);

		redo_state = polar_lock_redo_state(buf_hdr);

		if (redo_state & POLAR_REDO_OUTDATE)
			redo_state &= (~POLAR_REDO_OUTDATE);
		else
			redo_state &= (~POLAR_REDO_REPLAYING);

		polar_unlock_redo_state(buf_hdr, redo_state);

		if (page_lock != POLAR_INVALID_PAGE_LOCK)
			polar_logindex_mini_trans_unlock(instance->mini_trans, page_lock);
	}
	while (redo_state & POLAR_REDO_REPLAYING);

	MemoryContextSwitchTo(oldcontext);

	/* Now we can allow interrupts again */
	RESUME_INTERRUPTS();

	polar_replaying_buffer = NULL;

	return PageGetLSN(page) != origin_lsn;
}

static void
polar_promote_mark_buf_dirty(polar_logindex_redo_ctl_t instance, Buffer buffer, XLogRecPtr start_lsn)
{
	Page page;
	XLogRecPtr page_lsn;

	if (likely(!polar_bg_redo_state_is_parallel(instance)))
		return;

	page = BufferGetPage(buffer);
	page_lsn = PageGetLSN(page);

	/*
	 * If page lsn is larger than the last replayed xlog lsn, then this buffer
	 * was modified after online promote and we don't need to mark it dirty
	 * again
	 *
	 * There is a situation when a wal is parsed, dispatched, replayed and the
	 * page lsn is updated but GetXLogReplayRecPtr(NULL) has not been updated, so
	 * that the new page won't be marked as dirty, which is wrong.
	 * 
	 * GetXLogReplayRecPtr(NULL) won't be changed during online promote.
	 */
	if (polar_get_bg_redo_state(instance) != POLAR_BG_PARALLEL_REPLAYING &&
		page_lsn > GetXLogReplayRecPtr(NULL))
		return;

	/*
	 * During online promote the start_lsn is the background process replayed lsn which used as the lower limit
	 * to create logindex iterator. If page lsn is smaller or equal to start_lsn, then we have no xlog record
	 * to replay when visist this buffer, so we don't need to mark it dirty
	 */
	if (page_lsn <= start_lsn)
		return;

	MarkBufferDirty(buffer);
}

/*
 * Apply the specific buffer from the start lsn to the last parsed lsn.
 * We must acquire mini transaction page lock
 * to avoid add lsn for this page.This is used when enable page invalid and
 * apply buffer when a backend process use this buffer.
 */
//Updated
void
polar_logindex_lock_apply_buffer(polar_logindex_redo_ctl_t instance, Buffer *buffer)
{
	BufferDesc *buf_desc;
	XLogRecPtr bg_replayed_lsn;

	Assert(BufferIsValid(*buffer));
	buf_desc = GetBufferDescriptor(*buffer - 1);

	if (polar_redo_check_state(buf_desc, POLAR_REDO_OUTDATE))
	{
		SpinLockAcquire(&instance->info_lck);
		bg_replayed_lsn = instance->bg_replayed_lsn;
		POLAR_SET_BACKEND_READ_MIN_LSN(bg_replayed_lsn);
		SpinLockRelease(&instance->info_lck);

		/*
		 * If the buffer may lose some flashback log record,
		 * just insert it to flashback log list as a candidator.
		 */
//		if (polar_may_buf_lost_flog(flog_instance, instance, buf_desc))
//			polar_flog_insert(flog_instance, *buffer, true, false);

		polar_logindex_lock_apply_page_from(instance, bg_replayed_lsn, &buf_desc->tag, buffer);
		polar_promote_mark_buf_dirty(instance, *buffer, bg_replayed_lsn);

		POLAR_RESET_BACKEND_READ_MIN_LSN();
	}
}

static XLogReaderState *
polar_allocate_xlog_reader(void)
{
//	XLogReaderState *state = XLogReaderAllocate(wal_segment_size, &read_local_xlog_page, NULL);
    XLogReaderState *state =
            XLogReaderAllocate(wal_segment_size, NULL,
                               XL_ROUTINE(.page_read = &read_local_xlog_page,
                                          .segment_open = NULL,
                                          .segment_close = wal_segment_close),
                               NULL);
	if (!state)
	{
		ereport(FATAL,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));
	}

	return state;
}

static void
polar_logindex_apply_one_page(polar_logindex_redo_ctl_t instance, BufferTag *tag, Buffer *buffer, log_index_page_iter_t iter)
{
	static XLogReaderState *state = NULL;
	log_index_lsn_t *lsn_info;
	MemoryContext oldcontext;

	iter_context = iter;

	oldcontext = MemoryContextSwitchTo(polar_logindex_memory_context());

	if (unlikely(state == NULL))
		state = polar_allocate_xlog_reader();

	while ((lsn_info = polar_logindex_page_iterator_next(iter)) != NULL)
	{
		polar_logindex_read_xlog(state, lsn_info->lsn);
		polar_logindex_apply_one_record(instance, state, tag, buffer);

		CHECK_FOR_INTERRUPTS();
	}

	MemoryContextSwitchTo(oldcontext);
}

//updated
polar_logindex_bg_redo_ctl_t *
polar_create_bg_redo_ctl(polar_logindex_redo_ctl_t instance, bool enable_processes_pool)
{
	polar_logindex_bg_redo_ctl_t *ctl = palloc0(sizeof(polar_logindex_bg_redo_ctl_t));
	XLogRecPtr  bg_replayed_lsn = polar_bg_redo_get_replayed_lsn(instance);

	ctl->instance = instance;
	ctl->lsn_iter = polar_logindex_create_lsn_iterator(instance->wal_logindex_snapshot, bg_replayed_lsn);

	ctl->state = polar_allocate_xlog_reader();
	ctl->replay_batch_size = polar_bg_replay_batch_size;

//	if (enable_processes_pool)
//	{
//		ctl->sched_ctl = polar_create_parallel_replay_sched_ctl(ctl);
//		polar_start_proc_pool(ctl->sched_ctl);
//	}

	ereport(LOG, (errmsg("Start background replay iter from %X/%X",
						 (uint32)(bg_replayed_lsn >> 32),
						 (uint32)(bg_replayed_lsn))));
	return ctl;
}

//update
void
polar_release_bg_redo_ctl(polar_logindex_bg_redo_ctl_t *ctl)
{
//	if (ctl->sched_ctl)
//		polar_release_task_sched_ctl(ctl->sched_ctl);

	polar_logindex_release_lsn_iterator(ctl->lsn_iter);
	XLogReaderFree(ctl->state);

	pfree(ctl);
}

/*
 * POLAR: Apply record in background to catch up replayed lsn updated in Startup.
 *
 * In this func, it use a iterator from logindex to go through xlog record, and
 * apply record on related page if page buffer is in buffer pool. If current record
 * is not in logindex, such as clog record, it will skip replay of it and advance
 * bg_replayed_lsn directly. Initial xlog record lsn is set when reachedConsistency
 * turns true in Startup process.
 *
 * This func is called in Bgwriter process in Replica node. Each run, it apply
 * polar_bg_replay_batch_size xlog records at most.
 *
 * It will advance bg_replayed_lsn which will be replied to master as restart_lsn of
 * replication slot. And that will stop master from deleting wal segments before
 * bg_replayed_lsn.
 *
 * Return true if no more records need replay, otherwise false.
 */
static bool
polar_logindex_apply_xlog_background(polar_logindex_bg_redo_ctl_t *ctl)
{
	int     replayed_count = 0;
	XLogRecPtr  replayed_lsn;
	uint32      bg_redo_state = polar_get_bg_redo_state(ctl->instance);

	if (unlikely(bg_redo_state == POLAR_BG_REDO_NOT_START || bg_redo_state == POLAR_BG_WAITING_RESET))
		return true;

	replayed_lsn = polar_get_last_replayed_read_ptr(ctl->instance);

	/* Make sure there's room for us to pin buffer */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	for (replayed_count = 0; replayed_count < ctl->replay_batch_size; replayed_count++)
	{
		XLogRecPtr current_ptr;

		if (ctl->replay_page == NULL)
			ctl->replay_page =
				polar_logindex_lsn_iterator_next(ctl->instance->wal_logindex_snapshot, ctl->lsn_iter);

		/* If no more item in logindex, exit and retry next run */
		if (ctl->replay_page == NULL)
			break;

		Assert(!XLogRecPtrIsInvalid(ctl->replay_page->lsn));

		/* Now, we get one record from logindex lsn iterator */

		/* If current xlog record is beyond replayed lsn, just break */
		if (ctl->replay_page->lsn > replayed_lsn)
			break;

		/* we get a new record now, set current_ptr as the lsn of it */
		current_ptr = ctl->replay_page->lsn;

		/*
		 * replay each page of current record. because we checked log_index_page->lsn < replayed_lsn,
		 * we will get all related pages of the record, so use do-while to go through each page replay
		 */
		do
		{
			polar_only_replay_exists_buffer(ctl->instance, ctl->state, ctl->replay_page);
			ctl->replay_page =
				polar_logindex_lsn_iterator_next(ctl->instance->wal_logindex_snapshot, ctl->lsn_iter);
		}
		while (ctl->replay_page != NULL && current_ptr == ctl->replay_page->lsn);

		/* now, record related pages have all been replayed, so we can advance bg_replayed_lsn */
		polar_bg_redo_set_replayed_lsn(ctl->instance, current_ptr);
	}

	return (replayed_count < polar_bg_replay_batch_size);
}

/*
 * POLAR: Evict selectd buffer.
 *
 * In some case, we need to release buffer's content lock if we have before
 * put it back to free list. In other word, if the buffer is locked and pined
 * before this func, and it will be released and un-pined while being evicted.
 */
//updated
static bool
polar_evict_buffer(Buffer buffer)
{
	uint32 buf_state;
	BufferDesc *buf_desc = GetBufferDescriptor(buffer - 1);
	BufferTag tag = buf_desc->tag;
	uint32 hash = BufTableHashCode(&tag);
	LWLock *partition_lock = BufMappingPartitionLock(hash);
	bool evict = false;

	if (!polar_in_replica_mode())
		return evict;

	LWLockAcquire(partition_lock, LW_EXCLUSIVE);
	buf_state = LockBufHdr(buf_desc);

	/* We should already pin this buffer, and then evict it */
	if (BUF_STATE_GET_REFCOUNT(buf_state) == 1 && !(buf_state & BM_DIRTY)
			&& (buf_state & BM_TAG_VALID))
	{
		CLEAR_BUFFERTAG(buf_desc->tag);
		buf_state &= ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
		evict = true;
	}

	UnlockBufHdr(buf_desc, buf_state);

	if (evict)
	{
		uint32 redo_state;

		/*
		 * POLAR: Make true there are no flashback record of the buffer.
		 */
//		polar_make_true_no_flog(flog_instance, buf_desc);

		redo_state = polar_lock_redo_state(buf_desc);
		redo_state &= ~(POLAR_BUF_REDO_FLAG_MASK | POLAR_BUF_FLASHBACK_FLAG_MASK);
		polar_unlock_redo_state(buf_desc, redo_state);

		BufTableDelete(&tag, hash);
		ReleaseBuffer(buffer);
		/*
		 * Insert the buffer at the head of the list of free buffers.
		 */
		StrategyFreeBuffer(buf_desc);
	}

	LWLockRelease(partition_lock);
	return evict;
}

static void
polar_bg_redo_read_record(XLogReaderState *state, XLogRecPtr lsn)
{
	/* If needed record has not been read, just read it */
	if (state->ReadRecPtr != lsn)
	{
		char        *errormsg = NULL;
		XLogRecord *record = NULL;
		/* In XLogReadRecord, it may enlarge buffer of XlogReaderState, so we should switch context */
		MemoryContext oldcontext = MemoryContextSwitchTo(polar_logindex_memory_context());

		XLogBeginRead(state, lsn);
		record = XLogReadRecord(state, &errormsg);
		MemoryContextSwitchTo(oldcontext);

		if (record == NULL)
		{
			POLAR_LOG_CONSISTENT_LSN();
			POLAR_LOG_XLOG_RECORD_INFO(state);

			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not read record from WAL at %lX", lsn)));
		}
	}
}

static int
polar_bg_redo_get_record_id(XLogReaderState *state, BufferTag *tag)
{
	int i;

	for (i = 0; i <= state->max_block_id; i++)
	{
		BufferTag blk_tag;

		POLAR_GET_LOG_TAG(state, blk_tag, i);

		if (BUFFERTAGS_EQUAL(blk_tag, *tag))
			return i;
	}

	elog(PANIC, "Failed to find block id in XLogRecord when lsn=%lX for " POLAR_LOG_BUFFER_TAG_FORMAT,
		 state->ReadRecPtr, POLAR_LOG_BUFFER_TAG(tag));

	return -1;
}

//Updated
static void
polar_bg_redo_apply_read_record(polar_logindex_redo_ctl_t instance, XLogReaderState *state, XLogRecPtr lsn,
								BufferTag *tag, Buffer *buffer)
{
	BufferDesc *buf_desc;
	Page page;

	Assert(buffer != NULL);

	polar_bg_redo_read_record(state, lsn);

	if (BufferIsInvalid(*buffer))
	{
		int blk_id = polar_bg_redo_get_record_id(state, tag);

		/*
		 * When create new page, it call log_newpage which is FPI xlog record and then extend the file.
		 * So during online promote when replay FPI xlog record, RBM_NORMAL_NO_LOG could return InvalidBuffer.
		 * We use flag RBM_ZERO_ON_ERROR which will extend the file if page does not exist.
		 */
		if (XLogRecBlockImageApply(state, blk_id))
		{
			LWLockAcquire(POLAR_REL_SIZE_CACHE_LOCK(instance->rel_size_cache), LW_SHARED);

			/* Don't replay this block if it's truncated or dropped */
			if (polar_check_rel_block_valid_only(instance->rel_size_cache, lsn, tag))
			{
				*buffer = XLogReadBufferExtended(tag->rnode, tag->forkNum, tag->blockNum, RBM_ZERO_ON_ERROR);
				LWLockRelease(POLAR_REL_SIZE_CACHE_LOCK(instance->rel_size_cache));
			}
			else
			{
				LWLockRelease(POLAR_REL_SIZE_CACHE_LOCK(instance->rel_size_cache));
				return;
			}
		}
	}

	buf_desc = GetBufferDescriptor(*buffer - 1);
	page = BufferGetPage(*buffer);

	if (unlikely(polar_trace_logindex_messages <= DEBUG4))
	{
		POLAR_LOG_BUFFER_DESC(buf_desc);
		POLAR_LOG_PAGE_INFO(page);
		POLAR_LOG_CONSISTENT_LSN();
		POLAR_LOG_XLOG_RECORD_INFO(state);
	}

	/*
	 * Check redo state again, if it's replaying then this xlog will be replayed by replaying
	 * process.
	 */
	//TODO
	Assert(0);
//	polar_lock_buffer_ext(*buffer, BUFFER_LOCK_EXCLUSIVE, false);

	/* Critical section begin */

	/* Apply current record on page */
	if (PageGetLSN(page) <= lsn)
		polar_logindex_apply_one_record(instance, state, tag, buffer);

	polar_promote_mark_buf_dirty(instance, *buffer, lsn);

	/* Critical section end */
	LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
}

//Updated
/* Pin buffer if it exists in the buffer pool */
static Buffer
polar_pin_buffer_for_replay(BufferTag *tag)
{
	uint32 hash;
	LWLock *partition_lock;
	int buf_id;

	Assert(tag != NULL);
	hash = BufTableHashCode(tag);
	partition_lock = BufMappingPartitionLock(hash);

	/* See if the block is in the buffer pool already */
	LWLockAcquire(partition_lock, LW_SHARED);
	buf_id = BufTableLookup(tag, hash);

	/* If page is in buffer, we can apply record, otherwise we do nothing */
	if (buf_id >= 0)
	{
		Buffer buffer;
		BufferDesc *buf_desc = GetBufferDescriptor(buf_id);
		bool valid;

		/* Pin buffer from being evicted */
		Assert(0);
//		valid = polar_pin_buffer(buf_desc, NULL);
		/* Can release the mapping lock as soon as possible */
		LWLockRelease(partition_lock);

		buffer = BufferDescriptorGetBuffer(buf_desc);

		if (!valid)
		{
			uint32 redo_state = pg_atomic_read_u32(&buf_desc->polar_redo_state);

			/*
			 * We can only replay buffer after POLAR_REDO_IO_END is set, which means buffer content is read from storage.
			 * The backend process clear POLAR_REDO_REPLAYING and then set BM_VALID flag.
			 * If startup parse xlog and set POLAR_REDO_OUTDATE, POLAR_REDO_REPLAYING
			 * flag is cleard but BM_VALID is not set, then we need to replay this buffer
			 */
			if ((redo_state & POLAR_REDO_READ_IO_END) && 
					(redo_state & POLAR_REDO_OUTDATE) && !(redo_state & POLAR_REDO_REPLAYING))
				return buffer;

			/*
			 * Invalid buffer means some other process is reading or replaying this page
			 * currently. After reading or replaying, xlog replay will be done, so we don't need to
			 * replay it. Even error occurs while io, invalid buffer will be read from
			 * disk next access try, or evicted from buffer pool if no one else access it.
			 */
			ReleaseBuffer(buffer);
			return InvalidBuffer;
		}

		return buffer;
	}
	else
		LWLockRelease(partition_lock);

	return InvalidBuffer;
}

static buf_replay_stat_t
polar_buffer_need_replay(polar_logindex_redo_ctl_t instance, Buffer buffer, XLogRecPtr lsn)
{
	XLogRecPtr page_lsn;
	BufferDesc *buf_desc = GetBufferDescriptor(buffer - 1);
	uint32 redo_state = pg_atomic_read_u32(&buf_desc->polar_redo_state);
	char       *page = BufferGetPage(buffer);

	/*
	 * Background process don't need to replay if Buffer is replaying or already replayed
	 */
	if (redo_state & POLAR_REDO_INVALIDATE)
		return BUF_IS_TRUNCATED;
	else if (redo_state & POLAR_REDO_REPLAYING)
		return BUF_IS_REPLAYING;

	if (PageIsNew(page) || PageIsEmpty(page))
	{
		bool in_range;
		LWLockAcquire(POLAR_REL_SIZE_CACHE_LOCK(instance->rel_size_cache), LW_SHARED);
		in_range = polar_check_rel_block_valid_only(instance->rel_size_cache, lsn, &buf_desc->tag);
		LWLockRelease(POLAR_REL_SIZE_CACHE_LOCK(instance->rel_size_cache));

		if (!in_range)
			return BUF_IS_TRUNCATED;
	}
	else
	{
		page_lsn = BufferGetLSNAtomic(buffer);

		if (!XLogRecPtrIsInvalid(page_lsn))
		{
			/*
			 * Background process don't need to replay if page lsn is larger than xlog record's lsn,
			 * which means this record is already replayed by backend process
			 */
			if (page_lsn > lsn)
				return BUF_IS_REPLAYED;
		}
	}

	return BUF_NEED_REPLAY;
}

/*
 * POLAR: Apply one record on one page.
 *
 * We will check existence of buffer page in buffer pool first. If hit, replay record on it,
 * otherwise do nothing. While buffer hit, buffer should be pinned before replaying from being
 * evicted in which target buffer may be not the requested one.
 *
 * If pinned buffer is invalid, buffer is doing io now. After io operation, page will be replayed,
 * so no need to replay it in background.
 */
static void
polar_only_replay_exists_buffer(polar_logindex_redo_ctl_t instance, XLogReaderState *state, log_index_lsn_t *log_index_page)
{
	Buffer buffer;

	Assert(log_index_page != NULL);

	buffer = polar_pin_buffer_for_replay(log_index_page->tag);

	/* If page is in buffer, we can apply record, otherwise we do nothing */
	if (BufferIsValid(buffer))
	{
		buf_replay_stat_t stat = polar_buffer_need_replay(instance, buffer, log_index_page->lsn);

		if (stat == BUF_NEED_REPLAY)
		{
			XLogRecPtr consist_lsn = polar_get_primary_consist_ptr();

			if (consist_lsn > log_index_page->lsn && polar_evict_buffer(buffer))
			{
				elog(polar_trace_logindex(DEBUG3),
					 "Evict buffer=%d when consist_lsn=%lX and xlog_lsn=%lX for " POLAR_LOG_BUFFER_TAG_FORMAT,
					 buffer, consist_lsn, log_index_page->lsn, POLAR_LOG_BUFFER_TAG(log_index_page->tag));
				buffer = InvalidBuffer;
			}
			else
				polar_bg_redo_apply_read_record(instance, state, log_index_page->lsn, log_index_page->tag, &buffer);
		}
		else
		{
			elog(polar_trace_logindex(DEBUG3),
				 "The buf state=%d, so don't replay buf=%d when lsn=%lX for " POLAR_LOG_BUFFER_TAG_FORMAT,
				 stat, buffer, log_index_page->lsn, POLAR_LOG_BUFFER_TAG(log_index_page->tag));
		}
	}

	if (BufferIsValid(buffer))
		ReleaseBuffer(buffer);
}

/*
 * POLAR: Return max lsn of a and b.
 */
XLogRecPtr
polar_max_xlog_rec_ptr(XLogRecPtr a, XLogRecPtr b)
{
	if (XLogRecPtrIsInvalid(a))
		return b;
	else if (XLogRecPtrIsInvalid(b))
		return a;
	else
		return Max(a, b);
}

/*
 * POLAR: Return min lsn of a and b.
 */
XLogRecPtr
polar_min_xlog_rec_ptr(XLogRecPtr a, XLogRecPtr b)
{
	if (XLogRecPtrIsInvalid(a))
		return b;
	else if (XLogRecPtrIsInvalid(b))
		return a;
	else
		return Min(a, b);
}

/*
 * POLAR: Lock redo state - set POLAR_REDO_LOCKED in redo state
 */
uint32
polar_lock_redo_state(BufferDesc *desc)
{
	SpinDelayStatus delayStatus;
	uint32      old_redo_state;

	init_local_spin_delay(&delayStatus);

	while (true)
	{
		/* set POLAR_REDO_LOCKED flag */
		old_redo_state = pg_atomic_fetch_or_u32(&desc->polar_redo_state, POLAR_REDO_LOCKED);

		/* if it wasn't set before we're OK */
		if (!(old_redo_state & POLAR_REDO_LOCKED))
			break;

		perform_spin_delay(&delayStatus);
	}

	finish_spin_delay(&delayStatus);
	return old_redo_state | POLAR_REDO_LOCKED;
}

void
polar_log_page_iter_context(void)
{
	if (iter_context != NULL)
	{
		BufferTag *tag = polar_logindex_page_iterator_buf_tag(iter_context);

		elog(LOG, "Page iter start_lsn=%lX, end_lsn=%lX for page=" POLAR_LOG_BUFFER_TAG_FORMAT,
			 polar_logindex_page_iterator_min_lsn(iter_context),
			 polar_logindex_page_iterator_max_lsn(iter_context), POLAR_LOG_BUFFER_TAG(tag));
	}
}

/*
 * POLAR; if we read a future page, then call this function to restore old version page
 */
bool
polar_logindex_restore_fullpage_snapshot_if_needed(polar_logindex_redo_ctl_t instance, BufferTag *tag, Buffer *buffer)
{
	static XLogReaderState *state = NULL;
	log_index_lsn_t *lsn_info = NULL;
	MemoryContext oldcontext = NULL;
	XLogRecPtr replayed_lsn = InvalidXLogRecPtr;
	XLogRecPtr end_lsn = PG_INT64_MAX;
	Page page;
	bool    success = false;
	TimestampTz start_time = GetCurrentTimestamp();

	Assert(BufferIsValid(*buffer));
	page = BufferGetPage(*buffer);

retry:

	/*
	 * POLAR: If page lsn is larger than replayed xlog lsn,
	 * then this replica read a future page
	 */
	Assert(0);
	//TODO
//	if (!polar_is_future_page(GetBufferDescriptor(*buffer - 1)))
//		return false;

	pgstat_report_wait_start(WAIT_EVENT_LOGINDEX_WAIT_FULLPAGE);

	replayed_lsn = GetXLogReplayRecPtr(&ThisTimeLineID);

	Assert(fullpage_page_iter == NULL);

	/*
	 * Logindex record the start position of XLOG and we search LSN between [replayed_lsn, end_lsn].
	 * And end_lsn points to the end position of the last xlog, so we should subtract 1 here .
	 */
	fullpage_page_iter = polar_logindex_create_page_iterator(instance->fullpage_logindex_snapshot, tag,
															 replayed_lsn, end_lsn - 1, polar_get_bg_redo_state(instance) == POLAR_BG_ONLINE_PROMOTE);

	if (polar_logindex_page_iterator_state(fullpage_page_iter) != ITERATE_STATE_FINISHED)
	{
		elog(PANIC, "Failed to iterate data for "POLAR_LOG_BUFFER_TAG_FORMAT", which replayed_lsn=%X/%X and end_lsn=%X/%X",
			 POLAR_LOG_BUFFER_TAG(tag),
			 (uint32)(replayed_lsn >> 32), (uint32)replayed_lsn,
			 (uint32)(end_lsn >> 32), (uint32)end_lsn);
	}

	oldcontext = MemoryContextSwitchTo(polar_logindex_memory_context());

	if (state == NULL)
	{
//		state = XLogReaderAllocate(wal_segment_size, &read_local_xlog_page, NULL);
        state = XLogReaderAllocate(wal_segment_size, NULL,
                           XL_ROUTINE(.page_read = &read_local_xlog_page,
                                      .segment_open = NULL,
                                      .segment_close = wal_segment_close),
                           NULL);

		if (!state)
		{
			ereport(FATAL,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail("Failed while allocating a WAL reading processor.")));
		}
	}

	lsn_info = polar_logindex_page_iterator_next(fullpage_page_iter);

	if (lsn_info != NULL)
	{
		XLogRecPtr lsn = lsn_info->lsn;

		polar_logindex_read_xlog(state, lsn);
		polar_logindex_apply_one_record(instance, state, tag, buffer);
		success = true;
	}

	MemoryContextSwitchTo(oldcontext);

	polar_logindex_release_page_iterator(fullpage_page_iter);
	fullpage_page_iter = NULL;

	if (!success)
	{
		CHECK_FOR_INTERRUPTS();

		/* Force FATAL future page if we wait fullpage logindex too long */
		if (TimestampDifferenceExceeds(start_time, GetCurrentTimestamp(),
									   polar_wait_old_version_page_timeout))
			elog(FATAL, "Read a future page due to timeout, page lsn = %lx, replayed_lsn = %lx, page_tag=" POLAR_LOG_BUFFER_TAG_FORMAT,
				 PageGetLSN(page), replayed_lsn, POLAR_LOG_BUFFER_TAG(tag));

		pg_usleep(100); /* 0.1ms */
		pgstat_report_wait_end();
		goto retry;
	}

	pgstat_report_wait_end();

	elog(polar_trace_logindex(DEBUG3), "%s restore page " POLAR_LOG_BUFFER_TAG_FORMAT " LSN=%X/%X", __func__,
		 POLAR_LOG_BUFFER_TAG(tag), (uint32)(PageGetLSN(page) >> 32), (uint32)PageGetLSN(page));

	return true;
}

static Size
polar_logindex_redo_ctl_shmem_size(void)
{
	return MAXALIGN(sizeof(polar_logindex_redo_ctl_data_t));
}

//updated
Size
polar_logindex_redo_shmem_size(void)
{
	Size size = 0;

	if (polar_logindex_mem_size <= 0)
		return size;

	size = add_size(size, polar_logindex_redo_ctl_shmem_size());

	size = add_size(size, polar_logindex_mini_trans_shmem_size());

	size = add_size(size,
					polar_logindex_shmem_size(polar_logindex_convert_mem_tbl_size(polar_logindex_mem_size * WAL_LOGINDEX_MEM_RATIO),
											  polar_logindex_bloom_blocks * WAL_LOGINDEX_MEM_RATIO));

	if (polar_rel_size_cache_blocks > 0)
		size = add_size(size, polar_rel_size_shmem_size(polar_rel_size_cache_blocks));

	if (polar_xlog_queue_buffers > 0)
		size = add_size(size, polar_xlog_queue_size(polar_xlog_queue_buffers));

	if (polar_logindex_max_local_cache_segments > 0)
		size = add_size(size, polar_local_cache_shmem_size(polar_logindex_max_local_cache_segments));

	size = add_size(size,
					polar_logindex_shmem_size(polar_logindex_convert_mem_tbl_size(polar_logindex_mem_size * FULLPAGE_LOGINDEX_MEM_RATIO),
											  polar_logindex_bloom_blocks * FULLPAGE_LOGINDEX_MEM_RATIO));

	size = add_size(size, polar_fullpage_shmem_size());

	if (polar_logindex_max_local_cache_segments > 0)
		size = add_size(size, polar_local_cache_shmem_size(polar_logindex_max_local_cache_segments));

//	if (polar_parallel_replay_proc_num > 0 && polar_parallel_replay_task_queue_depth > 0)
//	{
//		size = add_size(size, polar_calc_task_sched_shmem_size(polar_parallel_replay_proc_num,
//															   sizeof(parallel_replay_task_node_t), polar_parallel_replay_task_queue_depth));
//	}

	return size;
}

static polar_logindex_redo_ctl_t
polar_logindex_redo_ctl_init(const char *name)
{
	bool found;
	polar_logindex_redo_ctl_t ctl;

	ctl = (polar_logindex_redo_ctl_t)ShmemInitStruct(name, polar_logindex_redo_ctl_shmem_size(), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		MemSet(ctl, 0, sizeof(polar_logindex_redo_ctl_data_t));

		/* Init logindex memory context which is static variable */
		polar_logindex_memory_context();
	}
	else
		Assert(found);

	return ctl;
}

static bool
polar_logindex_redo_table_flushable(struct log_mem_table_t *table, void *data)
{
	return POLAR_LOGINDEX_FLUSHABLE_LSN() > polar_logindex_mem_table_max_lsn(table);
}

//updated
void
polar_logindex_redo_shmem_init(void)
{
	polar_logindex_redo_ctl_t instance;

	if (polar_logindex_mem_size <= 0)
		return;

	instance = (polar_logindex_redo_ctl_t) polar_logindex_redo_ctl_init("logindex_redo_ctl");

	instance->mini_trans = polar_logindex_mini_trans_shmem_init("logindex_redo_minitrans");

	instance->wal_logindex_snapshot = polar_logindex_snapshot_shmem_init(WAL_LOGINDEX_DIR,
																		 polar_logindex_convert_mem_tbl_size(polar_logindex_mem_size * WAL_LOGINDEX_MEM_RATIO),
																		 polar_logindex_bloom_blocks * WAL_LOGINDEX_MEM_RATIO, LWTRANCHE_WAL_LOGINDEX_BEGIN, LWTRANCHE_WAL_LOGINDEX_END,
																		 polar_logindex_redo_table_flushable, NULL);

	if (polar_rel_size_cache_blocks > 0)
		instance->rel_size_cache = polar_rel_size_shmem_init(RELATION_SIZE_CACHE_DIR, polar_rel_size_cache_blocks);

	if (polar_xlog_queue_buffers > 0)
		instance->xlog_queue = polar_xlog_queue_init("polar_xlog_queue", LWTRANCHE_POLAR_XLOG_QUEUE,
													 polar_xlog_queue_buffers);

	if (polar_logindex_max_local_cache_segments > 0)
		polar_logindex_create_local_cache(instance->wal_logindex_snapshot, "wal_logindex",
										  polar_logindex_max_local_cache_segments);

	instance->fullpage_logindex_snapshot = polar_logindex_snapshot_shmem_init(FULLPAGE_LOGINDEX_DIR,
																			  polar_logindex_convert_mem_tbl_size(polar_logindex_mem_size * FULLPAGE_LOGINDEX_MEM_RATIO),
																			  polar_logindex_bloom_blocks * FULLPAGE_LOGINDEX_MEM_RATIO, LWTRANCHE_FULLPAGE_LOGINDEX_BEGIN,
																			  LWTRANCHE_FULLPAGE_LOGINDEX_END, polar_logindex_redo_table_flushable, NULL);

	instance->fullpage_ctl = polar_fullpage_shmem_init(FULLPAGE_LOGINDEX_DIR,
													   instance->xlog_queue, instance->fullpage_logindex_snapshot);

	if (polar_logindex_max_local_cache_segments > 0)
		polar_logindex_create_local_cache(instance->fullpage_logindex_snapshot, "fullpage_logindex",
										  polar_logindex_max_local_cache_segments);

	pg_atomic_init_u32(&instance->bg_redo_state, POLAR_BG_REDO_NOT_START);
	SpinLockInit(&instance->info_lck);

//	if (polar_parallel_replay_proc_num > 0 && polar_parallel_replay_task_queue_depth > 0)
//	{
//		instance->parallel_sched = polar_create_proc_task_sched("polar_parallel_replay_sched",
//																polar_parallel_replay_proc_num, sizeof(parallel_replay_task_node_t),
//																polar_parallel_replay_task_queue_depth, instance);
//	}

	polar_logindex_redo_instance = instance;
}

void
polar_logindex_remove_old_files(polar_logindex_redo_ctl_t instance)
{
	XLogRecPtr min_lsn;

	if (instance == NULL)
		return;

	min_lsn = polar_calc_min_used_lsn(true);

	/* Truncate relation size cache which saved in local file system*/
	if (instance->rel_size_cache)
		polar_truncate_rel_size_cache(instance->rel_size_cache, min_lsn);

	if (polar_in_replica_mode())
		return;

	/* Truncate the following files which saved in shared storage */

	/* Truncate logindex before removing wal files */
	if (instance->wal_logindex_snapshot != NULL)
		polar_logindex_truncate(instance->wal_logindex_snapshot, min_lsn);

	/* Truncate fullpage data */
	if (instance->fullpage_ctl != NULL)
		polar_remove_old_fullpage_files(instance->fullpage_ctl, min_lsn);
}

void
polar_logindex_remove_all(void)
{
	char path[MAXPGPATH] = {0};

	/* replica cannot remove file from storage */
	if (polar_in_replica_mode())
		return;

	POLAR_FILE_PATH(path, WAL_LOGINDEX_DIR);
	rmtree(path, false);

	POLAR_FILE_PATH(path, FULLPAGE_LOGINDEX_DIR);
	rmtree(path, false);
}

XLogRecPtr
polar_logindex_redo_start_lsn(polar_logindex_redo_ctl_t instance)
{
	if (instance && instance->wal_logindex_snapshot)
		return polar_logindex_start_lsn(instance->wal_logindex_snapshot);

	return InvalidXLogRecPtr;
}

XLogRecPtr
polar_logindex_redo_init(polar_logindex_redo_ctl_t instance, XLogRecPtr checkpoint_lsn, bool read_only)
{
	XLogRecPtr start_lsn = checkpoint_lsn;

	if (instance)
	{
		if (instance->wal_logindex_snapshot)
			start_lsn = polar_logindex_snapshot_init(instance->wal_logindex_snapshot, checkpoint_lsn, read_only);

		if (instance->fullpage_logindex_snapshot)
			polar_logindex_snapshot_init(instance->fullpage_logindex_snapshot, checkpoint_lsn, read_only);

		elog(LOG, "polar logindex change from %lx to %lx", checkpoint_lsn, start_lsn);

		instance->xlog_replay_from = checkpoint_lsn;
	}

	return start_lsn;
}

void
polar_logindex_redo_flush_data(polar_logindex_redo_ctl_t instance, XLogRecPtr checkpoint_lsn)
{
	if (polar_in_replica_mode() || !instance)
		return ;

	if (instance->fullpage_logindex_snapshot)
		polar_logindex_flush_table(instance->fullpage_logindex_snapshot, checkpoint_lsn);

	if (instance->wal_logindex_snapshot)
		polar_logindex_flush_table(instance->wal_logindex_snapshot, checkpoint_lsn);
}

bool
polar_logindex_redo_bg_flush_data(polar_logindex_redo_ctl_t instance)
{
	bool write_done = true;

	if (likely(instance))
	{
		if (instance->fullpage_logindex_snapshot)
			write_done &= polar_logindex_bg_write(instance->fullpage_logindex_snapshot);

		write_done &= polar_logindex_bg_write(instance->wal_logindex_snapshot);
	}

	return write_done;
}

//updated
static bool
polar_logindex_bg_dispatch(polar_logindex_bg_redo_ctl_t *ctl, bool *can_hold)
{
    Assert(0);
	polar_task_sched_ctl_t *sched_ctl = ctl->sched_ctl;
	bool dispatch_done = true;
	XLogRecPtr bg_replayed_lsn, backend_min_lsn, xlog_replayed_lsn;

	*can_hold = false;

	/* Remove finished task from running queue */
//	if (!polar_sched_empty_running_task(sched_ctl))
//		polar_sched_remove_finished_task(sched_ctl);

	/* Dispatch new task to replay */
	do
	{
		parallel_replay_task_node_t node, *dst_node;
		BufferTag *tag;
		xlog_replayed_lsn = GetXLogReplayRecPtr(NULL);

		if (!ctl->replay_page)
			ctl->replay_page = polar_logindex_lsn_iterator_next(ctl->instance->wal_logindex_snapshot,
																ctl->lsn_iter);

		/* If no more item in logindex, exit and retry next run */
		if (!ctl->replay_page)
			break;

		Assert(!XLogRecPtrIsInvalid(ctl->replay_page->lsn));

		/* If current xlog record is beyond replayed lsn, just break */
		if (ctl->replay_page->lsn > polar_get_last_replayed_read_ptr(ctl->instance))
			break;

		tag = ctl->replay_page->tag;
		INIT_BUFFERTAG(node.tag, tag->rnode, tag->forkNum, tag->blockNum);
		node.lsn = ctl->replay_page->lsn;
		node.prev_lsn = ctl->replay_page->prev_lsn;

//		dst_node = (parallel_replay_task_node_t *)polar_sched_add_task(sched_ctl, (polar_task_node_t *)&node);

		if (dst_node != NULL)
		{
			int proc = POLAR_TASK_NODE_PROC((polar_task_node_t *)dst_node);
			ctl->max_dispatched_lsn = node.lsn;
			ctl->replay_page = NULL;

			ereport(polar_trace_logindex(DEBUG2), (errmsg("Dispatch lsn=%lX, " POLAR_LOG_BUFFER_TAG_FORMAT " to proc=%d",
														  dst_node->lsn, POLAR_LOG_BUFFER_TAG(&dst_node->tag),
														  sched_ctl->sub_proc[proc].proc->pid),
												   errhidestmt(true), errhidecontext(true)));
		}
		else
		{
			/* We can not add this task because task queue is full, so the caller can hold a while */
			*can_hold = true;
			dispatch_done = false;
			/* Break this loop when fail to dispatch */
			break;
		}
	}
	while (true);

	/*
	 * Set dispatch_done to be true when there's no running task and no new WAL
	 * to dispatch.
	 *
	 * If dispatch done, bg_replayed_lsn need to be forwarded to
	 * GetXLogReplayRecPtr(NULL) to make it able to do the latest ckpt.
	 *
	 * Notice: we need to fetch GetXLogReplayRecPtr(NULL) at the very beginning,
	 * because WALs may be appended into logindex snapshot during our check.
	 * Forwarding to an improper RecPtr may skip the newly added WALs without
	 * replaying.
	 */
	if (dispatch_done)
		dispatch_done = polar_sched_empty_running_task(sched_ctl);

	if (dispatch_done)
	{
		Assert(ctl->instance != NULL);
		polar_bg_redo_set_replayed_lsn(ctl->instance, Max(xlog_replayed_lsn, ctl->max_dispatched_lsn));
		elog(polar_trace_logindex(DEBUG3), "%s: forward bg_replayed_lsn to %lX", __func__, xlog_replayed_lsn);
	}

	bg_replayed_lsn = polar_bg_redo_get_replayed_lsn(ctl->instance);
	pg_read_barrier();

//	backend_min_lsn = polar_get_backend_min_replay_lsn();

	ctl->instance->promote_oldest_lsn = XLogRecPtrIsInvalid(backend_min_lsn) ? bg_replayed_lsn : backend_min_lsn;

	return dispatch_done;
}

static bool
polar_logindex_bg_online_promote(polar_logindex_bg_redo_ctl_t *ctl, bool *can_hold)
{
//	bool dispatch_done = polar_logindex_bg_dispatch(ctl, can_hold);
//	polar_logindex_redo_ctl_t instance = ctl->instance;
//
//	if (dispatch_done && XLogRecPtrIsInvalid(polar_get_backend_min_replay_lsn()))
//	{
//		polar_set_bg_redo_state(instance, POLAR_BG_REDO_NOT_START);
//
//		/*
//		 * Request an (online) checkpoint now. This
//		 * isn't required for consistency, but the last restartpoint might be far
//		 * back, and in case of a crash, recovering from it might take a longer
//		 * than is appropriate now that we're not in standby mode anymore.
//		 */
//		RequestCheckpoint(CHECKPOINT_FORCE);
//
//		elog(LOG, "Background process finished replay for online promote, last_replayed_end_lsn=%lX",
//			 polar_bg_redo_get_replayed_lsn(instance));
//	}
//
//	return dispatch_done;
}

bool
polar_logindex_redo_bg_replay(polar_logindex_bg_redo_ctl_t *ctl, bool *can_hold)
{
	uint32 state;
	bool replay_done = true;

	Assert(ctl);

	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	state = polar_get_bg_redo_state(ctl->instance);
	/* Make sure we get background redo state */

	*can_hold = false;

	pg_read_barrier();

	switch (state)
	{
		case POLAR_BG_RO_BUF_REPLAYING:
			replay_done = polar_logindex_apply_xlog_background(ctl);
			break;

		case POLAR_BG_PARALLEL_REPLAYING:
			replay_done = polar_logindex_bg_dispatch(ctl, can_hold);
			break;

		case POLAR_BG_ONLINE_PROMOTE:
			replay_done = polar_logindex_bg_online_promote(ctl, can_hold);
			break;

		default:
			break;
	}

	return replay_done;
}

bool
polar_logindex_bg_promoted(polar_logindex_redo_ctl_t instance)
{
	return polar_get_bg_redo_state(instance) == POLAR_BG_REDO_NOT_START;
}

void
polar_logindex_redo_online_promote(polar_logindex_redo_ctl_t instance)
{
	if (!instance || !instance->wal_logindex_snapshot)
		return;

	if (instance->fullpage_logindex_snapshot)
		polar_logindex_online_promote(instance->fullpage_logindex_snapshot);

	polar_logindex_online_promote(instance->wal_logindex_snapshot);
}

void
polar_logindex_redo_abort(polar_logindex_redo_ctl_t instance)
{
	if (!instance)
		return;

	polar_logindex_abort_replaying_buffer();

	if (instance->mini_trans)
		polar_logindex_abort_mini_transaction(instance->mini_trans);

	if (wal_page_iter != NULL)
	{
		polar_logindex_release_page_iterator(wal_page_iter);
		wal_page_iter = NULL;
	}

	if (fullpage_page_iter != NULL)
	{
		polar_logindex_release_page_iterator(fullpage_page_iter);
		fullpage_page_iter = NULL;
	}
}

XLogRecPtr
polar_logindex_redo_get_min_replay_from_lsn(polar_logindex_redo_ctl_t instance, XLogRecPtr consist_lsn)
{
	XLogRecPtr result = consist_lsn;
	XLogRecPtr      bg_replayed_lsn;

	if (!instance || !instance->wal_logindex_snapshot)
		return result;

	if (instance->fullpage_logindex_snapshot)
	{
		/* This checkpoint lsn can avoid clear xlog when restore old fullpage */
		XLogRecPtr last_checkpoint_redo_lsn = GetRedoRecPtr();

		if (!XLogRecPtrIsInvalid(result) &&
				!XLogRecPtrIsInvalid(last_checkpoint_redo_lsn) &&
				last_checkpoint_redo_lsn < result)
			result = last_checkpoint_redo_lsn;
	}

	bg_replayed_lsn = polar_bg_redo_get_replayed_lsn(instance);

	if (!XLogRecPtrIsInvalid(bg_replayed_lsn) && bg_replayed_lsn < result)
		result = bg_replayed_lsn;

	return result;
}

//Updated
/* Apply xlog for buffer which already acquired buffer io lock */
bool
polar_logindex_io_lock_apply(polar_logindex_redo_ctl_t instance, BufferDesc *buf_hdr,
							 XLogRecPtr replay_from, XLogRecPtr checkpoint_lsn)
{
	Buffer buffer;
	XLogRecPtr start_lsn = replay_from;
	bool lsn_changed = false;

	Assert(buf_hdr);

	if (unlikely(InRecovery && !reachedConsistency))
	{
		POLAR_LOG_BACKTRACE();

		elog(PANIC, "%s apply buffer before reached consistency %X/%X, " POLAR_LOG_BUFFER_TAG_FORMAT, __func__,
			 (uint32)(replay_from >> 32),
			 (uint32) replay_from,
			 POLAR_LOG_BUFFER_TAG(&buf_hdr->tag));

		return lsn_changed;
	}

	buffer = BufferDescriptorGetBuffer(buf_hdr);
	Assert(BufferIsValid(buffer));

	/* If we read a future page, then restore old fullpage version and reset start_lsn */
	if (polar_in_replica_mode() &&
			polar_logindex_restore_fullpage_snapshot_if_needed(instance, &buf_hdr->tag, &buffer))
	{
		start_lsn = checkpoint_lsn;
		Assert(!XLogRecPtrIsInvalid(start_lsn));
	}

	/*
	 * When it is read from disk, it doesn't lose any flashback log because that
	 * the flashback log is flushed to disk before the data page.
	 * Mark it flashback lost checked to ignore to check it again.
	 */
//	polar_set_buf_flog_lost_checked(flog_instance, instance, buffer);

	lsn_changed = polar_logindex_lock_apply_page_from(instance, start_lsn, &buf_hdr->tag, &buffer);

	/* If we read buffer from storage and have no xlog to do replay, then it does not necessary to mark it dirty */
	if (lsn_changed)
		polar_promote_mark_buf_dirty(instance, buffer, start_lsn);

	return lsn_changed;
}

/*
 * POLAR: 1. Read xlog record from xlog queue and save to logindex
 * 2. Remove unnecessary data from xlog queue
 * 3. Flush fullpage active logindex table
 */
void
polar_logindex_rw_save(polar_logindex_redo_ctl_t instance)
{
	XLogRecord *record;
	static XLogReaderState *state = NULL;
	static TimestampTz last_flush_time = 0;

	if (!instance || !instance->xlog_queue)
		return;

	if (state == NULL)
//		state = XLogReaderAllocate(wal_segment_size, NULL, NULL);
        state = XLogReaderAllocate(wal_segment_size, NULL, NULL, NULL);

	while ((record = polar_xlog_send_queue_record_pop(instance->xlog_queue, state)))
	{
		if (polar_xlog_remove_payload(record))
			polar_logindex_save_lsn(instance, state);
	}

	polar_xlog_send_queue_keep_data(instance->xlog_queue);

	if (instance->fullpage_logindex_snapshot)
	{
		TimestampTz now = GetCurrentTimestamp();

		if (TimestampDifferenceExceeds(last_flush_time, now,
									   polar_write_logindex_active_table_delay))
		{
			polar_logindex_flush_active_table(instance->fullpage_logindex_snapshot);
			last_flush_time = now;
		}
	}
}

void
polar_set_bg_redo_state(polar_logindex_redo_ctl_t instance, uint32 state)
{
	if (instance)
		pg_atomic_write_u32(&instance->bg_redo_state, state);
}

/*
 * POLAR: Set background replayed lsn.
 */
void
polar_bg_redo_set_replayed_lsn(polar_logindex_redo_ctl_t instance, XLogRecPtr lsn)
{
	if (instance)
	{
		SpinLockAcquire(&instance->info_lck);
		instance->bg_replayed_lsn = lsn;
		SpinLockRelease(&instance->info_lck);
	}
}

/*
 * POLAR: Get background replayed lsn.
 */
XLogRecPtr
polar_bg_redo_get_replayed_lsn(polar_logindex_redo_ctl_t instance)
{
	XLogRecPtr lsn = InvalidXLogRecPtr;

	if (instance)
	{
		SpinLockAcquire(&instance->info_lck);
		lsn = instance->bg_replayed_lsn;
		SpinLockRelease(&instance->info_lck);
	}

	return lsn;
}

void
polar_set_last_replayed_read_ptr(polar_logindex_redo_ctl_t instance, XLogRecPtr lsn)
{
	if (instance)
	{
		SpinLockAcquire(&instance->info_lck);
		instance->last_replayed_read_ptr = lsn;
		SpinLockRelease(&instance->info_lck);
	}
}

XLogRecPtr
polar_get_last_replayed_read_ptr(polar_logindex_redo_ctl_t instance)
{
	XLogRecPtr lsn = InvalidXLogRecPtr;

	if (instance)
	{
		SpinLockAcquire(&instance->info_lck);
		lsn = instance->last_replayed_read_ptr;
		SpinLockRelease(&instance->info_lck);
	}

	return lsn;
}

void
polar_logindex_wakeup_bg_replay(polar_logindex_redo_ctl_t instance, XLogRecPtr read_ptr, XLogRecPtr end_ptr)
{
	if (instance)
	{
		uint32 state = POLAR_BG_REDO_NOT_START;

		SpinLockAcquire(&instance->info_lck);
		instance->last_replayed_read_ptr = read_ptr;
		instance->bg_replayed_lsn = end_ptr;
		SpinLockRelease(&instance->info_lck);

		if (POLAR_ENABLE_PARALLEL_REPLAY_STANDBY_MODE() ||
			(polar_in_replica_mode() && polar_enable_ro_prewarm))
		{
			instance->promote_oldest_lsn = end_ptr;

			state = POLAR_BG_PARALLEL_REPLAYING;
		}
		else if (polar_in_replica_mode())
			state = POLAR_BG_RO_BUF_REPLAYING;

		if (state != POLAR_BG_REDO_NOT_START)
		{
			pg_write_barrier();
			polar_set_bg_redo_state(instance, state);

			NOTIFY_LOGINDEX_BG_WORKER(instance->bg_worker_latch);

			if (instance->fullpage_ctl)
				polar_fullpage_bgworker_wakeup(instance->fullpage_ctl);
		}
	}
}

bool
polar_logindex_require_backend_redo(polar_logindex_redo_ctl_t instance, ForkNumber fork_num, XLogRecPtr *replay_from)
{
	bool required;
	XLogRecPtr consist_lsn;

	if (!instance)
		return false;

	required = polar_in_replica_mode() || polar_get_bg_redo_state(instance) == POLAR_BG_PARALLEL_REPLAYING;

	Assert(replay_from != NULL);
	*replay_from = InvalidXLogRecPtr;
	consist_lsn = polar_get_primary_consist_ptr();

	if (!required)
	{
		if (polar_get_bg_redo_state(instance) == POLAR_BG_ONLINE_PROMOTE)
		{
			SpinLockAcquire(&instance->info_lck);

			if (instance->bg_replayed_lsn <= instance->last_replayed_read_ptr)
			{
				*replay_from = instance->bg_replayed_lsn;
				required = true;
				POLAR_SET_BACKEND_READ_MIN_LSN(*replay_from);
			}

			SpinLockRelease(&instance->info_lck);
		}

		if (required)
		{
			if (!XLogRecPtrIsInvalid(consist_lsn))
				*replay_from = Max(consist_lsn, *replay_from);
		}
	}
	else
	{
		XLogRecPtr redo_lsn = GetRedoRecPtr();

		if (POLAR_IN_PARALLEL_REPLAY_STANDBY_MODE(polar_logindex_redo_instance))
		{
			/*
			 * POLAR: When in standby_mode with parallel_replay_enabled, the
			 * replay_from lsn should be bg_replayed_lsn
			 */
			XLogRecPtr bg_replayed_lsn = polar_bg_redo_get_replayed_lsn(polar_logindex_redo_instance);
			*replay_from = !XLogRecPtrIsInvalid(bg_replayed_lsn) ? bg_replayed_lsn : redo_lsn;
			Assert(!XLogRecPtrIsInvalid(*replay_from));
			POLAR_SET_BACKEND_READ_MIN_LSN(*replay_from);
		}
		else
		{
			/*
			 * POLAR: After replication is terminated, RO doesn't know what happened to RW.
			 * Meanwhile, if RW is in recovery state, it would reflushed some data block to
			 * make page's lsn lower than consist_lsn. If RO read these old pages and replay
			 * them from consist_lsn, RO will PANIC! So, we don't want RO replay data block
			 * from consist_lsn after replication is not streaming.
			 */
			*replay_from = (!XLogRecPtrIsInvalid(consist_lsn) && WalRcvStreaming()) ? consist_lsn : redo_lsn;
			Assert(!XLogRecPtrIsInvalid(redo_lsn));
			POLAR_SET_BACKEND_READ_MIN_LSN(redo_lsn);
		}
	}

	return required;
}

/*
 * POLAR: set valid information to enable logindex parse.
 * set it only when logindex snapshot state is writable in current node.
 * For Primary/Standby node, logindex snapshot is writable
 * For Replica node, logindex snapshot is readonly
 * For Datamax node, logindex won't be enabled
 */
void
polar_logindex_redo_set_valid_info(polar_logindex_redo_ctl_t instance, XLogRecPtr end_of_log)
{
	Assert(instance);

	if (instance->fullpage_logindex_snapshot)
	{
		if (polar_logindex_check_state(instance->fullpage_logindex_snapshot, POLAR_LOGINDEX_STATE_WRITABLE)
				&& XLogRecPtrIsInvalid(polar_logindex_check_valid_start_lsn(instance->fullpage_logindex_snapshot)))
			polar_logindex_set_start_lsn(instance->fullpage_logindex_snapshot, end_of_log);
	}

	if (polar_logindex_check_state(instance->wal_logindex_snapshot, POLAR_LOGINDEX_STATE_WRITABLE)
			&& XLogRecPtrIsInvalid(polar_logindex_check_valid_start_lsn(instance->wal_logindex_snapshot)))
		polar_logindex_set_start_lsn(instance->wal_logindex_snapshot, end_of_log);
}

XLogRecPtr
polar_online_promote_fake_oldest_lsn(void)
{
	Assert(!XLogRecPtrIsInvalid(polar_logindex_redo_instance->promote_oldest_lsn));

	return polar_logindex_redo_instance->promote_oldest_lsn;
}

static void
polar_clean_redo_context(int code, Datum arg)
{
	if (polar_redo_context)
	{
		MemoryContextDelete(polar_redo_context);
		polar_redo_context = NULL;
	}
}

MemoryContext
polar_get_redo_context(void)
{
	if (polar_redo_context == NULL)
	{
		polar_redo_context = AllocSetContextCreate(CurrentMemoryContext,
												   "polar working context",
												   ALLOCSET_DEFAULT_SIZES);

		before_shmem_exit(polar_clean_redo_context, 0);
	}

	return polar_redo_context;
}

static void *
get_parallel_replay_task_tag(polar_task_node_t *task)
{
	parallel_replay_task_node_t *parallel_task = (parallel_replay_task_node_t *)task;

	return &parallel_task->tag;
}

static void
parallel_replay_task_finished(polar_task_sched_t *sched, polar_task_node_t *task, polar_logindex_redo_ctl_t instance)
{
	parallel_replay_task_node_t *parallel_task = (parallel_replay_task_node_t *)task;
	parallel_replay_task_node_t *parallel_head;
	XLogRecPtr head_lsn = InvalidXLogRecPtr;

	SpinLockAcquire(&sched->lock);
	parallel_head = (parallel_replay_task_node_t *)POLAR_SCHED_RUNNING_QUEUE_HEAD(sched);

	head_lsn = parallel_head->lsn;
	SpinLockRelease(&sched->lock);

	Assert(!XLogRecPtrIsInvalid(head_lsn));

	if (head_lsn == parallel_task->lsn)
	{
		SpinLockAcquire(&instance->info_lck);

		if (instance->bg_replayed_lsn < parallel_task->lsn)
			instance->bg_replayed_lsn = parallel_task->lsn;

		SpinLockRelease(&instance->info_lck);
	}
}

//Updated
static Buffer
polar_xlog_need_replay(polar_logindex_redo_ctl_t instance, BufferTag *tag, XLogRecPtr lsn, buf_replay_stat_t *buf_stat)
{
	Buffer buffer;

	buffer = polar_pin_buffer_for_replay(tag);

	if (BufferIsValid(buffer))
	{
		*buf_stat = polar_buffer_need_replay(instance, buffer, lsn);

		if (*buf_stat == BUF_NEED_REPLAY && polar_in_replica_mode())
		{
			XLogRecPtr consist_lsn = polar_get_primary_consist_ptr();

			if (lsn < consist_lsn && polar_evict_buffer(buffer))
			{
				elog(polar_trace_logindex(DEBUG3),
					 "Evict buffer=%d when consist_lsn=%lX and xlog_lsn=%lX for " POLAR_LOG_BUFFER_TAG_FORMAT,
					 buffer, consist_lsn, lsn, POLAR_LOG_BUFFER_TAG(tag));

				buffer = InvalidBuffer;
				*buf_stat = BUF_IS_FLUSHED;
			}
		}
	}
	else
	{
		if (lsn < polar_get_primary_consist_ptr())
			*buf_stat = BUF_IS_FLUSHED;
		else
		{
			LWLockAcquire(POLAR_REL_SIZE_CACHE_LOCK(instance->rel_size_cache), LW_SHARED);

			/* Don't replay this block if it's truncated or dropped */
			if (polar_check_rel_block_valid_only(instance->rel_size_cache, lsn, tag))
			{
				buffer = XLogReadBufferExtended(tag->rnode, tag->forkNum, tag->blockNum, RBM_NORMAL_NO_LOG);
				*buf_stat = BUF_NEED_REPLAY;

				/* Mark the buffer no flashback log lost while read it from disk. */
//				polar_set_buf_flog_lost_checked(flog_instance, instance, buffer);
			}
			else
				*buf_stat = BUF_IS_TRUNCATED;

			LWLockRelease(POLAR_REL_SIZE_CACHE_LOCK(instance->rel_size_cache));
		}
	}

	return buffer;
}

static bool
handle_parallel_replay_task(polar_task_sched_t *sched, polar_task_node_t *task)
{
	static XLogReaderState *state = NULL;
	polar_logindex_redo_ctl_t instance = (polar_logindex_redo_ctl_t)(sched->run_arg);
	parallel_replay_task_node_t *parallel_task = (parallel_replay_task_node_t *)task;
	MemoryContext oldcontext = MemoryContextSwitchTo(polar_logindex_memory_context());
	buf_replay_stat_t buf_stat = BUF_NEED_REPLAY;
	bool handled = true;
	Buffer buffer;

	Assert(instance && instance->rel_size_cache);
	Assert(polar_bg_redo_state_is_parallel(instance));

	if (unlikely(state == NULL))
		state = polar_allocate_xlog_reader();

	buffer = polar_xlog_need_replay(instance, &parallel_task->tag, parallel_task->lsn, &buf_stat);

	if (buf_stat == BUF_NEED_REPLAY)
		polar_bg_redo_apply_read_record(instance, state, parallel_task->lsn, &parallel_task->tag, &buffer);
	else if (buf_stat == BUF_IS_REPLAYING)
		handled = false;
	else if (buf_stat == BUF_IS_REPLAYED && polar_bg_redo_state_is_parallel(instance))
	{
		BufferDesc *buf_desc = GetBufferDescriptor(buffer - 1);

		//todo
		Assert(0);
//		polar_lock_buffer_ext(buffer, BUFFER_LOCK_EXCLUSIVE, false);

		/*
		 * If the buffer may lose some flashback log record,
		 * just insert it to flashback log list as a candidator.
		 */
//		if (polar_may_buf_lost_flog(flog_instance, instance, buf_desc))
//			polar_flog_insert(flog_instance, buffer, true, false);

		polar_promote_mark_buf_dirty(instance, buffer, parallel_task->lsn);
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	}

	if (BufferIsValid(buffer))
		ReleaseBuffer(buffer);

	MemoryContextSwitchTo(oldcontext);

	if (handled)
		parallel_replay_task_finished(sched, task, instance);

	return handled;
}

static void
parallel_replay_task_startup(void *run_arg)
{
	/* When do parallel replay we will exit process on any error */
	ExitOnAnyError = true;

	polar_bg_replaying_process = POLAR_LOGINDEX_PARALLEL_REPLAY;

	/* Make sure there's room for us to pin buffer */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);
}

//updated
polar_task_sched_ctl_t *
polar_create_parallel_replay_sched_ctl(polar_logindex_bg_redo_ctl_t *bg_ctl)
{
    Assert(0);
	polar_task_sched_ctl_t *ctl;
//	polar_logindex_redo_ctl_t instance = bg_ctl->instance;
//
//	polar_sched_reg_handler(instance->parallel_sched, parallel_replay_task_startup, handle_parallel_replay_task,
//							NULL, get_parallel_replay_task_tag);

//	ctl = polar_create_task_sched_ctl(instance->parallel_sched, sizeof(BufferTag),
//									  NULL, NULL);

	return ctl;
}

bool
polar_need_do_bg_replay(polar_logindex_redo_ctl_t instance)
{
	if (unlikely(!instance))
		return false;

	return polar_in_replica_mode() ||
           polar_bg_redo_state_is_parallel(instance);
}

/*
 * Set bg_replayed_lsn(bl) as old_consist_lsn(ol) if ol < bl
 * Set the state of LogindexBgWriter as OnlinePromoting
 */
void
polar_reset_bg_replayed_lsn(polar_logindex_redo_ctl_t instance)
{
//	XLogRecPtr bg_replayed_lsn;
//	XLogRecPtr new_bg_replayed_lsn;
//	XLogRecPtr old_consist_lsn;
//	TimeLineID old_til;
//	XLogRecPtr oldest_apply_lsn = polar_get_oldest_applied_lsn();
//
//	old_consist_lsn = polar_get_primary_consist_ptr();
//
//	if (XLogRecPtrIsInvalid(old_consist_lsn))
//		GetOldestRestartPoint(&old_consist_lsn, &old_til);
//
//	Assert(!XLogRecPtrIsInvalid(old_consist_lsn));
//
//	if (XLogRecPtrIsInvalid(oldest_apply_lsn) || oldest_apply_lsn < old_consist_lsn)
//		polar_set_oldest_applied_lsn(old_consist_lsn, InvalidXLogRecPtr);
//
//	bg_replayed_lsn = polar_bg_redo_get_replayed_lsn(instance);
//
//	new_bg_replayed_lsn = XLogRecPtrIsInvalid(bg_replayed_lsn) ? old_consist_lsn :
//						  Min(old_consist_lsn, bg_replayed_lsn);
//
//	polar_bg_redo_set_replayed_lsn(instance, new_bg_replayed_lsn);
//	instance->promote_oldest_lsn = new_bg_replayed_lsn;
//
//	pg_write_barrier();
//	polar_set_bg_redo_state(instance, POLAR_BG_ONLINE_PROMOTE);
//
//	SetLatch(instance->bg_worker_latch);
//
//	elog(LOG, "startup complete online promote, and reset bg_replayed_lsn=%lX, old_consist_lsn=%lX", new_bg_replayed_lsn, old_consist_lsn);
}

void
polar_online_promote_data(polar_logindex_redo_ctl_t instance, TransactionId oldest_active_xid)
{
//	XLogRecPtr last_replayed_lsn = GetXLogReplayRecPtr(NULL);
//
//	elog(LOG, "Begin function %s", __func__);
//
//	/*
//	 * POLAR: promote clog/commit_ts/multixact/csn
//	 */
//	polar_promote_clog();
//	polar_promote_commit_ts();
//
//	polar_promote_multixact_offset();
//	polar_promote_multixact_member();
//
//	if (polar_csn_enable)
//		polar_promote_csnlog(oldest_active_xid);
//
//	/* reload persisted slot from polarstore */
//	polar_reload_replication_slots_from_shared_storage();
//
//	/*
//	 * POLAR: Notice: We don't support standby connect to ro node, so we can clear xlog queue data.
//	 * For rw node xlog queue keep the latest wal meta
//	 */
//	polar_ringbuf_reset(instance->xlog_queue);
//
//	polar_logindex_update_promoted_info(instance->wal_logindex_snapshot, last_replayed_lsn);
//
//	if (instance->fullpage_logindex_snapshot)
//		polar_logindex_update_promoted_info(instance->fullpage_logindex_snapshot, last_replayed_lsn);
//
//	elog(LOG, "End function %s", __func__);
}

void
polar_standby_promote_data(polar_logindex_redo_ctl_t instance)
{
	XLogRecPtr last_replayed_lsn = GetXLogReplayRecPtr(NULL);

	elog(LOG, "Begin function %s", __func__);

	Assert(instance);

	/* release reference in xlog_queue ringbuf, which is allocated during recovery */
	polar_xlog_send_queue_release_data_ref();

	/* Clear ringbuf, otherwise left WALs will be inserted into logindex twice */
	polar_ringbuf_reset(instance->xlog_queue);

	polar_logindex_update_promoted_info(instance->wal_logindex_snapshot, last_replayed_lsn);

	if (instance->fullpage_logindex_snapshot)
		polar_logindex_update_promoted_info(instance->fullpage_logindex_snapshot, last_replayed_lsn);

	elog(LOG, "End function %s", __func__);
}

void
polar_wait_logindex_bg_stop_replay(polar_logindex_redo_ctl_t instance, Latch *latch)
{
	int rc;

	/* Wait background process handle online promote signal */
	while (polar_get_bg_redo_state(instance) != POLAR_BG_WAITING_RESET)
	{
		HandleStartupProcInterrupts();
		rc = WaitLatch(latch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   10, WAIT_EVENT_RECOVERY_APPLY_DELAY);

		if (rc & WL_POSTMASTER_DEATH)
		{
			/* no cover begin */
			exit(1);
			/* no cover end */
		}

		ResetLatch(latch);
	}

	elog(LOG, "background process handled online promote signal");
}

/*
 * POLAR: find the first XLOG_FPI/XLOG_FPI_MULTI/XLOG_FPI_FOR_HIN WAL record
 * from start_lsn to end_lsn - 1.
 *
 * start_lsn: The start lsn.
 * end_len: The end lsn.
 * tag: The buffer tag.
 * buffer: The buffer.
 * apply: apply it.
 *
 * Find the first record is XLOG_FPI/XLOG_FPI_MULTI/XLOG_FPI_FOR_HINT return true,
 * otherwise return false.
 */
XLogRecPtr
polar_logindex_find_first_fpi(polar_logindex_redo_ctl_t instance, XLogRecPtr start_lsn,
		XLogRecPtr end_lsn, BufferTag *tag, Buffer *buffer, bool apply)
{
	static XLogReaderState *state = NULL;
	log_index_lsn_t *lsn_info;
	MemoryContext oldcontext;
	XLogRecPtr lsn = InvalidXLogRecPtr;

	if (unlikely(end_lsn <= start_lsn))
	{
		/*no cover begin*/
		elog(polar_trace_logindex(DEBUG3), "The end WAL LSN %lX to find a full page image is "
				"not larger than the start LSN %lX", start_lsn, end_lsn);
		return lsn;
		/*no cover end*/
	}

	Assert(wal_page_iter == NULL);
	oldcontext = MemoryContextSwitchTo(polar_logindex_memory_context());

	/*
	 * Logindex record the start position of XLOG and we search LSN between [start_lsn, end_lsn].
	 * And end_lsn points to the end position of the last xlog, so we should subtract 1 here .
	 */
	wal_page_iter = polar_logindex_create_page_iterator(instance->wal_logindex_snapshot,
			tag, start_lsn, end_lsn - 1, polar_get_bg_redo_state(instance) == POLAR_BG_ONLINE_PROMOTE);

	if (unlikely(polar_logindex_page_iterator_state(wal_page_iter) != ITERATE_STATE_FINISHED))
	{
		/*no cover line*/
		elog(PANIC, "Failed to iterate data for " POLAR_LOG_BUFFER_TAG_FORMAT ", which start_lsn=%X/%X and end_lsn=%X/%X",
			 POLAR_LOG_BUFFER_TAG(tag),
			 (uint32)(start_lsn >> 32), (uint32)start_lsn,
			 (uint32)(end_lsn >> 32), (uint32)end_lsn);
	}

	iter_context = wal_page_iter;

	if (unlikely(state == NULL))
		state = polar_allocate_xlog_reader();

	if ((lsn_info = polar_logindex_page_iterator_next(wal_page_iter)) != NULL)
	{
		uint8 info;
		RmgrId rmid;

		polar_logindex_read_xlog(state, lsn_info->lsn);
		rmid = XLogRecGetRmid(state);
		info = XLogRecGetInfo(state) & (~XLR_INFO_MASK);

		if (rmid == RM_XLOG_ID &&
				(info == XLOG_FPI || info == XLOG_FPI_MULTI || info == XLOG_FPI_FOR_HINT))
		{
			if (apply)
				polar_logindex_apply_one_record(instance, state, tag, buffer);

			lsn = lsn_info->lsn;
		}
		else
			elog(WARNING, "The first WAL record of " POLAR_LOG_BUFFER_TAG_FORMAT ""
					"from %lx to %lx is not a full page image", POLAR_LOG_BUFFER_TAG(tag),
					start_lsn, end_lsn);
	}

	MemoryContextSwitchTo(oldcontext);
	polar_logindex_release_page_iterator(wal_page_iter);
	wal_page_iter = NULL;

	return lsn;
}
