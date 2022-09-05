/*-------------------------------------------------------------------------
 *
 * polar_bufmgr.c
 *    polardb buffer manager interface routines
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
 *    src/backend/storage/buffer/polar_bufmgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/polar_log.h"
#include "access/polar_logindex_redo.h"
#include "access/xlog.h"
#include "polar_flashback/polar_flashback_log.h"
#include "polar_flashback/polar_flashback_log_repair_page.h"
//#include "postmaster/polar_parallel_bgwriter.h"
#include "storage/polar_bufmgr.h"
#include "storage/polar_copybuf.h"
//#include "storage/polar_fd.h"
#include "storage/polar_flushlist.h"
#include "storage/polar_io_stat.h"
#include "storage/polar_pbp.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/polar_backtrace.h"

#define BUF_WRITTEN             0x01
#define STOP_PARALLEL_BGWRITER_DELAY_FACTOR 10

/*
 * POLAR: some strategies are the same as polar_delay_dml_wait()
 */
#define POLAR_ENABLE_CRASH_RECOVERY_RTO() \
	(polar_enable_shared_storage_mode && polar_is_master() && \
	!InRecovery && (MyBackendId != InvalidBackendId) && \
	 PolarGlobalIOReadStats != NULL && PolarGlobalIOReadStats->enabled)

//extern ParallelBgwriterInfo *polar_parallel_bgwriter_info;
extern void ResourceOwnerEnlargeBuffers(ResourceOwner owner);

static int        prev_sync_count = 0;
static int64      consistent_lsn_delta = 0;

static XLogRecPtr polar_cal_cur_consistent_lsn(void);
static void polar_sync_buffer_from_copy_buffer(WritebackContext *wb_context, int flags);
static void polar_start_or_stop_parallel_bgwriter(bool is_normal_bgwriter, uint64 lag);
static int evaluate_sync_buffer_num(uint64 lag);
static XLogRecPtr update_consistent_lsn_delta(XLogRecPtr cur_consistent_lsn);
static Buffer polar_bulk_read_buffer_common(Relation reln, char relpersistence, ForkNumber forkNum,
											BlockNumber firstBlockNum, ReadBufferMode mode,
											BufferAccessStrategy strategy, bool *hit,
											int maxBlockCount);

/* POLAR: for crash recovery rto */
static void polar_get_exclusive_buffer_lock_delay(void);

POLAR_PROC_GLOBAL_IO_READ *PolarGlobalIOReadStats = NULL;

static inline uint64
polar_get_max_xlog_delay_throughtput(void)
{
	return PolarGlobalIOReadStats->max_throughtput;
}

/*
 * POLAR: xlog reserver delay
 */
static void
polar_get_exclusive_buffer_lock_delay(void)
{
#define DELAY_TIMES 10

	/*
	 * current_delay_lsn: current delay lsn of checkpoint lsn and xlog flush lsn
 	 * last_delay_lsn: the last time delay lsn of checkpoint lsn and xlog flush lsn
	 */
	static XLogRecPtr last_delay_lsn = InvalidXLogRecPtr;
	static int last_delay_count = InvalidXLogRecPtr;
	int delay_times = DELAY_TIMES;
	int delay_count = 1;
	XLogRecPtr current_delay_lsn = 0;

	if (PolarGlobalIOReadStats->max_throughtput == 0)
		return ;

	current_delay_lsn = polar_get_diff_checkpoint_flush_lsn();

	if (XLogRecPtrIsInvalid(current_delay_lsn))
		return ;

	if (last_delay_lsn == InvalidXLogRecPtr)
		last_delay_lsn = current_delay_lsn;

	/*
 	 * Strategy 1: if current_delay_lsn has already execeeded the max delay throughtput, it will
	 * trigger a delay strategy in force. This will delay at most DELAY_TIMES, the delay time
	 * will be power exponent rise.
	 */
	while (current_delay_lsn > polar_get_max_xlog_delay_throughtput() && delay_times--)
	{
		elog(DEBUG5, "polar_get_exclusive_buffer_lock_delay:exceed max gap, delay for %dms", delay_count);//polar_crash_recovery_rto_delay_count * delay_count);
		pg_usleep(polar_crash_recovery_rto_delay_count * 1000 * delay_count);
		delay_count = delay_count * 2;
		PolarGlobalIOReadStats->force_delay++;
	}
	if (delay_times < DELAY_TIMES)
	{
		elog(DEBUG5, "polar_get_exclusive_buffer_lock_delay:exceed max gap, has already delay, just return");
		return ;
	}

	/*
	 * Strategy 2: if current_delay_lsn has just execeeded the given percentage of the max delay throughtput
	 * (calculate as 'the max xlog delay throughtput' * polar_crash_recovery_rto_threshold), there are three
	 * small strategies
	 */
	if (current_delay_lsn > polar_get_max_xlog_delay_throughtput()) //* polar_crash_recovery_rto_threshold)
	{
		/*
		 * Strategy 2.1: if current_delay_lsn is less than or equal as last_delay_lsn, means in the last delay strategy, it is
 		 * effective, just follow it in current delay strategy.
		 */
		if (current_delay_lsn <= last_delay_lsn)
		{
			elog(DEBUG5, "polar_get_exclusive_buffer_lock_delay:exceed threshold gap, current has less gap, delay for %dms", polar_crash_recovery_rto_delay_count * last_delay_count);
			pg_usleep(polar_crash_recovery_rto_delay_count * 1000 * last_delay_count);
			PolarGlobalIOReadStats->less_than_delay++;
		}
		/*
		 * Strategy 2.2: if current_delay_lsn is more than last_delay_lsn, means in the last delay strategy, it is
 		 * ineffective, the delay lsn of checkpoint and xlog flush has grown, so we delay as a power time.
		 */
		else if (current_delay_lsn > last_delay_lsn)
		{
			elog(DEBUG5, "polar_get_exclusive_buffer_lock_delay:exceed threshold gap, currrent has more gap, delay for %dms", polar_crash_recovery_rto_delay_count * last_delay_count);
			last_delay_count = (last_delay_count >= 1024) ? 1024 : last_delay_count * 2;
			pg_usleep(polar_crash_recovery_rto_delay_count * 1000 * last_delay_count);
			PolarGlobalIOReadStats->more_than_delay++;
		}
	}
	/*
	 * Strategy 3: Every time if it no need to delay xlog reserve, reset last_delay_count.
	 */
	else
		last_delay_count = 1;
	last_delay_lsn = polar_get_diff_checkpoint_flush_lsn();
}

/* Reset oldest lsn to invalid and remove it from flushlist. */
void
polar_reset_buffer_oldest_lsn(BufferDesc *buf_hdr)
{
	XLogRecPtr oldest_lsn = polar_buffer_get_oldest_lsn(buf_hdr);
	XLogRecPtr consistent_lsn = polar_get_consistent_lsn();

	if (XLogRecPtrIsInvalid(oldest_lsn))
	{
		Assert(buf_hdr->recently_modified_count == 0);
		/*
		 * In master, we should check all flush related flag clear,
		 * so we assert each flags.
		 */
		Assert((buf_hdr->polar_flags & POLAR_BUF_OLDEST_LSN_IS_FAKE) == 0);
		Assert((buf_hdr->polar_flags & POLAR_BUF_FIRST_TOUCHED_AFTER_COPY) == 0);
	}
	else
	{
		/* This should not happen */
		if (!XLogRecPtrIsInvalid(consistent_lsn) && oldest_lsn < consistent_lsn)
		{
			elog(WARNING, "Buffer ([%u,%u,%u], %u, %d) oldest lsn %X/%X is less than consistent lsn %X/%X",
				 buf_hdr->tag.rnode.spcNode,
				 buf_hdr->tag.rnode.dbNode,
				 buf_hdr->tag.rnode.relNode,
				 buf_hdr->tag.blockNum,
				 buf_hdr->tag.forkNum,
				 (uint32)(oldest_lsn >> 32), (uint32) oldest_lsn,
				 (uint32)(consistent_lsn >> 32), (uint32) consistent_lsn);
		}

		buf_hdr->recently_modified_count = 0;
		buf_hdr->polar_flags = 0;

		polar_remove_buffer_from_flush_list(buf_hdr);
	}
}

/*
 * polar_bg_buffer_sync - Write out some dirty buffers in the flushlist.
 *
 * This is called periodically by the background writer process.
 *
 * Returns true if it's appropriate for the bgwriter process to go into
 * low-power hibernation mode.
 */
bool
polar_bg_buffer_sync(WritebackContext *wb_context, int flags)
{
	bool       res = false;
	XLogRecPtr consistent_lsn = InvalidXLogRecPtr;

	/* Use normal BgBufferSync */
	if (!polar_enable_shared_storage_mode || polar_in_replica_mode() ||
			polar_get_bg_redo_state(polar_logindex_redo_instance) == POLAR_BG_WAITING_RESET)
		return BgBufferSync(wb_context);

	/*
	 * For polardb, it should enable the flush list, otherwise, the consistent
	 * lsn always is invalid.
	 */
	if (polar_flush_list_enabled())
	{
		if (polar_enable_normal_bgwriter)
			res = BgBufferSync(wb_context);

		res = polar_buffer_sync(wb_context, &consistent_lsn, true, flags) || res;
	}
	else
		res = BgBufferSync(wb_context);

	polar_set_consistent_lsn(consistent_lsn);

	return res;
}

/* Whether we should control the vm buffer to flush. */
bool
polar_donot_control_vm_buffer(BufferDesc *buf_hdr)
{
	Assert(buf_hdr != NULL);
	return buf_hdr->tag.forkNum == VISIBILITYMAP_FORKNUM &&
		   !polar_enable_control_vm_flush;
}

/*
 * Calculate current consistent lsn, it is the minimum between first buffer of
 * flush list and all copy buffers. If oldest lsn of all buffers are invalid,
 * we use current insert RecPtr, because some commands generate wal records
 * but do not set any buffers oldest lsn.
 */
static XLogRecPtr
polar_cal_cur_consistent_lsn(void)
{
	BufferDesc *buf;
	XLogRecPtr  clsn;
	XLogRecPtr  lsn;

	Assert(polar_flush_list_enabled());

	SpinLockAcquire(&polar_flush_list_ctl->flushlist_lock);

	if (polar_flush_list_is_empty())
	{
		if (unlikely(polar_bg_redo_state_is_parallel(polar_logindex_redo_instance)))
			lsn = polar_online_promote_fake_oldest_lsn();
		else
			lsn = polar_max_valid_lsn();

		SpinLockRelease(&polar_flush_list_ctl->flushlist_lock);

		if (polar_enable_debug)
			elog(DEBUG1,
				 "The flush list is empty, so use current insert lsn %X/%X as consistent lsn.",
				 (uint32)(lsn >> 32),
				 (uint32) lsn);

		return lsn;
	}

	Assert(polar_flush_list_ctl->first_flush_buffer >= 0);

	buf = GetBufferDescriptor(polar_flush_list_ctl->first_flush_buffer);
	lsn = pg_atomic_read_u64((pg_atomic_uint64 *) &buf->oldest_lsn);

	SpinLockRelease(&polar_flush_list_ctl->flushlist_lock);

	clsn = polar_copy_buffers_get_oldest_lsn();

	if (!XLogRecPtrIsInvalid(clsn))
		lsn = Min(lsn, clsn);

	return lsn;
}

bool
polar_buffer_can_be_flushed(BufferDesc *buf_hdr,
							XLogRecPtr oldest_apply_lsn,
							bool use_cbuf)
{
	uint32              buf_state;
	bool                res = false;
	XLogRecPtr          latest_lsn = BufferGetLSN(buf_hdr);
	CopyBufferDesc      *cbuf = NULL;

	if (!polar_enable_shared_storage_mode ||
			XLogRecPtrIsInvalid(oldest_apply_lsn) ||
			(latest_lsn <= oldest_apply_lsn) ||
			(buf_hdr->tag.forkNum == FSM_FORKNUM) ||
			(buf_hdr->tag.forkNum == INIT_FORKNUM) ||
			(polar_donot_control_vm_buffer(buf_hdr)))
		return true;
	else if (use_cbuf)
	{
		buf_state = LockBufHdr(buf_hdr);
		cbuf = buf_hdr->copy_buffer;

		if (cbuf)
			res = polar_copy_buffer_get_lsn(cbuf) <= oldest_apply_lsn;

		UnlockBufHdr(buf_hdr, buf_state);

		if (cbuf && !res)
			pg_atomic_add_fetch_u32(&cbuf->pass_count, 1);
	}

	return res;
}

/*
 * For lazy checkpoint, we only flush buffers that background writer does not
 * flush; for normal checkpoint, we check whether the buffer can be flushed,
 * buffers that can be flushed will be flushed by checkpoint.
 */
bool
polar_buffer_can_be_flushed_by_checkpoint(BufferDesc *buf_hdr,
										  XLogRecPtr oldest_apply_lsn,
										  int flags)
{
	if (flags & CHECKPOINT_LAZY)
	{
		if (buf_hdr->tag.forkNum == FSM_FORKNUM ||
				buf_hdr->tag.forkNum == INIT_FORKNUM)
			return true;
		else
			return false;
	}
	else
		return polar_buffer_can_be_flushed(buf_hdr, oldest_apply_lsn, false);
}

/* polar_buffer_sync - Sync buffers that are getting from flushlist.
 *
 * Returns true if it's appropriate for the bgwriter process to go into
 * low-power hibernation mode.
 */
bool
polar_buffer_sync(WritebackContext *wb_context,
				  XLogRecPtr *consistent_lsn,
				  bool is_normal_bgwriter,
				  int flags)
{
	static int       *batch_buf = NULL;

	XLogRecPtr cur_consistent_lsn;
	XLogRecPtr oldest_apply_lsn;
	int        num_written = 0;
	int        num_to_sync = 0;
	int        sync_count = 0;
	uint64     sleep_lag = 0;
	uint64     lag = 0;

	if (batch_buf == NULL)
		/* This is a static variable, do not need to free it explicitly. */
		batch_buf = (int *) malloc(polar_bgwriter_batch_size_flushlist * sizeof(int));

	cur_consistent_lsn = polar_get_consistent_lsn();
	sleep_lag = polar_bgwriter_sleep_lsn_lag * 1024 * 1024L;
	oldest_apply_lsn = polar_get_oldest_applied_lsn();

	if (XLogRecPtrIsInvalid(oldest_apply_lsn))
		lag = polar_max_valid_lsn() - cur_consistent_lsn;
	/* POLAR: when enable fullpage snapshot, we don't care about oledest_applied_lsn anymore */
	else if (POLAR_LOGINDEX_ENABLE_FULLPAGE())
	{
		uint64  consistent_lsn_lag = polar_max_valid_lsn() - cur_consistent_lsn;

		/* If consistent_lsn is too old, start to trigger to write fullpage */
		if (consistent_lsn_lag > polar_fullpage_snapshot_oldest_lsn_delay_threshold)
			lag = consistent_lsn_lag - polar_fullpage_snapshot_oldest_lsn_delay_threshold;
		else
			lag = 0;
	}
	else
		lag = oldest_apply_lsn > cur_consistent_lsn ? oldest_apply_lsn - cur_consistent_lsn : 0;

	/* Normal background writer will create or close some parallel background workers. */
	polar_start_or_stop_parallel_bgwriter(is_normal_bgwriter, lag);

	/* Evaluate the number of buffers to sync. */
	num_to_sync = evaluate_sync_buffer_num(lag);

	/* Only normal bgwriter sync buffers from the copy buffer. */
	if (is_normal_bgwriter)
		polar_sync_buffer_from_copy_buffer(wb_context, flags);

	if (polar_enable_debug)
		elog(DEBUG1, "Try to get %d buffers to flush from flushlist", num_to_sync);

	/* Make sure we can handle the pin inside SyncOneBuffer */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	while (sync_count < num_to_sync)
	{
		int num = 0;
		int i = 0;

		/* Get buffers from flush list */
		num = polar_get_batch_flush_buffer(batch_buf);

		if (num == 0 || batch_buf == NULL)
			break;

		/* Sync buffers */
		while (i < num)
		{
			int sync_state = 0;

			sync_state = SyncOneBuffer(batch_buf[i],
									   false,
									   wb_context);
			i++;

			if (sync_state & BUF_WRITTEN)
				num_written++;
		}

		sync_count += num;
	}

	BgWriterStats.m_buf_written_clean += num_written;

	/*
	 * Reset StrategyControl->numBufferAllocs and report buffer alloc counts to pgstat.
	 *
	 * If BgBufferSync() is enabled, we can't do it. We should let BgBufferSync() handle it.
	 * Because BgBufferSync() use StrategyControl->numBufferAllocs as an important parameter
	 * to calculate the counter of pages to be scanned/flushed.
	 */
	if (!polar_enable_normal_bgwriter)
	{
		uint32     recent_alloc = 0;
		StrategySyncStart(NULL, &recent_alloc);
		BgWriterStats.m_buf_alloc += recent_alloc;
	}

	*consistent_lsn = update_consistent_lsn_delta(cur_consistent_lsn);

	return lag < sleep_lag;
}

static XLogRecPtr
update_consistent_lsn_delta(XLogRecPtr cur_consistent_lsn)
{
	XLogRecPtr next_consistent_lsn;

	if (polar_logindex_redo_instance)
	{
		if (unlikely(polar_get_bg_redo_state(polar_logindex_redo_instance) == POLAR_BG_WAITING_RESET))
		{
			/*
			 * During online promote when node is in rw mode, the logindex background state could be POLAR_BG_WAITING_RESET or POLAR_BG_ONLINE_PROMOTE.
			 * If logindex background state is POLAR_BG_WAITING_RESET then we can not accept connection
			 * from the client and the dirty buffer flush list is empty.
			 * So we don't update consistent lsn when the state is POLAR_BG_WAITING_RESET.
			 */
			return cur_consistent_lsn;
		}
	}

	next_consistent_lsn = polar_cal_cur_consistent_lsn();
	consistent_lsn_delta = next_consistent_lsn - cur_consistent_lsn;

	if (consistent_lsn_delta < 0)
	{
		if (polar_logindex_redo_instance
				&& (XLogRecPtrIsInvalid(polar_logindex_redo_instance->xlog_replay_from)
					|| next_consistent_lsn < polar_logindex_redo_instance->xlog_replay_from))
		{
			/*
			 * BgWriter may start before logindex is initialized, and then it set consistent lsn as the checkpoint redo position.
			 * When logindex is initialized, it may change to replay from previsiou lsn, which is smaller than checkpoint redo lsn.
			 * Before xlog_replay_from we only insert record to logindex table and don't replay page buffer, so there's no dirty
			 * buffer in flush list. But we may set lastReplayedEndRecPtr as next consistent lsn, which is smaller than previous
			 * calculated consistent lsn. We ignore new calculated value in this case.
			 */
			consistent_lsn_delta = 0;
			return cur_consistent_lsn;
		}

		elog(PANIC,
			 "Current consistent lsn %X/%X is great than next consistent lsn %X/%X",
			 (uint32)(cur_consistent_lsn >> 32),
			 (uint32) cur_consistent_lsn,
			 (uint32)(next_consistent_lsn >> 32),
			 (uint32) next_consistent_lsn);
	}

	return next_consistent_lsn;
}

/* Evaluate the number of buffers to sync. */
static int
evaluate_sync_buffer_num(uint64 lag)
{
	int        num_to_sync = 0;
	double     sync_per_lsn;
	uint64     max_lag = 0;
	double     cons_delta_per_worker;
	int        current_workers = 0;

	if (polar_parallel_bgwriter_enabled())
		;//current_workers = CURRENT_PARALLEL_WORKERS;

	cons_delta_per_worker = consistent_lsn_delta / (current_workers + 1);

	sync_per_lsn = (double) prev_sync_count / (cons_delta_per_worker + 1);

	/* Avoid overflow */
	max_lag = polar_bgwriter_max_batch_size / (sync_per_lsn + 1);

	if (lag > max_lag)
		num_to_sync = polar_bgwriter_max_batch_size;
	else
		num_to_sync = lag * sync_per_lsn;

	Assert(num_to_sync >= 0);

	if (num_to_sync == 0)
		num_to_sync = polar_bgwriter_batch_size_flushlist / 10 + 1;

	prev_sync_count = num_to_sync;

	return num_to_sync;
}

/* Normal background writer can start or stop some parallel background workers. 
static void
polar_start_or_stop_parallel_bgwriter(bool is_normal_bgwriter, uint64 lag)
{
	static TimestampTz last_check_start_tz = 0;
	static TimestampTz last_check_stop_tz = 0;

	bool        ok;
	int         at_most_workers;
	int         current_workers = 0;

	if (!is_normal_bgwriter || RecoveryInProgress() || lag <= 0 ||
			!polar_parallel_bgwriter_enabled() ||
			!polar_parallel_bgwriter_enable_dynamic)
		return;

	/*
	 * The consistent lsn does not be updated. It may be unhelpful to add or
	 * stop parallel writers, so we do nothing.
	 *
	if (consistent_lsn_delta == 0)
	{
		if (polar_enable_debug)
		{
			XLogRecPtr  consistent_lsn = polar_get_consistent_lsn();
			elog(LOG,
				 "Consistent lsn does not update, current consistent lsn is %X/%X",
				 (uint32)(consistent_lsn >> 32),
				 (uint32) consistent_lsn);
		}

		return;
	}

	Assert(consistent_lsn_delta > 0);
	current_workers = CURRENT_PARALLEL_WORKERS;
	at_most_workers = POLAR_MAX_BGWRITER_WORKERS;

	/* Try to start one parallel background writer *
	if (lag > (polar_parallel_new_bgwriter_threshold_lag * 1024 * 1024L))
	{
		/* Reset the last check stop timestamp *
		last_check_stop_tz = 0;

		if (current_workers >= at_most_workers)
		{
			if (polar_enable_debug)
				elog(LOG,
					 "Can not start new parallel background workers, current workers: %d, at most workers %d",
					 current_workers, at_most_workers);

			return;
		}

		if (last_check_start_tz == 0)
		{
			last_check_start_tz = GetCurrentTimestamp();
			return;
		}

		ok = TimestampDifferenceExceeds(last_check_start_tz,
										GetCurrentTimestamp(),
										polar_parallel_new_bgwriter_threshold_time * 1000L);

		/* Once start one *
		if (ok)
		{
			polar_register_parallel_bgwriter_workers(1, true);
			last_check_start_tz = 0;
		}
	}

	/* Try to stop one parallel background writer *
	else if (current_workers > polar_parallel_bgwriter_workers)
	{
		/* Reset the last check start timestamp *
		last_check_start_tz = 0;

		if (last_check_stop_tz == 0)
		{
			last_check_stop_tz = GetCurrentTimestamp();
			return;
		}

		/* Stop a background writer slowly, so wait more time. *
		ok = TimestampDifferenceExceeds(last_check_stop_tz,
										GetCurrentTimestamp(),
										polar_parallel_new_bgwriter_threshold_time *
										1000L * STOP_PARALLEL_BGWRITER_DELAY_FACTOR);

		/* Once stop one *
		if (ok)
		{
			polar_shutdown_parallel_bgwriter_workers(1);
			last_check_stop_tz = 0;
		}
	}
}
*/

/*
 * polar_redo_set_buffer_oldest_lsn - Set the buffer oldest lsn when redo.
 *
 * When redo, we have a valid record, so we set buffer oldest lsn to
 * record->ReadRecPtr. Do not use record->EndRecPtr, because oldest lsn
 * is always the start of wal not the end.
 */
void
polar_redo_set_buffer_oldest_lsn(Buffer buffer, XLogRecPtr lsn)
{
	BufferDesc *buf_hdr;
	XLogRecPtr  oldest_lsn;

	if (!polar_flush_list_enabled())
		return;

	if (!BufferIsValid(buffer))
		elog(ERROR, "bad buffer ID: %d", buffer);

	buf_hdr = GetBufferDescriptor(buffer - 1);

	if (XLogRecPtrIsInvalid(lsn))
	{
		POLAR_LOG_BUFFER_DESC(buf_hdr);
		elog(PANIC, "Set invalid oldest lsn.");
	}

	Assert(BufferIsPinned(buffer));
	/* POLAR: If ro promoted to be rw, then we will mark buffer dirty when read buffer from storage and replay xlog */
	Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf_hdr),
								LW_EXCLUSIVE)
		   || LWLockHeldByMeInMode(BufferDescriptorGetIOLock(buf_hdr), LW_EXCLUSIVE));

	oldest_lsn = polar_buffer_get_oldest_lsn(buf_hdr);

	/*
	 * For standby or master in recovery, it always call this function to set
	 * a valid oldest lsn.
	 */
	if (XLogRecPtrIsInvalid(oldest_lsn))
		polar_put_buffer_to_flush_list(buf_hdr, lsn);

	buf_hdr->recently_modified_count++;
}

/*
 * polar_set_buffer_fake_oldest_lsn - Set a fake oldest_lsn for the buffer.
 *
 * Since generating lsn and inserting buffer to flushlist is not atomic, when
 * call MarkBufferDirty, we set a fake lsn for buffer and put it into flushlist.
 * The fake lsn is from GetXLogInsertRecPtr, it is incremental.
 */
void
polar_set_buffer_fake_oldest_lsn(BufferDesc *buf_hdr)
{
	XLogRecPtr  oldest_lsn;
	bool        set = false;

	/*
	 * If server is in recovery, the buffer should be set a real lsn, so do not
	 * need to set a fake.
	 */
	if (!polar_flush_list_enabled() ||
		(RecoveryInProgress() &&
		 !polar_bg_redo_state_is_parallel(polar_logindex_redo_instance)))
		return;

	Assert(BufferIsPinned(BufferDescriptorGetBuffer(buf_hdr)));

	/*
	 * POLAR: If ro promoted to be rw, then we will mark buffer dirty when read
	 * buffer from storage and replay xlog.
	 */
	Assert(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf_hdr),
								LW_EXCLUSIVE)
		   || LWLockHeldByMeInMode(BufferDescriptorGetIOLock(buf_hdr), LW_EXCLUSIVE));

	oldest_lsn = polar_buffer_get_oldest_lsn(buf_hdr);

	/*
	 * If oldest lsn is invalid, set a fake one and put buffer into flush list.
	 * If already has a valid oldest lsn, and it is first touched after copy,
	 * we also set one new fake oldest lsn.
	 */
	if (XLogRecPtrIsInvalid(oldest_lsn))
	{
		polar_put_buffer_to_flush_list(buf_hdr, InvalidXLogRecPtr);
		buf_hdr->polar_flags |= POLAR_BUF_OLDEST_LSN_IS_FAKE;
		set = true;
	}
	else if (polar_buffer_first_touch_after_copy(buf_hdr))
	{
		/* Adjust its position in the flush list */
		polar_adjust_position_in_flush_list(buf_hdr);

		/* First touch after copy done */
		buf_hdr->polar_flags |= POLAR_BUF_FIRST_TOUCHED_AFTER_COPY;
		set = true;
	}

	if (unlikely(polar_trace_logindex_messages <= DEBUG3) && set)
		POLAR_LOG_BUFFER_DESC(buf_hdr);

	buf_hdr->recently_modified_count++;
}

static void
polar_sync_buffer_from_copy_buffer(WritebackContext *wb_context, int flags)
{
	int            i;
	CopyBufferDesc *cbuf;
	int32          buf_id;

	if (!polar_enable_copy_buffer)
		return;

	/* Make sure we can handle the pin inside SyncOneBuffer */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	for (i = 0; i < polar_copy_buffers; i++)
	{
		cbuf = polar_get_copy_buffer_descriptor(i);
		buf_id = pg_atomic_read_u32((pg_atomic_uint32 *) &cbuf->origin_buffer) - 1;

		if (buf_id < 0)
			continue;

		/*
		 * SyncOneBuffer try to flush the original buffer, if it can be flushed,
		 * flush it and free its copy buffer, otherwise, flush its copy buffer.
		 */
		SyncOneBuffer(buf_id,
					  false,
					  wb_context);
	}
}

/*
 * POLAR: An extended version LockBuffer. It can detect outdate status
 * before obtaining the lock.
 *
 * If fresh_check is True, it will try to check flag of corresponding buffer
 * descriptor and do replay, otherwise it will do what the origin one do.
 */
void
polar_lock_buffer_ext(Buffer buffer, int mode, bool fresh_check)
{
	BufferDesc  *buf_desc;

	Assert(BufferIsValid(buffer));

	if (BufferIsLocal(buffer))
		return;                 /* local buffers need no lock */

	buf_desc = GetBufferDescriptor(buffer - 1);

	do
	{
		if (mode == BUFFER_LOCK_UNLOCK)
		{
			/* POLAR: clear unrecoverable flag safe now */
			polar_clear_buffer_unrecoverable_flag(buf_desc);

			LWLockRelease(BufferDescriptorGetContentLock(buf_desc));

			return;
		}
		else if (mode == BUFFER_LOCK_SHARE)
			LWLockAcquire(BufferDescriptorGetContentLock(buf_desc), LW_SHARED);
		else if (mode == BUFFER_LOCK_EXCLUSIVE)
		{
			bool	need_origin_buffer = !fresh_check && !RecoveryInProgress() && polar_is_flog_enabled(flog_instance);

			/* POLAR: check if we should delay xlog reserve insert for crash recovery rto */
			if (POLAR_ENABLE_CRASH_RECOVERY_RTO())
				polar_get_exclusive_buffer_lock_delay();

			LWLockAcquire(BufferDescriptorGetContentLock(buf_desc), LW_EXCLUSIVE);

			/* Add the page to origin buffer */
			if (need_origin_buffer &&
					polar_is_flog_needed(flog_instance, polar_logindex_redo_instance,
					buf_desc->tag.forkNum, BufHdrGetBlock(buf_desc),
					pg_atomic_read_u32(&buf_desc->state) & BM_PERMANENT,
					InvalidXLogRecPtr, InvalidXLogRecPtr))
			{
				polar_add_origin_buf(flog_instance->list_ctl, buf_desc);
			}

			/*
			 * POLAR: buffer maybe dirty later, mark buffer unrecoverable,
			 * avoid to reuse
			 */
			polar_mark_buffer_unrecoverable_flag(buf_desc);
		}
		else
		{
			elog(ERROR, "unrecognized buffer lock mode: %d", mode);
			return;
		}

		/* Is this fresh buffer? */
		if (!fresh_check || !polar_redo_check_state(buf_desc, POLAR_REDO_OUTDATE))
			break;

		switch (mode)
		{
			case BUFFER_LOCK_SHARE:
				/* release s-lock and acquire x-lock for redo */
				LWLockRelease(BufferDescriptorGetContentLock(buf_desc));
				LWLockAcquire(BufferDescriptorGetContentLock(buf_desc), LW_EXCLUSIVE);
				break;

			case BUFFER_LOCK_EXCLUSIVE:
				break;

			default:
				elog(ERROR, "unrecognized buffer lock mode: %d", mode);
				return;
		}

		/*
		 * Now, we hold exclusive lock. Because we released lock
		 * while BUFFER_LOCK_SHARE mode, so we should re-check
		 * buffer outdate state.
		 */
		polar_logindex_lock_apply_buffer(polar_logindex_redo_instance, &buffer);

		switch (mode)
		{
			case BUFFER_LOCK_SHARE:
				/*
				 * target lock mode is shared one, so we release exclusive lock
				 * and try to hold shared one
				 */
				LWLockRelease(BufferDescriptorGetContentLock(buf_desc));

				continue;

			case BUFFER_LOCK_EXCLUSIVE:
				/* target lock mode is exclusive, so we are done here, just return. */
				return;

			default:
				elog(ERROR, "unrecognized buffer lock mode: %d", mode);
				return;
		}
	}
	while (true);
}

/*
 * POLAR: An extended version ConditionalLockBuffer. It can detect outdate status
 * before obtaining the lock.
 *
 * If fresh_check is True, it will try to check flag of corresponding buffer
 * descriptor and redo the buffer, otherwise it will do what the origin one do.
 */
bool
polar_conditional_lock_buffer_ext(Buffer buffer, bool fresh_check)
{
	BufferDesc  *buf_desc;
	bool        result;

	Assert(BufferIsValid(buffer));

	if (BufferIsLocal(buffer))
		return true;            /* act as though we got it */

	buf_desc = GetBufferDescriptor(buffer - 1);

	result = LWLockConditionalAcquire(BufferDescriptorGetContentLock(buf_desc),
									  LW_EXCLUSIVE);

	/* POLAR: if lock successfully, mark unrecoverable flag now */
	if (result)
		polar_mark_buffer_unrecoverable_flag(buf_desc);

	if (fresh_check && result)
		polar_logindex_lock_apply_buffer(polar_logindex_redo_instance, &buffer);

	return result;
}

bool
polar_pin_buffer(BufferDesc *buf_desc, BufferAccessStrategy strategy)
{
	return PinBuffer(buf_desc, strategy);
}


Buffer
polar_bulk_read_buffer_extended(Relation reln, ForkNumber forkNum, BlockNumber blockNum,
								ReadBufferMode mode, BufferAccessStrategy strategy,
								int maxBlockCount)
{
	bool        hit;
	Buffer      buf;

	/* Open it at the smgr level if not already done */
	RelationOpenSmgr(reln);

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(reln))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	/*
	 * Read the buffer, and update pgstat counters to reflect a cache hit or
	 * miss.
	 */
	pgstat_count_buffer_read(reln);
	buf = polar_bulk_read_buffer_common(reln, reln->rd_rel->relpersistence,
										forkNum, blockNum, mode, strategy, &hit, maxBlockCount);

	if (hit)
		pgstat_count_buffer_hit(reln);

	return buf;
}

/*
 * polar_bulk_read_buffer_common -- common logic for bulk read multi buffer one time.
 *
 * Try to read as many buffers as possible, up to maxBlockCount.
 * If first block hits shared_buffers, return immediately.
 * IF first block doesn't hit shared_buffer,
 * read buffers until up to maxBlockCount or
 * up to first block which hit shared_buffer.
 *
 * Returns: the buffer number for the first block blockNum.
 *      The returned buffer has been pinned.
 *      Does not return on error --- elog's instead.
 *
 * *hit is set to true if the first blockNum was satisfied from shared buffer cache.
 *
 * Deadlock avoidance：Only ascending bulk read are allowed
 * to avoid dead lock on io_in_progress_lock.
 */
static Buffer
polar_bulk_read_buffer_common(Relation reln, char relpersistence, ForkNumber forkNum,
							  BlockNumber firstBlockNum, ReadBufferMode mode,
							  BufferAccessStrategy strategy, bool *hit,
							  int maxBlockCount)
{
	SMgrRelation smgr = reln->rd_smgr;
	BufferDesc *bufHdr;
	BufferDesc *first_buf_hdr;
	Block       bufBlock;
	bool        found;
	bool        isLocalBuf = SmgrIsTemp(smgr);
	int         actual_bulk_io_count;
	int         index;
	char        *buf_read;
	char        *aligned_buf_read;
	int         remaining_lwlock_slot_count;
	XLogRecPtr  checkpoint_redo_lsn = InvalidXLogRecPtr;
	XLogRecPtr  replay_from;
	polar_redo_action       redo_action;
	int         repeat_read_times = 0;
	bool        *checksum_fail;

	/* POLAR: make sure that buffer pool has been inited */
	if (!polar_buffer_pool_is_inited)
		elog(PANIC, "buffer pool is not inited");

	/* POLAR end */

	polar_pgstat_count_bulk_read_calls(reln);

	maxBlockCount = Min(maxBlockCount, polar_bulk_read_size);

	if (firstBlockNum == P_NEW ||
			maxBlockCount <= 1 ||
			mode != RBM_NORMAL)
	{
		return ReadBuffer_common(smgr, relpersistence, forkNum, firstBlockNum,
								 mode, strategy, hit);
	}

	Assert(!polar_bulk_io_is_in_progress);
	Assert(0 == polar_bulk_io_in_progress_count);

	/* bulk read begin */
	polar_bulk_io_is_in_progress = true;

	/*
	 * Alloc buffer for polar_bulk_io_in_progress_buf and polar_bulk_io_is_for_input on demand.
	 * If bulk read is called once, there is a great possibility that bulk read will be called later.
	 * polar_bulk_io_in_progress_buf and polar_bulk_io_is_for_input will be not freed, until backend exit.
	 */
	if (NULL == polar_bulk_io_in_progress_buf)
	{
		Assert(NULL == polar_bulk_io_is_for_input);
		polar_bulk_io_in_progress_buf = MemoryContextAlloc(TopMemoryContext,
														   64 * sizeof(polar_bulk_io_in_progress_buf[0]));
		polar_bulk_io_is_for_input = MemoryContextAlloc(TopMemoryContext,
														64 * sizeof(polar_bulk_io_is_for_input[0]));
	}

	*hit = false;

	/* Make sure we will have room to remember the buffer pin */
	ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

	TRACE_POSTGRESQL_BUFFER_READ_START(forkNum, firstBlockNum,
									   smgr->smgr_rnode.node.spcNode,
									   smgr->smgr_rnode.node.dbNode,
									   smgr->smgr_rnode.node.relNode,
									   smgr->smgr_rnode.backend,
									   false); /* false == isExtend */

	if (isLocalBuf)
	{
		bufHdr = LocalBufferAlloc(smgr, forkNum, firstBlockNum, &found);

		if (found)
			pgBufferUsage.local_blks_hit++;
		else
			pgBufferUsage.local_blks_read++;
	}
	else
	{
		/*
		* lookup the buffer.  IO_IN_PROGRESS is set if the requested block is
		* not currently in memory.
		* If not found, polar_bulk_io_in_progress_count will be added by 1.
		*/
		bufHdr = BufferAlloc(smgr, relpersistence, forkNum, firstBlockNum,
							 strategy, &found);

		if (found)
			pgBufferUsage.shared_blks_hit++;
		else
			pgBufferUsage.shared_blks_read++;
	}

	/* At this point we do NOT hold any locks. */

	/* if it was already in the buffer pool, we're done */
	if (found)
	{
		/* Just need to update stats before we exit */
		*hit = true;
		VacuumPageHit++;

		if (VacuumCostActive)
			VacuumCostBalance += VacuumCostPageHit;

		TRACE_POSTGRESQL_BUFFER_READ_DONE(forkNum, firstBlockNum,
										  smgr->smgr_rnode.node.spcNode,
										  smgr->smgr_rnode.node.dbNode,
										  smgr->smgr_rnode.node.relNode,
										  smgr->smgr_rnode.backend,
										  isExtend,
										  found);

		Assert(0 == polar_bulk_io_in_progress_count);
		/* important, mark bulk_io end */
		polar_bulk_io_is_in_progress = false;

		return BufferDescriptorGetBuffer(bufHdr);
	}

	/*
	 * if we have gotten to this point, we have allocated a buffer for the
	 * page but its contents are not yet valid.  IO_IN_PROGRESS is set for it,
	 * if it's a shared buffer.
	 *
	 * Note: if smgrextend fails, we will end up with a buffer that is
	 * allocated but not marked BM_VALID.  P_NEW will still select the same
	 * block number (because the relation didn't get any longer on disk) and
	 * so future attempts to extend the relation will find the same buffer (if
	 * it's not been recycled) but come right back here to try smgrextend
	 * again.
	 */
	Assert(!(pg_atomic_read_u32(&bufHdr->state) & BM_VALID));   /* spinlock not needed */

	Assert(1 == polar_bulk_io_in_progress_count);
	Assert(bufHdr == polar_bulk_io_in_progress_buf[polar_bulk_io_in_progress_count - 1]);

	/*
	 * Hold the first block bufHdr,
	 * after TerminateBufferIO(), polar_bulk_io_in_progress_buf is freed.
	 */
	first_buf_hdr = bufHdr;

	/*
	 * Make sure than single bulk_read will not read blocks across files.
	 *
	 * (BlockNumber) RELSEG_SIZE - (blockNum % (BlockNumber) RELSEG_SIZE) always >= 1, and maxBlockCount always >= 1,
	 * so finaly maxBlockCount always >= 1.
	 */
	maxBlockCount = Min(maxBlockCount, (BlockNumber) RELSEG_SIZE - (firstBlockNum % (BlockNumber) RELSEG_SIZE));
	/*
	 * avoid lwlock count overflow.
	 * -2 : for temporary use of buf_mapping_partition_lock, content_lock&io_in_progress_lock(for flushing evicted dirty page)
	 *      during multi BufferAlloc().
	 *
	 * if remaining_lwlock_slot_count < 0, then maxBlockCount < 0, no bulk read, just ok.
	 */
	remaining_lwlock_slot_count = polar_remaining_lwlock_slot_count() - 2;
	maxBlockCount = Min(maxBlockCount, remaining_lwlock_slot_count);

	for (index = 1; index < maxBlockCount; index ++)
	{
		BlockNumber blockNum = firstBlockNum + index;

		/* Make sure we will have room to remember the buffer pin */
		ResourceOwnerEnlargeBuffers(CurrentResourceOwner);

		/*
		 * lookup the buffer.  IO_IN_PROGRESS is set if the requested block is
		 * not currently in memory.
		 *
		 * If not found, polar_bulk_io_in_progress_count will be added by 1 by StartBufferIO().
		 */
		if (isLocalBuf)
			bufHdr = LocalBufferAlloc(smgr, forkNum, blockNum, &found);
		else
			bufHdr = BufferAlloc(smgr, relpersistence, forkNum, blockNum,
								 strategy, &found);

		/* For extra block, don't update pgBufferUsage.shared_blks_hit or pgBufferUsage.shared_blks_read */
		/* bufHdr == NULL, all buffers are pinned. */
		if (found || bufHdr == NULL)
		{
			/* important: this buffer is the upper boundary, it should be excluded. */
			if (bufHdr != NULL)
				ReleaseBuffer(BufferDescriptorGetBuffer(bufHdr));

			break;
		}

		Assert(!(pg_atomic_read_u32(&bufHdr->state) & BM_VALID));   /* spinlock not needed */
	}

	Assert(index == polar_bulk_io_in_progress_count);

	/*
	 * Until now, as to {blockNum + [0, polar_bulk_io_in_progress_count)} block buffers,
	 * IO_IN_PROGRESS flag is set and io_in_progress_lock is holded.
	 *
	 * Other proc(include backend sql exec、start xlog replay) which read there buffers,
	 * would be blocked on io_in_progress_lock.
	 *
	 * Deadlock avoidance：Only ascending bulk read are allowed to avoid dead lock on io_in_progress_lock.
	 */

	/*
	 * polar_bulk_io_in_progress_count will be reduced by TerminateBufferIO(),
	 * For safety, its copy actual_bulk_io_count is used.
	 */
	actual_bulk_io_count = polar_bulk_io_in_progress_count;

	/* for eliminating palloc and memcpy */
	if (1 == actual_bulk_io_count)
		buf_read = isLocalBuf ?
				   (char *)LocalBufHdrGetBlock(first_buf_hdr) :
				   (char *)BufHdrGetBlock(first_buf_hdr);
	else
		buf_read = (char *) palloc(polar_enable_buffer_alignment ?
								   POLAR_BUFFER_EXTEND_SIZE(actual_bulk_io_count * BLCKSZ) :
								   actual_bulk_io_count * BLCKSZ);

	if (polar_enable_buffer_alignment)
		aligned_buf_read = (char *) POLAR_BUFFER_ALIGN(buf_read);
	else
		aligned_buf_read = buf_read;

	checksum_fail = (bool *)palloc0(actual_bulk_io_count * sizeof(bool));

repeat_read:
	/*
	 * POLAR: page-replay.
	 *
	 * Get consistent and redo lsn which used by replay.
	 *
	 * Note: All modifications about replay-page must be
	 *       applied to both polar_bulk_read_buffer_common() and ReadBuffer_common().
	 */
	redo_action = polar_require_backend_redo(isLocalBuf, mode, forkNum, &replay_from);
	if (redo_action != POLAR_REDO_NO_ACTION)
	{
		checkpoint_redo_lsn = GetRedoRecPtr();
		Assert(!XLogRecPtrIsInvalid(checkpoint_redo_lsn));
	}
	/* POLAR end */

	/*
	 * Read in the page, unless the caller intends to overwrite it and
	 * just wants us to allocate a buffer.
	 */
	{
		instr_time  io_start,
					io_time;

		if (track_io_timing)
			INSTR_TIME_SET_CURRENT(io_start);

		polar_smgrbulkread(smgr, forkNum, firstBlockNum, actual_bulk_io_count, aligned_buf_read);

		if (track_io_timing)
		{
			INSTR_TIME_SET_CURRENT(io_time);
			INSTR_TIME_SUBTRACT(io_time, io_start);
			pgstat_count_buffer_read_time(INSTR_TIME_GET_MICROSEC(io_time));
			INSTR_TIME_ADD(pgBufferUsage.blk_read_time, io_time);
		}

		for (index = 0; index < actual_bulk_io_count; index++)
		{
			BlockNumber blockNum = firstBlockNum + index;
			bufBlock = (Block)(aligned_buf_read + index * BLCKSZ);

			/* check for garbage data */
			if (!PageIsVerified((Page) bufBlock, blockNum))
			{
				if (zero_damaged_pages)
				{
					char *relpath = relpath(smgr->smgr_rnode, forkNum);
					ereport(WARNING,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("invalid page in block %u of relation %s; zeroing out page",
									blockNum, relpath)));
					MemSet((char *) bufBlock, 0, BLCKSZ);
					pfree(relpath);
				}
				else if (polar_has_partial_write && polar_in_replica_mode())
				{
					/*
					 * POLAR: If filesystem doesn't support atomic wirte. rw could write part of the page while ro read this page and failed to verify checksum.
					 * So in replica mode we will delay some time and read this page again
					 */
					repeat_read_times++;

					if (repeat_read_times % POLAR_WARNING_REPEAT_TIMES == 0)
					{
						char *relpath = relpath(smgr->smgr_rnode, forkNum);
						ereport(WARNING, (errmsg("%d times to repeat read from block %u of relation %s",
												 repeat_read_times, firstBlockNum, relpath)));
						pfree(relpath);

						CHECK_FOR_INTERRUPTS();
					}

					POLAR_DELAY_REPEAT_READ();
					POLAR_REPEAT_READ();
				}
				else if (fullPageWrites && (redo_action != POLAR_REDO_NO_ACTION))
				{
					/*
					 * POLAR: It's doing online promote if redo_required but is not in replica mode.
					 * We will replay this page from last checkpoint, so we can find the first FPI log to replay
					 */
					char *relpath = relpath(smgr->smgr_rnode, forkNum);
					ereport(WARNING,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("invalid page in block %u of relation %s; will replay from %lX",
									blockNum, relpath, checkpoint_redo_lsn)));
					MemSet((char *) bufBlock, 0, BLCKSZ);
					checksum_fail[index] = true;
				}
				else
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("invalid page in block %u of relation %s",
									blockNum,
									relpath(smgr->smgr_rnode, forkNum))));
			}
		}
	}

	/*
	 * notice:
	 * 1. buffers must be processed by TerminateBufferIO() from back to front.
	 *    a) TerminateBufferIO() release polar_bulk_io_in_progress_buf[] in decrement order.
	 *    b) For better performance, LWLockRelease() release io_in_progress_lock in decrement order.
	 * 2. polar_bulk_io_in_progress_count was reduced by TerminateBufferIO().
	 *    a) polar_bulk_io_in_progress_count must not be used here.
	 */
	for (index = actual_bulk_io_count - 1; index >= 0 ; index--)
	{
		BlockNumber blockNum = firstBlockNum + index;

		bufHdr = polar_bulk_io_in_progress_buf[index];
		bufBlock = isLocalBuf ? LocalBufHdrGetBlock(bufHdr) : BufHdrGetBlock(bufHdr);

		/* need copy page content from aligned_buf_read to block shared_buffer */
		if (actual_bulk_io_count != 1)
			memcpy((char *)bufBlock, aligned_buf_read + index * BLCKSZ, BLCKSZ);

		if (unlikely(polar_trace_logindex_messages <= DEBUG3))
		{
			BufferTag *tag = &bufHdr->tag;
			Buffer buf = BufferDescriptorGetBuffer(bufHdr);
			Page page = BufferGetPage(buf);

			ereport(LOG, (errmsg("read buffer lsn=%lX, redo_act=%d buf=%d, " POLAR_LOG_BUFFER_TAG_FORMAT,
								 PageGetLSN(page),
								 redo_action,
								 buf,
								 POLAR_LOG_BUFFER_TAG(tag)),
						  errhidestmt(true),
						  errhidecontext(true)));
		}

		/*
		 * POLAR: page-replay.
		 *
		 * Apply logs to this old page when read from disk.
		 *
		 * Note: All modifications about replay-page must be
		 *       applied to both polar_bulk_read_buffer_common() and ReadBuffer_common().
		 */
		if (redo_action == POLAR_REDO_REPLAY_XLOG)
		{
			XLogRecPtr __replay_from = checksum_fail[index] ? checkpoint_redo_lsn : replay_from;

			/*
			 * POLAR: we want to do record replay on page only in
			 * non-Startup process and consistency reached Startup process.
			 * This judge is *ONLY* valid in replica mode, so we set
			 * replica check above with polar_in_replica_mode().
			 */
			polar_apply_io_locked_page(bufHdr, __replay_from, checkpoint_redo_lsn);
		}
		else if (redo_action == POLAR_REDO_MARK_OUTDATE)
		{
			uint32 redo_state = polar_lock_redo_state(bufHdr);
			redo_state |= POLAR_REDO_OUTDATE;
			polar_unlock_redo_state(bufHdr, redo_state);
		}

		/* POLAR end */

		/*
		 * After TerminateBufferIO, polar_bulk_io_in_progress_buf[index]
		 * can not be used any more.
		 */
		/* Set BM_VALID, terminate IO, and wake up any waiters */
		if (isLocalBuf)
		{
			/* Only need to adjust flags */
			uint32      buf_state = pg_atomic_read_u32(&bufHdr->state);

			buf_state |= BM_VALID;
			pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);
			/* bulk io */
			polar_bulk_io_in_progress_count--;
		}
		else
			TerminateBufferIO(bufHdr, false, BM_VALID);

		/* important: buffers except firstBlockNum should release pin */
		if (index != 0)
			ReleaseBuffer(BufferDescriptorGetBuffer(bufHdr));

		VacuumPageMiss++;

		if (VacuumCostActive)
			VacuumCostBalance += VacuumCostPageMiss;
	}

	/*
	 * POLAR: page-replay.
	 *
	 * Reset consistent lsn which used by replay.
	 *
	 * Note: All modifications about replay-page must be
	 *       applied to both polar_bulk_read_buffer_common() and ReadBuffer_common().
	 */
	if (redo_action != POLAR_REDO_NO_ACTION)
		POLAR_RESET_BACKEND_READ_MIN_LSN();

	/* Polar end */

	TRACE_POSTGRESQL_BUFFER_READ_DONE(forkNum, firstBlockNum,
									  smgr->smgr_rnode.node.spcNode,
									  smgr->smgr_rnode.node.dbNode,
									  smgr->smgr_rnode.node.relNode,
									  smgr->smgr_rnode.backend,
									  isExtend,
									  found);

	/*
	 * notice: polar_bulk_io_in_progress_count was reduced by TerminateBufferIO().
	 * polar_bulk_io_in_progress_count must not be used here.
	 */
	if (actual_bulk_io_count != 1)
		pfree(buf_read);

	pfree(checksum_fail);

	polar_pgstat_count_bulk_read_calls_IO(reln);
	polar_pgstat_count_bulk_read_blocks_IO(reln, actual_bulk_io_count);

	Assert(0 == polar_bulk_io_in_progress_count);
	/* important, mark bulk_io end */
	polar_bulk_io_is_in_progress = false;

	return BufferDescriptorGetBuffer(first_buf_hdr);
}

/*
 * POLAR: check buffer need write fullpage snapshot image
 */
bool
polar_buffer_need_fullpage_snapshot(BufferDesc *buf_hdr, XLogRecPtr oldest_apply_lsn)
{
	XLogRecPtr  cur_insert_lsn = InvalidXLogRecPtr;
	XLogRecPtr  buf_oldest_lsn = pg_atomic_read_u64((pg_atomic_uint64 *) &buf_hdr->oldest_lsn);
	XLogRecPtr  page_latest_lsn = BufferGetLSN(buf_hdr);
	CopyBufferDesc *copy_buf = NULL;
	uint32      buf_state = 0;
	int     replay_threshold = polar_fullpage_snapshot_replay_delay_threshold;
	int     oldest_lsn_threshold = polar_fullpage_snapshot_oldest_lsn_delay_threshold;
	int     min_modified_count = polar_fullpage_snapshot_min_modified_count;

	if (unlikely(polar_bg_redo_state_is_parallel(polar_logindex_redo_instance)))
		return false;

	/*
	 * 1. if it's not a future page, quick check
	 * 2. only support main fork data page
	 */
	if (PageIsNew(BufHdrGetBlock(buf_hdr)) ||
			page_latest_lsn <= oldest_apply_lsn ||
			buf_hdr->tag.forkNum != MAIN_FORKNUM)
		return false;

	buf_state = LockBufHdr(buf_hdr);
	copy_buf = buf_hdr->copy_buffer;

	if (copy_buf)
	{
		Assert(!XLogRecPtrIsInvalid(buf_oldest_lsn));
		Assert(!XLogRecPtrIsInvalid(polar_copy_buffer_get_lsn(copy_buf)));
		buf_oldest_lsn = Min(pg_atomic_read_u64((pg_atomic_uint64 *) &copy_buf->oldest_lsn), buf_oldest_lsn);
	}

	UnlockBufHdr(buf_hdr, buf_state);

	if (!POLAR_LOGINDEX_ENABLE_FULLPAGE() ||
			RecoveryInProgress() || /* Standby don't support fullpage snapshot */
			XLogRecPtrIsInvalid(oldest_apply_lsn) ||
			XLogRecPtrIsInvalid(buf_oldest_lsn))
		return false;

#define ONE_MB (1024 * 1024L)
	cur_insert_lsn = GetXLogInsertRecPtr();

	/*
	 * In following case togather, we need to write fullpage
	 * 1. buf_oldest_lsn is too old, block to advance consist_lsn
	 * 2. replica is too slow
	 * 3. is hot page
	 */
	if (cur_insert_lsn >= buf_oldest_lsn + oldest_lsn_threshold * ONE_MB &&
			(page_latest_lsn >= oldest_apply_lsn + replay_threshold * ONE_MB ||
			 buf_hdr->recently_modified_count > min_modified_count))
		return true;

	return false;
}

/*
 * In most scenarios, we always call MarkBufferDirty first, then call XLogInsert
 * to generate WAL and real lsn, so the buffer's oldest lsn will be set by
 * MarkBufferDirty. But there are always exceptions that call XLogInsert first
 * to generate WAL, then call MarkBufferDirty. The later MarkBufferDirty will
 * set a oldest lsn that greater than the real lsn, that will cause a wrong
 * consistent lsn, it is not as expected.
 *
 * So if we call XLogInsert first, we will call this function to set a fake oldest
 * lsn like MarkBufferDirty.
 */
void
polar_set_reg_buffer_oldest_lsn(Buffer buffer)
{
	BufferDesc *buf_hdr;
	XLogRecPtr  oldest_lsn;

	if (BufferIsInvalid(buffer))
		return;

	/*
	 * If server is in recovery, the buffer should be set a real lsn, so do not
	 * need to set a fake.
	 */
	if (!polar_flush_list_enabled() || 
		(RecoveryInProgress() &&
		 !polar_bg_redo_state_is_parallel(polar_logindex_redo_instance)))
		return;

	buf_hdr = GetBufferDescriptor(buffer - 1);
	Assert(BufferIsPinned(buffer));

	/*
	 * Not all registered buffers hold the content exclusive lock, like
	 * hashbucketcleanup, if buffer does not hold the lock, we do not set
	 * its oldest lsn.
	 */
	if (!LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf_hdr),
							  LW_EXCLUSIVE))
		return;

	/*
	 * If buffer oldest lsn is not set or it is first touch after copy, we
	 * believe that the MarkBufferDirty is not called, it's time to set buffer
	 * oldest lsn and mark it dirty. If buffer oldest lsn is set, but it is
	 * not mark dirty, it will always stay in the flush list, that will block
	 * the consistent lsn.
	 */
	oldest_lsn = polar_buffer_get_oldest_lsn(buf_hdr);

	if (XLogRecPtrIsInvalid(oldest_lsn) ||
			polar_buffer_first_touch_after_copy(buf_hdr))
		MarkBufferDirty(buffer);
}

bool
polar_is_future_page(BufferDesc *buf_hdr)
{
	XLogRecPtr replayed_lsn = GetXLogReplayRecPtr(NULL);
	Page page = BufferGetPage(BufferDescriptorGetBuffer(buf_hdr));

	if (!XLogRecPtrIsInvalid(replayed_lsn) &&
			PageGetLSN(page) > replayed_lsn &&
			buf_hdr->tag.forkNum == MAIN_FORKNUM)
	{
		if (!POLAR_LOGINDEX_ENABLE_FULLPAGE())
			elog(FATAL, "Read a future page, page lsn = %lx, replayed_lsn = %lx, page_tag = " POLAR_LOG_BUFFER_TAG_FORMAT,
				 PageGetLSN(page), replayed_lsn, POLAR_LOG_BUFFER_TAG(&buf_hdr->tag));

		return true;
	}

	return false;
}

/*
 * Check whether we can use lazy checkpoint, if yes, set CHECKPOINT_LAZY and
 * set consistent lsn as lazy checkpoint redo lsn.
 */
bool
polar_check_lazy_checkpoint(bool shutdown, int *flags, XLogRecPtr *lazy_redo)
{
	bool is_lazy = false;
	XLogRecPtr consistent_lsn;

	if (!polar_lazy_checkpoint_enabled())
	{
		*flags &= ~CHECKPOINT_LAZY;
		return false;
	}

	/*
	 * polar can use lazy checkpoint for end of recovery, if we use
	 * lazy checkpoint, the end of recovery checkpoint will be ignored,
	 * we will create a normal lazy checkpoint, we set the lazy flag at
	 * StartupXLOG.
	 */
	if (*flags & CHECKPOINT_LAZY)
		is_lazy = true;
	/*
	 * for CHECKPOINT_CAUSE_XLOG or CHECKPOINT_CAUSE_TIME, we always use
	 * lazy checkpoint.
	 */
	else if (!shutdown && (*flags & (CHECKPOINT_CAUSE_TIME | CHECKPOINT_CAUSE_XLOG)))
		is_lazy = true;

	if (!is_lazy)
		return false;

	/*
	 * shutdown checkpoint must not be lazy. For lazy checkpoint, we use
	 * the consistent lsn as its redo lsn, so we should check whether the
	 * consistent lsn is valid, if not, reset the lazy flag and make a normal
	 * checkpoint.
	 */
	Assert(!shutdown || !is_lazy);

	consistent_lsn = polar_get_consistent_lsn();

	/* We have a valid consistent lsn, use it */
	if (!XLogRecPtrIsInvalid(consistent_lsn))
		*lazy_redo = consistent_lsn;
	else
	{
		/*
		 * Try to calculate the consistent lsn by myself, maybe bgwriter
		 * is not started, nobody update the consistent lsn.
		 */
		consistent_lsn = polar_cal_cur_consistent_lsn();

		if (!XLogRecPtrIsInvalid(consistent_lsn))
		{
			*lazy_redo = consistent_lsn;
			polar_set_consistent_lsn(consistent_lsn);
		}
		else
			is_lazy = false;
	}

	if (is_lazy)
	{
		*flags |= CHECKPOINT_LAZY;
		elog(LOG,
			 "Try to create lazy checkpoint, redo lsn is %X/%X",
			 (uint32)((*lazy_redo) >> 32),
			 (uint32) *lazy_redo);
	}
	else
		*flags &= ~CHECKPOINT_LAZY;

	return is_lazy;
}
