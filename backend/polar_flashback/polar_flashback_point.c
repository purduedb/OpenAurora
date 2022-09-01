/*-------------------------------------------------------------------------
 *
 * polar_flashback_point.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_point.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "polar_flashback/polar_flashback_log_file.h"
#include "polar_flashback/polar_flashback_log_index.h"
#include "polar_flashback/polar_flashback_point.h"

/* GUCs */
int polar_flashback_point_segments;
int polar_flashback_point_timeout;

void
polar_set_fbpoint_wal_info(flog_buf_ctl_t buf_ctl, XLogRecPtr fbpoint_lsn,
						   pg_time_t fbpoint_time, XLogRecPtr bg_replayed_lsn, bool is_restart_point)
{
	XLogRecPtr prior_fbpoint_lsn;

	SpinLockAcquire(&buf_ctl->info_lck);
	prior_fbpoint_lsn = buf_ctl->wal_info.fbpoint_lsn;

	/*
	 * There are some ignored cases to update WAL information:
	 * 1. The flashback point WAL lsn is unchanged.
	 * 2. The current flashback point lsn less than
	 * prior flashback point lsn in the standby.
	 * 3. Max sequential background replay lsn less than
	 * prior flashback point lsn in the standby.
	 *
	 * NB: But it don't means that there are no flashback log record.
	 */
	if (likely(fbpoint_lsn != prior_fbpoint_lsn))
	{
		if ((fbpoint_lsn > prior_fbpoint_lsn) &&
				(!is_restart_point || (bg_replayed_lsn >= prior_fbpoint_lsn)))
		{
			buf_ctl->keep_wal_lsn = buf_ctl->wal_info.prior_fbpoint_lsn = prior_fbpoint_lsn;
			buf_ctl->wal_info.fbpoint_lsn = fbpoint_lsn;
			buf_ctl->wal_info.fbpoint_time = fbpoint_time;
		}
		/*no cover begin*/
		else if (!is_restart_point)
		{
			elog(PANIC, "The previous flashback point WAL lsn %X/%X is larger than "
				 "the current flashback point WAL lsn %X/%X in the RW node",
				 (uint32)(prior_fbpoint_lsn >> 32), (uint32) prior_fbpoint_lsn,
				 (uint32)(fbpoint_lsn >> 32), (uint32) fbpoint_lsn);
		}

		/*no cover end*/
	}

	SpinLockRelease(&buf_ctl->info_lck);

	if (unlikely(polar_flashback_log_debug))
	{
		elog(LOG, "The previous flashback point WAL lsn now is %X/%X, "
			 "the current flashback point WAL lsn now is %X/%X",
			 (uint32)(prior_fbpoint_lsn >> 32), (uint32) prior_fbpoint_lsn,
			 (uint32)(fbpoint_lsn >> 32), (uint32) fbpoint_lsn);
	}
}

XLogRecPtr
polar_get_curr_fbpoint_lsn(flog_buf_ctl_t buf_ctl)
{
	XLogRecPtr curr_fbpoint_lsn = InvalidXLogRecPtr;

	SpinLockAcquire(&buf_ctl->info_lck);
	curr_fbpoint_lsn = buf_ctl->wal_info.fbpoint_lsn;
	SpinLockRelease(&buf_ctl->info_lck);
	return curr_fbpoint_lsn;
}

/*
 * POLAR: Get the local flashback point lsn without spinlock.
 *
 * NB: Must call the function before call polar_flog_insert because the
 * add_buf_to_list will use the local flashback point lsn.
 */
XLogRecPtr
polar_get_local_fbpoint_lsn(flog_buf_ctl_t buf_ctl, XLogRecPtr page_lsn, XLogRecPtr redo_lsn)
{
	static XLogRecPtr local_fbpoint_lsn = InvalidXLogRecPtr;

	if (redo_lsn != InvalidXLogRecPtr)
		local_fbpoint_lsn = redo_lsn;
	else if (local_fbpoint_lsn == InvalidXLogRecPtr || page_lsn > local_fbpoint_lsn)
		local_fbpoint_lsn = polar_get_curr_fbpoint_lsn(buf_ctl);

	return local_fbpoint_lsn;
}

/* POLAR: Is the page first modified after the redo_lsn (or flashback point lsn) */
bool
polar_is_page_first_modified(flog_buf_ctl_t buf_ctl, XLogRecPtr page_lsn, XLogRecPtr redo_lsn)
{
	bool is_first_modified = false;
	XLogRecPtr curr_fbpoint_lsn = InvalidXLogRecPtr;

	curr_fbpoint_lsn = polar_get_local_fbpoint_lsn(buf_ctl, page_lsn, redo_lsn);

	is_first_modified = (page_lsn <= curr_fbpoint_lsn);

	if (unlikely(polar_flashback_log_debug && is_first_modified))
		elog(LOG, "The page lsn %X/%X is the first modify after the current flashback point lsn %X/%X",
			 (uint32)(page_lsn >> 32), (uint32) page_lsn,
			 (uint32)(curr_fbpoint_lsn >> 32), (uint32) curr_fbpoint_lsn);

	return is_first_modified;
}

XLogRecPtr
polar_get_prior_fbpoint_lsn(flog_buf_ctl_t buf_ctl)
{
	XLogRecPtr fbpoint_lsn;

	SpinLockAcquire(&buf_ctl->info_lck);
	fbpoint_lsn = buf_ctl->wal_info.prior_fbpoint_lsn;

	/*
	 * The prior_fbpoint_lsn is 0 before the first flashback point.
	 * We can't set the flashback log record redo lsn is 0, just set
	 * to wal_info.fbpoint_lsn;
	 */
	if (XLogRecPtrIsInvalid(fbpoint_lsn))
		fbpoint_lsn = buf_ctl->wal_info.fbpoint_lsn;

	SpinLockRelease(&buf_ctl->info_lck);
	return fbpoint_lsn;
}

/*
 * Get the flashback point start pointer.
 * The flog_start_ptr will be updated by flashback point.
 */
polar_flog_rec_ptr
polar_get_fbpoint_start_ptr(flog_buf_ctl_t ctl)
{
	polar_flog_rec_ptr ptr;

	LWLockAcquire(&ctl->ctl_file_lock, LW_SHARED);
	ptr = ctl->fbpoint_info.flog_start_ptr;
	LWLockRelease(&ctl->ctl_file_lock);
	return ptr;
}

/*
 * POLAR: Check whether we've consumed enough xlog space or time that
 * a flashback point is needed.
 */
bool
polar_is_flashback_point(flog_ctl_t instance, XLogRecPtr checkpoint_lsn, XLogRecPtr bg_replayed_lsn,
						 int *flags, bool is_restart_point)
{
	XLogRecPtr  fbpoint_lsn;
	pg_time_t   fbpoint_time;
	pg_time_t   now;
	flog_buf_ctl_t buf_ctl;
	XLogRecPtr  prior_fbpoint_lsn;
	XLogSegNo   old_segno;
	XLogSegNo   new_segno;
	bool is_shutdown = *flags & CHECKPOINT_IS_SHUTDOWN;
	bool result = false;

	if (!polar_is_flog_enabled(instance))
		return false;

	buf_ctl = instance->buf_ctl;
	Assert(polar_is_flog_buf_ready(buf_ctl));

	SpinLockAcquire(&buf_ctl->info_lck);
	fbpoint_lsn = buf_ctl->wal_info.fbpoint_lsn;
	fbpoint_time =  buf_ctl->wal_info.fbpoint_time;
	prior_fbpoint_lsn = buf_ctl->wal_info.prior_fbpoint_lsn;
	SpinLockRelease(&buf_ctl->info_lck);

	/* First checkpoint after flashback log enable is a flashback point */
	if (unlikely(prior_fbpoint_lsn == InvalidXLogRecPtr))
	{
		elog(LOG, "It is a first checkpoint after flashback log enable, treat it as a flashback point.");
		result = true;
	}

	/* If it is a shutdown checkpoint, it will be treated as a flashback point */
	if (unlikely(is_shutdown))
	{
		elog(LOG, "It is a shutdown checkpoint, treat it as a flashback point.");
		result = true;
	}

	/* Process the parallel standby mode */
	if (bg_replayed_lsn > InvalidXLogRecPtr && bg_replayed_lsn < prior_fbpoint_lsn)
	{
		Assert(prior_fbpoint_lsn > InvalidXLogRecPtr);

		return false;
	}

	/*
	 * Ugh, it is a end of recovery checkpoint or lazy checkpoint,
	 * it cann't get a right snapshot, so return false.
	 */
	if ((*flags & CHECKPOINT_END_OF_RECOVERY) || (*flags & CHECKPOINT_LAZY))
		return false;

	/* There are nothing to do */
	if (unlikely(checkpoint_lsn == fbpoint_lsn))
		return false;

	/*
	 * In some cases, the checkpoint lsn is less than
	 * the previous lsn in the standby mode.
	 */
	if (unlikely(checkpoint_lsn < fbpoint_lsn))
	{
		if (is_restart_point)
			return false;
		else
			/*no cover line*/
			elog(FATAL, "The WAL lsn %X/%X is less than last flashback point lsn %X/%X, "
				 "something is wrong.", (uint32)(checkpoint_lsn >> 32), (uint32)(checkpoint_lsn),
				 (uint32)(fbpoint_lsn >> 32), (uint32)(fbpoint_lsn));
	}

	/* Check the timeout */
	now = (pg_time_t) time(NULL);

	if (now - fbpoint_time >= polar_flashback_point_timeout)
	{
		elog(LOG, "The checkpoint is treated as a flashback point cause to timeout");
		result = true;
	}

	/* Check the WAL segments */
	XLByteToSeg(fbpoint_lsn, old_segno, wal_segment_size);
	XLByteToSeg(checkpoint_lsn, new_segno, wal_segment_size);

	if ((new_segno - old_segno) >= polar_flashback_point_segments)
	{
		elog(LOG, "The checkpoint is treated as a flashback point cause to WAL distance");
		result = true;
	}

	if (result)
		*flags = *flags | CHECKPOINT_FLASHBACK;

	return result;
}
