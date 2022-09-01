/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_mem.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_log_mem.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "polar_flashback/polar_flashback_log.h"
#include "polar_flashback/polar_flashback_log_file.h"
#include "polar_flashback/polar_flashback_log_index.h"
#include "polar_flashback/polar_flashback_log_mem.h"
#include "polar_flashback/polar_flashback_point.h"
#include "postmaster/startup.h"
#include "storage/lwlock.h"
#include "storage/polar_bufmgr.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

/* Name suffix */
#define CTL_NAME_SUFFIX " Ctl"
#define INSERT_LOCKS_NAME_SUFFIX "_insert"
#define BUF_MAPPING_LOCK_NAME_SUFFIX "_buf_mapping"
#define WRITE_LOCK_NAME_SUFFIX "_write"
#define INIT_LOCK_NAME_SUFFIX "_init"
#define CTL_FILE_LOCK_NAME_SUFFIX "_control_file"

/* local var */
static int  my_lock_no = 0;
/* possibly out-of-date copy of shared write_result. */
static polar_flog_rec_ptr write_result;
static flog_insert_lock_padded *flog_insert_locks = NULL;
static LWLock *flog_buf_mapping_lock = NULL;
static LWLock *flog_write_lock = NULL;

/* Macro to advance to next buffer index. */
#define FLOG_NEXT_BUF_IDX(idx, ctl)     \
	(((idx) == ctl->cache_blck) ? 0 : ((idx) + 1))

#define INSERT_FREESPACE(endptr)    \
	(((endptr) % POLAR_FLOG_BLCKSZ == 0) ? 0 : (POLAR_FLOG_BLCKSZ - (endptr) % POLAR_FLOG_BLCKSZ))

/* Local file operate */
static int  open_log_file = -1;
static uint64 open_log_segno = 0;

/*
 * Acquire a flashback log insertion lock, for inserting to flashback log.
 */
static void
flog_insert_lock_acquire(flog_buf_ctl_t ctl)
{
	bool        immed;

	/*
	 * It doesn't matter which of the flashback log insertion locks we acquire, so try
	 * the one we used last time.  If the system isn't particularly busy, it's
	 * a good bet that it's still available, and it's good to have some
	 * affinity to a particular lock so that you don't unnecessarily bounce
	 * cache lines between processes when there's no contention.
	 *
	 * If this is the first time through in this backend, pick a lock
	 * (semi-)randomly.  This allows the locks to be used evenly if you have a
	 * lot of very short connections.
	 */
	static int  lock_to_try = -1;

	if (lock_to_try == -1)
		lock_to_try = MyProc->pgprocno % ctl->insert.insert_locks_num;

	my_lock_no = lock_to_try;

	/*
	 * The inserting_at value is initially set to 0, as we don't know our
	 * insert location yet.
	 */
	immed = LWLockAcquire(&flog_insert_locks[my_lock_no].l.lock, LW_EXCLUSIVE);

	if (!immed)
	{
		/*
		 * If we couldn't get the lock immediately, try another lock next
		 * time.  On a system with more insertion locks than concurrent
		 * inserters, this causes all the inserters to eventually migrate to a
		 * lock that no-one else is using.  On a system with more inserters
		 * than locks, it still helps to distribute the inserters evenly
		 * across the locks.
		 */
		lock_to_try = (lock_to_try + 1) % ctl->insert.insert_locks_num;
	}
}

static void
flog_insert_lock_release(void)
{

	LWLockReleaseClearVar(&flog_insert_locks[my_lock_no].l.lock,
						  &flog_insert_locks[my_lock_no].l.inserting_at,
						  0);
}

/*
 * Update our insertingAt value, to let others know that we've finished
 * inserting up to that point.
 */
static void
insertlock_update_insert_at(polar_flog_rec_ptr insertingAt)
{
	LWLockUpdateVar(&flog_insert_locks[my_lock_no].l.lock,
					&flog_insert_locks[my_lock_no].l.inserting_at,
					insertingAt);
}

/*
 * Reserves the right amount of space for a record of given size from the flashback log.
 * *start_ptr is set to the beginning of the reserved section, *end_ptr to
 * its end+1.
 *
 * That must be serialized across backends. The rest can happen mostly in parallel. Try to keep this
 * section as short as possible, insertpos_lck can be heavily contended on a
 * busy system.
 *
 * NB: The space calculation here must match the code in polar_copy_flashback_log_rec2buf,
 * where we actually copy the record to the reserved space.
 */
static void
reserve_flog_insert_location(flog_buf_ctl_t buf_ctl, flog_index_queue_ctl_t queue_ctl, int size, polar_flog_rec_ptr *start_ptr,
							 polar_flog_rec_ptr *end_ptr, polar_flog_rec_ptr *prev_ptr,
							 uint32 queue_data_size, size_t *queue_pos)
{
	flog_ctl_insert *insert = &buf_ctl->insert;
	uint64      start_bytepos;
	uint64      end_bytepos;
	uint64      prev_bytepos;

	size = MAXALIGN(size);

	/* All records should contain data. */
	Assert(size >= FLOG_REC_HEADER_SIZE);

	/*
	 * The duration the spinlock needs to be held is minimized by minimizing
	 * the calculations that have to be done while holding the lock. The
	 * current tip of reserved flashback log is kept in curr_pos, as a byte position
	 * that only counts "usable" bytes in flashback log, that is, it excludes all
	 * page headers. The mapping between "usable" byte positions and physical
	 * positions (polar_flog_rec_ptr) can be done outside the locked region, and
	 * because the usable byte position doesn't include any headers, reserving
	 * X bytes from flashback log is almost as simple as "curr_pos += X".
	 */

	for (;;)
	{
		SpinLockAcquire(&insert->insertpos_lck);

		/* Reserve the flashback logindex queue size */
		if (likely(queue_ctl))
		{
			if (!polar_flog_index_queue_free(queue_ctl, queue_data_size))
			{
				SpinLockRelease(&insert->insertpos_lck);
				polar_flog_index_queue_free_up(queue_ctl, queue_data_size);
				continue;
			}

			*queue_pos = polar_flog_index_queue_reserve(queue_ctl, queue_data_size);
		}

		start_bytepos = insert->curr_pos;
		prev_bytepos = insert->prev_pos;
		end_bytepos = start_bytepos + size;
		insert->curr_pos = end_bytepos;
		insert->prev_pos = start_bytepos;
		SpinLockRelease(&insert->insertpos_lck);
		break;
	}

	/* Set packet length */
	if (likely(queue_ctl))
		polar_flog_index_queue_set_pkt_len(queue_ctl, *queue_pos, queue_data_size);

	*start_ptr = polar_flog_pos2ptr(start_bytepos);
	*end_ptr = polar_flog_pos2endptr(end_bytepos);

	/* When we met the first flashback log record, set prev ptr to zero */
	if (*start_ptr == FLOG_LONG_PHD_SIZE)
		*prev_ptr = POLAR_INVALID_FLOG_REC_PTR;
	else
		*prev_ptr = polar_flog_pos2ptr(prev_bytepos);

	/* When we met a crash recovery, set prev ptr to zero */
	if (*prev_ptr == FLOG_LONG_PHD_SIZE &&
			(*start_ptr % POLAR_FLOG_SEG_SIZE == FLOG_LONG_PHD_SIZE))
		*prev_ptr = POLAR_INVALID_FLOG_REC_PTR;

	/*
	 * Check that the conversions between "usable byte positions" and
	 * polar_flog_rec_ptr work consistently in both directions.
	 */
	Assert(polar_flog_ptr2pos(*start_ptr) == start_bytepos);
	Assert(polar_flog_ptr2pos(*end_ptr) == end_bytepos);
	Assert(polar_flog_ptr2pos(*prev_ptr) == prev_bytepos);
}

/*
 * Wait for any flashback log insertions < upto to finish.
 *
 * Returns the location of the oldest insertion that is still in-progress.
 * Any flashback log prior to that point has been fully copied into flashback log buffers, and
 * can be flushed out to disk. Because this waits for any insertions older
 * than 'upto' to finish, the return value is always >= 'upto'.
 *
 * Note: When you are about to write out flashback log, you must call this function
 * *before* acquiring flog_write_lock, to avoid deadlocks. This function might
 * need to wait for an insertion to finish (or at least advance to next
 * uninitialized page), and the inserter might need to evict an old flashback log buffer
 * to make room for a new one, which in turn requires write lock.
 */
static polar_flog_rec_ptr
wait_flog_insertions_finish(flog_buf_ctl_t ctl, polar_flog_rec_ptr upto)
{
	uint64      bytepos;
	polar_flog_rec_ptr  reserved_upto;
	polar_flog_rec_ptr  finished_upto;
	flog_ctl_insert *insert = &ctl->insert;
	int         i;

	if (MyProc == NULL)
		elog(PANIC, "cannot wait without a PGPROC structure");

	/* Read the current insert position */
	SpinLockAcquire(&insert->insertpos_lck);
	bytepos = insert->curr_pos;
	SpinLockRelease(&insert->insertpos_lck);
	reserved_upto = polar_flog_pos2endptr(bytepos);

	/*
	 * No-one should request to flush a piece of flashback log that hasn't even been
	 * reserved yet. However, it can happen if there is a block with a bogus
	 * LSN on disk, for example. polar_flashback_log_flush checks for that situation and
	 * complains, but only after the flush. Here we just assume that to mean
	 * that all flashback log that has been reserved needs to be finished. In this
	 * corner-case, the return value can be smaller than 'upto' argument.
	 */
	if (upto > reserved_upto)
	{
		/*no cover begin*/
		elog(LOG, "request to flush past end of generated flashback log; request %X/%X, currpos %X/%X",
			 (uint32)(upto >> 32), (uint32) upto,
			 (uint32)(reserved_upto >> 32), (uint32) reserved_upto);
		upto = reserved_upto;
		/*no cover end*/
	}

	/*
	 * Loop through all the locks, sleeping on any in-progress insert older
	 * than 'upto'.
	 *
	 * finishedUpto is our return value, indicating the point upto which all
	 * the flashback log insertions have been finished. Initialize it to the head of
	 * reserved flashback log, and as we iterate through the insertion locks, back it
	 * out for any insertion that's still in progress.
	 */
	finished_upto = reserved_upto;

	for (i = 0; i < ctl->insert.insert_locks_num; i++)
	{
		polar_flog_rec_ptr  inserting_at = POLAR_INVALID_FLOG_REC_PTR;

		do
		{
			/*
			 * See if this insertion is in progress. LWLockWait will wait for
			 * the lock to be released, or for the 'value' to be set by a
			 * LWLockUpdateVar call.  When a lock is initially acquired, its
			 * value is 0 (POLAR_INVALID_FLOG_REC_PTR), which means that we don't know
			 * where it's inserting yet.  We will have to wait for it.  If
			 * it's a small insertion, the record will most likely fit on the
			 * same page and the inserter will release the lock without ever
			 * calling LWLockUpdateVar.  But if it has to sleep, it will
			 * advertise the insertion point with LWLockUpdateVar before
			 * sleeping.
			 */
			if (LWLockWaitForVar(&flog_insert_locks[i].l.lock,
								 &flog_insert_locks[i].l.inserting_at,
								 inserting_at, &inserting_at))
			{
				/* the lock was free, so no insertion in progress */
				inserting_at = POLAR_INVALID_FLOG_REC_PTR;
				break;
			}

			/*
			 * This insertion is still in progress. Have to wait, unless the
			 * inserter has proceeded past 'upto'.
			 */
		}
		while (inserting_at < upto);

		if (inserting_at != POLAR_INVALID_FLOG_REC_PTR && inserting_at < finished_upto)
			finished_upto = inserting_at;
	}

	return finished_upto;
}

/*
 * Close the current logfile segment for writing.
 */
static void
flog_file_close(void)
{
	Assert(open_log_file >= 0);

	if (close(open_log_file))
	{
		char name[FLOG_MAX_FNAME_LEN];
		/*no cover begin*/
		get_flog_fname(name, open_log_segno, POLAR_FLOG_SEG_SIZE, FLOG_DEFAULT_TIMELINE);
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close log file %s: %m", name)));
		/*no cover end*/
	}

	open_log_file = -1;
}

/*
 * Write and fsync the log at least as far as write_request indicates.
 *
 * If flexible == true, we don't have to write as far as write_request, but
 * may stop at any convenient boundary (such as a cache or logfile boundary).
 * This option allows us to avoid uselessly issuing multiple writes when a
 * single one would do.
 *
 * Must be called with write_lock held. wait_flashback_log_insertions2finish(write_request)
 * must be called before grabbing the lock, to make sure the data is ready to
 * write.
 */
static int
flog_write(flog_buf_ctl_t ctl, polar_flog_rec_ptr write_request, bool flexible)
{
	bool        is_partial_page;
	bool        last_iteration;
	bool        finishing_seg;
	bool        use_existent;
	int         curridx;
	int         npages;
	int         startidx;
	uint32      startoffset;
	int         pages_written;
	int         segs_added;
	int         total_usecs;
	long        total_secs;
	TimestampTz start_t;
	TimestampTz end_t;

	/*
	 * Update local write_result (caller probably did this already, but...)
	 */
	write_result = ctl->write_result;

	/* Log before the write. */
	if (unlikely(polar_flashback_log_debug))
		elog(LOG, "The flashback log write result is %X/%X, the write request is %X/%X",
				(uint32)(write_result >> 32), (uint32) write_result,
				(uint32)(write_request >> 32), (uint32) write_request);

	/*
	 * Since successive pages in the flashback cache are consecutively allocated,
	 * we can usually gather multiple pages together and issue just one
	 * write() call. npages is the number of pages we have determined can be
	 * written together; startidx is the cache block index of the first one,
	 * and startoffset is the file offset at which it should go. The latter
	 * two variables are only valid when npages > 0, but we must initialize
	 * all of them to keep the compiler quiet.
	 */
	npages = 0;
	startidx = 0;
	startoffset = 0;

	/* Reset the stat */
	pages_written = 0;
	segs_added = 0;
	start_t = GetCurrentTimestamp();


	/*
	 * Within the loop, curridx is the cache block index of the page to
	 * consider writing.  Begin at the buffer containing the next unwritten
	 * page, or last partially written page.
	 */
	curridx = FLOG_PTR2BUF_IDX(write_result, ctl);

	while (write_result < write_request)
	{
		/*
		 * Make sure we're not ahead of the insert process. This could happen
		 * if we're passed a bogus write_request that is past the end of the
		 * last page that's been initialized by advance_insert_buffer.
		 */
		polar_flog_rec_ptr  EndPtr = ctl->blocks[curridx];

		if (write_result >= EndPtr)
			/*no cover line*/
			elog(PANIC, "flashback log write request %X/%X is past end of log %X/%X",
				 (uint32)(write_result >> 32),
				 (uint32) write_result,
				 (uint32)(EndPtr >> 32), (uint32) EndPtr);

		/* Advance write_result to end of current buffer page */
		write_result = EndPtr;
		is_partial_page = write_request < write_result;

		if (!ptr_prev_in_flog_seg(write_result, open_log_segno,
								  POLAR_FLOG_SEG_SIZE))
		{
			/*
			 * Switch to new logfile segment.  We cannot have any pending
			 * pages here (since we dump what we have at segment end).
			 */
			Assert(npages == 0);

			if (open_log_file >= 0)
				flog_file_close();

			open_log_segno = flog_ptr_prev_to_seg(write_result,
												  POLAR_FLOG_SEG_SIZE);

			/* create/use new log file */
			use_existent = true;
			open_log_file = polar_flog_file_init(ctl, open_log_segno, &use_existent);

			if (!use_existent)
				segs_added++;

			polar_flush_flog_max_seg_no(ctl, open_log_segno);
		}

		/* Make sure we have the current logfile open */
		if (open_log_file < 0)
		{
			open_log_segno = flog_ptr_prev_to_seg(write_result,
												  POLAR_FLOG_SEG_SIZE);
			/* create/use new log file */
			use_existent = true;
			open_log_file = polar_flog_file_init(ctl, open_log_segno, &use_existent);

			if (!use_existent)
				segs_added++;

			polar_flush_flog_max_seg_no(ctl, open_log_segno);
		}

		/* Add current page to the set of pending pages-to-dump */
		if (npages == 0)
		{
			/* first of group */
			startidx = curridx;
			startoffset = FLOG_SEGMENT_OFFSET(write_result - POLAR_FLOG_BLCKSZ,
											  POLAR_FLOG_SEG_SIZE);
		}

		npages++;

		/*
		 * Dump the set if this will be the last loop iteration, or if we are
		 * at the last page of the cache area (since the next page won't be
		 * contiguous in memory), or if we are at the end of the logfile
		 * segment.
		 */
		last_iteration = (write_request <= write_result);

		finishing_seg = !is_partial_page &&
						(startoffset + npages * POLAR_FLOG_BLCKSZ) >= POLAR_FLOG_SEG_SIZE;

		if (last_iteration ||
				curridx == ctl->cache_blck ||
				finishing_seg)
		{
			char       *from;
			Size        nbytes;
			Size        nleft;
			int         written;

			/* OK to write the page(s) */
			from = ctl->pages + startidx * (Size) POLAR_FLOG_BLCKSZ;
			nbytes = npages * (Size) POLAR_FLOG_BLCKSZ;
			nleft = nbytes;
			pages_written += npages;

			do
			{
				errno = 0;
				pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_LOG_WRITE);
				written = pg_pwrite(open_log_file, from, nleft, startoffset);
				pgstat_report_wait_end();

				if (written <= 0)
				{
					/*no cover begin*/
					char name[FLOG_MAX_FNAME_LEN];

					if (errno == EINTR)
						continue;

					get_flog_fname(name, open_log_segno, POLAR_FLOG_SEG_SIZE, FLOG_DEFAULT_TIMELINE);
					ereport(PANIC,
							(errcode_for_file_access(),
							 errmsg("could not write to flashback log file %s "
									"at offset %u, length %zu: %m", name,
									startoffset, nbytes)));
					/*no cover end*/
				}

				nleft -= written;
				from += written;
				startoffset += written;
			}
			while (nleft > 0);

			/* Update state for write */
			npages = 0;

			/*
			 * If we just wrote the whole last page of a logfile segment,
			 * fsync the segment immediately.  This avoids having to go back
			 * and re-open prior segments when an fsync request comes along
			 * later. Doing it here ensures that one and only one backend will
			 * perform this fsync.
			 *
			 * NB: before fsync, add max segment no, so when crash, we can
			 * swtich a new semgent.
			 */
			if (finishing_seg)
			{
				if (pg_fsync(open_log_file) != 0)
				{
					/*no cover begin*/
					int         save_errno = errno;
					char name[FLOG_MAX_FNAME_LEN];
					get_flog_fname(name, open_log_segno, POLAR_FLOG_SEG_SIZE, FLOG_DEFAULT_TIMELINE);
					close(open_log_file);
					errno = save_errno;
					ereport(PANIC,
							(errcode_for_file_access(),
							 errmsg("could not fsync file \"%s\": %m", name)));
					/*no cover end*/
				}
			}
		}

		if (is_partial_page)
		{
			/* Only asked to write a partial page */
			write_result = write_request;
			break;
		}

		curridx = FLOG_NEXT_BUF_IDX(curridx, ctl);

		/* If flexible, break out of loop as soon as we wrote something */
		if (flexible && npages == 0)
			break;
	}

	Assert(npages == 0);

	end_t = GetCurrentTimestamp();
	TimestampDifference(start_t, end_t, &total_secs, &total_usecs);

	/* To avoid the log too noisy. */
	if (unlikely(polar_flashback_log_debug))
		elog(LOG, "Flashback log write in %s process: wrote %d flashback log buffers (%.1f%%); "
			 "%d flashback log file(s) added; "
			 "cost %ld.%03d s; "
			 "the write result is %X/%X",
			 flexible ? "background" : "normal",
			 pages_written,
			 (double) pages_written * 100 / (ctl->cache_blck + 1),
			 segs_added, total_secs, total_usecs / 1000,
			 (uint32)(write_result >> 32), (uint32) write_result);

	/*
	 * Update shared-memory status
	 *
	 * We make sure that the shared 'request' values do not fall behind the
	 * 'result' values.  This is not absolutely essential, but it saves some
	 * code in a couple of places.
	 */
	{
		SpinLockAcquire(&ctl->info_lck);
		ctl->write_result = write_result;

		if (ctl->write_request < write_request)
			ctl->write_request = write_request;

		SpinLockRelease(&ctl->info_lck);
	}

	/* Update the statistics */
	pg_atomic_fetch_add_u64(&ctl->write_total_num, pages_written);
	pg_atomic_fetch_add_u64(&ctl->segs_added_total_num, segs_added);

	if (flexible)
		ctl->bg_write_num += pages_written;

	return pages_written;
}

/*
 * Initialize flashback log buffers, writing out old buffers if they still contain
 * unwritten data, upto the page containing 'upto'. Or if 'opportunistic' is
 * true, initialize as many pages as we can without having to write out
 * unwritten data. Any new pages are initialized to zeros, with pages headers
 * initialized properly.
 */
static void
advance_insert_buffer(flog_buf_ctl_t ctl, polar_flog_rec_ptr upto, bool opportunistic)
{
	int         nextidx;
	polar_flog_rec_ptr  old_page_reqst_ptr;
	polar_flog_rec_ptr  write_rqst;
	polar_flog_rec_ptr  new_page_endptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr  new_page_beginptr;
	flog_page_header new_page;
	int         npages = 0;

	LWLockAcquire(flog_buf_mapping_lock, LW_EXCLUSIVE);

	/*
	 * Now that we have the lock, check if someone initialized the page
	 * already.
	 */
	while (upto >= ctl->initalized_upto || opportunistic)
	{
		nextidx = FLOG_PTR2BUF_IDX(ctl->initalized_upto, ctl);

		/*
		 * Get ending-offset of the buffer page we need to replace (this may
		 * be zero if the buffer hasn't been used yet).  Fall through if it's
		 * already written out.
		 */
		old_page_reqst_ptr = ctl->blocks[nextidx];

		if (write_result < old_page_reqst_ptr)
		{
			/*
			 * Nope, got work to do. If we just want to pre-initialize as much
			 * as we can without flushing, give up now.
			 */
			if (opportunistic)
				break;

			/* Before waiting, get info_lck and update write_result */
			SpinLockAcquire(&ctl->info_lck);

			if (ctl->write_request < old_page_reqst_ptr)
				/*no cover line*/
				ctl->write_request = old_page_reqst_ptr;

			write_result = ctl->write_result;
			SpinLockRelease(&ctl->info_lck);

			/*
			 * Now that we have an up-to-date write_result value, see if we
			 * still need to write it or if someone else already did.
			 */
			if (write_result < old_page_reqst_ptr)
			{
				/*
				 * Must acquire write lock. Release flashback_log_buf_mapping_lock first,
				 * to make sure that all insertions that we need to wait for
				 * can finish (up to this same position). Otherwise we risk
				 * deadlock.
				 */
				LWLockRelease(flog_buf_mapping_lock);

				wait_flog_insertions_finish(ctl, old_page_reqst_ptr);

				LWLockAcquire(flog_write_lock, LW_EXCLUSIVE);

				write_result = ctl->write_result;

				if (write_result >= old_page_reqst_ptr)
				{
					/* OK, someone wrote it already */
					LWLockRelease(flog_write_lock);
				}
				else
				{
					/* Have to write it ourselves */
					write_rqst = old_page_reqst_ptr;
					flog_write(ctl, write_rqst, false);
					LWLockRelease(flog_write_lock);
				}

				/* Re-acquire flog_buf_mapping_lock and retry */
				LWLockAcquire(flog_buf_mapping_lock, LW_EXCLUSIVE);
				continue;
			}
		}

		/*
		 * Now the next buffer slot is free and we can set it up to be the
		 * next output page.
		 */
		new_page_beginptr = ctl->initalized_upto;
		new_page_endptr = new_page_beginptr + POLAR_FLOG_BLCKSZ;

		Assert(FLOG_PTR2BUF_IDX(new_page_beginptr, ctl) == nextidx);

		new_page = (flog_page_header)(ctl->pages + nextidx * (Size) POLAR_FLOG_BLCKSZ);

		/*
		 * Be sure to re-zero the buffer so that bytes beyond what we've
		 * written will look like zeroes and not valid flashback log records...
		 */
		MemSet((char *) new_page, 0, POLAR_FLOG_BLCKSZ);

		/*
		 * Fill the new page's header
		 */
		new_page->xlp_magic = FLOG_PAGE_MAGIC;
		new_page->xlp_version = FLOG_PAGE_VERSION;
		new_page->xlp_pageaddr = new_page_beginptr;

		/* Log the buffer init */
		if (unlikely(polar_flashback_log_debug))
			elog(LOG, "Init flashback log buffer at %X/%X",
				 (uint32)(new_page_beginptr >> 32), (uint32)new_page_beginptr);

		/*
		 * If first page of an flashback log segment file, make it a long header.
		 */
		if ((FLOG_SEGMENT_OFFSET(new_page->xlp_pageaddr, POLAR_FLOG_SEG_SIZE)) == 0)
		{
			polar_long_page_header NewLongPage = (polar_long_page_header) new_page;

			NewLongPage->xlp_sysid = GetSystemIdentifier();
			NewLongPage->xlp_seg_size = POLAR_FLOG_SEG_SIZE;
			NewLongPage->xlp_blcksz = POLAR_FLOG_BLCKSZ;
			new_page->xlp_info |= FLOG_LONG_PAGE_HEADER;
		}

		/*
		 * Make sure the initialization of the page becomes visible to others
		 * before the blocks update. polar_get_flog_buf() reads blocks without
		 * holding a lock.
		 */
		pg_write_barrier();

		*((volatile polar_flog_rec_ptr *) &ctl->blocks[nextidx]) = new_page_endptr;

		ctl->initalized_upto = new_page_endptr;

		npages++;
	}

	LWLockRelease(flog_buf_mapping_lock);
}

/*
 * Get a pointer to the right location in the flashback log buffer containing the
 * given polar_flog_rec_ptr.
 *
 * If the page is not initialized yet, it is initialized. That might require
 * evicting an old dirty buffer from the buffer cache, which means I/O.
 *
 * The caller must ensure that the page containing the requested location
 * isn't evicted yet, and won't be evicted. The way to ensure that is to
 * hold onto a flashback log insertion lock with the insertingAt position set to
 * something <= ptr. polar_get_flog_buf() will update inserting_at
 * if it needs to evict an old page from the buffer. (This means that once you call
 * polar_get_flog_buf() with a given 'ptr', you must not access anything before
 * that point anymore, and must not call polar_get_flog_buf() with an older 'ptr'
 * later, because older buffers might be recycled already)
 */
static char *
polar_get_flog_buf(flog_buf_ctl_t ctl, polar_flog_rec_ptr ptr)
{
	int         idx;
	polar_flog_rec_ptr  endptr;
	static uint64 cached_page = 0;
	static char *cached_pos = NULL;
	polar_flog_rec_ptr  expected_endptr;

	/*
	 * Fast path for the common case that we need to access again the same
	 * page as last time.
	 */
	if (cached_pos && ptr / POLAR_FLOG_BLCKSZ == cached_page)
	{
		Assert(((flog_page_header) cached_pos)->xlp_magic == FLOG_PAGE_MAGIC);
		Assert(((flog_page_header) cached_pos)->xlp_pageaddr == ptr - (ptr % POLAR_FLOG_BLCKSZ));
		return cached_pos + ptr % POLAR_FLOG_BLCKSZ;
	}

	/*
	 * The flashback log buffer cache is organized so that a page is always loaded to a
	 * particular buffer.  That way we can easily calculate the buffer a given
	 * page must be loaded into, from the polar_flog_rec_ptr alone.
	 */
	idx = FLOG_PTR2BUF_IDX(ptr, ctl);

	/*
	 * See what page is loaded in the buffer at the moment. It could be the
	 * page we're looking for, or something older. It can't be anything newer
	 * - that would imply the page we're looking for has already been written
	 * out to disk and evicted, and the caller is responsible for making sure
	 * that doesn't happen.
	 *
	 * However, we don't hold a lock while we read the value. If someone has
	 * just initialized the page, it's possible that we get a "torn read" of
	 * the polar_flog_rec_ptr if 64-bit fetches are not atomic on this platform.
	 * In that case we will see a bogus value. That's ok, we'll grab the mapping
	 * lock (in advance_insert_buffer) and retry if we see anything else than
	 * the page we're looking for. But it means that when we do this unlocked
	 * read, we might see a value that appears to be ahead of the page we're
	 * looking for. Don't PANIC on that, until we've verified the value while
	 * holding the lock.
	 */
	expected_endptr = ptr;
	expected_endptr += POLAR_FLOG_BLCKSZ - ptr % POLAR_FLOG_BLCKSZ;

	endptr = ctl->blocks[idx];

	if (expected_endptr != endptr)
	{
		polar_flog_rec_ptr  initialized_upto;

		/*
		 * Before calling advance_insert_buffer(), which can block, let others
		 * know how far we're finished with inserting the record.
		 *
		 * NB: If 'ptr' points to just after the page header, advertise a
		 * position at the beginning of the page rather than 'ptr' itself. If
		 * there are no other insertions running, someone might try to flush
		 * up to our advertised location. If we advertised a position after
		 * the page header, someone might try to flush the page header, even
		 * though page might actually not be initialized yet. As the first
		 * inserter on the page, we are effectively responsible for making
		 * sure that it's initialized, before we let insertingAt to move past
		 * the page header.
		 */
		if (ptr % POLAR_FLOG_BLCKSZ == FLOG_SHORT_PHD_SIZE &&
				FLOG_SEGMENT_OFFSET(ptr, POLAR_FLOG_SEG_SIZE) > POLAR_FLOG_BLCKSZ)
			initialized_upto = ptr - FLOG_SHORT_PHD_SIZE;
		else if (ptr % POLAR_FLOG_BLCKSZ == FLOG_LONG_PHD_SIZE &&
				 FLOG_SEGMENT_OFFSET(ptr, POLAR_FLOG_SEG_SIZE) < POLAR_FLOG_BLCKSZ)
			initialized_upto = ptr - FLOG_LONG_PHD_SIZE;
		else
			initialized_upto = ptr;

		insertlock_update_insert_at(initialized_upto);
		advance_insert_buffer(ctl, ptr, false);
		endptr = ctl->blocks[idx];

		if (expected_endptr != endptr)
			/*no cover line*/
			elog(PANIC, "could not find flashback log buffer for %X/%X",
				 (uint32)(ptr >> 32), (uint32) ptr);
	}
	else
	{
		/*
		 * Make sure the initialization of the page is visible to us, and
		 * won't arrive later to overwrite the flashback log data we write on the page.
		 */
		pg_memory_barrier();
	}

	/*
	 * Found the buffer holding this page. Return a pointer to the right
	 * offset within the page.
	 */
	cached_page = ptr / POLAR_FLOG_BLCKSZ;
	cached_pos = ctl->pages + idx * (Size) POLAR_FLOG_BLCKSZ;

	Assert(((flog_page_header) cached_pos)->xlp_magic == FLOG_PAGE_MAGIC);
	Assert(((flog_page_header) cached_pos)->xlp_pageaddr == ptr - (ptr % POLAR_FLOG_BLCKSZ));

	return cached_pos + ptr % POLAR_FLOG_BLCKSZ;
}

/*
 * Subroutine of polar_flog_insert_record.
 * Copies a flashback log record to an already-reserved
 * area in the flashback log.
 */
static void
polar_copy_flog_rec2buf(flog_buf_ctl_t ctl, flog_record *rdata, polar_flog_rec_ptr start_ptr, polar_flog_rec_ptr end_ptr)
{
	char       *currpos;
	int         freespace;
	int         written;
	polar_flog_rec_ptr  curr_ptr;
	flog_page_header pagehdr;
	char       *rdata_data = (char *)rdata;
	int         write_len = rdata->xl_tot_len;
	int         rdata_len = rdata->xl_tot_len;

	/*
	 * Get a pointer to the right place in the right flashback log buffer to start
	 * inserting to.
	 */
	curr_ptr = start_ptr;
	currpos = polar_get_flog_buf(ctl, curr_ptr);
	freespace = INSERT_FREESPACE(curr_ptr);

	/*
	 * there should be enough space for at least the first field (xl_tot_len)
	 * on this page.
	 */
	Assert(freespace >= sizeof(uint32));

	/* Copy record data */
	written = 0;

	while (rdata_len > freespace)
	{
		/*
		 * Write what fits on this page, and continue on the next page.
		 */
		Assert(curr_ptr % POLAR_FLOG_BLCKSZ >= FLOG_SHORT_PHD_SIZE || freespace == 0);
		memcpy(currpos, rdata_data, freespace);
		rdata_data += freespace;
		rdata_len -= freespace;
		written += freespace;
		curr_ptr += freespace;

		/*
		 * Get pointer to beginning of next page, and set the xlp_rem_len
		 * in the page header. Set FLASHBACK_LOG_FIRST_IS_CONTRECORD.
		 *
		 * It's safe to set the contrecord flag and xlp_rem_len without a
		 * lock on the page. All the other flags were already set when the
		 * page was initialized, in advance_insert_buffer, and we're the
		 * only backend that needs to set the contrecord flag.
		 */
		currpos = polar_get_flog_buf(ctl, curr_ptr);
		pagehdr = (flog_page_header) currpos;
		pagehdr->xlp_rem_len = write_len - written;
		pagehdr->xlp_info |= FLOG_FIRST_IS_CONTRECORD;

		/* Log the contrecord */
		if (unlikely(polar_flashback_log_debug))
			elog(LOG, "Flashback log at %X/%X is a contrecord",
				 (uint32)(start_ptr >> 32), (uint32)start_ptr);

		/* skip over the page header */
		if (FLOG_SEGMENT_OFFSET(curr_ptr, POLAR_FLOG_SEG_SIZE) == 0)
		{
			curr_ptr += FLOG_LONG_PHD_SIZE;
			currpos += FLOG_LONG_PHD_SIZE;
		}
		else
		{
			curr_ptr += FLOG_SHORT_PHD_SIZE;
			currpos += FLOG_SHORT_PHD_SIZE;
		}

		freespace = INSERT_FREESPACE(curr_ptr);
	}

	Assert(curr_ptr % POLAR_FLOG_BLCKSZ >= FLOG_SHORT_PHD_SIZE || rdata_len == 0);
	memcpy(currpos, rdata_data, rdata_len);
	currpos += rdata_len;
	curr_ptr += rdata_len;
	freespace -= rdata_len;
	written += rdata_len;

	Assert(written == write_len);
	/* Align the end position, so that the next record starts aligned */
	curr_ptr = MAXALIGN64(curr_ptr);

	if (curr_ptr != end_ptr)
		/*no cover begin*/
		elog(PANIC, "space reserved for flashback log record does not match what was written, "
			 "the current pointer is %ld and end pointer expected is %ld",
			 curr_ptr, end_ptr);

	/*no cover end*/
}

/*
 * Set the flashback log checkpoint information.
 *
 * This is only called by startup flog init.
 */
static void
polar_set_fbpoint_info(flog_buf_ctl_t ctl, flog_ctl_file_data_t *ctl_file_data)
{
	flog_buf_state buf_state;

	if (ctl_file_data->version_no & FLOG_SHUTDOWNED)
		buf_state = FLOG_BUF_SHUTDOWN_RECOVERY;
	else
		buf_state = FLOG_BUF_CRASH_RECOVERY;

	LWLockAcquire(&ctl->ctl_file_lock, LW_EXCLUSIVE);
	ctl->fbpoint_info = ctl_file_data->fbpoint_info;
	ctl->max_seg_no = ctl_file_data->max_seg_no;
	ctl->wal_info = ctl->fbpoint_info.wal_info;
	ctl->buf_state = buf_state;
	LWLockRelease(&ctl->ctl_file_lock);

	if (unlikely(polar_flashback_log_debug))
	{
		elog(LOG, "Startup the flashback log buffer: "
			 "the next point in the flashback log at the last flashback point begining: %X/%X, "
			 "the next point in the flashback log at the last flashback point end: %X/%X, "
			 "the previous point of the flashback log at the last flashback point end: %X/%X, "
			 "the max flashback log segment no: %lu, "
			 "the current flashback point WAL lsn is %X/%X, "
			 "the prevoius flashback point WAL lsn is %X/%X",
			 (uint32)(ctl_file_data->fbpoint_info.flog_start_ptr >> 32),
			 (uint32) ctl_file_data->fbpoint_info.flog_start_ptr,
			 (uint32)(ctl_file_data->fbpoint_info.flog_end_ptr >> 32),
			 (uint32) ctl_file_data->fbpoint_info.flog_end_ptr,
			 (uint32)(ctl_file_data->fbpoint_info.flog_end_ptr_prev >> 32),
			 (uint32) ctl_file_data->fbpoint_info.flog_end_ptr_prev,
			 ctl_file_data->max_seg_no,
			 (uint32)(ctl_file_data->fbpoint_info.wal_info.fbpoint_lsn >> 32),
			 (uint32) ctl_file_data->fbpoint_info.wal_info.fbpoint_lsn,
			 (uint32)(ctl_file_data->fbpoint_info.wal_info.prior_fbpoint_lsn >> 32),
			 (uint32) ctl_file_data->fbpoint_info.wal_info.prior_fbpoint_lsn);
	}
}

/*
 * get the size of shared memory for flashback log
 */
Size
polar_flog_buf_size(int insert_locks_num, int log_buffers)
{
	Size        size = 0;

	/* flashback log control data */
	size = sizeof(flog_buf_ctl_data_t);
	/* flashback log insertion locks, plus alignment */
	size = add_size(size, mul_size(sizeof(flog_insert_lock_padded),
								   insert_locks_num + 1));
	/* blocks array */
	size = add_size(size, mul_size(sizeof(polar_flog_rec_ptr), log_buffers));
	/* extra alignment padding for flashback log I/O buffers */
	size = add_size(size, POLAR_FLOG_BLCKSZ);
	/* and the buffers themselves */
	size = add_size(size, mul_size(POLAR_FLOG_BLCKSZ, log_buffers));

	return size;
}

/*
 * POLAR: Init the flashback log buffer.
 * And do basic initialization of flog_ctl shared data.
 * polar_startup_flog_buf will fill in additional info.
 */
void
polar_flog_buf_init_data(flog_buf_ctl_t ctl, const char *name,
						 int insert_locks_num, int log_buffers)
{
	char *allocptr;
	int  i;
	char buf_mapping_lock_name[FL_OBJ_MAX_NAME_LEN];
	char write_lock_name[FL_OBJ_MAX_NAME_LEN];
	char init_lock_name[FL_OBJ_MAX_NAME_LEN];
	char insert_locks_name[FL_OBJ_MAX_NAME_LEN];
	char ctl_file_lock_name[FL_OBJ_MAX_NAME_LEN];

	FLOG_GET_OBJ_NAME(insert_locks_name, name, INSERT_LOCKS_NAME_SUFFIX);
	FLOG_GET_OBJ_NAME(buf_mapping_lock_name, name, BUF_MAPPING_LOCK_NAME_SUFFIX);
	FLOG_GET_OBJ_NAME(write_lock_name, name, WRITE_LOCK_NAME_SUFFIX);
	FLOG_GET_OBJ_NAME(init_lock_name, name, INIT_LOCK_NAME_SUFFIX);
	FLOG_GET_OBJ_NAME(ctl_file_lock_name, name, CTL_FILE_LOCK_NAME_SUFFIX);

	MemSet(ctl, 0, sizeof(flog_buf_ctl_data_t));
	pg_atomic_init_u64(&ctl->write_total_num, 0);
	pg_atomic_init_u64(&ctl->segs_added_total_num, 0);

	LWLockRegisterTranche(LWTRANCHE_POLAR_FLASHBACK_LOG_BUFFER_MAPPING, buf_mapping_lock_name);
	LWLockInitialize(&ctl->buf_mapping_lock,
					 LWTRANCHE_POLAR_FLASHBACK_LOG_BUFFER_MAPPING);

	LWLockRegisterTranche(LWTRANCHE_POLAR_FLASHBACK_LOG_WRITE, write_lock_name);
	LWLockInitialize(&ctl->write_lock,
					 LWTRANCHE_POLAR_FLASHBACK_LOG_WRITE);

	LWLockRegisterTranche(LWTRANCHE_POLAR_FLASHBACK_LOG_INIT, init_lock_name);
	LWLockInitialize(&ctl->init_lock,
					 LWTRANCHE_POLAR_FLASHBACK_LOG_INIT);

	LWLockRegisterTranche(LWTRANCHE_POLAR_FLASHBACK_LOG_CTL_FILE, ctl_file_lock_name);
	LWLockInitialize(&ctl->ctl_file_lock,
					LWTRANCHE_POLAR_FLASHBACK_LOG_CTL_FILE);

	allocptr = ((char *) ctl) + sizeof(flog_buf_ctl_data_t);

	ctl->blocks = (polar_flog_rec_ptr *) allocptr;
	MemSet(ctl->blocks, 0,
		   sizeof(polar_flog_rec_ptr) * log_buffers);
	allocptr += sizeof(polar_flog_rec_ptr) * log_buffers;

	/* flashback log insertion locks. Ensure they're aligned to the full padded size */
	allocptr += sizeof(flog_insert_lock_padded) -
				((uintptr_t) allocptr) % sizeof(flog_insert_lock_padded);

	flog_insert_locks = ctl->insert.insert_locks =
							(flog_insert_lock_padded *) allocptr;
	allocptr += sizeof(flog_insert_lock_padded) * insert_locks_num;

	LWLockRegisterTranche(LWTRANCHE_POLAR_FLASHBACK_LOG_INSERT, insert_locks_name);

	for (i = 0; i < insert_locks_num; i++)
	{
		LWLockInitialize(&flog_insert_locks[i].l.lock, LWTRANCHE_POLAR_FLASHBACK_LOG_INSERT);
		flog_insert_locks[i].l.inserting_at = POLAR_INVALID_FLOG_REC_PTR;
	}

	/*
	 * Align the start of the page buffers to a full flashback log block size boundary.
	 * This simplifies some calculations in flashback log insertion. It is also
	 * required for O_DIRECT.
	 */
	allocptr = (char *) TYPEALIGN(POLAR_FLOG_BLCKSZ, allocptr);
	ctl->pages = allocptr;
	MemSet(ctl->pages, 0,
		   (Size) POLAR_FLOG_BLCKSZ * log_buffers);

	/*
	 * Do basic initialization of ctl shared data. (polar_startup_flog_buf will fill
	 * in additional info.)
	 */
	ctl->cache_blck = log_buffers - 1;
	ctl->write_result = POLAR_INVALID_FLOG_REC_PTR;
	ctl->write_request = POLAR_INVALID_FLOG_REC_PTR;
	ctl->max_seg_no = POLAR_INVALID_FLOG_SEGNO;
	StrNCpy(ctl->dir, name, FL_INS_MAX_NAME_LEN);
	ctl->insert.insert_locks_num = insert_locks_num;
	ctl->tli = FLOG_DEFAULT_TIMELINE;
	ctl->buf_state = FLOG_BUF_INIT;

	SpinLockInit(&ctl->insert.insertpos_lck);
	SpinLockInit(&ctl->info_lck);

	/* init the local var */
	flog_insert_locks = ctl->insert.insert_locks;
	write_result = ctl->write_result;
	flog_buf_mapping_lock = &ctl->buf_mapping_lock;
	flog_write_lock = &ctl->write_lock;
}

/*
 * Initialization of shared memory for flashback log buffers
 */
flog_buf_ctl_t
polar_flog_buf_init(const char *name, int insert_locks_num, int log_buffers)
{
	flog_buf_ctl_t buf_ctl;
	bool found;
	char ctl_name[FL_OBJ_MAX_NAME_LEN];
	char insert_locks_name[FL_OBJ_MAX_NAME_LEN];

	FLOG_GET_OBJ_NAME(ctl_name, name, CTL_NAME_SUFFIX);
	FLOG_GET_OBJ_NAME(insert_locks_name, name, INSERT_LOCKS_NAME_SUFFIX);

	buf_ctl = (flog_buf_ctl_t)ShmemInitStruct(ctl_name,
											  polar_flog_buf_size(insert_locks_num, log_buffers), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		/*
		 * Do basic initialization of buffer control shared data.
		 * polar_startup_flog_buf will fill in additional info.
		 */
		polar_flog_buf_init_data(buf_ctl, name, insert_locks_num, log_buffers);
	}
	else
	{
		Assert(found);
		/* init the local var */
		flog_insert_locks = buf_ctl->insert.insert_locks;
		LWLockRegisterTranche(LWTRANCHE_POLAR_FLASHBACK_LOG_INSERT, insert_locks_name);
		flog_buf_mapping_lock = &buf_ctl->buf_mapping_lock;
		flog_write_lock = &buf_ctl->write_lock;
		SpinLockAcquire(&buf_ctl->info_lck);
		write_result = buf_ctl->write_result;
		SpinLockRelease(&buf_ctl->info_lck);
	}

	return buf_ctl;
}

/* Fill the flashback log control info */
void
polar_startup_flog_buf(flog_buf_ctl_t ctl, CheckPoint *checkpoint)
{
	flog_ctl_file_data_t ctl_file_data;

	/* Read the flashback log control file */
	if (polar_read_flog_ctl_file(ctl, &ctl_file_data))
		polar_set_fbpoint_info(ctl, &ctl_file_data);
	else
	{
		polar_set_fbpoint_wal_info(ctl, checkpoint->redo, checkpoint->time, InvalidXLogRecPtr, false);
		ctl->fbpoint_info.wal_info = ctl->wal_info;
		/* There no flashback, so just set buffer ready */
		polar_set_flog_buf_state(ctl, FLOG_BUF_READY);
	}
}

flog_buf_state
polar_get_flog_buf_state(flog_buf_ctl_t ctl)
{
	pg_read_barrier();
	return ctl->buf_state;
}

void
polar_set_flog_buf_state(flog_buf_ctl_t ctl, flog_buf_state buf_state)
{
	ctl->buf_state = buf_state;
	pg_write_barrier();
}

polar_flog_rec_ptr
polar_get_flog_min_recover_lsn(flog_buf_ctl_t ctl)
{
	pg_read_barrier();
	return ctl->min_recover_lsn;
}

void
polar_set_flog_min_recover_lsn(flog_buf_ctl_t ctl, polar_flog_rec_ptr ptr)
{
	ctl->min_recover_lsn = ptr;
	pg_write_barrier();
}

/* POLAR: Track and log flashback log service state. */
void
polar_log_flog_buf_state(flog_buf_state state)
{
	/*no cover begin*/
	switch (state)
	{
		case FLOG_BUF_INIT:
			elog(LOG, "The state of flashback log buffer is init.");
			break;

		case FLOG_BUF_CRASH_RECOVERY:
			elog(LOG, "The state of flashback log buffer is crash recovery.");
			break;

		case FLOG_BUF_SHUTDOWN_RECOVERY:
			elog(LOG, "The state of flashback log buffer is shutdown recovery.");
			break;

		case FLOG_BUF_READY:
			elog(LOG, "The state of flashback log buffer is ready.");
			break;

		case FLOG_BUF_SHUTDOWNED:
			elog(LOG, "The state of flashback log buffer is shutdowned.");
			break;

		/*no cover begin*/
		default:
			elog(FATAL, "The state of flashback log buffer is in unknown state.");
			break;
		/*no cover end*/
	}
}

/*
 * Insert an flashback log record represented by an already-constructed record data.
 *
 * Returns the flashback log pointer to end of record.
 *
 * This can be used as LSN for buffer affected by the logged action.
 * (LSN is the flashback log point up to which the flashback log must be flushed to disk
 * before the buffer can be written out. This implements the basic
 * flashback log rule "write the origin data before the new data".)
 *
 */
polar_flog_rec_ptr
polar_flog_rec_insert(flog_buf_ctl_t buf_ctl, flog_index_queue_ctl_t queue_ctl, flog_record *rec, polar_flog_rec_ptr *start_ptr)
{
	polar_flog_rec_ptr end_ptr;
	pg_crc32c   rdata_crc;
	/* The reserved start point from flashback logindex queue */
	size_t      queue_pos = -1;
	/* The size for flashback logindex queue data */
	uint32      queue_data_size = 0;

	queue_data_size = FLOG_INDEX_QUEUE_DATA_SIZE;
	Assert(rec->xl_tot_len >= FLOG_REC_HEADER_SIZE);

	pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_LOG_BUF_READY);

	/* Check the flashback log shared buffer ready */
	if (!polar_is_flog_buf_ready(buf_ctl))
		/*no cover line*/
		elog(PANIC, "The flashback log buffer must be ready before insert flashback log record.");

	pgstat_report_wait_end();

	/*----------
	 *
	 * We have now done all the preparatory work we can without holding a
	 * lock or modifying shared state. From here on, inserting the new flashback log
	 * record to the shared flashback log buffer cache is a two-step process:
	 *
	 * 1. Reserve the right amount of space from the flashback log. The current head of
	 *    reserved space is kept in insert->curr_pos, and is protected by
	 *    insertpos_lck.
	 *
	 * 2. Copy the record to the reserved flashback log space. This involves finding the
	 *    correct flashback log buffer containing the reserved space, and copying the
	 *    record in place. This can be done concurrently in multiple processes.
	 *
	 * To keep track of which insertions are still in-progress, each concurrent
	 * inserter acquires an insertion lock. In addition to just indicating that
	 * an insertion is in progress, the lock tells others how far the inserter
	 * has progressed. There is a small fixed number of insertion locks,
	 * determined by polar_flashback_log_buf_insert_locks. When an inserter crosses a page
	 * boundary, it updates the value stored in the lock to the how far it has
	 * inserted, to allow the previous buffer to be flushed.
	 *
	 * Holding onto an insertion lock also protects log_rec_ptr from changing
	 * until the insertion is finished.
	 *
	 * Step 2 can usually be done completely in parallel. If the required flashback log
	 * page is not initialized yet, you have to grab flog_write_lock to
	 * initialize it, but the flashback log writer tries to do that ahead of insertions
	 * to avoid that from happening in the critical path.
	 *
	 *----------
	 */
	flog_insert_lock_acquire(buf_ctl);

	/*
	 * Reserve space for the record in the flashback log.
	 */
	reserve_flog_insert_location(buf_ctl, queue_ctl, rec->xl_tot_len, start_ptr, &end_ptr,
								 &rec->xl_prev, queue_data_size, &queue_pos);

	/*
	 * Now that xl_prev has been filled in, calculate CRC of the record
	 * header.
	 */
	INIT_CRC32C(rdata_crc);

	if (rec->xl_tot_len > FLOG_REC_HEADER_SIZE)
		COMP_CRC32C(rdata_crc, (char *)rec + FLOG_REC_HEADER_SIZE,
					rec->xl_tot_len - FLOG_REC_HEADER_SIZE);

	COMP_CRC32C(rdata_crc, (char *)rec, offsetof(flog_record, xl_crc));
	FIN_CRC32C(rdata_crc);
	rec->xl_crc = rdata_crc;

	/*
	 * Must be inside CRIT_SECTION.
	 * Hold off signal to avoid mess up memory data contain
	 * flashback log buffer and flashback logindex queue data.
	 */
	START_CRIT_SECTION();
	/*
	 * All the record data, including the header, is now ready to be
	 * inserted. Copy the record in the space reserved.
	 */
	polar_copy_flog_rec2buf(buf_ctl, rec, *start_ptr, end_ptr);

	/*
	 * Done! Let others know that we're finished.
	 */
	flog_insert_lock_release();

	/*
	 * Must be inside CRIT_SECTION.
	 * Hold off signal to avoid mess up queue data.
	 * Copy the content to flashback logindex queue buffer.
	 */
	if (likely(queue_ctl))
	{
		/* Don't need a error report while the polar_flashback_logindex_queue_push catch it. */
		polar_flog_index_queue_push(queue_ctl, queue_pos, rec, queue_data_size,
									*start_ptr, rec->xl_tot_len);
	}

	END_CRIT_SECTION();

	/*
	 * Update shared write_request, if we crossed page boundary.
	 */
	if (*start_ptr / POLAR_FLOG_BLCKSZ != end_ptr / POLAR_FLOG_BLCKSZ)
	{
		SpinLockAcquire(&buf_ctl->info_lck);

		/* advance global request to include new block(s) */
		if (buf_ctl->write_request < end_ptr)
			buf_ctl->write_request = end_ptr;

		/* update local result copy while I have the chance */
		write_result = buf_ctl->write_result;
		SpinLockRelease(&buf_ctl->info_lck);
	}

	return end_ptr;
}

/*
 * Get the flashback log record write pointer and
 * update the local write result.
 *
 * NB: It may be a callback, so we don't use the flog_buf_ctl_t as a parameter.
 */
polar_flog_rec_ptr
polar_get_flog_write_result(flog_buf_ctl_t buf_ctl)
{
	polar_flog_rec_ptr ptr = POLAR_INVALID_FLOG_REC_PTR;

	SpinLockAcquire(&buf_ctl->info_lck);
	ptr = buf_ctl->write_result;
	SpinLockRelease(&buf_ctl->info_lck);
	write_result = ptr;

	return ptr;
}

/* Get the flashback log record request pointer */
polar_flog_rec_ptr
polar_get_flog_write_request(flog_buf_ctl_t ctl)
{
	polar_flog_rec_ptr ptr = POLAR_INVALID_FLOG_REC_PTR;

	SpinLockAcquire(&ctl->info_lck);
	ptr = ctl->write_request;
	SpinLockRelease(&ctl->info_lck);

	return ptr;
}

/* Get the flashback log record current end pointer */
polar_flog_rec_ptr
polar_get_curr_flog_ptr(flog_buf_ctl_t ctl, polar_flog_rec_ptr *prev_ptr)
{
	flog_ctl_insert *insert;
	polar_flog_rec_ptr ptr;
	uint64 curr_pos;
	uint64 prev_pos;

	insert = &ctl->insert;
	SpinLockAcquire(&insert->insertpos_lck);
	curr_pos = insert->curr_pos;
	prev_pos = insert->prev_pos;
	SpinLockRelease(&insert->insertpos_lck);

	/* The point may be a end of flashback log blocks */
	ptr = polar_flog_pos2endptr(curr_pos);
	*prev_ptr = polar_flog_pos2ptr(prev_pos);

	if (FLOG_REC_PTR_IS_INVAILD(ptr) &&
			*prev_ptr == FLOG_LONG_PHD_SIZE)
		prev_ptr = POLAR_INVALID_FLOG_REC_PTR;

	return ptr;
}

/*no cover begin*/
uint64
polar_get_flog_max_seg_no(flog_buf_ctl_t ctl)
{
	uint64 max_seg_no = 0;

	SpinLockAcquire(&ctl->info_lck);
	max_seg_no = ctl->max_seg_no;
	SpinLockRelease(&ctl->info_lck);
	return max_seg_no;
}
/*no cover end*/

/*
 * POLAR: Write & flush flashback log, but without specifying exactly where to.
 * We will write complete blocks only.
 * This routine is invoked periodically by the background process.
 *
 * Return the flush result.
 */
polar_flog_rec_ptr
polar_flog_flush_bg(flog_buf_ctl_t ctl)
{
	polar_flog_rec_ptr write_reqst;

	/* read write result and update local state */
	SpinLockAcquire(&ctl->info_lck);
	write_result = ctl->write_result;
	write_reqst = ctl->write_request;
	SpinLockRelease(&ctl->info_lck);

	if (write_reqst - write_result >= polar_flashback_log_flush_max_size * 1024)
		write_reqst =  write_result + polar_flashback_log_flush_max_size * 1024;

	/* back off to last completed page boundary */
	write_reqst -= write_reqst % POLAR_FLOG_BLCKSZ;

	/*
	 * If already known flushed, we're done. Just need to check if we are
	 * holding an open file handle to a logfile that's no longer in use,
	 * preventing the file from being deleted.
	 */
	if (write_reqst <= write_result)
	{
		if (open_log_file >= 0)
		{
			if (!ptr_prev_in_flog_seg(write_result, open_log_segno,
									  POLAR_FLOG_SEG_SIZE))
				flog_file_close();
		}

		return write_result;
	}

	/* now wait for any in-progress insertions to finish and get write lock */
	wait_flog_insertions_finish(ctl, write_reqst);
	LWLockAcquire(flog_write_lock, LW_EXCLUSIVE);
	write_result = ctl->write_result;

	if (write_reqst > write_result)
		flog_write(ctl, write_reqst, true);

	LWLockRelease(flog_write_lock);

	/*
	 * Great, done. To take some work off the critical path, try to initialize
	 * as many of the no-longer-needed WAL buffers for future use as we can.
	 */
	advance_insert_buffer(ctl, POLAR_INVALID_FLOG_REC_PTR, true);

	/*
	 * If we determined that we need to write data, but somebody else
	 * wrote/flushed already, it should be considered as being active, to
	 * avoid hibernating too early.
	 */
	return write_result;
}

void
polar_get_flog_write_stat(flog_buf_ctl_t ctl, uint64 *write_total_num, uint64 *bg_write_num,
						  uint64 *segs_added_total_num)
{
	*write_total_num = pg_atomic_read_u64(&ctl->write_total_num);
	*bg_write_num = ctl->bg_write_num;
	*segs_added_total_num = pg_atomic_read_u64(&ctl->segs_added_total_num);
}

polar_flog_rec_ptr
polar_get_flog_buf_initalized_upto(flog_buf_ctl_t ctl)
{
	polar_flog_rec_ptr initalized_upto;

	SpinLockAcquire(&ctl->info_lck);
	initalized_upto = ctl->initalized_upto;
	SpinLockRelease(&ctl->info_lck);
	return initalized_upto;
}

/* POLAR: flush the flashback log to end_ptr */
void
polar_flog_flush(flog_buf_ctl_t ctl, polar_flog_rec_ptr end_ptr)
{
	/* Larger than the local write result */
	if (end_ptr > write_result)
	{
		wait_flog_insertions_finish(ctl, end_ptr);
		LWLockAcquire(flog_write_lock, LW_EXCLUSIVE);
		flog_write(ctl, end_ptr, false);
		LWLockRelease(flog_write_lock);
	}
}

void
polar_flog_get_keep_wal_lsn(flog_buf_ctl_t ctl, XLogRecPtr *keep)
{
	XLogRecPtr fl_keep_wal_lsn;

	fl_keep_wal_lsn = ctl->keep_wal_lsn;

	if (fl_keep_wal_lsn == InvalidXLogRecPtr)
		return;
	else if (*keep != InvalidXLogRecPtr)
		*keep = Min(*keep, fl_keep_wal_lsn);
	else
		*keep = fl_keep_wal_lsn;
}

char *
polar_get_flog_dir(flog_buf_ctl_t ctl)
{
	if (ctl)
		return ctl->dir;
	else
		return POLAR_FL_DEFAULT_DIR;
}
