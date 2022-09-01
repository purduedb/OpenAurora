/*-------------------------------------------------------------------------
 *
 * polar_flashback_log.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_log.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "miscadmin.h"
#include "polar_flashback/polar_flashback_log.h"
#include "polar_flashback/polar_flashback_log_file.h"
#include "polar_flashback/polar_flashback_log_index.h"
#include "polar_flashback/polar_flashback_log_insert.h"
#include "polar_flashback/polar_flashback_log_worker.h"
#include "polar_flashback/polar_flashback_point.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
#include "storage/bufmgr.h"
#include "utils/guc.h"

/* GUCs */
bool polar_enable_flashback_log;
/* For the logindex */
int polar_flashback_logindex_mem_size;
int polar_flashback_logindex_bloom_blocks;
int polar_flashback_logindex_queue_buffers;
/* For the flashback log buffers */
int polar_flashback_log_buffers;
int polar_flashback_log_insert_locks;

flog_ctl_t flog_instance = NULL;

static bool
flog_data_need_remove_all(void)
{
	return !polar_enable_flashback_log && !polar_in_replica_mode();
}

/*
 * POLAR: Remove all the flashback relative files contain log files and
 * logindex files. And keep the flashback logindex dir.
 */
static void
flog_data_remove_all(flog_ctl_t instance)
{
	logindex_snapshot_t snapshot = NULL;
	flog_buf_ctl_t buf_ctl = NULL;

	if (instance)
	{
		snapshot = instance->logindex_snapshot;
		buf_ctl = instance->buf_ctl;
	}

	polar_flog_index_remove_all(snapshot);
	polar_flog_remove_all(buf_ctl);
}

static Size
flog_ctl_size(void)
{
	return MAXALIGN(sizeof(flog_ctl_data_t));
}

/*
 * POLAR: Set the flashback log buffer state according the database state.
 *
 * NB: Only startup process will call the function, so it is lock free.
 */
static void
polar_set_flog_state(flog_ctl_t instance, flashback_state state)
{
	instance->state = state;
	pg_write_barrier();
}

void
polar_flog_ctl_init_data(flog_ctl_t ctl)
{
	MemSet(ctl, 0, sizeof(flog_ctl_data_t));
	ctl->state = FLOG_INIT;
}

static flog_ctl_t
polar_flog_ctl_init(const char *name)
{
	flog_ctl_t ctl;
	bool found;

	ctl = (flog_ctl_t)ShmemInitStruct(name, flog_ctl_size(), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		polar_flog_ctl_init_data(ctl);
	}
	else
		Assert(found);

	return ctl;
}

static polar_flog_rec_ptr
cal_min_keep_flog_ptr(logindex_snapshot_t snapshot, polar_flog_rec_ptr ptr)
{
	polar_flog_rec_ptr keep_ptr;

	keep_ptr = polar_get_flog_index_meta_max_ptr(snapshot);
	keep_ptr = keep_ptr < ptr ? keep_ptr : ptr;
	return keep_ptr;
}

/*
 * POLAR: Remove the flashback log and logindex after the flashback log record pointer.
 *
 * NB: Must keep the flashback log for flashback logindex.
 */
static void
polar_remove_flog_data(flog_ctl_t instance, polar_flog_rec_ptr ptr)
{
	polar_flog_rec_ptr keep_ptr;

	keep_ptr = cal_min_keep_flog_ptr(instance->logindex_snapshot, ptr);
	polar_logindex_truncate(instance->logindex_snapshot, keep_ptr);
	polar_truncate_flog_before(instance->buf_ctl, keep_ptr);
}

/*
 * POLAR: Flush all the flashback data and update control file.
 *
 * NB: The flashback log must have been flushed before flush the buffer, so we don't
 * need to flush the flashback log in this function. If need flush all the flashback
 * log, we can call polar_flashback_log_flush.
 *
 * We don't need a precise value of flashback log point after checkpoint, but
 * a little larger value. When the database in crash recovery mode, ignore the error
 * flashback log after the value.
 * And we can't find the previous point of the checkpoint end point, but it is
 * fine and can be updated in the shutdown checkpoint.
 */
static void
polar_flush_flog_data(flog_buf_ctl_t ctl, polar_flog_rec_ptr ckp_start, bool shutdown)
{
	polar_flog_rec_ptr ckp_end;
	polar_flog_rec_ptr ckp_end_prev = POLAR_INVALID_FLOG_REC_PTR;

	if (shutdown)
	{
		ckp_end =
			polar_get_curr_flog_ptr(ctl, &ckp_end_prev);
		/*
		 * Flush all the flashback log.
		 *
		 * NB: In the RW, this work has been done in buffer flushed to disk
		 * while the buffer isn't always flushed in shutdown restartpoint of standby.
		 */
		polar_flog_flush(ctl, ckp_end);
		polar_set_flog_buf_state(ctl, FLOG_BUF_SHUTDOWNED);
	}
	else
		ckp_end = polar_get_flog_write_result(ctl);

	if (polar_flashback_log_debug)
		elog(LOG, "The point of flashback log in the checkpoint end is %X/%X",
				(uint32) (ckp_end >> 32), (uint32) ckp_end);

	if (!FLOG_REC_PTR_IS_INVAILD(ckp_end))
		polar_flush_fbpoint_info(ctl, ckp_start,
								  ckp_end, ckp_end_prev);
}

void
set_buf_flog_state(BufferDesc *buf_desc, uint32 flog_state)
{
	uint32 state;

	state = polar_lock_redo_state(buf_desc);
	state |= flog_state;
	polar_unlock_redo_state(buf_desc, state);
}

void
clean_buf_flog_state(BufferDesc *buf_hdr, uint32 flog_state)
{
	uint32 state;

	state = polar_lock_redo_state(buf_hdr);
	state &= ~(flog_state);
	polar_unlock_redo_state(buf_hdr, state);
}

bool
polar_is_flog_mem_enabled(void)
{
	return polar_enable_flashback_log && !polar_is_datamax() && !IsBootstrapProcessingMode();
}

/*
 * POLAR: Is the flashback log enabled?
 */
bool
polar_is_flog_enabled(flog_ctl_t instance)
{
	return instance && !polar_in_replica_mode();
}

bool
polar_is_flog_ready(flog_ctl_t instance)
{
	pg_read_barrier();
	return instance->state == FLOG_READY;
}

/*
 * POLAR: Is the flashback log startup?
 *
 * The state will larger than FLOG_INIT after call polar_startup_flog.
 * The startup action contain:
 * 1. read the control file and fill the flashback log buf_ctl info.
 * 2. call polar_logindex_snapshot_init to do logindex snapshot init.
 */
bool
polar_has_flog_startup(flog_ctl_t instance)
{
	pg_read_barrier();
	return instance->state > FLOG_INIT;
}

/*
 * POLAR: Get the flashback log relative memory internal function.
 */
Size
polar_flog_shmem_size_internal(int insert_locks_num, int log_buffers, int logindex_mem_size,
							   int logindex_bloom_blocks, int queue_buffers_MB)
{
	Size size = 0;

	size = add_size(size, flog_ctl_size());
	size = add_size(size, polar_flog_buf_size(insert_locks_num, log_buffers));
	size = add_size(size, polar_flog_index_shmem_size(logindex_mem_size, logindex_bloom_blocks));
	size = add_size(size, polar_flog_index_queue_shmem_size(queue_buffers_MB));
	size = add_size(size, polar_flog_async_list_shmem_size());
	return size;
}

/*
 * POLAR: Get the flashback log relative memory.
 */
Size
polar_flog_shmem_size(void)
{
	Size size = 0;

	if (polar_enable_flashback_log)
	{
		/*no cover begin*/
		if (polar_flashback_logindex_mem_size == 0)
			elog(FATAL, "Cannot enable flashback log when \"polar_flashback_logindex_mem_size\" is zero.");

		if (polar_logindex_mem_size == 0)
			elog(FATAL, "Cannot enable flashback log when \"polar_logindex_mem_size\" is zero.");

		/*no cover end*/
	}
	else
		return size;

	return polar_flog_shmem_size_internal(polar_flashback_log_insert_locks,
										  polar_flashback_log_buffers, polar_flashback_logindex_mem_size,
										  polar_flashback_logindex_bloom_blocks, polar_flashback_logindex_queue_buffers);
}

/*
 * POLAR: Initialization of shared memory for flashback log internal function
 */
flog_ctl_t
polar_flog_shmem_init_internal(const char *name, int insert_locks_num,
							   int log_buffers, int logindex_mem_size, int logindex_bloom_blocks, int queue_buffers_MB)
{
	flog_ctl_t ctl;

	ctl = polar_flog_ctl_init(name);
	ctl->list_ctl = polar_flog_async_list_init(name);
	ctl->buf_ctl = polar_flog_buf_init(name, insert_locks_num, log_buffers);
	ctl->logindex_snapshot = polar_flog_index_shmem_init(name, logindex_mem_size,
														 logindex_bloom_blocks, ctl->buf_ctl);
	ctl->queue_ctl = polar_flog_index_queue_shmem_init(name, queue_buffers_MB);
	return ctl;
}

void
polar_flog_shmem_init(void)
{
	if (polar_is_flog_mem_enabled())
		flog_instance = polar_flog_shmem_init_internal(POLAR_FL_DEFAULT_DIR,
													   polar_flashback_log_insert_locks, polar_flashback_log_buffers,
													   polar_flashback_logindex_mem_size, polar_flashback_logindex_bloom_blocks,
													   polar_flashback_logindex_queue_buffers);
}

/*
 * POLAR: Do something after the flashback point is done.
 *
 * 1. Flush the flashback log.
 * 2. remove the old flashback log.
 */
void
polar_flog_do_fbpoint(flog_ctl_t instance, polar_flog_rec_ptr ckp_start, bool shutdown)
{
	flog_buf_ctl_t buf_ctl = instance->buf_ctl;

	polar_flush_flog_data(buf_ctl, ckp_start, shutdown);
	polar_remove_flog_data(instance, ckp_start);
}

/*
 * POLAR: Is the buffer flashback log enabled?
 *
 * Now we will disable the flashback log for the buffer while repair its
 * partial problem or flashback it.
 */
bool
polar_is_buf_flog_enabled(flog_ctl_t instance, Buffer buf)
{
	BufferDesc *buf_hdr;

	buf_hdr = GetBufferDescriptor(buf - 1);
	return polar_is_flog_enabled(flog_instance) &&
			!polar_check_buf_flog_state(buf_hdr, POLAR_BUF_FLOG_DISABLE);
}

/*
 * POLAR: Is the page need a flashback log.
 *
 * instance: flog_instance.
 * forkno: The forkno of the page, now just MAIN_FORKNUM fork need a flashback log.
 * page: The page
 * is_premanent: is a permanent page?
 * fbpoint_lsn: The WAL redo point of the flashback point, just vaild in insertxlog.
 * redo_lsn: The end+1 to replay, use it in the standby recovery mode or ro online promote.
 */
bool
polar_is_flog_needed(flog_ctl_t flog_ins, polar_logindex_redo_ctl_t redo_ins,
		ForkNumber forkno, Page page, bool is_permanent,
		XLogRecPtr fbpoint_lsn, XLogRecPtr redo_lsn)
{
	Assert(flog_ins);

	if (!is_need_flog(forkno) || !is_permanent)
		return false;

	if (redo_lsn != InvalidXLogRecPtr)
	{
		Assert(fbpoint_lsn == InvalidXLogRecPtr);

		fbpoint_lsn = polar_get_curr_fbpoint_lsn(flog_ins->buf_ctl);

		if (POLAR_IN_PARALLEL_REPLAY_STANDBY_MODE(redo_ins) && redo_lsn <= fbpoint_lsn)
			fbpoint_lsn = polar_get_prior_fbpoint_lsn(flog_ins->buf_ctl);

		if (redo_lsn <= fbpoint_lsn)
			return false;
	}

	return  polar_is_page_first_modified(flog_ins->buf_ctl, PageGetLSN(page), fbpoint_lsn);
}

/*
 * POLAR: Insert into flashback log.
 *
 * buf:         The buffer id.
 * is_candidate:Is a candidate or not?
 * is_recovery:	Is in recovery.
 *
 * NB: Now we can get the origin page in memory in the recovery mode, so just insert it
 * to flashback log shared buffer to avoid to read from disk which may cause more time to
 * recovery. In the product mode, just insert into the flashback log async list.
 */
void
polar_flog_insert(flog_ctl_t instance, Buffer buf, bool is_candidate, bool is_recovery)
{
	Assert(BufferIsValid(buf));

	/* Add the buffer to origin buffer in the recovery mode */
	if (is_recovery)
	{
		BufferDesc *buf_desc = GetBufferDescriptor(buf - 1);

		polar_add_origin_buf(instance->list_ctl, buf_desc);
	}

	polar_push_buf_to_flog_list(instance->list_ctl, instance->buf_ctl, buf, is_candidate);
}

/*
 * POLAR: Check the origin page before the page with a full page image wal record
 * is a empty page.
 *
 * NB: When the page is registered by log_newpage, the origin page before it must be a empty page.
 */
void
polar_check_fpi_origin_page(RelFileNode rnode, ForkNumber forkno, BlockNumber block, uint8 xl_info)
{
	BufferTag tag;
	PGAlignedBlock  page_tmp;

	if (xl_info != XLOG_FPI_FOR_HINT)
	{
		INIT_BUFFERTAG(tag, rnode, forkno, block);
		read_origin_page_from_file(&tag, page_tmp.data);

		if (!PageIsNew(page_tmp.data) && !polar_page_is_just_inited(page_tmp.data))
			/*no cover line*/
			elog(PANIC, "The page [%u, %u, %u], %d, %u has a full page image wal record, "
					"but its origin page is not a empty page", rnode.spcNode, rnode.dbNode, rnode.relNode,
					forkno, block);
	}
}

/*
 * POLAR: Flush the flashback log record of the buffer.
 *
 * is_invalidate: Is the buffer invalidate?
 *
 * When we drop the relation or database, the buffer is invalidate and its flashback log is
 * unnecessary.
 */
void
polar_flush_buf_flog_rec(BufferDesc *buf_hdr, flog_ctl_t instance, bool is_invalidate)
{
	if (!polar_is_flog_enabled(instance))
		return;

	polar_insert_buf_flog_rec_sync(instance->list_ctl, instance->buf_ctl,
			instance->queue_ctl, buf_hdr, is_invalidate);

	polar_flush_buf_flog(instance->list_ctl, instance->buf_ctl, buf_hdr, is_invalidate);
}

/*
 * POLAR: startup all about flashback log.
 *
 * state: the database state.
 * instance: the flashback log instance.
 *
 * NB: It just fill some control info. The flashback log
 * buffer recovery and logindex recovery will be
 * done by the flashback log background writer.
 */
void
polar_startup_flog(CheckPoint *checkpoint, flog_ctl_t instance)
{
	if (!IsUnderPostmaster)
		return;

	/* Remove all when flashback log is unenable */
	if (flog_data_need_remove_all())
	{
		flog_data_remove_all(instance);
		return;
	}

	/* Validate directory and startup the flashback log and flashback logindex in the rw and standby */
	if (polar_is_flog_enabled(instance))
	{
		logindex_snapshot_t snapshot = instance->logindex_snapshot;
		flog_buf_ctl_t buf_ctl = instance->buf_ctl;

		Assert(instance);
		polar_validate_flog_dir(buf_ctl);
		polar_validate_flog_index_dir(snapshot);

		/* Remove all the temporary flashback log files */
		polar_remove_tmp_flog_file(buf_ctl->dir);

		polar_startup_flog_buf(buf_ctl, checkpoint);
		polar_startup_flog_index(snapshot,
								 VALID_FLOG_PTR(polar_get_fbpoint_start_ptr(buf_ctl)));

		if (buf_ctl->buf_state == FLOG_BUF_READY)
			polar_set_flog_state(instance, FLOG_READY);
		else
		{
			/*
			 * Recover the flashback log buffer, so we can insert flashback log
			 * in the startup process without wait.
			 */
			polar_recover_flog_buf(instance);
			polar_set_flog_state(instance, FLOG_STARTUP);
		}
	}
}

/*
 * POLAR: Recover the flashback log.
 *
 * Set the flashback log record write result and initalize_upto,
 * insert->curr_pos.
 *
 * We don't care about the flashback logindex here.
 */
void
polar_recover_flog_buf(flog_ctl_t instance)
{
	flog_buf_ctl_t  buf_ctl = instance->buf_ctl;
	flog_ctl_insert *insert = &buf_ctl->insert;
	polar_flog_rec_ptr ckp_end_ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr prev_ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr pos;
	polar_flog_rec_ptr prev_pos;
	polar_flog_rec_ptr block_end_ptr;
	uint64    seg_no = 0;
	flog_buf_state state;

	LWLockAcquire(&buf_ctl->ctl_file_lock, LW_SHARED);
	ckp_end_ptr = buf_ctl->fbpoint_info.flog_end_ptr;
	prev_ptr = buf_ctl->fbpoint_info.flog_end_ptr_prev;
	seg_no = buf_ctl->max_seg_no;
	LWLockRelease(&buf_ctl->ctl_file_lock);

	Assert(prev_ptr <= ckp_end_ptr);

	/* If there is no flashback log record, just return */
	if (seg_no == POLAR_INVALID_FLOG_SEGNO)
	{
		polar_set_flog_buf_state(buf_ctl, FLOG_BUF_READY);
		polar_set_flog_state(instance, FLOG_READY);
		elog(LOG, "There is no flashback log data, so just skip the recovery of"
			 " the flashback log and logindex");
		return;
	}

	state = polar_get_flog_buf_state(buf_ctl);
	polar_log_flog_buf_state(state);

	/*
	 * Note the previous pointer is not correct. Its expected value
	 * is start of a valid flashback log record, but it is a value bigger
	 * than ckp_end_ptr. It is nothing serious, we just process this case
	 * in the reader function.
	 */
	if (state == FLOG_BUF_SHUTDOWN_RECOVERY)
		ptr = ckp_end_ptr;
	else if (state == FLOG_BUF_CRASH_RECOVERY)
	{
		ptr = (seg_no + 1) * POLAR_FLOG_SEG_SIZE;
		prev_ptr = POLAR_INVALID_FLOG_REC_PTR;

		Assert(ptr % POLAR_FLOG_BLCKSZ == 0);

		/* Write the history and control file */
		if (ptr != ckp_end_ptr)
		{
			/*
			 * Must be inside CRIT_SECTION.
			 * The shared memory has been updated already, so we must
			 * write the file successfully.
			 */
			START_CRIT_SECTION();

			polar_write_flog_history_file(buf_ctl->dir, buf_ctl->tli, ckp_end_ptr, ptr);

			/* Change the flashback log control checkpoint info and write file */
			LWLockAcquire(&buf_ctl->ctl_file_lock, LW_SHARED);
			buf_ctl->fbpoint_info.flog_end_ptr = ptr;
			buf_ctl->fbpoint_info.flog_end_ptr_prev = prev_ptr;
			LWLockRelease(&buf_ctl->ctl_file_lock);

			END_CRIT_SECTION();

			elog(LOG, "The flashback log will switch from %X/%X to %X/%X",
				 (uint32)(ckp_end_ptr >> 32), (uint32)ckp_end_ptr,
				 (uint32)(ptr >> 32), (uint32)ptr);
		}
	}
	else
		/*no cover line*/
		ereport(PANIC, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("The invalid flashback log buffer recovery state: %u", state)));

	/* Write control file, update the flashback state area */
	LWLockAcquire(&buf_ctl->ctl_file_lock, LW_EXCLUSIVE);
	polar_write_flog_ctl_file(buf_ctl);
	LWLockRelease(&buf_ctl->ctl_file_lock);

	/*
	 * When the buffer is in shutdown recovery mode, copy the
	 * last block to the buffer.
	 *
	 * NB: When it is in crash recovery mode, the ptr % POLAR_FLASHBACK_LOG_BLCKSZ
	 * must be zero.
	 */
	if (ptr % POLAR_FLOG_BLCKSZ != 0)
	{
		char       *page;
		int         len;
		int         first_idx;
		polar_flog_rec_ptr  page_begin_ptr;

		page_begin_ptr = ptr - (ptr % POLAR_FLOG_BLCKSZ);
		first_idx = FLOG_PTR2BUF_IDX(ptr, buf_ctl);

		/* Copy the valid part of the last block, and zero the rest */
		page = &buf_ctl->pages[first_idx * POLAR_FLOG_BLCKSZ];
		len = ptr % POLAR_FLOG_BLCKSZ;
		polar_flog_read(page, POLAR_FLOG_SEG_SIZE, page_begin_ptr,
						POLAR_FLOG_BLCKSZ, buf_ctl->dir);
		MemSet(page + len, 0, POLAR_FLOG_BLCKSZ - len);
		block_end_ptr = page_begin_ptr + POLAR_FLOG_BLCKSZ;
		buf_ctl->blocks[first_idx] = block_end_ptr;
	}
	else
		block_end_ptr = ptr;

	pos = polar_flog_ptr2pos(ptr);
	/*
	 * The prev_pos may be not right after crash recovery. So we ignore the
	 * check in some cases.
	 */
	prev_pos = polar_flog_ptr2pos(prev_ptr);

	SpinLockAcquire(&buf_ctl->info_lck);
	buf_ctl->write_result = ptr;
	buf_ctl->initalized_upto = block_end_ptr;
	buf_ctl->write_request = ptr;
	SpinLockRelease(&buf_ctl->info_lck);

	SpinLockAcquire(&insert->insertpos_lck);
	insert->curr_pos = pos;
	insert->prev_pos = prev_pos;
	SpinLockRelease(&insert->insertpos_lck);
	polar_set_flog_buf_state(buf_ctl, FLOG_BUF_READY);

	polar_set_flog_min_recover_lsn(buf_ctl, ptr);

	elog(LOG, "The flashback log shared buffer is ready now, the current point(position)"
				 " is %X/%X(%X/%X), previous point(position) is %X/%X(%X/%X), initalized upto point is"
				 " %X/%X", (uint32)(ptr >> 32), (uint32)(ptr), (uint32)(pos >> 32), (uint32)(pos),
				 (uint32)(prev_ptr >> 32), (uint32)(prev_ptr),
				 (uint32)(prev_pos >> 32), (uint32)(prev_pos),
				 (uint32)(block_end_ptr >> 32), (uint32)(block_end_ptr));

	Assert(polar_flog_pos2endptr(pos) == ptr ||
		   polar_flog_pos2ptr(pos) == ptr);
	Assert(polar_flog_pos2endptr(prev_pos) == prev_ptr ||
		   polar_flog_pos2ptr(prev_pos) == prev_ptr);
}

/*
 * POLAR: Set the buffer has been flashback log lost checked.
 */
void
polar_set_buf_flog_lost_checked(flog_ctl_t flog_ins,
		polar_logindex_redo_ctl_t redo_ins, Buffer buffer)
{
	BufferDesc *buf_desc;

	buf_desc = GetBufferDescriptor(buffer - 1);

	if (polar_is_flog_enabled(flog_ins) &&
			polar_get_bg_redo_state(redo_ins) == POLAR_BG_ONLINE_PROMOTE)
		set_buf_flog_state(buf_desc, POLAR_BUF_FLOG_LOST_CHECKED);
}

/*
 * POLAR: Mark the buffer lost some flashback log records in the online promote.
 *
 * We can't protect the flashback log to lose nothing in the rw crash,
 * so we must do something to recheck in the online promote.
 * Because the flashback log flush to disk before the page, so the buffer whose
 * page lsn is larger than max page lsn in the disk may lose some flashback log.
 * We think the max page lsn in the disk is the max value of current flashback point lsn
 * and primary consist lsn. The pages whose lsn less than the value must have been
 * flushed to disk already.
 */
bool
polar_may_buf_lost_flog(flog_ctl_t flog_ins, polar_logindex_redo_ctl_t redo_instance,
		BufferDesc *buf_desc)
{
	static XLogRecPtr max_page_lsn_in_disk = InvalidXLogRecPtr;
	static XLogRecPtr replay_end_lsn = InvalidXLogRecPtr;
	XLogRecPtr page_lsn;

	if (!polar_is_flog_enabled(flog_ins))
		return false;

	/* Only used by online promote */
	if (polar_get_bg_redo_state(redo_instance) != POLAR_BG_ONLINE_PROMOTE)
		return false;

	/* The buffer have been checked already */
	if (polar_check_buf_flog_state(buf_desc, POLAR_BUF_FLOG_LOST_CHECKED))
		return false;

	page_lsn = BufferGetLSN(buf_desc);

	/* The max page lsn in the disk is fixed in the online promote */
	if (max_page_lsn_in_disk == InvalidXLogRecPtr)
		max_page_lsn_in_disk = Max(polar_get_local_fbpoint_lsn(flog_ins->buf_ctl,
				InvalidXLogRecPtr, InvalidXLogRecPtr), polar_get_primary_consist_ptr());

	if (replay_end_lsn == InvalidXLogRecPtr)
		replay_end_lsn = GetXLogReplayRecPtr(NULL);

	/*
	 * Note this: the page whose lsn is equal to consist_ptr may be not flushed.
	 */
	if (page_lsn < max_page_lsn_in_disk)
		return false;
	else if (page_lsn <= replay_end_lsn)
	{
		set_buf_flog_state(buf_desc, POLAR_BUF_FLOG_LOST_CHECKED);
		return true;
	}

	return false;
}

/*
 * The flashback log must be flushed before the buffer evicted.
 */
void
polar_make_true_no_flog(flog_ctl_t instance, BufferDesc *buf)
{
	if (!polar_is_flog_enabled(instance))
		return;

	if (is_buf_in_flog_list(buf))
	{
		/*no cover line*/
		elog(PANIC, "The buffer %d [%u, %u, %u], %u, %u will be evicted "
			"but there is flashback log record of it not flushed",
			buf->buf_id, buf->tag.rnode.spcNode, buf->tag.rnode.dbNode,
			buf->tag.rnode.relNode, buf->tag.forkNum, buf->tag.blockNum);
	}
}

/* Recover the flashback log, now just recover the logindex */
void
polar_recover_flog(flog_ctl_t instance)
{
	polar_recover_flog_index(instance->logindex_snapshot, instance->queue_ctl, instance->buf_ctl);
	polar_set_flog_state(instance, FLOG_READY);
}
