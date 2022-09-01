/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_mem.h
 *
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
 *    src/include/polar_flashback/polar_flashback_log_mem.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_MEM_H
#define POLAR_FLASHBACK_LOG_MEM_H

#include "access/polar_logindex.h"
#include "access/xlogdefs.h"
#include "catalog/pg_control.h"
#include "polar_flashback/polar_flashback_log_index_queue.h"
#include "polar_flashback/polar_flashback_log_record.h"

#define polar_is_flog_buf_ready(ctl) (polar_get_flog_buf_state(ctl) == FLOG_BUF_READY)

typedef struct
{
	LWLock      lock;
	polar_flog_rec_ptr  inserting_at;
} flog_insert_lock;

/*
 * All the flashback log insertion locks are allocated as an array in shared memory.
 * We force the array stride to be a power of 2, which saves a few cycles in
 * indexing, but more importantly also ensures that individual slots don't
 * cross cache line boundaries. (Of course, we have to also ensure that the
 * array start address is suitably aligned.)
 */
typedef union flog_insert_lock_padded
{
	flog_insert_lock l;
	char        pad[PG_CACHE_LINE_SIZE];
} flog_insert_lock_padded;

typedef struct fbpoint_wal_info_data_t
{
	XLogRecPtr fbpoint_lsn;
	pg_time_t  fbpoint_time;
	XLogRecPtr prior_fbpoint_lsn;
} fbpoint_wal_info_data_t;

typedef struct fbpoint_info_data_t
{
	polar_flog_rec_ptr flog_start_ptr;/* the pointer before lastest flashback point start */
	polar_flog_rec_ptr flog_end_ptr; /* After it, the flashback log can be ignore in next startup. */
	polar_flog_rec_ptr flog_end_ptr_prev; /* The previous pointer of flog_end_ptr */
	fbpoint_wal_info_data_t wal_info; /* The copy of wal_info in the flog_buf_ctl_data_t */
} fbpoint_info_data_t;

/*
 * Shared state data for flashback log insertion.
 */
typedef struct flog_ctl_insert
{
	slock_t     insertpos_lck;  /* protects curr_pos and prev_pos */

	/*
	 * curr_pos is the end of reserved flashback log. The next record will be
	 * inserted at that position. prev_pos is the start position of the
	 * previously inserted (or rather, reserved) record - it is copied to the
	 * prev-link of the next record. These are stored as "usable byte
	 * positions" rather than polar_flog_rec_ptr (see polar_flog_pos2ptr()).
	 * XLogBytePosToRecPtr()).
	 */
	uint64      curr_pos;
	uint64      prev_pos;

	/*
	 * Make sure the above heavily-contended spinlock and byte positions are
	 * on their own cache line.
	 */
	char        pad[PG_CACHE_LINE_SIZE];

	/*
	 * insertion locks.
	 */
	flog_insert_lock_padded *insert_locks;
	int  insert_locks_num;
} flog_ctl_insert;

typedef struct flog_buf_ctl_data_t
{
	flog_ctl_insert insert; /* insert informations */
	polar_flog_rec_ptr write_request; /* write request pointer */
	polar_flog_rec_ptr write_result; /* write result pointer */
	polar_flog_rec_ptr min_recover_lsn; /* The minimal lsn logindex will recover to */
	fbpoint_wal_info_data_t wal_info; /* The flashback point wal info */
	slock_t     info_lck;       /* locks shared variables shown above */

	XLogRecPtr  keep_wal_lsn;   /* keep lsn of WAL */
	polar_flog_rec_ptr  initalized_upto; /* initalized upto location, protect by buf_mapping_lock */

	fbpoint_info_data_t fbpoint_info; /* The information about flashback log point */
	uint64      max_seg_no; /* The max segment no */

	LWLock buf_mapping_lock; /* protect the blocks */
	LWLock write_lock; /* protect the flashback log write action */
	LWLock init_lock; /* protect the flashback log file init action */
	LWLock ctl_file_lock; /* protect the flashback_log_control file (fbpoint_info, max_seg_no) */

	/*
	 * These values do not change after startup, although the pointed-to pages
	 * and blocks values certainly do.  blocks values are protected by
	 * buf_mapping_lock.
	 */
	char *pages;            /* buffers for unwritten flashback log pages */
	polar_flog_rec_ptr *blocks;     /* 1st byte ptr-s + POLAR_FLASHBACK_LOG_BLCKSZ */
	int  cache_blck; /* highest allocated flashback log buffer index */

	flog_buf_state buf_state; /* Just one process can change it without lock */

	/* Something about statistics */
	pg_atomic_uint64    write_total_num; /* The total num of flashback log buffer page written */
	pg_atomic_uint64    segs_added_total_num; /* The total num of flashback log segments added */
	uint64    bg_write_num; /* The num of flashback log buffer page written by background */
	char      dir[FL_INS_MAX_NAME_LEN]; /* The dir of the flashback log */
	/* TODO: the timeline will be used by archive, now is default id 1 */
	TimeLineID tli; /* The timeline id */
} flog_buf_ctl_data_t;

typedef flog_buf_ctl_data_t *flog_buf_ctl_t;

extern Size polar_flog_buf_size(int insert_locks_num, int log_buffers);
extern void polar_flog_buf_init_data(flog_buf_ctl_t ctl, const char *name,
									 int insert_locks_num, int log_buffers);
extern flog_buf_ctl_t polar_flog_buf_init(const char *name, int insert_locks_num, int log_buffers);

extern void polar_startup_flog_buf(flog_buf_ctl_t ctl, CheckPoint *checkpoint);

extern flog_buf_state polar_get_flog_buf_state(flog_buf_ctl_t ctl);
extern void polar_set_flog_buf_state(flog_buf_ctl_t ctl, flog_buf_state buf_state);
extern void polar_log_flog_buf_state(flog_buf_state state);

extern polar_flog_rec_ptr polar_flog_rec_insert(flog_buf_ctl_t buf_ctl, flog_index_queue_ctl_t queue_ctl, flog_record *rec, polar_flog_rec_ptr *start_ptr);

extern uint64 polar_get_flog_max_seg_no(flog_buf_ctl_t ctl);

extern polar_flog_rec_ptr polar_get_flog_write_result(flog_buf_ctl_t buf_ctl);
extern polar_flog_rec_ptr polar_get_flog_write_request(flog_buf_ctl_t ctl);
extern polar_flog_rec_ptr polar_get_curr_flog_ptr(flog_buf_ctl_t ctl, polar_flog_rec_ptr *prev_ptr);
extern polar_flog_rec_ptr polar_get_flog_buf_initalized_upto(flog_buf_ctl_t ctl);

extern void polar_flog_get_keep_wal_lsn(flog_buf_ctl_t ctl, XLogRecPtr *keep);

extern polar_flog_rec_ptr polar_flog_flush_bg(flog_buf_ctl_t ctl);
extern void polar_flog_flush(flog_buf_ctl_t ctl, polar_flog_rec_ptr end_ptr);

extern void polar_get_flog_write_stat(flog_buf_ctl_t ctl, uint64 *write_total_num, uint64 *bg_write_num, uint64 *segs_added_total_num);
extern char *polar_get_flog_dir(flog_buf_ctl_t ctl);

extern polar_flog_rec_ptr polar_get_flog_min_recover_lsn(flog_buf_ctl_t ctl);
extern void polar_set_flog_min_recover_lsn(flog_buf_ctl_t ctl, polar_flog_rec_ptr ptr);

#endif
