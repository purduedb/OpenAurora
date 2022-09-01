/*-------------------------------------------------------------------------
 *
 * polar_fullpage.h
 *
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
 * IDENTIFICATION
 *		src/include/access/polar_fullpage.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_FULLPAGE_H
#define POLAR_FULLPAGE_H

#include "access/polar_log.h"
#include "access/polar_logindex.h"
#include "access/polar_queue_manager.h"
#include "access/xlogdefs.h"
#include "storage/buf_internals.h"
#include "storage/lwlock.h"
#include "storage/relfilenode.h"
#include "utils/guc.h"

#define MAX_FULLPAGE_DIR_NAME_LEN (32)
typedef struct polar_fullpage_ctl_data_t
{
	/* New file init lock*/
	LWLockMinimallyPadded       file_lock;
	char                        name[MAX_FULLPAGE_DIR_NAME_LEN];
	pg_atomic_uint64            max_fullpage_no;
	logindex_snapshot_t         logindex_snapshot;
	polar_ringbuf_t             queue;
	pg_atomic_uint32            procno;
} polar_fullpage_ctl_data_t;

typedef polar_fullpage_ctl_data_t *polar_fullpage_ctl_t;

#define FULLPAGE_SEGMENT_SIZE           (256 * 1024 * 1024) /* 256MB */

#define FULLPAGE_NUM_PER_FILE (FULLPAGE_SEGMENT_SIZE / BLCKSZ)
#define FULLPAGE_FILE_SEG_NO(fullpage_no) (fullpage_no / FULLPAGE_NUM_PER_FILE)
#define FULLPAGE_FILE_OFFSET(fullpage_no) ((fullpage_no * BLCKSZ) % FULLPAGE_SEGMENT_SIZE)

#define FULLPAGE_FILE_NAME(ctl, path, fullpage_no) \
	snprintf((path), MAXPGPATH, "%s/%s/%08lX.fp", POLAR_DATA_DIR(), polar_get_logindex_snapshot_dir((ctl)->logindex_snapshot), \
			 FULLPAGE_FILE_SEG_NO(fullpage_no));
#define FULLPAGE_SEG_FILE_NAME(ctl, path, seg_no) \
	snprintf((path), MAXPGPATH, "%s/%s/%08lX.fp", POLAR_DATA_DIR(), polar_get_logindex_snapshot_dir((ctl)->logindex_snapshot), seg_no);

#define LOG_INDEX_FULLPAGE_FILE_LOCK(ctl)                     \
	(&((ctl)->file_lock.lock))

extern Size polar_fullpage_shmem_size(void);
extern polar_fullpage_ctl_t  polar_fullpage_shmem_init(const char *name, polar_ringbuf_t queue, logindex_snapshot_t logindex_snapshot);

extern void polar_read_fullpage(polar_fullpage_ctl_t ctl, Page page, uint64 fullpage_no);
extern int polar_fullpage_file_init(polar_fullpage_ctl_t ctl, uint64 fullpage_no);
extern void polar_update_max_fullpage_no(polar_fullpage_ctl_t ctl, uint64 fullpage_no);
extern void polar_prealloc_fullpage_files(polar_fullpage_ctl_t ctl);
extern void polar_remove_old_fullpage_files(polar_fullpage_ctl_t ctl, XLogRecPtr min_lsn);
extern void polar_remove_old_fullpage_file(polar_fullpage_ctl_t ctl, const char *segname, uint64 min_fullpage_seg_no);
extern void polar_logindex_calc_max_fullpage_no(polar_fullpage_ctl_t ctl);
extern XLogRecPtr polar_log_fullpage_snapshot_image(polar_fullpage_ctl_t ctl, Buffer buffer, XLogRecPtr oldest_apply_lsn);
extern void polar_fullpage_set_online_promote(bool online_promote);
extern bool polar_fullpage_get_online_promote(void);
extern void polar_bgworker_fullpage_snapshot_replay(polar_fullpage_ctl_t ctl);
extern void polar_fullpage_bgworker_wait_notify(polar_fullpage_ctl_t ctl);
extern void polar_fullpage_bgworker_wakeup(polar_fullpage_ctl_t ctl);

#endif  /* POLAR_FULLPAGE_H */
