/*-------------------------------------------------------------------------
 *
 * polar_flashback_log.h
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
 *    src/include/polar_flashback/polar_flashback_log.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_H
#define POLAR_FLASHBACK_LOG_H
#include "postgres.h"

#include "access/polar_logindex.h"
#include "access/polar_logindex_redo.h"
#include "access/xlogdefs.h"
#include "catalog/pg_control.h"
#include "polar_flashback/polar_flashback_log_index_queue.h"
#include "polar_flashback/polar_flashback_log_internal.h"
#include "polar_flashback/polar_flashback_log_list.h"
#include "polar_flashback/polar_flashback_log_mem.h"
#include "storage/buf_internals.h"

#define is_need_flog(forkno) (forkno == MAIN_FORKNUM)
#define polar_check_buf_flog_state(buf_hdr, state) polar_redo_check_state(buf_hdr, state)

typedef struct flog_ctl_data_t
{
	flog_buf_ctl_t buf_ctl;
	flog_list_ctl_t list_ctl;
	logindex_snapshot_t logindex_snapshot;
	flog_index_queue_ctl_t queue_ctl;
	flashback_state state;
} flog_ctl_data_t;

typedef flog_ctl_data_t *flog_ctl_t;

extern flog_ctl_t flog_instance;

/* GUCs */
extern int polar_flashback_logindex_mem_size;
extern int polar_flashback_logindex_bloom_blocks;
extern int polar_flashback_logindex_queue_buffers;
/* For the flashback log buffers */
extern int polar_flashback_log_buffers;
extern int polar_flashback_log_insert_locks;

extern void set_buf_flog_state(BufferDesc *buf_desc, uint32 flog_state);
extern void clean_buf_flog_state(BufferDesc *buf_hdr, uint32 flog_state);

extern bool polar_is_flog_mem_enabled(void);
extern bool polar_is_flog_enabled(flog_ctl_t instance);
extern bool polar_is_flog_needed(flog_ctl_t flog_ins, polar_logindex_redo_ctl_t redo_ins, ForkNumber forkno, Page page, bool is_permanent, XLogRecPtr fbpoint_lsn, XLogRecPtr redo_lsn);
extern bool polar_is_buf_flog_enabled(flog_ctl_t instance, Buffer buf);

extern bool polar_is_flog_ready(flog_ctl_t instance);
extern bool polar_has_flog_startup(flog_ctl_t instance);

extern Size polar_flog_shmem_size_internal(int insert_locks_num,
										   int log_buffers, int logindex_mem_size, int logindex_bloom_blocks, int queue_buffers_MB);
extern Size polar_flog_shmem_size(void);

extern void polar_flog_ctl_init_data(flog_ctl_t ctl);
extern flog_ctl_t polar_flog_shmem_init_internal(const char *name, int insert_locks_num, int log_buffers,
												 int logindex_mem_size, int logindex_bloom_blocks, int queue_buffers_MB);
extern void polar_flog_shmem_init(void);

extern void polar_flog_do_fbpoint(flog_ctl_t instance, polar_flog_rec_ptr ckp_start, bool shutdown);

extern void polar_flog_insert(flog_ctl_t instance, Buffer buf, bool is_candidate, bool is_recovery);
extern void polar_check_fpi_origin_page(RelFileNode rnode, ForkNumber forkno, BlockNumber block, uint8 xl_info);
extern void polar_flush_buf_flog_rec(BufferDesc *buf_hdr, flog_ctl_t instance, bool is_invalidate);

extern void polar_startup_flog(CheckPoint *checkpoint, flog_ctl_t instance);
extern void polar_recover_flog_buf(flog_ctl_t instance);
extern void polar_recover_flog(flog_ctl_t instance);
extern void polar_set_buf_flog_lost_checked(flog_ctl_t flog_ins, polar_logindex_redo_ctl_t redo_ins, Buffer buffer);
extern bool polar_may_buf_lost_flog(flog_ctl_t flog_instance, polar_logindex_redo_ctl_t logindex_redo_instance, BufferDesc *buf_desc);

extern void polar_make_true_no_flog(flog_ctl_t instance, BufferDesc *buf);
#endif
