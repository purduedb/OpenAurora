/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_index.h
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
 *    src/include/polar_flashback/polar_flashback_log_index.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_INDEX_H
#define POLAR_FLASHBACK_LOG_INDEX_H
#include "access/polar_logindex.h"
#include "access/polar_logindex_internal.h"
#include "polar_flashback/polar_flashback_log_index_queue.h"
#include "polar_flashback/polar_flashback_log_internal.h"

#define FL_LOGINDEX_SUFFIX "_index"

typedef enum
{
	NONE,
	LOGINDEX_QUEUE,
	LOG_FILE,
	ANY
} flashback_log_source;

extern bool polar_flog_index_table_flushable(struct log_mem_table_t *table, void *data);

extern Size polar_flog_index_shmem_size(int logindex_mem_size, int logindex_bloom_blocks);
extern logindex_snapshot_t polar_flog_index_shmem_init(const char *name, int logindex_mem_size, int logindex_bloom_blocks, void *extra_data);

extern void polar_flog_index_insert(logindex_snapshot_t snapshot, flog_index_queue_ctl_t queue_ctl, flog_buf_ctl_t buf_ctl, polar_flog_rec_ptr max_ptr, flashback_log_source source);

extern void polar_validate_flog_index_dir(logindex_snapshot_t snapshot);
extern void polar_flog_index_remove_all(logindex_snapshot_t snapshot);

extern void polar_startup_flog_index(logindex_snapshot_t snapshot, polar_flog_rec_ptr checkpoint_ptr);
extern void polar_recover_flog_index(logindex_snapshot_t snapshot, flog_index_queue_ctl_t queue_ctl,  flog_buf_ctl_t buf_ctl);

extern polar_flog_rec_ptr polar_get_flog_index_max_ptr(logindex_snapshot_t snapshot);
extern polar_flog_rec_ptr polar_get_flog_index_meta_max_ptr(logindex_snapshot_t snapshot);

#endif
