/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_index_queue.h
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
 *    src/include/polar_flashback/polar_flashback_log_index_queue.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_INDEX_QUEUE_H
#define POLAR_FLASHBACK_LOG_INDEX_QUEUE_H
#include "access/polar_ringbuf.h"
#include "polar_flashback/polar_flashback_log_internal.h"
#include "polar_flashback/polar_flashback_log_record.h"

#define FLOG_INDEX_QUEUE_DATA_SIZE (sizeof(BufferTag))
/* The header size of the flashback log queue */
#define FLOG_INDEX_QUEUE_HEAD_SIZE (sizeof(polar_flog_rec_ptr) + sizeof(uint32))

/* The flashback log index queue package size */
#define FLOG_INDEX_QUEUE_PKT_SIZE(len) \
	POLAR_RINGBUF_PKT_SIZE((len) + FLOG_INDEX_QUEUE_HEAD_SIZE)

#define FLOG_INDEX_QUEUE_KEEP_RATIO (0.8)

typedef struct flog_index_queue_stat
{
	pg_atomic_uint64  free_up_total_times; /* The logindex queue free up total times */
	/* Just in on backend (flashback log bg worker), so get and set them without lock */
	uint64  read_from_file_rec_nums; /* The logindex read from flashback logindex queue times */
	uint64  read_from_queue_rec_nums; /* The logindex read from flashback log file times */
} flog_index_queue_stat;

typedef struct flog_index_queue_ctl_data_t
{
	polar_ringbuf_t queue;
	flog_index_queue_stat *queue_stat;
} flog_index_queue_ctl_data_t;

typedef flog_index_queue_ctl_data_t *flog_index_queue_ctl_t;

typedef struct flog_index_queue_lsn_info
{
	polar_flog_rec_ptr ptr;
	uint32 log_len;
	BufferTag tag;
} flog_index_queue_lsn_info;

extern Size polar_flog_index_queue_shmem_size(int queue_buffers_MB);
extern void polar_flog_index_queue_init_data(flog_index_queue_ctl_t ctl, const char *name, int queue_buffers_MB);
extern flog_index_queue_ctl_t polar_flog_index_queue_shmem_init(const char *name, int queue_buffers_MB);

extern bool polar_flog_index_queue_free(flog_index_queue_ctl_t ctl, ssize_t len);
extern void polar_flog_index_queue_free_up(flog_index_queue_ctl_t ctl, size_t len);
extern size_t polar_flog_index_queue_reserve(flog_index_queue_ctl_t ctl, size_t size);
extern void polar_flog_index_queue_set_pkt_len(flog_index_queue_ctl_t ctl, size_t idx, uint32 len);

extern bool polar_flog_index_queue_push(flog_index_queue_ctl_t ctl, size_t rbuf_pos, flog_record *record,
										int copy_len, polar_flog_rec_ptr start_lsn, uint32 log_len);
extern bool polar_flog_index_queue_ref_pop(polar_ringbuf_ref_t *ref, flog_index_queue_lsn_info *lsn_info, polar_flog_rec_ptr max_ptr);

extern bool polar_flog_read_info_from_queue(polar_ringbuf_ref_t *ref, polar_flog_rec_ptr ptr_expected, BufferTag *tag, uint32 *log_len, polar_flog_rec_ptr max_ptr);

extern void polar_get_flog_index_queue_stat(flog_index_queue_stat *queue_stat, uint64 *free_up_times,
											uint64 *read_from_file_rec_nums, uint64 *read_from_queue_rec_nums);
extern void polar_update_flog_index_queue_stat(flog_index_queue_stat *queue_stat, uint64 read_from_file_added, uint64 read_from_queue_added);
#endif
