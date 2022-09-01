/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_list.h
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
 *   src/include/polar_flashback/polar_flashback_log_list.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_LIST_H
#define POLAR_FLASHBACK_LOG_LIST_H

#include "postgres.h"

#include "access/xlogdefs.h"
#include "polar_flashback/polar_flashback_log_internal.h"
#include "polar_flashback/polar_flashback_log_mem.h"
#include "storage/buf.h"

#define POLAR_ORIGIN_PAGE_BUF_NUM_PER_ARRAY (32)
#define POLAR_ORIGIN_PAGE_BUF_ARRAY_NUM (4)
#define POLAR_ORIGIN_PAGE_BUF_NUM (POLAR_ORIGIN_PAGE_BUF_NUM_PER_ARRAY * POLAR_ORIGIN_PAGE_BUF_ARRAY_NUM)
#define get_origin_buf_array_id(index) (index / POLAR_ORIGIN_PAGE_BUF_NUM_PER_ARRAY)
#define get_origin_buf_bit(ctl, index) (pg_atomic_read_u32(&ctl->origin_buf_bitmap[get_origin_buf_array_id(index)]) >> index)
#define is_buf_in_flog_list(buf_hdr) polar_check_buf_flog_state(buf_hdr, POLAR_BUF_IN_FLOG_LIST)

typedef struct flog_list_slot
{
	int prev_buf;           /* the previous buffer id (from 0) */
	int next_buf;           /* the next buffer id (from 0) */
	XLogRecPtr redo_lsn; /* the WAL redo lsn of the origin page */
	XLogRecPtr fbpoint_lsn; /* the flashback point WAL lsn of the origin page */
	polar_flog_rec_ptr flashback_ptr; /* the flashback log record end point of the buffer */
	uint8 info;          /* the info of the entry, update in the last */
	int8 origin_buf_index; /* the index of the origin page buffer */
} flog_list_slot;

/* The info of the flashback log list slot */
#define FLOG_LIST_SLOT_EMPTY      0
#define FLOG_LIST_SLOT_READY      1
/*
 * Someone register a init page (_hash_freeovflpage) in the WAL record without
 * WILL_INIT. We think it's a first change from last checkpoint, but it isn't.
 *
 * And when the page has been flushed to disk after register a full page image in the WAL.
 * In the recovery stage, it may be treated as first modify after checkpoint, but its origin
 * page has been update to a new page and its origin page must be in the flashback log because
 * that the origin page will be flushed to disk before the new page.
 *
 * These cases above is harmless, so ignore it.
*/
#define FLOG_LIST_SLOT_CANDIDATE  2

#define NOT_IN_FLOG_LIST    (-1)

#define INVAILD_ORIGIN_BUF_INDEX (-1)
#define INVAILD_ORIGIN_BUF_ARRAY (-1)

typedef struct flog_list_ctl_data
{
	int head;
	int tail;
	slock_t info_lck;       /* locks the list structure */
	flog_list_slot *flashback_list;
	char *origin_buf; /* the origin page buffers, size is POLAR_ORIGIN_PAGE_BUF_NUM */
	BufferTag *buf_tag; /* the origin page buffer tags, size is POLAR_ORIGIN_PAGE_BUF_NUM */
	pg_atomic_uint32	*origin_buf_bitmap; /* the bitmap array of the origin page buffer */
	pg_atomic_uint64    insert_total_num; /* The total num of flashback log async list insert */
	pg_atomic_uint64    remove_total_num; /* The total num of flashback log async list remove */
	uint64    bg_remove_num; /* The num of flashback log async list removed by background */
} flog_list_ctl_data_t;

typedef flog_list_ctl_data_t *flog_list_ctl_t;

extern void polar_clean_origin_buf_bit(flog_list_ctl_t ctl, int buf_id, int8 origin_buf_index);
extern void read_origin_page_from_file(BufferTag *tag, char *origin_page);
extern Size polar_flog_async_list_shmem_size(void);
extern void polar_flog_list_init_data(flog_list_ctl_t ctl);
extern flog_list_ctl_t polar_flog_async_list_init(const char *name);
extern void polar_flog_get_async_list_info(flog_list_ctl_t ctl, int *head, int *tail);

extern void polar_push_buf_to_flog_list(flog_list_ctl_t ctl, flog_buf_ctl_t buf_ctl, Buffer buf, bool is_candidate);
extern void polar_insert_flog_rec_from_list_bg(flog_list_ctl_t list_ctl, flog_buf_ctl_t buf_ctl, flog_index_queue_ctl_t queue_ctl);

extern void polar_insert_buf_flog_rec_sync(flog_list_ctl_t list_ctl, flog_buf_ctl_t buf_ctl, flog_index_queue_ctl_t queue_ctl, BufferDesc *buf_hdr, bool is_validate);
extern void polar_flush_buf_flog(flog_list_ctl_t list_ctl, flog_buf_ctl_t buf_ctl, BufferDesc *buf, bool is_invalidate);

extern void polar_get_flog_list_stat(flog_list_ctl_t ctl, uint64 *insert_total_num, uint64 *remove_total_num, uint64 *bg_remove_num);

extern int8 polar_add_origin_buf(flog_list_ctl_t ctl, BufferDesc *buf_desc);
#endif
