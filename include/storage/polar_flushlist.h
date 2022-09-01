/*-------------------------------------------------------------------------
 *
 * polar_flushlist.c
 *	  Routines for managing the buffer pool's flushlist.
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
 *	  src/backend/storage/polar_flushlist.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLUSHLIST_H
#define POLAR_FLUSHLIST_H

#include "postgres.h"

#include "storage/buf_internals.h"
#include "utils/guc.h"

#define FLUSH_LIST_LEN (pg_atomic_read_u32(&polar_flush_list_ctl->count))

/* Log information of BufferDesc and FlushList */
#define POLAR_LOG_BUFFER_DESC_WITH_FLUSHLIST(buf) \
    do { \
        elog(LOG, "%s buffer tag info: page_tag='[%u, %u, %u], %d, %u', state=%u, oldest_lsn=%lX, polar_flags=%u, flushlist size: %d, flush_prev: %d, flush_next: %d", \
             __func__, (buf)->tag.rnode.spcNode, (buf)->tag.rnode.dbNode, (buf)->tag.rnode.relNode, \
             (buf)->tag.forkNum, (buf)->tag.blockNum, pg_atomic_read_u32(&((buf)->state)), \
             (buf)->oldest_lsn, (buf)->polar_flags, FLUSH_LIST_LEN, (buf)->flush_prev, (buf)->flush_next); \
    } while (0)

#define polar_flush_list_is_empty() \
    (polar_flush_list_ctl->first_flush_buffer == POLAR_FLUSHNEXT_END_OF_LIST && \
    polar_flush_list_ctl->last_flush_buffer == POLAR_FLUSHNEXT_END_OF_LIST)
#define polar_flush_list_enabled() \
	(polar_enable_shared_storage_mode && polar_enable_flushlist)

/*
 * The free_next field is either the index of the next flushlist entry,
 * or one of these special values:
 */
#define POLAR_FLUSHNEXT_END_OF_LIST	(-1)
#define POLAR_FLUSHNEXT_NOT_IN_LIST	(-2)

/* The shared flushlist control information. */
typedef struct FlushListControl
{
	/* The number of buffers in flushlist */
	pg_atomic_uint32 count;

	/* Spinlock: protects the values below */
	slock_t		flushlist_lock;

	int			first_flush_buffer;	/* Head of list of dirty buffers */
	int			last_flush_buffer;  /* Tail of list of dirty buffers */
	int 		current_pos;        /* Parallel workers get buffer from this position */
	int         latest_flush_count; /* The number of buffers all parallel bgwriters flushed latest */

	/* statistic info */
	pg_atomic_uint64 insert;
	pg_atomic_uint64 remove;
	pg_atomic_uint64 find;
	pg_atomic_uint64 batch_read;
	pg_atomic_uint64 cbuf;
} FlushListControl;

extern FlushListControl *polar_flush_list_ctl;

extern int          polar_get_batch_flush_buffer(int *batch_buf);
extern void         polar_remove_buffer_from_flush_list(BufferDesc *buf);
extern void         polar_put_buffer_to_flush_list(BufferDesc *buf, XLogRecPtr lsn);
extern void         polar_adjust_position_in_flush_list(BufferDesc *buf);
extern Size         polar_flush_list_ctl_shmem_size(void);
extern void         polar_init_flush_list_ctl(bool init);
#endif							/* POLAR_FLUSHLIST_H */