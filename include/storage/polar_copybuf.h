/*-------------------------------------------------------------------------
 *
 * polar_copybuf.h
 *	  Basic copy buffer manager data types.
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
 *      src/include/storage/polar_copybuf.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_COPYBUF_H
#define POLAR_COPYBUF_H

#include "postgres.h"

#include "pgstat.h"
#include "storage/buf_internals.h"

enum CopyBufferState
{
	POLAR_COPY_BUFFER_UNUSED = 0, /* The copy buffer is still free */
	POLAR_COPY_BUFFER_USED        /* The copy buffer is used to store data */
};

/*
 * CopyBufferDesc - shared descriptor/state data for a single copy buffer.
 *
 * The copied buffer structure for polardb used on primary when a normal page
 * can't be flushed because its latest lsn greater than the oldest apply lsn.
 */
typedef struct CopyBufferDesc
{
	BufferTag   tag;                /* ID of page contained in copy buffer */
	int         buf_id;             /* copy buffer's index number (from 0) */

	int         free_next;          /* link in freelist chain */
	XLogRecPtr  oldest_lsn;    		/* the first lsn which marked this buffer dirty */
	uint32  	origin_buffer;      /* pointer for its original buffer ID (Buffer) */

	enum CopyBufferState state;     /* indicate if the copy page is used or not */

	/*
	 * Number of times this copy buffer was evaluated for flushing but it can't
	 * be flushed because the LSN constraint is not met.
	 */
	pg_atomic_uint32 pass_count;
	bool			 is_flushed;
} CopyBufferDesc;

#define COPYBUFFERDESC_PAD_TO_SIZE	(SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union CopyBufferDescPadded
{
	CopyBufferDesc	copy_buf_desc;
	char			pad[COPYBUFFERDESC_PAD_TO_SIZE];
} CopyBufferDescPadded;

//Updated
//!!!! TODO
//#define CopyBufHdrGetBlock(copy_buf_hdr) \
//	((Block) (polar_copy_buffer_blocks + ((Size) (copy_buf_hdr)->buf_id) * BLCKSZ))
#define CopyBufHdrGetBlock(copy_buf_hdr) \
	((Block) (((Size) (copy_buf_hdr)->buf_id) * BLCKSZ))
//#define CopyBufHdrGetBlock(copy_buf_hdr) (Assert(0))

#define SET_COPYBUFFERTAG(a, b) \
( \
	INIT_BUFFERTAG((a), (b).rnode, (b).forkNum, (b).blockNum) \
)

#define polar_get_copy_buffer_descriptor(id) \
	(&polar_copy_buffer_descriptors[(id)].copy_buf_desc)

#define polar_copy_buffer_get_lsn(copy_buf_hdr) \
	(PageGetLSN(CopyBufHdrGetBlock(copy_buf_hdr)))

#define polar_copy_buffer_enabled() \
	(polar_enable_shared_storage_mode && polar_enable_copy_buffer)

/* The copy buffer freelist control information. */
typedef struct CopyBufferControl
{
	/* Spinlock: protects the values below */
	slock_t		copy_buffer_ctl_lock;

	int			first_free_buffer;   /* Head of list of unused copy buffers */
	int			last_free_buffer;    /* Tail of list of unused copy buffers */

	/* Statistic info */
	pg_atomic_uint64	flushed_count;
	pg_atomic_uint64	release_count;
	pg_atomic_uint64	copied_count;
	pg_atomic_uint64	unavailable_count;
	pg_atomic_uint64	full_count;
} CopyBufferControl;

extern CopyBufferControl 	*polar_copy_buffer_ctl;
extern CopyBufferDescPadded *polar_copy_buffer_descriptors;
extern char                 *polar_copy_buffer_blocks;

extern void polar_init_copy_buffer_pool(void);
extern Size polar_copy_buffer_shmem_size(void);
extern void polar_free_copy_buffer(BufferDesc *buf);
extern void polar_flush_copy_buffer(BufferDesc *buf, SMgrRelation reln);
extern void polar_buffer_copy_if_needed(BufferDesc *buf, XLogRecPtr oldest_apply_lsn);
extern bool polar_buffer_copy_is_satisfied(BufferDesc *buf, XLogRecPtr oldest_apply_lsn, bool check_io);
extern XLogRecPtr polar_copy_buffers_get_oldest_lsn(void);

#endif							/* POLAR_COPYBUF_H */
