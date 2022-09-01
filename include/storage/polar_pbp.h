/*-------------------------------------------------------------------------
 *
 * polar_pbp.h
 *	 polardb persisted buffer pool manager.
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
 *      src/include/storage/polar_pbp.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef	POLAR_PBP_H
#define POLAR_PBP_H

#include "postgres.h"

#include "access/xlogreader.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "port/atomics.h"
#include "storage/spin.h"
#include "utils/relcache.h"

#define POLAR_BUFFER_POOL_MAGIC			0x50477c7c
#define POLAR_BUFFER_POOL_CTL_VERSION	2

/*
 * polar_buffer_pool_ctl state
 * 	last three bits store node type
 * 	the third store the error state, when reuse buffer error, we will set it.
 */
#define POLAR_PBP_STATE_NODE_TYPE_MASK	0x0007
#define POLAR_PBP_STATE_ERROR           (1U << 3)

typedef struct polar_buffer_pool_ctl_t
{
	/* Spinlock: protects the values below */
	slock_t		lock;
	int			version;
	uint64		header_magic;
	/*
	 * Unique system identifier --- to ensure we match up xlog files with the
	 * installation that produced them.
	 */
	uint64		system_identifier;
	XLogRecPtr	last_checkpoint_lsn;
	XLogRecPtr	last_flush_lsn;
	uint32 		state;				/* Last three bits store node type. */
	int			nbuffers;
	int			buf_desc_size;
	int			buffer_pool_ctl_shmem_size;
	Size		buffer_pool_shmem_size;
	Size		copy_buffer_shmem_size;
	Size		flush_list_ctl_shmem_size;
	Size		strategy_shmem_size;
	uint64		tail_magic;
} polar_buffer_pool_ctl_t;

typedef union polar_buffer_pool_ctl_padded
{
	polar_buffer_pool_ctl_t	buffer_pool_ctl;
	char		pad[PG_CACHE_LINE_SIZE];
} polar_buffer_pool_ctl_padded;

extern bool	polar_buffer_pool_is_inited;
extern polar_buffer_pool_ctl_t *polar_buffer_pool_ctl;

#define polar_buffer_pool_ctl_set_node_type(node_type) \
	(polar_buffer_pool_ctl->state |= node_type)
#define polar_buffer_pool_ctl_get_node_type() \
	(polar_buffer_pool_ctl->state & POLAR_PBP_STATE_NODE_TYPE_MASK)
#define polar_buffer_pool_ctl_set_error_state()	\
	(polar_buffer_pool_ctl->state |= POLAR_PBP_STATE_ERROR)
#define polar_buffer_pool_ctl_reset_error_state()	\
	(polar_buffer_pool_ctl->state &= ~POLAR_PBP_STATE_ERROR)
#define polar_buffer_pool_ctl_get_error_state()	\
	(polar_buffer_pool_ctl->state & POLAR_PBP_STATE_ERROR)
/*
 * save last_flush_lsn, last_flush_lsn can help us to discard
 * pages which have ahead wal
 */
#define polar_buffer_pool_ctl_set_last_flush_lsn(lsn) \
	(pg_atomic_write_u64((pg_atomic_uint64 *) &polar_buffer_pool_ctl->last_flush_lsn, lsn))

/*
 * save last_checkpoint_lsn, last_checkpoint_lsn can help us to know data
 * dirtied by other node whether or not
 */
#define polar_buffer_pool_ctl_set_last_checkpoint_lsn(lsn) \
	(pg_atomic_write_u64((pg_atomic_uint64 *) &polar_buffer_pool_ctl->last_checkpoint_lsn, lsn))

#define polar_clean_buffer_reused_flag(buf_desc) \
	do { \
		uint32	buf_state = LockBufHdr(buf_desc); \
		buf_desc->polar_flags &= ~POLAR_BUF_REUSED; \
		UnlockBufHdr(buf_desc, buf_state); \
	} \
	while(0)

/* POLAR */
extern void polar_try_reuse_buffer_pool(void);
extern void polar_mark_buffer_unrecoverable_flag(BufferDesc *buf_desc);
extern void polar_clear_buffer_unrecoverable_flag(BufferDesc *buf_desc);
extern void polar_redo_check_reused_buffer(XLogReaderState *record, Buffer buffer);
extern void polar_check_buffer_pool_consistency(void);
extern void polar_check_buffer_pool_consistency_init(void);
extern bool polar_verify_buffer_pool_ctl(void);
extern bool polar_buffer_can_be_reused(BufferDesc *buf, uint32 buf_state);

#endif							/* POLAR_PBP_H */
