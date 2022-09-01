/*-------------------------------------------------------------------------
 *
 * polar_bufmgr.h
 *    Polardb buffer manager definitions.
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
 *      src/include/storage/polar_bufmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_BUFMGR_H
#define POLAR_BUFMGR_H

#include "storage/buf.h"
#include "storage/bufpage.h"

extern bool polar_mount_pfs_readonly_mode;

typedef struct BufferDesc BufferDesc;
typedef struct WritebackContext WritebackContext;

extern XLogRecPtr polar_online_promote_fake_oldest_lsn(void);

#define polar_fake_oldest_lsn() \
	(polar_bg_redo_state_is_parallel(polar_logindex_redo_instance) ? \
	 polar_online_promote_fake_oldest_lsn() : GetXLogInsertRecPtr())

#define polar_buffer_get_oldest_lsn(buf_hdr) \
	(pg_atomic_read_u64((pg_atomic_uint64 *) &buf_hdr->oldest_lsn))

#define polar_buffer_first_touch_after_copy(buf_hdr) \
	(buf_hdr->copy_buffer && !(buf_hdr->polar_flags & POLAR_BUF_FIRST_TOUCHED_AFTER_COPY))

/* lazy checkpoint is not allowed when full page write is on */
#define polar_lazy_checkpoint_is_allowed() \
	(polar_enable_shared_storage_mode && polar_enable_lazy_checkpoint && (!fullPageWrites))

#define polar_lazy_checkpoint_enabled() \
	(polar_lazy_checkpoint_is_allowed() && (!RecoveryInProgress()))

/* Unconditionally set the oldest lsn. */
#define polar_buffer_set_oldest_lsn(bufHdr,lsn) \
	(pg_atomic_write_u64((pg_atomic_uint64 *) &((bufHdr)->oldest_lsn), (lsn)))

#define polar_buffer_set_fake_oldest_lsn(bufHdr) \
	(polar_buffer_set_oldest_lsn(bufHdr, polar_fake_oldest_lsn()))

#define polar_lazy_end_of_recovery_checkpoint_enabled() \
	(polar_lazy_checkpoint_is_allowed() && polar_enable_lazy_end_of_recovery_checkpoint && polar_is_master())

#define polar_buffer_adjust_oldest_lsn(lsn) \
	(polar_bg_redo_state_is_parallel(polar_logindex_redo_instance) ? \
	 polar_online_promote_fake_oldest_lsn() : (lsn))

#define POLAR_REPEAT_READ() goto repeat_read
#define POLAR_DELAY_REPEAT_READ() pg_usleep(10)
#define POLAR_WARNING_REPEAT_TIMES (100)

extern void polar_redo_set_buffer_oldest_lsn(Buffer buffer, XLogRecPtr lsn);
extern void polar_set_buffer_fake_oldest_lsn(BufferDesc *buf_hdr);
extern void polar_reset_buffer_oldest_lsn(BufferDesc *buf_hdr);
extern bool polar_buffer_can_be_flushed(BufferDesc *buf_hdr, XLogRecPtr oldest_apply_lsn, bool use_cbuf);
extern bool polar_buffer_can_be_flushed_by_checkpoint(BufferDesc *buf_hdr, XLogRecPtr oldest_apply_lsn, int flags);
extern bool polar_bg_buffer_sync(WritebackContext *wb_context, int flags);
extern bool polar_donot_control_vm_buffer(BufferDesc *buf_hdr);
extern bool polar_buffer_sync(WritebackContext *wb_context, XLogRecPtr *consistent_lsn, bool is_normal_bgwriter, int flags);
extern bool polar_pin_buffer(BufferDesc *buf, BufferAccessStrategy strategy);
extern Buffer polar_bulk_read_buffer_extended(Relation reln, ForkNumber forkNum, BlockNumber blockNum,
											  ReadBufferMode mode, BufferAccessStrategy strategy,
											  int maxBlockCount);
extern void polar_set_reg_buffer_oldest_lsn(Buffer buffer);
extern bool polar_buffer_need_fullpage_snapshot(BufferDesc *buf_hdr, XLogRecPtr oldest_apply_lsn);
extern bool polar_is_future_page(BufferDesc *buf_hdr);
extern Size polar_persisted_buffer_pool_size(void);
extern void polar_reset_buffer_pool(void);
extern bool polar_check_lazy_checkpoint(bool shutdown, int *flags, XLogRecPtr *lazy_redo);
#endif							/* POLAR_BUFMGR_H */