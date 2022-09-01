/*-------------------------------------------------------------------------
 *
 * polar_xlogbuf.h
 *	  xlog buffer manager data types.
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
 * src/include/storage/polar_xlogbuf.h
 *
 * Xlog buffer will reserve recent xlog pages in memory to reduce io overhead.
 * When lookup a xlog page, if buffer matched or current buffer is olaer than 
 * requested page, we will return buffer id and hold content lock of buffer.
 * Caller should release content lock after read or update buffer.
 *
 * For now, replace strategy is to reserve newer xlog pages than old ones. So
 * in polar_xlog_buffer_should_evict() we will check lsn from buffer meta and 
 * requested page, if requested page is newer we will evict current buffer, 
 * otherwise requested page should be obtained by io directly.
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_XLOGBUF_H
#define POLAR_XLOGBUF_H

#include "access/xlogdefs.h"
#include "storage/buf.h"
#include "storage/lwlock.h"

/*
 * POLAR: polar_enable_master_xlog_read_ahead default set off, master will not ahead read xlog.
 * replica and standby always ahead read xlog.
 */
#define POLAR_ENABLE_XLOG_BUFFER() \
	(polar_enable_shared_storage_mode && polar_enable_xlog_buffer && \
	((polar_enable_master_xlog_read_ahead && AmStartupProcess()) || InHotStandby || polar_in_replica_mode() \
	 || polar_bg_redo_state_is_parallel(polar_logindex_redo_instance)))

#define polar_get_xlog_buffer_id(lsn) (((lsn) / XLOG_BLCKSZ) % (polar_xlog_page_buffers * 1024 / (XLOG_BLCKSZ / 1024)))
#define polar_get_xlog_buffer_desc(buf_id) (&(polar_xlog_buffer_ctl->buffer_descriptors[(buf_id)].desc))
#define polar_get_xlog_buffer(buf_id) ((char *)(polar_xlog_buffer_ctl->buffers + ((Size)(buf_id)) * XLOG_BLCKSZ))
#define polar_xlog_buffer_desc_get_lock(buf) ((LWLock*)&(polar_xlog_buffer_ctl->buffer_lock_array[(buf)->buf_id].lock))

#define polar_xlog_offset_aligned(lsn) ((lsn) % XLOG_BLCKSZ == 0)

#define XLOGBUFFERDESC_PAD_TO_SIZE	(SIZEOF_VOID_P == 8 ? 64 : 1)

typedef struct polar_xlog_buffer_desc
{
	int		buf_id; /* buffer index */
	XLogRecPtr	start_lsn;
	XLogRecPtr	end_lsn;
} polar_xlog_buffer_desc;

typedef union polar_xlog_buffer_desc_padded
{
	polar_xlog_buffer_desc	desc;
	char		pad[XLOGBUFFERDESC_PAD_TO_SIZE];
} polar_xlog_buffer_desc_padded;

typedef struct polar_xlog_buffer_ctl_data_t
{
	polar_xlog_buffer_desc_padded 	*buffer_descriptors;
	char 					*buffers;
	LWLockMinimallyPadded 	*buffer_lock_array;
} polar_xlog_buffer_ctl_data_t;
typedef polar_xlog_buffer_ctl_data_t *polar_xlog_buffer_ctl_t;

/* in polar_xlogbuf.c */
extern polar_xlog_buffer_ctl_t	polar_xlog_buffer_ctl;

extern Size polar_xlog_buffer_shmem_size(void);
extern void polar_init_xlog_buffer(void);

extern void polar_xlog_buffer_lock(int buf_id, LWLockMode mode);
extern void polar_xlog_buffer_unlock(int buf_id);

extern bool polar_xlog_buffer_lookup(XLogRecPtr start_lsn, int len, bool doEvict, bool doCount, int *buf_id);
extern void polar_xlog_buffer_update(XLogRecPtr lsn);
extern void polar_xlog_buffer_remove(XLogRecPtr lsn);
extern void polar_xlog_buffer_reset_all_buffer(void);
/* internal begin */
extern bool polar_xlog_buffer_should_evict(polar_xlog_buffer_desc *desc, XLogRecPtr lsn, int len);
/* internal end */

#endif /* POLAR_XLOGBUF_H */
