/*-------------------------------------------------------------------------
 *
 * polar_queue_manager.h
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
 *    src/include/access/polar_queue_manager.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_QUEUE_MANAGER_H
#define POLAR_QUEUE_MANAGER_H

#include "access/polar_logindex.h"
#include "access/polar_ringbuf.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "access/xlog_internal.h"

extern int           polar_xlog_queue_buffers;

#define POLAR_XLOG_HEAD_SIZE (sizeof(XLogRecPtr) + sizeof(uint32))
#define POLAR_XLOG_PKT_SIZE(len) POLAR_RINGBUF_PKT_SIZE((len) + POLAR_XLOG_HEAD_SIZE)

#define POLAR_XLOG_QUEUE_NEW_REF(ref, queue, strong, name) \
	do { \
		if (!polar_ringbuf_new_ref((queue), (strong), (ref), (name)))   \
			elog(PANIC, "Failed to create xlog queue reference for %s", name); \
		polar_ringbuf_auto_release_ref((ref)); \
	} while (0)

#define POLAR_XLOG_QUEUE_FREE_SIZE(queue, size) \
	(polar_ringbuf_free_size(queue) >= POLAR_XLOG_PKT_SIZE(size))

#define POLAR_XLOG_QUEUE_FREE_UP(queue, len) \
	polar_ringbuf_free_up((queue), POLAR_XLOG_PKT_SIZE(len), NULL)

#define POLAR_XLOG_QUEUE_SET_PKT_LEN(queue, idx, size) \
	polar_ringbuf_set_pkt_length((queue), (idx), (size) + POLAR_XLOG_HEAD_SIZE)

#define POLAR_XLOG_QUEUE_RESERVE(queue, size) \
	polar_ringbuf_pkt_reserve((queue), POLAR_XLOG_PKT_SIZE(size))

#define POLAR_XLOG_QUEUE_DATA_KEEP_RATIO (0.6)

#define POLAR_COPY_QUEUE_CONTENT(ref, offset, _dst, _size) \
	do {\
		ssize_t len = polar_ringbuf_read_next_pkt((ref), (offset), \
												  (uint8*)(_dst), (_size)); \
		if(len != (_size)) \
		{ \
			ereport(PANIC, (errmsg("polar: Failed to read from xlog recv queue for ref %s from offset %ld and size %ld, the return value is %ld", \
								   (ref)->ref_name, (offset), (long)(_size), len))); \
		} \
		(offset) += len; \
	} while (0)

extern Size polar_xlog_queue_size(int size_MB);
extern polar_ringbuf_t  polar_xlog_queue_init(const char *name, int tranche_id, int size_MB);
extern bool polar_xlog_send_queue_push(polar_ringbuf_t queue, size_t rbuf_pos, struct XLogRecData *rdata, int copy_len,
									   XLogRecPtr end_lsn, uint32 xlog_len);
extern void polar_standby_xlog_send_queue_push(polar_ringbuf_t queue, XLogReaderState *xlogreader);
extern Size polar_xlog_reserve_size(XLogRecData *rdata);

extern ssize_t polar_xlog_send_queue_raw_data_pop(polar_ringbuf_ref_t *ref,  uint8 *data, size_t size, XLogRecPtr *max_lsn);

extern XLogRecord *polar_xlog_send_queue_record_pop(polar_ringbuf_t queue, XLogReaderState *state);
extern void polar_xlog_send_queue_keep_data(polar_ringbuf_t queue);
extern void polar_xlog_send_queue_release_data_ref(void);
extern bool polar_xlog_send_queue_check(polar_ringbuf_ref_t *ref, XLogRecPtr start_point);

extern bool polar_xlog_recv_queue_push(polar_ringbuf_t queue, char *buf, size_t len, polar_interrupt_callback callback);
extern XLogRecord *polar_xlog_recv_queue_pop(polar_ringbuf_t queue, XLogReaderState *state, XLogRecPtr RecPtr, char **errormsg);
extern void polar_xlog_decode_data(XLogReaderState *state);
extern void polar_xlog_recv_queue_push_storage_begin(polar_ringbuf_t queue, polar_interrupt_callback callback);

extern XLogRecPtr polar_xlog_send_queue_next_lsn(polar_ringbuf_ref_t *ref, size_t *len);

extern bool polar_xlog_remove_payload(XLogRecord *record);

extern bool polar_xlog_queue_decode(XLogReaderState *state, XLogRecord *record, bool decode_payload, char **errormsg);
#endif
