/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_internal.h
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
 *    src/include/polar_flashback/polar_flashback_log_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_INTERNAL_H
#define POLAR_FLASHBACK_LOG_INTERNAL_H

/* Must be a multiple of the alignment requirement for direct-I/O buffers */
#define POLAR_FLOG_BLCKSZ (8*1024)
/* Must be a power of 2 */
#define POLAR_FLOG_SEG_SIZE (256*1024*1024)
#define POLAR_INVALID_FLOG_REC_PTR 0
#define POLAR_INVALID_FLOG_SEGNO (0xFFFFFFFFFFFFFFFF)

/* Default values about GUCs */
#define POLAR_FLOG_DEFAULT_BUFFERS 2048
#define POLAR_FLOG_DEFAULT_BUFF_INSERT_LOCKS 8

typedef uint64 polar_flog_rec_ptr;

/* GUCs */
extern bool polar_enable_flashback_log;
extern bool polar_has_partial_write;
extern int polar_flashback_log_keep_segments;
extern int polar_flashback_log_bgwrite_delay;
extern int polar_flashback_log_insert_list_max_num;
extern int polar_flashback_log_flush_max_size;
extern bool polar_flashback_log_debug;
extern int polar_flashback_log_sync_buf_timeout;
extern int polar_flashback_log_insert_list_delay;

extern int polar_flashback_point_segments;
extern int polar_flashback_point_timeout;

#define FLOG_SEGMENT_OFFSET(ptr, segsz_bytes)   \
	((ptr) & ((segsz_bytes) - 1))

#define flog_seg_offset_to_ptr(segno, offset, segsz_bytes, dest) \
	(dest) = (segno) * (segsz_bytes) + (offset)

/*
 * Flashback log default dir.
 * The flashback logindex default dir is POLAR_FL_DEFAULT_DIR + '_index'.
 */
#define POLAR_FL_DEFAULT_DIR "polar_flog"
#define POLAR_FL_INDEX_DEFAULT_DIR "polar_flog_index"

#define FL_INS_MAX_NAME_LEN 32
#define FL_OBJ_MAX_NAME_LEN (2 * FL_INS_MAX_NAME_LEN)

#define FLOG_REC_PTR_IS_INVAILD(ptr) \
	(ptr == POLAR_INVALID_FLOG_REC_PTR)

#define FLOG_GET_OBJ_NAME(dst_name, src_name, suffix) \
	snprintf(dst_name, FL_OBJ_MAX_NAME_LEN, "%s%s", src_name, suffix);

/*
 * FLOG_PTR2BUF_IDX returns the index of the flashback log buffer that holds, or
 * would hold if it was in cache, the page containing 'recptr'.
 */
#define FLOG_PTR2BUF_IDX(rec_ptr, ctl)  \
	(((rec_ptr) / POLAR_FLOG_BLCKSZ) % (ctl->cache_blck + 1))

typedef enum
{
	FLOG_BUF_INIT,
	FLOG_BUF_CRASH_RECOVERY,
	FLOG_BUF_SHUTDOWN_RECOVERY,
	FLOG_BUF_READY,
	FLOG_BUF_SHUTDOWNED
} flog_buf_state;

typedef enum
{
	/* FLOG_INIT must be first */
	FLOG_INIT,
	FLOG_STARTUP,
	FLOG_READY
} flashback_state;

#endif
