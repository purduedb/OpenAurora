/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_file.h
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
 *    src/include/polar_flashback/polar_flashback_log_file.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_FILE_H
#define POLAR_FLASHBACK_LOG_FILE_H
#include "access/xlogdefs.h"
#include "polar_flashback/polar_flashback_log_mem.h"

/* Something about flashback log ctl file */
#define FLOG_CTL_FILE_VERSION 0x0001
#define FLOG_CTL_FILE "flashback_log_control"

/* Something about flashback log history file */
#define FLOG_HISTORY_FILE "flashback_log.history"
#define FLOG_TMP_HISTORY_FILE "flashback_log.history.tmp"

#define FLOG_MAX_FNAME_LEN      (32)
/* Length of flashback log file name */
#define FLOG_FNAME_LEN     (24)

#define FLOG_DEFAULT_TIMELINE (1)

#define HISTORY_FILE_FIELDS (5)

/*
 * Is an flashback log ptr within a particular segment?
 *
 * For ptr_in_flog_seg, do the computation at face value.
 * For ptr_prev_in_flog_seg, a boundary byte is taken to be in the previous segment.
 */
#define ptr_in_flog_seg(ptr, seg_no, segsz_bytes) \
	(((ptr) / (segsz_bytes)) == (seg_no))

#define ptr_prev_in_flog_seg(ptr, seg_no, segsz_bytes) \
	((((ptr) - 1) / (segsz_bytes)) == (seg_no))

#define segs_per_flog_id(segsz_bytes)   \
	(UINT64CONST(0x100000000) / (segsz_bytes))

#define get_flog_fname(fname, seg_no, segsz_bytes, tli) \
	snprintf(fname, FLOG_MAX_FNAME_LEN, "%08X%08X%08X", \
			 tli, (uint32) ((seg_no) / segs_per_flog_id(segsz_bytes)), \
			 (uint32) ((seg_no) % segs_per_flog_id(segsz_bytes)))

#define get_flog_seg_from_fname(fname, seg_no, seg_size) \
	do {                                                \
		uint32 log;                                     \
		uint32 seg;                                     \
		TimeLineID tli;                                 \
		sscanf(fname, "%08X%08X%08X", &tli, &log, &seg); \
		*seg_no = (uint64) log * segs_per_flog_id(seg_size) + seg; \
	} while (0)

#define is_flashback_log_file(fname) \
	(strlen(fname) == FLOG_FNAME_LEN && \
	 strspn(fname, "0123456789ABCDEF") == FLOG_FNAME_LEN)

/*
 * These are the number of bytes in a flashback log page usable for flashback log data.
 */
#define USABLE_BYTES_IN_PAGE (POLAR_FLOG_BLCKSZ - FLOG_SHORT_PHD_SIZE)
#define USABLE_BYTES_IN_SEG \
	((POLAR_FLOG_SEG_SIZE / POLAR_FLOG_BLCKSZ * USABLE_BYTES_IN_PAGE) - (FLOG_LONG_PHD_SIZE - FLOG_SHORT_PHD_SIZE))

/*
 * Compute a segment number from an flashback_log_rec_ptr.
 *
 * For flog_ptr_to_seg, do the computation at face value.
 * For flog_ptr_to_seg, a boundary byte is taken to
 * be in the previous segment. This is suitable for deciding which segment
 * to write given a pointer to a record end, for example.
 */
#define flog_ptr_to_seg(ptr, segsz_bytes) \
	((ptr) / (segsz_bytes))

#define flog_ptr_prev_to_seg(ptr, segsz_bytes) \
	(((ptr) - 1) / (segsz_bytes))

typedef struct flog_ctl_file_data_t
{
	uint16  version_no; /* upper 8 bits are flashback log state, lower 8 bits are version no */
	fbpoint_info_data_t fbpoint_info;
	uint64      max_seg_no; /* The max segment no */
	TimeLineID  tli;
	pg_crc32c   crc;
} flog_ctl_file_data_t;

#define FLOG_CTL_VERSION_MASK 0x0011
#define FLOG_SHUTDOWNED 0x1000

typedef struct flog_history_entry
{
	TimeLineID tli;
	polar_flog_rec_ptr  switch_ptr;         /* the start of switch pointer */
	polar_flog_rec_ptr  next_ptr;           /* the next pointer */
} flog_history_entry;

extern polar_flog_rec_ptr polar_flog_pos2ptr(uint64 bytepos);
extern polar_flog_rec_ptr polar_flog_pos2endptr(uint64 bytepos);
extern uint64 polar_flog_ptr2pos(polar_flog_rec_ptr ptr);

extern void polar_write_flog_ctl_file(flog_buf_ctl_t ctl);
extern bool polar_read_flog_ctl_file(flog_buf_ctl_t ctl, flog_ctl_file_data_t *ctl_file_data);
extern void polar_flush_fbpoint_info(flog_buf_ctl_t ctl,
									  polar_flog_rec_ptr flashback_log_ptr_ckp_start,
									  polar_flog_rec_ptr flashback_log_ptr_ckp_end,
									  polar_flog_rec_ptr flashback_log_ptr_ckp_end_prev);
extern void polar_flush_flog_max_seg_no(flog_buf_ctl_t ctl, uint64 seg_no);

extern void polar_validate_flog_dir(flog_buf_ctl_t ctl);
extern void polar_flog_clean_dir_internal(const char *dir_path);
extern void polar_flog_remove_all(flog_buf_ctl_t ctl);

extern bool polar_is_flog_file_exist(const char *dir, polar_flog_rec_ptr ptr, int elevel);

extern int polar_flog_file_open(uint64 segno, const char *dir);
extern int polar_flog_file_init(flog_buf_ctl_t ctl, uint64 logsegno, bool *use_existent);

extern void polar_flog_read(char *buf, int segsize, polar_flog_rec_ptr startptr, Size count, const char *dir);

extern void polar_truncate_flog_before(flog_buf_ctl_t ctl, polar_flog_rec_ptr ptr);
extern void polar_prealloc_flog_files(flog_buf_ctl_t ctl, int num);

extern void polar_write_flog_history_file(const char *dir, TimeLineID tli, polar_flog_rec_ptr switch_ptr, polar_flog_rec_ptr next_ptr);
extern List *polar_read_flog_history_file(const char *dir);

extern polar_flog_rec_ptr convert_to_first_valid_ptr(polar_flog_rec_ptr page_ptr);
extern polar_flog_rec_ptr polar_get_next_flog_ptr(polar_flog_rec_ptr ptr, uint32 log_len);

extern void polar_remove_tmp_flog_file(const char *dir);
#endif
