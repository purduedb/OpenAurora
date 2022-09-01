/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_record.h
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
 *    src/include/polar_flashback/polar_flashback_log_record.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_RECORD_H
#define POLAR_FLASHBACK_LOG_RECORD_H
#include "access/xlogdefs.h"
#include "access/xlogrecord.h"
#include "polar_flashback/polar_flashback_log_internal.h"
#include "port/pg_crc32c.h"
#include "storage/buf_internals.h"

/*
 * Each page of flashback log file has a header like this:
 */
#define FLOG_PAGE_MAGIC   (0x07e4) /* can be used as flashback log version indicator */
#define FLOG_PAGE_VERSION (0x0001) /* flashback log page version for compatible */

typedef struct flog_page_header_data
{
	uint16      xlp_magic;      /* magic value for correctness checks */
	uint16      xlp_info;       /* flag bits, see below */
	uint16      xlp_version;    /* version for compatible check */
	polar_flog_rec_ptr  xlp_pageaddr;   /* the begin of flashback log address in this page */

	/*
	 * When there is not enough space on current page for whole record, we
	 * continue on the next page.  xlp_rem_len is the number of bytes
	 * remaining from a previous page.
	 *
	 * Note that xl_rem_len includes backup-block data; that is, it tracks
	 * xl_tot_len not xl_len in the initial header.  Also note that the
	 * continuation data isn't necessarily aligned.
	 */
	uint32      xlp_rem_len;    /* total len of remaining data for record */
} flog_page_header_data;

#define FLOG_SHORT_PHD_SIZE MAXALIGN(sizeof(flog_page_header_data))

typedef flog_page_header_data *flog_page_header;

/*
 * When the FLASHBACK_LOG_LONG_HEADER flag is set, we store additional fields in the
 * page header.  (This is ordinarily done just in the first page of an
 * flashback log file.) The additional fields serve to identify the file accurately.
 */
typedef struct flog_long_page_header_data
{
	flog_page_header_data std;      /* standard header fields */
	uint64      xlp_sysid;      /* system identifier from pg_control */
	uint32      xlp_seg_size;   /* just as a cross-check */
	uint32      xlp_blcksz; /* just as a cross-check */

} flog_long_page_header_data;

#define FLOG_LONG_PHD_SIZE MAXALIGN(sizeof(flog_long_page_header_data))

typedef flog_long_page_header_data *polar_long_page_header;

/* When record crosses page boundary, set this flag in new page's header */
#define FLOG_FIRST_IS_CONTRECORD 0x0001
/* This flag indicates a "long" page header */
#define FLOG_LONG_PAGE_HEADER         0x0002
/* All defined flag bits in xlp_info (used for validity checking of header) */
#define FLOG_ALL_FLAGS              0x0003

#define FLOG_PAGE_HEADER_SIZE(hdr)      \
	(((hdr)->xlp_info & FLOG_LONG_PAGE_HEADER) ? FLOG_LONG_PHD_SIZE : FLOG_SHORT_PHD_SIZE)

/* Make the invalid ptr to first record ptr in flashback log */
#define VALID_FLOG_PTR(ptr) \
	((ptr == POLAR_INVALID_FLOG_REC_PTR) ? FLOG_LONG_PHD_SIZE : ptr)

/* Check if an flog ptr value is in a plausible range */
#define FLOG_REC_PTR_IS_VAILD(ptr) \
	((ptr) % POLAR_FLOG_BLCKSZ >= FLOG_SHORT_PHD_SIZE)

/*
 * The overall layout of an flashback log record is:
 *      Fixed-size header (polar_flashback_log_record struct as the same as XLogRecord)
 *      record data:
 *        - xl_rmid = 0:
 *          - origin_page_rec_extra_info struct (if the origin page is empty, not contain the follow struct)
 *          - [Fixed-size block image header] (fl_rec_img_header struct)
 *          - [Fixed-size block image header] (fl_rec_img_comp_header struct if the origin page is compressed)
 *          - [page data] (may be compressed)
 *
 *  Now just use XLogRecord.
 */
typedef struct XLogRecord flog_record;
#define FLOG_REC_HEADER_SIZE    SizeOfXLogRecord

/* RMGR ID */
#define ORIGIN_PAGE_ID 0x00

/* Every thing for origin page record (xl_rmid = ORIGIN_PAGE_ID in the flashback log) */
/* xl_info */
#define ORIGIN_PAGE_EMPTY 0x00
#define ORIGIN_PAGE_FULL  0x01
#define FROM_ORIGIN_BUF   0x02 /* Is the origin page from origin bufffer */
#define ORIGIN_PAGE_TYPE_MASK 0x01

/* The record data for the origin page record */
typedef struct fl_origin_page_rec_data
{
	BufferTag   tag; /* The tag of the buffer */
	XLogRecPtr  redo_lsn; /* The redo point for the origin page */
} fl_origin_page_rec_data;

#define FL_ORIGIN_PAGE_REC_INFO_SIZE \
	(offsetof(fl_origin_page_rec_data, redo_lsn) + sizeof(XLogRecPtr))

/* Additional header information when a origin page image is included */
typedef struct XLogRecordBlockImageHeader fl_rec_img_header;

#define FL_REC_IMG_HEADER_SIZE  SizeOfXLogRecordBlockImageHeader

/* Information stored in fl_rec_img_header.bimg_info */
#define IMAGE_HAS_HOLE      0x01    /* page image has "hole" */
#define IMAGE_IS_COMPRESSED     0x02    /* page image is compressed */

/*
 * Extra header information used when page image has "hole" and
 * is compressed.
 */
typedef struct XLogRecordBlockCompressHeader fl_rec_img_comp_header;

#define FL_REC_IMG_COMP_HEADER_SIZE SizeOfXLogRecordBlockCompressHeader

#define FL_GET_ORIGIN_PAGE_REC_DATA(rec) \
	((fl_origin_page_rec_data *)((char *)rec + FLOG_REC_HEADER_SIZE))
#define FL_GET_ORIGIN_PAGE_IMG_HEADER(rec) \
	((fl_rec_img_header *)((char *)rec + FLOG_REC_HEADER_SIZE + FL_ORIGIN_PAGE_REC_INFO_SIZE))
#endif
