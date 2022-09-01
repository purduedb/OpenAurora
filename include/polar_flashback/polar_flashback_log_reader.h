/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_reader.h
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
 *   src/include/polar_flashback/polar_flashback_log_reader.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_READER_H
#define POLAR_FLASHBACK_LOG_READER_H
#include "polar_flashback/polar_flashback_log_mem.h"
#include "polar_flashback/polar_flashback_log_record.h"

#define REC_UNFLUSHED_ERROR_MSG "The flashback log record is not flushed, please read in next time"

typedef struct flog_reader_state flog_reader_state;

/* Function type definition for the read_page callback */
typedef int (*page_read_callback)(flog_reader_state *reader,
								  polar_flog_rec_ptr targetPagePtr, int reqLen,
								  polar_flog_rec_ptr targetRecPtr, char *read_buf);

struct flog_reader_state
{
	/* ----------------------------------------
	 * Public parameters
	 * ----------------------------------------
	 */

	/*
	 * Segment size of the to-be-parsed data (mandatory).
	 */
	int         segment_size;

	/*
	 * Data input callback (mandatory).
	 *
	 * This callback shall read at least reqLen valid bytes of the flashback log page
	 * starting at targetPagePtr, and store them in readBuf.  The callback
	 * shall return the number of bytes read (never more than POLAR_FLASHBACK_LOG_BLCKSZ), or
	 * -1 on failure.  The callback shall sleep, if necessary, to wait for the
	 * requested bytes to become available.  The callback will not be invoked
	 * again for the same page unless more than the returned number of bytes
	 * are needed.
	 */
	page_read_callback read_page;

	/*
	 * System identifier of the flashback log files we're about to read.  Set to zero
	 * (the default value) if unknown or unimportant.
	 */
	uint64      system_identifier;

	/*
	 * Opaque data for callbacks to use.
	 */
	void       *private_data;

	/*
	 * Start and end point of last record read.  EndRecPtr is also used as the
	 * position to read next, if polar_read_flashback_log_record receives an invalid recptr.
	 */
	polar_flog_rec_ptr  read_rec_ptr;       /* start of last record read */
	polar_flog_rec_ptr  end_rec_ptr;        /* end+1 of last record read */

	/* ----------------------------------------
	 * private/internal state
	 * ----------------------------------------
	 */

	/*
	 * Buffer for currently read page (POLAR_FLASHBACK_LOG_BLCKSZ bytes, valid up to at least
	 * readLen bytes)
	 */
	char       *read_buf;
	uint32      read_len;

	/* last read segment, segment offset for data currently in readBuf */
	uint64  read_seg_no;
	uint32  read_off;

	/*
	 * beginning of prior page read.
	 */
	polar_flog_rec_ptr  latest_page_ptr;

	/* beginning of the flashback log record being read. */
	polar_flog_rec_ptr  curr_rec_ptr;

	/* Buffer for current ReadRecord result (expandable) */
	char       *read_record_buf;
	uint32      read_record_buf_size;

	/* Buffer to hold error message */
	char       *errormsg_buf;
	List       *switch_ptrs;

	/* Is the record in the switch point region ? */
	bool      in_switch_region;

	flog_buf_ctl_t flog_buf_ctl; /* The point to flog buf ctl */
};

extern flog_record *polar_read_flog_record(flog_reader_state *state,
										   polar_flog_rec_ptr RecPtr, char **errormsg);

extern int polar_flog_page_read(flog_reader_state *state,
								polar_flog_rec_ptr targetPagePtr, int reqLen,
								polar_flog_rec_ptr targetRecPtr, char *cur_page);

extern flog_reader_state *polar_flog_reader_allocate(int segment_size, page_read_callback pagereadfunc, void *private_data, flog_buf_ctl_t flog_buf_ctl);
extern void polar_flog_reader_free(flog_reader_state *state);
extern bool polar_is_flog_rec_ignore(polar_flog_rec_ptr *ptr, uint32 log_len, flog_reader_state *reader);
#endif
