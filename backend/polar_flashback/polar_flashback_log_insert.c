/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_insert.c
 *
 *
<<<<<<< HEAD
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
=======
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
>>>>>>> logindex_flashback_datamax_opensource
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_log_insert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "common/pg_lzcompress.h"
#include "miscadmin.h"
#include "polar_flashback/polar_flashback_log_insert.h"
#include "polar_flashback/polar_flashback_log_list.h"
#include "polar_flashback/polar_flashback_point.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "utils/guc.h"
#include "utils/memutils.h"

/* Buffer size required to store a compressed version of backup block image */
#define PGLZ_MAX_BLCKSZ PGLZ_MAX_OUTPUT(BLCKSZ)

/* GUCs */
bool polar_flashback_log_debug;

static void
log_flog_rec(flog_record *record, polar_flog_rec_ptr ptr)
{
#define MAX_EXTRA_INFO_SIZE 512

	char extra_info[MAX_EXTRA_INFO_SIZE];
	fl_origin_page_rec_data *info;

	switch (record->xl_rmid)
	{
		case ORIGIN_PAGE_ID:
			info = FL_GET_ORIGIN_PAGE_REC_DATA(record);
			snprintf(extra_info, MAX_EXTRA_INFO_SIZE, "It is a origin page record, "
					 "the origin page is %s page. The redo lsn of the origin page is %X/%X, "
					 "the page tag is [%u, %u, %u], %d, %u",
					 (record->xl_info == ORIGIN_PAGE_EMPTY ? "empty" : "not empty"),
					 (uint32)(info->redo_lsn >> 32), (uint32)(info->redo_lsn),
					 info->tag.rnode.spcNode, info->tag.rnode.dbNode, info->tag.rnode.relNode,
					 info->tag.forkNum, info->tag.blockNum);
			break;

		default:
			/*no cover begin*/
			elog(ERROR, "The type of the record %X/%08X is wrong\n",
				 (uint32)(ptr >> 32), (uint32)ptr);
			break;
			/*no cover end*/
	}

	elog(LOG, "Insert a flashback log record at %X/%X: total length is %u, "
		 "the previous pointer is %X/%X. %s",
		 (uint32)(ptr >> 32), (uint32)ptr, record->xl_tot_len,
		 (uint32)(record->xl_prev >> 32), (uint32)(record->xl_prev), extra_info);
}

/*
 * Assemble a flashback log record from the buffers into an
 * polar_flashback_log_record, ready for insertion with
 * polar_flashback_log_insert_record().
 *
 * tag is the buffer tag of buffer which is a origin page.
 * redo_ptr is the last checkpoint XLOG record pointer.
 *
 * Return the flashback log record which palloc in this function.
 * The caller can switch the memory context and pfree.
 *
 * The record will be contain the (compressed) block data.
 * The record header fields are filled in, except for the CRC field and
 * previous pointer.
 *
 */
static flog_record *
flog_rec_assemble(flog_insert_context insert_context)
{
	bool   include_origin_page = false;
	Page   page = NULL;
	fl_rec_img_header b_img = {0, 0, 0};
	fl_rec_img_comp_header cb_img = {0};
	flog_record *rec;
	char data[PGLZ_MAX_BLCKSZ];
	uint16 data_len = BLCKSZ;
	uint32 xl_tot_len = FLOG_REC_HEADER_SIZE;
	bool is_empty_page = true;
	bool is_compressed = false;
	bool from_origin_buf = false;

	if (insert_context.rmgr == ORIGIN_PAGE_ID)
		include_origin_page = true;

	if (include_origin_page)
	{
		BufferTag *tag;
		bool need_checksum_again;

		need_checksum_again = from_origin_buf = insert_context.info & FROM_ORIGIN_BUF;
		tag = insert_context.buf_tag;
		page = insert_context.origin_page;
		xl_tot_len += FL_ORIGIN_PAGE_REC_INFO_SIZE;

		if (!PageIsNew(page) && !polar_page_is_just_inited(page))
		{
			uint16      lower;
			uint16      upper;

			/* Assume we can omit data between pd_lower and pd_upper */
			lower = ((PageHeader) page)->pd_lower;
			upper = ((PageHeader) page)->pd_upper;

			if (lower >= SizeOfPageHeaderData &&
					upper > lower &&
					upper <= BLCKSZ)
			{
				b_img.hole_offset = lower;
				b_img.bimg_info |= IMAGE_HAS_HOLE;
				cb_img.hole_length = upper - lower;
				/*
				 * Check the checksum before compute it again, so we will
				 * not change the rightness of checksum.
				 *
				 * When it is from origin buffer, the checksum may be wrong,
				 * so we don't check the pages from origin page buffer.
				 *
				 * We do nothing while the checksum is wrong here, but
				 * the decoder will verify the page.
				 */
				if (!from_origin_buf)
					need_checksum_again =
						(pg_checksum_page((char *) page, tag->blockNum) == ((PageHeader) page)->pd_checksum);
			}
			else
			{
				/* No "hole" to compress out */
				b_img.hole_offset = 0;
				cb_img.hole_length = 0;
			}

			/* Compute checksum again */
			if (need_checksum_again && DataChecksumsEnabled())
			{
				MemSet((char *)page + b_img.hole_offset, 0, cb_img.hole_length);
				((PageHeader) page)->pd_checksum = pg_checksum_page((char *) page, tag->blockNum);
			}

			/* Try to compress flashback log */
			is_compressed = polar_compress_block_in_log(page, b_img.hole_offset, cb_img.hole_length,
														data, &data_len, FL_REC_IMG_COMP_HEADER_SIZE);

			if (is_compressed)
			{
				b_img.bimg_info |= IMAGE_IS_COMPRESSED;
				b_img.length = data_len;

				if (cb_img.hole_length != 0)
					xl_tot_len += FL_REC_IMG_COMP_HEADER_SIZE;
			}
			else
				b_img.length = BLCKSZ - cb_img.hole_length;

			xl_tot_len += FL_REC_IMG_HEADER_SIZE + b_img.length;
			is_empty_page = false;
		}
	}

	/* Construct the flashback log record */
	rec = palloc(xl_tot_len);
	rec->xl_tot_len = xl_tot_len;
	rec->xl_prev = POLAR_INVALID_FLOG_REC_PTR;
	rec->xl_rmid = insert_context.rmgr;
	rec->xl_info = insert_context.info;
	rec->xl_xid = 0;

	if (include_origin_page)
	{
		fl_origin_page_rec_data rec_data;
		char    *scratch = (char *)rec + FLOG_REC_HEADER_SIZE;

		/* Copy the record data for the origin page. */
		rec_data.redo_lsn = insert_context.redo_lsn;
		INIT_BUFFERTAG(rec_data.tag, insert_context.buf_tag->rnode,
				insert_context.buf_tag->forkNum, insert_context.buf_tag->blockNum);
		memcpy(scratch, &rec_data, FL_ORIGIN_PAGE_REC_INFO_SIZE);
		scratch += FL_ORIGIN_PAGE_REC_INFO_SIZE;

		if (is_empty_page)
		{
			Assert(xl_tot_len == (FLOG_REC_HEADER_SIZE + FL_ORIGIN_PAGE_REC_INFO_SIZE));
			rec->xl_info = ORIGIN_PAGE_EMPTY;
		}
		else
		{
			Assert(xl_tot_len >= FLOG_REC_HEADER_SIZE + FL_ORIGIN_PAGE_REC_INFO_SIZE +
				   FL_REC_IMG_HEADER_SIZE + b_img.length);

			rec->xl_info = ORIGIN_PAGE_FULL;
			memcpy(scratch, &b_img, FL_REC_IMG_HEADER_SIZE);
			scratch += FL_REC_IMG_HEADER_SIZE;

			if (is_compressed)
			{
				if (cb_img.hole_length != 0)
				{
					memcpy(scratch, &cb_img, FL_REC_IMG_COMP_HEADER_SIZE);
					scratch += FL_REC_IMG_COMP_HEADER_SIZE;
					Assert(xl_tot_len == FLOG_REC_HEADER_SIZE + FL_ORIGIN_PAGE_REC_INFO_SIZE +
						   FL_REC_IMG_HEADER_SIZE + FL_REC_IMG_COMP_HEADER_SIZE + b_img.length);
				}

				memcpy(scratch, data, b_img.length);
			}
			else
			{
				Assert(xl_tot_len == FLOG_REC_HEADER_SIZE + FL_ORIGIN_PAGE_REC_INFO_SIZE +
					   FL_REC_IMG_HEADER_SIZE + b_img.length);

				if (cb_img.hole_length != 0)
				{
					Assert(b_img.length < BLCKSZ);
					Assert(b_img.hole_offset >= SizeOfPageHeaderData);

					memcpy(scratch, (char *)page, b_img.hole_offset);
					scratch += b_img.hole_offset;
					memcpy(scratch, (char *)page + b_img.hole_offset + cb_img.hole_length,
						   b_img.length - b_img.hole_offset);
				}
				else
				{
					Assert(b_img.length == BLCKSZ);
					memcpy(scratch, (char *)page, b_img.length);
				}
			}
		}

		/* Record the page come from origin buffer */
		if (from_origin_buf)
			rec->xl_info |= FROM_ORIGIN_BUF;
	}

	return rec;
}

/*
 * POLAR: Insert a flashback log record to flashback log shared buffer.
 *
 * insert_context: Everything about the insertion.
 */
polar_flog_rec_ptr
polar_flog_insert_into_buffer(flog_buf_ctl_t buf_ctl, flog_index_queue_ctl_t queue_ctl,
							  flog_insert_context insert_context)
{
	polar_flog_rec_ptr start_ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr end_ptr = POLAR_INVALID_FLOG_REC_PTR;
	flog_record *rec = NULL;

	/* Assemble the flashback log record without the previous pointer and the CRC field */
	rec = flog_rec_assemble(insert_context);
	/*
	 * Fill the previous pointer and the CRC field and insert the flashback log record to flashback log
	 * shared buffers.
	 */
	end_ptr = polar_flog_rec_insert(buf_ctl, queue_ctl, rec, &start_ptr);

	if (unlikely(polar_flashback_log_debug))
		log_flog_rec(rec, start_ptr);

	pfree(rec);

	return end_ptr;
}

polar_flog_rec_ptr
polar_insert_buf_flog_rec(flog_buf_ctl_t buf_ctl, flog_index_queue_ctl_t queue_ctl, BufferTag *tag,
		XLogRecPtr redo_lsn, XLogRecPtr fbpoint_lsn, uint8 info, Page origin_page, bool from_origin_buf)
{
	flog_insert_context insert_context;

	/*
	 * It is candidate, check it in here.
	 */
	if ((info & FLOG_LIST_SLOT_CANDIDATE) && PageGetLSN(origin_page) > fbpoint_lsn)
		return POLAR_INVALID_FLOG_REC_PTR;

	Assert(PageGetLSN(origin_page) <= fbpoint_lsn);

	/* Construct the insert context */
	insert_context.buf_tag = tag;
	insert_context.origin_page = (Page)origin_page;
	insert_context.redo_lsn = redo_lsn;
	insert_context.rmgr = ORIGIN_PAGE_ID;
	/* This will be update in the flashback log record */
	insert_context.info = ORIGIN_PAGE_FULL;

	if (from_origin_buf)
		insert_context.info |= FROM_ORIGIN_BUF;

	return polar_flog_insert_into_buffer(buf_ctl, queue_ctl, insert_context);
}
