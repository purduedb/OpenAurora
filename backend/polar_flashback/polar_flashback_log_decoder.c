/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_decoder.c
 *    Implementation of flashback log decoder
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_log_decoder.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/polar_logindex_redo.h"
#include "access/xlog.h"
#include "common/pg_lzcompress.h"
#include "polar_flashback/polar_flashback_log.h"
#include "polar_flashback/polar_flashback_log_decoder.h"
#include "polar_flashback/polar_flashback_log_reader.h"
#include "polar_flashback/polar_flashback_log_record.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"

static bool
fl_decode_unempty_origin_page(flog_record *rec,
							  Page page, polar_flog_rec_ptr ptr)
{
	fl_origin_page_rec_data *rec_data;
	fl_rec_img_header *img;
	fl_rec_img_comp_header *c_img;
	char *origin_page;
	int record_data_len;
	PGAlignedBlock tmp;
	uint16 hole_length = 0;

	rec_data = FL_GET_ORIGIN_PAGE_REC_DATA(rec);
	img = FL_GET_ORIGIN_PAGE_IMG_HEADER(rec);
	origin_page = (char *)img + FL_REC_IMG_HEADER_SIZE;
	record_data_len = img->length;

	if (img->bimg_info & IMAGE_IS_COMPRESSED)
	{
		if (img->bimg_info & IMAGE_HAS_HOLE)
		{
			c_img = (fl_rec_img_comp_header *) origin_page;
			origin_page += FL_REC_IMG_COMP_HEADER_SIZE;
			hole_length = c_img->hole_length;
		}

		if (pglz_decompress(origin_page, record_data_len, tmp.data,
							BLCKSZ - hole_length) < 0)
		{
			/*no cover line*/
			elog(ERROR, "Invalid compressed origin page ([%u, %u, %u]), %u, %u, "
				 "from flashback log at %X/%X",
				 rec_data->tag.rnode.spcNode,
				 rec_data->tag.rnode.dbNode,
				 rec_data->tag.rnode.relNode,
				 rec_data->tag.forkNum,
				 rec_data->tag.blockNum,
				 (uint32)(ptr >> 32),
				 (uint32) ptr);
		}

		origin_page = tmp.data;
	}
	else if (img->bimg_info & IMAGE_HAS_HOLE)
		hole_length = BLCKSZ - img->length;

	/* generate page, taking into account hole if necessary */
	if (hole_length == 0)
		memcpy((char *)page, origin_page, BLCKSZ);
	else
	{
		memcpy((char *)page, origin_page, img->hole_offset);
		/* must zero-fill the hole */
		MemSet((char *)page + img->hole_offset, 0, hole_length);
		memcpy((char *)page + (img->hole_offset + hole_length),
			   origin_page + img->hole_offset,
			   BLCKSZ - (img->hole_offset + hole_length));
	}

	/* Checksum again */
	if (!PageIsVerified(page, rec_data->tag.forkNum, rec_data->tag.blockNum, NULL))
		/*no cover line*/
		elog(ERROR, "The checksum of origin page ([%u, %u, %u]), %u, %u, "
			 "from flashback log at %X/%X is wrong",
			 rec_data->tag.rnode.spcNode,
			 rec_data->tag.rnode.dbNode,
			 rec_data->tag.rnode.relNode,
			 rec_data->tag.forkNum,
			 rec_data->tag.blockNum,
			 (uint32)(ptr >> 32), (uint32) ptr);

	return true;
}

/*
 * POLAR: Decode the origin page flashback log record.
 * Check the checkpoint lsn and crc field.
 */
static bool
decode_origin_page_rec(polar_flog_rec_ptr ptr, Page page, XLogRecPtr *checkpoint_lsn,
		BufferTag *tag, flog_buf_ctl_t buf_ctl)
{
	uint8       info;
	bool        is_valid = false;
	flog_record *rec;
	flog_reader_state *reader;
	char       *errormsg;

	reader = polar_flog_reader_allocate(POLAR_FLOG_SEG_SIZE,
										&polar_flog_page_read, NULL, buf_ctl);

	if (reader == NULL)
		/*no cover line*/
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("Can not allocate the flashback log reader memory")));

	/* Read the flashback log record until the flashback log is invalid */
	rec = polar_read_flog_record(reader, ptr, &errormsg);

	if (rec != NULL && rec->xl_rmid == ORIGIN_PAGE_ID)
	{
		BufferTag tag_in_rec;

		tag_in_rec = FL_GET_ORIGIN_PAGE_REC_DATA(rec)->tag;

		if (!BUFFERTAGS_EQUAL(tag_in_rec, *tag))
			/*no cover line*/
			elog(ERROR, "The buffer tag flashback log record at %X/%X is "
				 "([%u, %u, %u]), %u, %u not ([%u, %u, %u]), %u, %u",
				 (uint32)(ptr >> 32), (uint32) ptr,
				 tag_in_rec.rnode.spcNode, tag_in_rec.rnode.dbNode,
				 tag_in_rec.rnode.relNode, tag_in_rec.forkNum,
				 tag_in_rec.blockNum, tag->rnode.spcNode, tag->rnode.dbNode,
				 tag->rnode.relNode, tag->forkNum, tag->blockNum);

		info = rec->xl_info;

		switch (info & ORIGIN_PAGE_TYPE_MASK)
		{
			case ORIGIN_PAGE_EMPTY:
				is_valid = true;
				PageInit(page, BLCKSZ, 0);
				break;

			case ORIGIN_PAGE_FULL:
				is_valid = fl_decode_unempty_origin_page(rec, page, ptr);
				break;

			default:
				/*no cover line*/
				elog(ERROR, "Parse flashback log rec: unknown op code %u", info);
		}
	}
	else if (rec == NULL)
		/*no cover line*/
		elog(ERROR, "The flashback log record at %X/%X is invaid with error: %s",
			 (uint32)(ptr >> 32),
			 (uint32) ptr, errormsg);
	else
		/*no cover line*/
		elog(ERROR, "The flashback log record at %X/%X not a origin page record, its rmid is "
			 "%d",
			 (uint32)(ptr >> 32),
			 (uint32) ptr, rec->xl_rmid);

	if (is_valid)
		*checkpoint_lsn = FL_GET_ORIGIN_PAGE_REC_DATA(rec)->redo_lsn;

	polar_flog_reader_free(reader);
	return is_valid;
}

/*
 * POLAR: Get the origin page in the time of checkpoint_lsn to page.
 *
 * tag: The bufferTag of the target page.
 * Page: The target page.
 * start_ptr: The start pointer of the flashback logindex to search.
 * end_ptr: The end pointer of the flashback logindex to search.
 * replay_start_lsn: The replay start WAL lsn.
 *
 * Return true when we get a right origin page.
 *
 * When is_partial_write is true, we will get the origin page whose checkpoint_lsn
 * is equal to the checkpoint_lsn. It means that the origin page is the origin version
 * before the partial write. So it can be used to repair the partial written page.
 *
 * When is_partial_write is false, we will get the origin page whose checkpoint_lsn
 * is larger than the checkpoint_lsn. It means that the origin page is the origin version
 * before the checkpoint_lsn. So it can be used to get a certain version at a certain time.
 *
 * The two scenarios above are not the same. Partial write is a known issue, we can repair it.
 * And now we don't like to repair issue unknown which may cause something unexpected.
 *
 * Please make true the end_ptr larger than start_ptr.
 *
 */
bool
polar_get_origin_page(flog_ctl_t instance, BufferTag *tag, Page page, polar_flog_rec_ptr start_ptr,
					  polar_flog_rec_ptr end_ptr, XLogRecPtr *replay_start_lsn)
{

	log_index_page_iter_t originpage_iter;
	log_index_lsn_t *lsn_info = NULL;
	polar_flog_rec_ptr ptr;
	bool found = false;
	logindex_snapshot_t snapshot = instance->logindex_snapshot;

	Assert(start_ptr < end_ptr);

	originpage_iter =
		polar_logindex_create_page_iterator(snapshot, tag, start_ptr, end_ptr - 1, false);

	if (polar_logindex_page_iterator_state(originpage_iter) != ITERATE_STATE_FINISHED)
	{
		/*no cover begin*/
		elog(ERROR, "Failed to iterate data for ([%u, %u, %u]), %u, %u flashback log, "
			 "which start pointer =%X/%X and end pointer =%X/%X",
			 tag->rnode.spcNode,
			 tag->rnode.dbNode,
			 tag->rnode.relNode,
			 tag->forkNum,
			 tag->blockNum,
			 (uint32)((start_ptr) >> 32), (uint32)start_ptr,
			 (uint32)((end_ptr - 1) >> 32), (uint32)(end_ptr - 1));
		return false;
		/*no cover end*/
	}

	if ((lsn_info = polar_logindex_page_iterator_next(originpage_iter)) != NULL)
	{
		ptr = (polar_flog_rec_ptr) lsn_info->lsn;
		Assert(BUFFERTAGS_EQUAL(*(lsn_info->tag), *tag));
		found = decode_origin_page_rec(ptr, page, replay_start_lsn, tag, instance->buf_ctl);
	}

	polar_logindex_release_page_iterator(originpage_iter);

	if (!found)
	{
		/*no cover line*/
		elog(LOG, "Can't find a valid origin page for page ([%u, %u, %u]), %u, %u "
			 "with flashback log start location %X/%X and end location %X/%X",
			 tag->rnode.spcNode,
			 tag->rnode.dbNode,
			 tag->rnode.relNode,
			 tag->forkNum,
			 tag->blockNum,
			 (uint32)((start_ptr) >> 32), (uint32)start_ptr,
			 (uint32)((end_ptr - 1) >> 32), (uint32)(end_ptr - 1));
	}
	else if (unlikely(polar_flashback_log_debug))
	{
		*replay_start_lsn = Max(*replay_start_lsn, PageGetLSN(page));
		elog(LOG, "We find a valid origin page for page ([%u, %u, %u]), %u, %u "
			 "with flashback log start location %X/%X and end location %X/%X, its "
			 "WAL replay start lsn is %X/%X",
			 tag->rnode.spcNode,
			 tag->rnode.dbNode,
			 tag->rnode.relNode,
			 tag->forkNum,
			 tag->blockNum,
			 (uint32)((start_ptr) >> 32), (uint32)start_ptr,
			 (uint32)((end_ptr - 1) >> 32), (uint32)(end_ptr - 1),
			 (uint32)(*replay_start_lsn >> 32), (uint32)(*replay_start_lsn));
	}

	return found;
}

/*
 * POLAR: Flashback the buffer.
 * instance: flashback log instance.
 * buf: The buffer to flashback.
 * tag: The origin page buffer tag.
 * start_ptr: The flashback log start point to search origin page.
 * end_ptr: The flashback log end point to search origin page.
 * start_lsn: The WAL start lsn to replay.
 * end_lsn: The WAL end lsn to replay.
 * elevel: Log level.
 * apply_fpi: apply full page image or not.
 *
 * NB: Please make sure end_ptr >= start_ptr.
 */
bool
polar_flashback_buffer(flog_ctl_t instance, Buffer *buf, BufferTag *tag,
		polar_flog_rec_ptr start_ptr, polar_flog_rec_ptr end_ptr,
		XLogRecPtr start_lsn, XLogRecPtr end_lsn, int elevel,
		bool apply_fpi)
{
	Page page;
	XLogRecPtr lsn = InvalidXLogRecPtr;
	BufferDesc *buf_desc;

	if (start_ptr == end_ptr)
	{
		elog(WARNING, "The page ([%u, %u, %u]), %u, %u has no origin page between flashback log "
				"%X/%X and %X/%X",
				tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->forkNum, tag->blockNum,
				(uint32)(start_ptr >> 32), (uint32) start_ptr,
				(uint32)(end_ptr >> 32), (uint32) end_ptr);
		return false;
	}
	else if (start_ptr > end_ptr)
	{
		/*no cover begin*/
		elog(ERROR, "The range to flashback page ([%u, %u, %u]), %u, %u is wrong, "
				"the flashback log start pointer %X/%X, the flashback log end pointer %X/%X",
				tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->forkNum, tag->blockNum,
				(uint32)(start_ptr >> 32), (uint32) start_ptr,
				(uint32)(end_ptr >> 32), (uint32) end_ptr);
		/*no cover end*/
	}

	/* Disable the flashback log for the buffer in the flashback. */
	buf_desc = GetBufferDescriptor(*buf - 1);
	set_buf_flog_state(buf_desc, POLAR_BUF_FLOG_DISABLE);

	page = BufferGetPage(*buf);

	if (!polar_get_origin_page(instance, tag, page, start_ptr, end_ptr, &lsn))
	{
		/* If not found, check its first modify is a XLOG_FPI_MULTI/XLOG_FPI/XLOG_FPI_FOR_HINT record? */
		lsn = polar_logindex_find_first_fpi(polar_logindex_redo_instance,
						start_lsn, end_lsn, tag, buf, apply_fpi);

		if (!XLogRecPtrIsInvalid(lsn))
		{
			elog(LOG, "The first modify of ([%u, %u, %u]), %u, %u after %X/%X "
					"is a new full page image, its origin page is a empty page or the image",
					 tag->rnode.spcNode,
					 tag->rnode.dbNode,
					 tag->rnode.relNode,
					 tag->forkNum,
					 tag->blockNum,
					 (uint32)(start_lsn >> 32), (uint32) start_lsn);

			if (apply_fpi)
				lsn = PageGetLSN(page);
			else
				MemSet(page, 0, BLCKSZ);
		}
		else
		{
			elog(elevel, "Can't find a valid origin page for ([%u, %u, %u]), %u, %u from flashback log",
				 tag->rnode.spcNode,
				 tag->rnode.dbNode,
				 tag->rnode.relNode,
				 tag->forkNum,
				 tag->blockNum);
			return false;
		}
	}

	Assert(!XLogRecPtrIsInvalid(lsn));

	if (unlikely(polar_flashback_log_debug))
	{
		elog(LOG, "The origin page ([%u, %u, %u]), %u, %u need to replay from %X/%X to %X/%X",
			 tag->rnode.spcNode,
			 tag->rnode.dbNode,
			 tag->rnode.relNode,
			 tag->forkNum,
			 tag->blockNum,
			 (uint32) (lsn >> 32), (uint32) lsn,
			 (uint32) (end_lsn >> 32), (uint32) end_lsn);
	}

	/* The lsn can be larger than or equal to end_lsn, it means no WAL record to replay */
	polar_logindex_apply_page(polar_logindex_redo_instance, lsn, end_lsn, tag, buf);
	return true;
}
