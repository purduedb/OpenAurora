/*-------------------------------------------------------------------------
 *
 * polar_brin_xlog_idx.c
 *    WAL redo parse logic for brin index.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 * 
 * IDENTIFICATION
 *           src/backend/access/logindex/polar_brin_xlog_idx.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/brin_page.h"
#include "access/brin_pageops.h"
#include "access/brin_tuple.h"
#include "access/brin_xlog.h"
#include "access/bufmask.h"
#include "access/polar_logindex_redo.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "storage/buf_internals.h"

static void
polar_brin_xlog_insert_update_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	polar_logindex_redo_parse(instance, record, 0);

	polar_logindex_redo_parse(instance, record, 1);
}

static void
polar_brin_xlog_insert_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	polar_brin_xlog_insert_update_parse(instance, record);
}

static void
polar_brin_xlog_update_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	BufferTag old_tag;
	polar_page_lock_t  old_lock;
	Buffer  old_buf;

	POLAR_MINI_TRANS_REDO_PARSE(instance, record, 2, old_tag, old_lock, old_buf);

	polar_brin_xlog_insert_update_parse(instance, record);

	if (BufferIsValid(old_buf))
		UnlockReleaseBuffer(old_buf);

	polar_logindex_mini_trans_unlock(instance->mini_trans, old_lock);

}

static void
polar_brin_xlog_revmap_extend_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	BufferTag meta_tag;
	polar_page_lock_t    meta_lock;
	Buffer    meta_buf;

	POLAR_MINI_TRANS_REDO_PARSE(instance, record, 0, meta_tag, meta_lock, meta_buf);

	polar_logindex_redo_parse(instance, record, 1);

	if (BufferIsValid(meta_buf))
		UnlockReleaseBuffer(meta_buf);

	polar_logindex_mini_trans_unlock(instance->mini_trans, meta_lock);
}

/*
 * xlog replay routines
 */
static XLogRedoAction
polar_brin_xlog_createidx(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_brin_createidx *xlrec = (xl_brin_createidx *) XLogRecGetData(record);
	Page        page;
	BufferTag   meta_tag;

	POLAR_GET_LOG_TAG(record, meta_tag, 0);

	if (!BUFFERTAGS_EQUAL(meta_tag, *tag))
		return BLK_NOTFOUND;

	/* create the index' metapage */
	POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
	Assert(BufferIsValid(*buffer));
	page = (Page) BufferGetPage(*buffer);
	brin_metapage_init(page, xlrec->pagesPerRange, xlrec->version);
	PageSetLSN(page, lsn);

	return BLK_NEEDS_REDO;
}

/*
 * Common part of an insert or update. Inserts the new tuple and updates the
 * revmap.
 */
static XLogRedoAction
polar_brin_xlog_insert_update(XLogReaderState *record,
							  xl_brin_insert *xlrec, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	XLogRedoAction action = BLK_NOTFOUND;
	Page        page;
	BufferTag   index_tag, revmap_tag;;

	POLAR_GET_LOG_TAG(record, index_tag, 0);

	if (BUFFERTAGS_EQUAL(*tag, index_tag))
	{
		/*
		 * If we inserted the first and only tuple on the page, re-initialize the
		 * page from scratch.
		 */
		if (XLogRecGetInfo(record) & XLOG_BRIN_INIT_PAGE)
		{
			POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
			page = BufferGetPage(*buffer);
			brin_page_init(page, BRIN_PAGETYPE_REGULAR);
			action = BLK_NEEDS_REDO;
		}
		else
			action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		/* insert the index item into the page */
		if (action == BLK_NEEDS_REDO)
		{
			OffsetNumber offnum;
			BrinTuple  *tuple;
			Size        tuplen;

			tuple = (BrinTuple *) XLogRecGetBlockData(record, 0, &tuplen);

			Assert(tuple->bt_blkno == xlrec->heapBlk);

			page = (Page) BufferGetPage(*buffer);
			offnum = xlrec->offnum;

			if (PageGetMaxOffsetNumber(page) + 1 < offnum)
			{
				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "polar_brin_xlog_insert_update: invalid max offset number, page_max_off=%ld, offnum=%d",
					 PageGetMaxOffsetNumber(page), offnum);
			}

			offnum = PageAddItem(page, (Item) tuple, tuplen, offnum, true, false);

			if (offnum == InvalidOffsetNumber)
			{
				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "polar_brin_xlog_insert_update: failed to add tuple");
			}

			PageSetLSN(page, lsn);
		}

		return action;
	}

	POLAR_GET_LOG_TAG(record, revmap_tag, 1);

	if (BUFFERTAGS_EQUAL(*tag, revmap_tag))
	{
		/* update the revmap */
		action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			ItemPointerData tid;

			ItemPointerSet(&tid, index_tag.blockNum, xlrec->offnum);
			page = (Page) BufferGetPage(*buffer);

			brinSetHeapBlockItemptr(*buffer, xlrec->pagesPerRange, xlrec->heapBlk,
									tid);
			PageSetLSN(page, lsn);
		}
	}

	return action;
}

/*
 * replay a BRIN index insertion
 */
static XLogRedoAction
polar_brin_xlog_insert(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	xl_brin_insert *xlrec = (xl_brin_insert *) XLogRecGetData(record);

	return polar_brin_xlog_insert_update(record, xlrec, tag, buffer);
}

/*
 * replay a BRIN index update
 */
static XLogRedoAction
polar_brin_xlog_update(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_brin_update *xlrec = (xl_brin_update *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	BufferTag old_tag;

	POLAR_GET_LOG_TAG(record, old_tag, 2);

	if (BUFFERTAGS_EQUAL(*tag, old_tag))
	{
		/* First remove the old tuple */
		action = POLAR_READ_BUFFER_FOR_REDO(record, 2, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			Page        page;
			OffsetNumber offnum;

			page = (Page) BufferGetPage(*buffer);

			offnum = xlrec->oldOffnum;

			PageIndexTupleDeleteNoCompact(page, offnum);

			PageSetLSN(page, lsn);
		}

		return action;
	}

	/* Then insert the new tuple and update revmap, like in an insertion. */
	return polar_brin_xlog_insert_update(record, &xlrec->insert, tag, buffer);
}

/*
 * Update a tuple on a single page.
 */
static XLogRedoAction
polar_brin_xlog_samepage_update(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	XLogRedoAction action = BLK_NOTFOUND;
	xl_brin_samepage_update *xlrec;
	BufferTag page_tag;

	POLAR_GET_LOG_TAG(record, page_tag, 0);

	if (!BUFFERTAGS_EQUAL(page_tag, *tag))
		return action;

	xlrec = (xl_brin_samepage_update *) XLogRecGetData(record);
	action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

	if (action == BLK_NEEDS_REDO)
	{
		Size        tuplen;
		BrinTuple  *brintuple;
		Page        page;
		OffsetNumber offnum;

		brintuple = (BrinTuple *) XLogRecGetBlockData(record, 0, &tuplen);

		page = (Page) BufferGetPage(*buffer);

		offnum = xlrec->offnum;

		if (!PageIndexTupleOverwrite(page, offnum, (Item) brintuple, tuplen))
		{
			POLAR_LOG_REDO_INFO(page, record);
			elog(PANIC, "polar_brin_xlog_samepage_update: failed to replace tuple");
		}

		PageSetLSN(page, lsn);
	}

	return action;
}

/*
 * Replay a revmap page extension
 */
static XLogRedoAction
polar_brin_xlog_revmap_extend(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	XLogRedoAction action = BLK_NOTFOUND;
	xl_brin_revmap_extend *xlrec;
	Page        page;
	BlockNumber targetBlk;
	BufferTag  meta_tag, revmap_tag;

	xlrec = (xl_brin_revmap_extend *) XLogRecGetData(record);

	XLogRecGetBlockTag(record, 1, NULL, NULL, &targetBlk);
	Assert(xlrec->targetBlk == targetBlk);

	POLAR_GET_LOG_TAG(record, meta_tag, 0);

	if (BUFFERTAGS_EQUAL(*tag, meta_tag))
	{
		/* Update the metapage */
		action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			Page        metapg;
			BrinMetaPageData *metadata;

			metapg = BufferGetPage(*buffer);
			metadata = (BrinMetaPageData *) PageGetContents(metapg);

			Assert(metadata->lastRevmapPage == xlrec->targetBlk - 1);
			metadata->lastRevmapPage = xlrec->targetBlk;

			PageSetLSN(metapg, lsn);
		}

		return action;
	}

	POLAR_GET_LOG_TAG(record, revmap_tag, 1);

	if (BUFFERTAGS_EQUAL(*tag, revmap_tag))
	{
		/*
		 * Re-init the target block as a revmap page.  There's never a full- page
		 * image here.
		 */
		POLAR_INIT_BUFFER_FOR_REDO(record, 1, buffer);
		page = (Page) BufferGetPage(*buffer);
		brin_page_init(page, BRIN_PAGETYPE_REVMAP);

		PageSetLSN(page, lsn);
		action = BLK_NEEDS_REDO;
	}

	return action;
}

static XLogRedoAction
polar_brin_xlog_desummarize_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	XLogRedoAction action = BLK_NOTFOUND;
	xl_brin_desummarize *xlrec;
	BufferTag revmap_tag, left_tag;

	xlrec = (xl_brin_desummarize *) XLogRecGetData(record);
	POLAR_GET_LOG_TAG(record, revmap_tag, 0);

	if (BUFFERTAGS_EQUAL(*tag, revmap_tag))
	{
		/* Update the revmap */
		action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			ItemPointerData iptr;

			ItemPointerSetInvalid(&iptr);
			brinSetHeapBlockItemptr(*buffer, xlrec->pagesPerRange, xlrec->heapBlk, iptr);

			PageSetLSN(BufferGetPage(*buffer), lsn);
		}

		return action;
	}

	POLAR_GET_LOG_TAG(record, left_tag, 1);

	if (BUFFERTAGS_EQUAL(*tag, left_tag))
	{
		/* remove the leftover entry from the regular page */
		action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			Page        regPg = BufferGetPage(*buffer);

			PageIndexTupleDeleteNoCompact(regPg, xlrec->regOffset);

			PageSetLSN(regPg, lsn);
		}
	}

	return action;
}

void
polar_brin_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_BRIN_OPMASK)
	{
		case XLOG_BRIN_CREATE_INDEX:
			polar_logindex_save_block(instance, record, 0);
			break;

		case XLOG_BRIN_INSERT:
			polar_logindex_save_block(instance, record, 0);
			polar_logindex_save_block(instance, record, 1);
			break;

		case XLOG_BRIN_UPDATE:
			polar_logindex_save_block(instance, record, 2);
			polar_logindex_save_block(instance, record, 0);
			polar_logindex_save_block(instance, record, 1);
			break;

		case XLOG_BRIN_SAMEPAGE_UPDATE:
			polar_logindex_save_block(instance, record, 0);
			break;

		case XLOG_BRIN_REVMAP_EXTEND:
			polar_logindex_save_block(instance, record, 0);
			polar_logindex_save_block(instance, record, 1);
			break;

		case XLOG_BRIN_DESUMMARIZE:
			polar_logindex_save_block(instance, record, 0);
			polar_logindex_save_block(instance, record, 1);
			break;

		default:
			elog(PANIC, "polar_brin_idx_save: unknown op code %u", info);
	}
}

bool
polar_brin_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_BRIN_OPMASK)
	{
		case XLOG_BRIN_CREATE_INDEX:
			polar_logindex_redo_parse(instance, record, 0);
			break;

		case XLOG_BRIN_INSERT:
			polar_brin_xlog_insert_parse(instance, record);
			break;

		case XLOG_BRIN_UPDATE:
			polar_brin_xlog_update_parse(instance, record);
			break;

		case XLOG_BRIN_SAMEPAGE_UPDATE:
			polar_logindex_redo_parse(instance, record, 0);
			break;

		case XLOG_BRIN_REVMAP_EXTEND:
			polar_brin_xlog_revmap_extend_parse(instance, record);
			break;

		case XLOG_BRIN_DESUMMARIZE:
			polar_logindex_redo_parse(instance, record, 0);
			polar_logindex_redo_parse(instance, record, 1);
			break;

		default:
			elog(PANIC, "polar_brin_idx_parse: unknown op code %u", info);
	}

	return true;
}

XLogRedoAction
polar_brin_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_BRIN_OPMASK)
	{
		case XLOG_BRIN_CREATE_INDEX:
			return polar_brin_xlog_createidx(record, tag, buffer);

		case XLOG_BRIN_INSERT:
			return polar_brin_xlog_insert(record, tag, buffer);

		case XLOG_BRIN_UPDATE:
			return polar_brin_xlog_update(record, tag, buffer);

		case XLOG_BRIN_SAMEPAGE_UPDATE:
			return polar_brin_xlog_samepage_update(record, tag, buffer);

		case XLOG_BRIN_REVMAP_EXTEND:
			return polar_brin_xlog_revmap_extend(record, tag, buffer);

		case XLOG_BRIN_DESUMMARIZE:
			return polar_brin_xlog_desummarize_page(record, tag, buffer);

		default:
			elog(PANIC, "polar_brin_idx_redo: unknown op code %u", info);
	}

	return BLK_NOTFOUND;
}
