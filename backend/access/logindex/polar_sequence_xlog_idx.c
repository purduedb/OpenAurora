/*-------------------------------------------------------------------------
 *
 * polar_sequence_xlog_idx.c
 *    WAL redo parse logic for sequence index.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *           src/backend/access/logindex/polar_sequence_xlog_idx.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/polar_logindex.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "commands/sequence.h"
#include "storage/buf_internals.h"
#include "storage/kv_interface.h"
#include "access/xlog.h"

static XLogRedoAction
polar_seq_xlog_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	Page        page;
	Page        localpage;
	char       *item;
	Size        itemsz;
	xl_seq_rec *xlrec = (xl_seq_rec *) XLogRecGetData(record);
	sequence_magic *sm;
	BufferTag   tag0;

	POLAR_GET_LOG_TAG(record, tag0, 0);

	if (!BUFFERTAGS_EQUAL(tag0, *tag))
		return BLK_NOTFOUND;

	POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
	page = (Page) BufferGetPage(*buffer);

	/*
	 * We always reinit the page.  However, since this WAL record type is also
	 * used for updating sequences, it's possible that a hot-standby backend
	 * is examining the page concurrently; so we mustn't transiently trash the
	 * buffer.  The solution is to build the correct new page contents in
	 * local workspace and then memcpy into the buffer.  Then only bytes that
	 * are supposed to change will change, even transiently. We must palloc
	 * the local page for alignment reasons.
	 */
	localpage = (Page) palloc(BufferGetPageSize(*buffer));

	PageInit(localpage, BufferGetPageSize(*buffer), sizeof(sequence_magic));
	sm = (sequence_magic *) PageGetSpecialPointer(localpage);
	sm->magic = SEQ_MAGIC;

	item = (char *) xlrec + sizeof(xl_seq_rec);
	itemsz = XLogRecGetDataLen(record) - sizeof(xl_seq_rec);

	if (PageAddItem(localpage, (Item) item, itemsz,
					FirstOffsetNumber, false, false) == InvalidOffsetNumber)
	{
		elog(PANIC, "seq_redo: failed to add item to page");
	}

	PageSetLSN(localpage, lsn);

	memcpy(page, localpage, BufferGetPageSize(*buffer));
	pfree(localpage);

	return BLK_NEEDS_REDO;
}

bool
polar_seq_idx_save( XLogReaderState *record)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_SEQ_LOG:
            ParseXLogBlocksLsn(record, 0);
			break;

		default:
			elog(PANIC, "polar_seq_idx_save: unknown op code %u", info);
			break;
	}
    return true;
}

bool
polar_seq_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	int tagCount = 0;

	RelFileNode rnode;
	ForkNumber forkNumber;
	BlockNumber blockNumber;


	switch (info)
	{
		case XLOG_SEQ_LOG:
			*buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 1);

			XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
			INIT_BUFFERTAG(*buffertagList[0], rnode, forkNumber, blockNumber);
			*tagNum = 1;
//			ParseXLogBlocksLsn(record, 0);
			break;

		default:
			elog(PANIC, "polar_seq_idx_save: unknown op code %u", info);
			break;
	}
	return true;
}
//
//bool
//polar_seq_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
//{
//	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
//
//	/*
//	 * These operations don't overwrite MVCC data so no conflict processing is
//	 * required. The ones in heap2 rmgr do.
//	 */
//
//	switch (info)
//	{
//		case XLOG_SEQ_LOG:
//			polar_logindex_redo_parse(instance, record, 0);
//			break;
//
//		default:
//			elog(PANIC, "polar_seq_idx_parse: unknown op code %u", info);
//			break;
//	}
//
//	return true;
//}

XLogRedoAction
polar_seq_idx_redo(XLogReaderState *record,  BufferTag *tag, Buffer *buffer)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/*
	 * These operations don't overwrite MVCC data so no conflict processing is
	 * required. The ones in heap2 rmgr do.
	 */

	switch (info)
	{
		case XLOG_SEQ_LOG:
			return polar_seq_xlog_redo(record, tag, buffer);

		default:
			elog(PANIC, "polar_seq_idx_redo: unknown op code %u", info);
	}

	return BLK_NOTFOUND;
}
