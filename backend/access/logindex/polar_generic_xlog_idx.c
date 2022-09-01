/*-------------------------------------------------------------------------
 *
 * polar_generic_xlog_idx.c
 *    WAL redo parse logic for generic index.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *           src/backend/access/logindex/polar_generic_xlog_idx.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/generic_xlog.h"
#include "access/polar_logindex_redo.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "storage/buf_internals.h"

void
polar_generic_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	int block_id;

	for (block_id = 0; block_id <= MAX_GENERIC_XLOG_PAGES; block_id++)
	{
		if (XLogRecHasBlockRef(record, block_id))
			polar_logindex_save_block(instance, record, block_id);
	}
}

bool
polar_generic_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	int block_id;
	Buffer buffers[MAX_GENERIC_XLOG_PAGES];
	polar_page_lock_t page_lock[MAX_GENERIC_XLOG_PAGES];

	/* Protect limited size of buffers[] array */
	Assert(record->max_block_id < MAX_GENERIC_XLOG_PAGES);

	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		BufferTag tag;

		if (!XLogRecHasBlockRef(record, block_id))
		{
			buffers[block_id] = InvalidBuffer;
			page_lock[block_id] = 0;
			continue;
		}

		POLAR_MINI_TRANS_REDO_PARSE(instance, record, block_id, tag, page_lock[block_id], buffers[block_id]);
	}

	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		if (XLogRecHasBlockRef(record, block_id))
		{
			if (BufferIsValid(buffers[block_id]))
				UnlockReleaseBuffer(buffers[block_id]);

			polar_logindex_mini_trans_unlock(instance->mini_trans, page_lock[block_id]);
		}
	}

	return true;
}

XLogRedoAction
polar_generic_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	int block_id;
	XLogRedoAction action = BLK_NOTFOUND;
	XLogRecPtr  lsn = record->EndRecPtr;

	/* Protect limited size of buffers[] array */
	Assert(record->max_block_id < MAX_GENERIC_XLOG_PAGES);

	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		BufferTag page_tag;

		if (!XLogRecHasBlockRef(record, block_id))
			continue;

		POLAR_GET_LOG_TAG(record, page_tag, block_id);

		if (BUFFERTAGS_EQUAL(*tag, page_tag))
		{
			action = POLAR_READ_BUFFER_FOR_REDO(record, block_id, buffer);

			/* Apply redo to given block if needed */
			if (action == BLK_NEEDS_REDO)
			{
				Page        page;
				PageHeader  pageHeader;
				char       *blockDelta;
				Size        blockDeltaSize;

				page = BufferGetPage(*buffer);
				blockDelta = XLogRecGetBlockData(record, block_id, &blockDeltaSize);
				applyPageRedo(page, blockDelta, blockDeltaSize);

				/*
				 * Since the delta contains no information about what's in the
				 * "hole" between pd_lower and pd_upper, set that to zero to
				 * ensure we produce the same page state that application of the
				 * logged action by GenericXLogFinish did.
				 */
				pageHeader = (PageHeader) page;
				memset(page + pageHeader->pd_lower, 0,
					   pageHeader->pd_upper - pageHeader->pd_lower);

				PageSetLSN(page, lsn);
			}

			break;
		}
	}

	return action;
}

