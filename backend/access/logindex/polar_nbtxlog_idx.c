/*-------------------------------------------------------------------------
 *
 * polar_nbtxlog_idx.c
 *   Implementation of parse btree records.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/access/logindex/polar_nbtxlog_idx.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/nbtxlog.h"
#include "access/nbtree.h"
#include "access/polar_logindex_redo.h"
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/standby.h"

static void
polar_btree_xlog_insert_save(polar_logindex_redo_ctl_t instance, bool isleaf, bool ismeta, XLogReaderState *record)
{
	if (!isleaf)
		polar_logindex_save_block(instance, record, 1);

	polar_logindex_save_block(instance, record, 0);

	if (ismeta)
		polar_logindex_save_block(instance, record, 2);
}

static void
polar_btree_xlog_insert_parse(polar_logindex_redo_ctl_t instance, bool isleaf, bool ismeta, XLogReaderState *record)
{
	if (!isleaf)
		polar_logindex_redo_parse(instance, record, 1);

	polar_logindex_redo_parse(instance, record, 0);

	if (ismeta)
		polar_logindex_redo_parse(instance, record, 2);
}

static void
polar_btree_xlog_split_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	if (XLogRecHasBlockRef(record, 3))
		polar_logindex_save_block(instance, record, 3);

	polar_logindex_save_block(instance, record, 1);
	polar_logindex_save_block(instance, record, 0);

	if (XLogRecHasBlockRef(record, 2))
		polar_logindex_save_block(instance, record, 2);
}

static void
polar_btree_xlog_split_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	BufferTag rtag;
	Buffer    rbuf;
	polar_page_lock_t    rpage_lock;

	/* block id 3 is set only when isleaf is false */
	if (XLogRecHasBlockRef(record, 3))
		polar_logindex_redo_parse(instance, record, 3);

	POLAR_MINI_TRANS_REDO_PARSE(instance, record, 1, rtag, rpage_lock, rbuf);

	polar_logindex_redo_parse(instance, record, 0);

	if (BufferIsValid(rbuf))
		UnlockReleaseBuffer(rbuf);

	polar_logindex_mini_trans_unlock(instance->mini_trans, rpage_lock);

	if (XLogRecHasBlockRef(record, 2))
		polar_logindex_redo_parse(instance, record, 2);
}

static void
polar_btree_xlog_delete_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	/*
	 * If we have any conflict processing to do, it must happen before we
	 * update the page.
	 *
	 * Btree delete records can conflict with standby queries.  You might
	 * think that vacuum records would conflict as well, but we've handled
	 * that already.  XLOG_HEAP2_CLEANUP_INFO records provide the highest xid
	 * cleaned by the vacuum of the heap and so we can resolve any conflicts
	 * just once when that arrives.  After that we know that no conflicts
	 * exist from individual btree vacuum records on that index.
	 */
	if (polar_enable_resolve_conflict && reachedConsistency && InHotStandby)
	{
//		TransactionId latestRemovedXid = btree_xlog_delete_get_latestRemovedXid(record);
//		RelFileNode rnode;
//
//		XLogRecGetBlockTag(record, 0, &rnode, NULL, NULL);
//
//		ResolveRecoveryConflictWithSnapshot(latestRemovedXid, rnode);
	}

	polar_logindex_redo_parse(instance, record, 0);
}

static void
polar_btree_xlog_reuse_page_parse(XLogReaderState *record)
{
	/*
	 * Btree reuse_page records exist to provide a conflict point when we
	 * reuse pages in the index via the FSM.  That's all they do though.
	 *
	 * latestRemovedXid was the page's btpo.xact.  The btpo.xact <
	 * RecentGlobalXmin test in _bt_page_recyclable() conceptually mirrors the
	 * pgxact->xmin > limitXmin test in GetConflictingVirtualXIDs().
	 * Consequently, one XID value achieves the same exclusion effect on
	 * master and standby.
	 */
	if (polar_enable_resolve_conflict && reachedConsistency && InHotStandby)
	{
		xl_btree_reuse_page *xlrec = (xl_btree_reuse_page *) XLogRecGetData(record);
		ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid,
											xlrec->node);
	}
}

static void
polar_btree_xlog_unlink_page_save(polar_logindex_redo_ctl_t instance, uint8 info, XLogReaderState *record)
{
	polar_logindex_save_block(instance, record, 2);

	if (XLogRecHasBlockRef(record, 1))
		polar_logindex_save_block(instance, record, 1);

	polar_logindex_save_block(instance, record, 0);

	if (XLogRecHasBlockRef(record, 3))
		polar_logindex_save_block(instance, record, 3);

	if (info == XLOG_BTREE_UNLINK_PAGE_META)
		polar_logindex_save_block(instance, record, 4);
}

static void
polar_btree_xlog_unlink_page_parse(polar_logindex_redo_ctl_t instance, uint8 info, XLogReaderState *record)
{
	polar_logindex_redo_parse(instance, record, 2);

	/* block 1 is set when leftsib is not P_NONE */
	if (XLogRecHasBlockRef(record, 1))
		polar_logindex_redo_parse(instance, record, 1);

	polar_logindex_redo_parse(instance, record, 0);

	if (XLogRecHasBlockRef(record, 3))
		polar_logindex_redo_parse(instance, record, 3);

	if (info == XLOG_BTREE_UNLINK_PAGE_META)
		polar_logindex_redo_parse(instance, record, 4);
}

static void
polar_btree_xlog_newroot_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	polar_logindex_save_block(instance, record, 0);

	if (XLogRecHasBlockRef(record, 1))
		polar_logindex_save_block(instance, record, 1);

	polar_logindex_save_block(instance, record, 2);
}

static void
polar_btree_xlog_newroot_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	BufferTag root_tag;
	Buffer    root_buf;
	polar_page_lock_t    root_lock;

	POLAR_MINI_TRANS_REDO_PARSE(instance, record, 0, root_tag, root_lock, root_buf);

	/* block 1 is set when xlrec->level > 0 */
	if (XLogRecHasBlockRef(record, 1))
		polar_logindex_redo_parse(instance, record, 1);

	if (BufferIsValid(root_buf))
		UnlockReleaseBuffer(root_buf);

	polar_logindex_mini_trans_unlock(instance->mini_trans, root_lock);

	polar_logindex_redo_parse(instance, record, 2);
}

/*
 * _bt_clear_incomplete_split -- clear INCOMPLETE_SPLIT flag on a page
 *
 * This is a common subroutine of the redo functions of all the WAL record
 * types that can insert a downlink: insert, split, and newroot.
 */
static XLogRedoAction
polar_bt_clear_incomplete_split(XLogReaderState *record, uint8 block_id, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	XLogRedoAction action = BLK_NOTFOUND;

	action = POLAR_READ_BUFFER_FOR_REDO(record, block_id, buffer);

	if (action == BLK_NEEDS_REDO)
	{
		Page        page = (Page) BufferGetPage(*buffer);
		BTPageOpaque pageop = (BTPageOpaque) PageGetSpecialPointer(page);

		Assert(P_INCOMPLETE_SPLIT(pageop));
		pageop->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
		PageSetLSN(page, lsn);
	}

	return action;
}

static XLogRedoAction
polar_bt_restore_meta(XLogReaderState *record, uint8 block_id, Buffer *metabuf)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	Page        metapg;
	BTMetaPageData *md;
	BTPageOpaque pageop;
	xl_btree_metadata *xlrec;
	char       *ptr;
	Size        len;

	POLAR_INIT_BUFFER_FOR_REDO(record, block_id, metabuf);

	ptr = XLogRecGetBlockData(record, block_id, &len);

	Assert(len == sizeof(xl_btree_metadata));
	Assert(BufferGetBlockNumber(*metabuf) == BTREE_METAPAGE);
	xlrec = (xl_btree_metadata *) ptr;
	metapg = BufferGetPage(*metabuf);

	_bt_pageinit(metapg, BufferGetPageSize(*metabuf));

	md = BTPageGetMeta(metapg);
	md->btm_magic = BTREE_MAGIC;
	md->btm_version = BTREE_VERSION;
	md->btm_root = xlrec->root;
	md->btm_level = xlrec->level;
	md->btm_fastroot = xlrec->fastroot;
	md->btm_fastlevel = xlrec->fastlevel;
	md->btm_oldest_btpo_xact = xlrec->oldest_btpo_xact;
	md->btm_last_cleanup_num_heap_tuples = xlrec->last_cleanup_num_heap_tuples;

	pageop = (BTPageOpaque) PageGetSpecialPointer(metapg);
	pageop->btpo_flags = BTP_META;

	/*
	 * Set pd_lower just past the end of the metadata.  This is essential,
	 * because without doing so, metadata will be lost if xlog.c compresses
	 * the page.
	 */
	((PageHeader) metapg)->pd_lower =
		((char *) md + sizeof(BTMetaPageData)) - (char *) metapg;

	PageSetLSN(metapg, lsn);

	return BLK_NEEDS_REDO;
}

static XLogRedoAction
polar_btree_xlog_insert(bool isleaf, bool ismeta, XLogReaderState *record,
						BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_btree_insert *xlrec = (xl_btree_insert *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Page        page;
	BufferTag   tags[3];

	if (!isleaf)
	{
		POLAR_GET_LOG_TAG(record, tags[1], 1);

		/*
		 * Insertion to an internal page finishes an incomplete split at the child
		 * level.  Clear the incomplete-split flag in the child.  Note: during
		 * normal operation, the child and parent pages are locked at the same
		 * time, so that clearing the flag and inserting the downlink appear
		 * atomic to other backends.  We don't bother with that during replay,
		 * because readers don't care about the incomplete-split flag and there
		 * cannot be updates happening.
		 */
		if (BUFFERTAGS_EQUAL(*tag, tags[1]))
			return polar_bt_clear_incomplete_split(record, 1, buffer);
	}

	POLAR_GET_LOG_TAG(record, tags[0], 0);

	if (BUFFERTAGS_EQUAL(*tag, tags[0]))
	{
		action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			Size        datalen;
			char       *datapos = XLogRecGetBlockData(record, 0, &datalen);

			page = BufferGetPage(*buffer);

			if (PageAddItem(page, (Item) datapos, datalen, xlrec->offnum,
							false, false) == InvalidOffsetNumber)
			{
				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "btree_insert_redo: failed to add item");
			}

			PageSetLSN(page, lsn);
		}

		return action;
	}

	if (ismeta)
	{
		POLAR_GET_LOG_TAG(record, tags[2], 2);

		if (BUFFERTAGS_EQUAL(*tag, tags[2]))
		{
			/*
			 * Note: in normal operation, we'd update the metapage while still holding
			 * lock on the page we inserted into.  But during replay it's not
			 * necessary to hold that lock, since no other index updates can be
			 * happening concurrently, and readers will cope fine with following an
			 * obsolete link from the metapage.
			 */
			return polar_bt_restore_meta(record, 2, buffer);
		}
	}

	return action;
}

static BTPageOpaque
polar_restore_right(XLogReaderState *record, Page rpage, Size size)
{
	xl_btree_split *xlrec = (xl_btree_split *) XLogRecGetData(record);
	bool        isleaf = (xlrec->level == 0);
	char       *datapos;
	Size        datalen;
	BlockNumber rnext;
	BlockNumber leftsib;
	BTPageOpaque ropaque;

	XLogRecGetBlockTag(record, 0, NULL, NULL, &leftsib);

	if (!XLogRecGetBlockTag(record, 2, NULL, NULL, &rnext))
		rnext = P_NONE;

	datapos = XLogRecGetBlockData(record, 1, &datalen);

	_bt_pageinit(rpage, size);
	ropaque = (BTPageOpaque) PageGetSpecialPointer(rpage);

	ropaque->btpo_prev = leftsib;
	ropaque->btpo_next = rnext;
	ropaque->btpo.level = xlrec->level;
	ropaque->btpo_flags = isleaf ? BTP_LEAF : 0;
	ropaque->btpo_cycleid = 0;

	_bt_restore_page(rpage, datapos, datalen);

	return ropaque;
}

static XLogRedoAction
polar_btree_xlog_vacuum(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	XLogRedoAction action = BLK_NOTFOUND;
	Page        page;
	BTPageOpaque opaque;
	BufferTag vacuum_tag;

#ifdef UNUSED
	/* Should never be here, we delete some unused codes */
	Assert(0);
#endif

	POLAR_GET_LOG_TAG(record, vacuum_tag, 0);

	if (!BUFFERTAGS_EQUAL(*tag, vacuum_tag))
		return action;

	/*
	 * Like in btvacuumpage(), we need to take a cleanup lock on every leaf
	 * page. See nbtree/README for details.
	 */
	action = XLogReadBufferForRedoExtended(record, 0, POLAR_READ_MODE(*buffer), true, buffer);

	if (action == BLK_NEEDS_REDO)
	{
		char       *ptr;
		Size        len;

		ptr = XLogRecGetBlockData(record, 0, &len);

		page = (Page) BufferGetPage(*buffer);

		if (len > 0)
		{
			OffsetNumber *unused;
			OffsetNumber *unend;

			unused = (OffsetNumber *) ptr;
			unend = (OffsetNumber *)((char *) ptr + len);

			if ((unend - unused) > 0)
				PageIndexMultiDelete(page, unused, unend - unused);
		}

		/*
		 * Mark the page as not containing any LP_DEAD items --- see comments
		 * in _bt_delitems_vacuum().
		 */
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		opaque->btpo_flags &= ~BTP_HAS_GARBAGE;

		PageSetLSN(page, lsn);
	}

	return action;
}

static XLogRedoAction
xi_btree_xlog_dedup(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr	lsn = record->EndRecPtr;
    xl_btree_dedup *xlrec = (xl_btree_dedup *) XLogRecGetData(record);
    XLogRedoAction action = BLK_NOTFOUND;
    BufferTag dedupTag;

    POLAR_GET_LOG_TAG(record, dedupTag, 0);
    if (!BUFFERTAGS_EQUAL(*tag, dedupTag))
        return action;
    action = XLogReadBufferForRedo(record, 0, buffer) == BLK_NEEDS_REDO;
    if (action == BLK_NEEDS_REDO)
    {
        char	   *ptr = XLogRecGetBlockData(record, 0, NULL);
        Page		page = (Page) BufferGetPage(*buffer);
        BTPageOpaque opaque = (BTPageOpaque) PageGetSpecialPointer(page);
        OffsetNumber offnum,
                minoff,
                maxoff;
        BTDedupState state;
        BTDedupInterval *intervals;
        Page		newpage;

        state = (BTDedupState) palloc(sizeof(BTDedupStateData));
        state->deduplicate = true;	/* unused */
        state->nmaxitems = 0;	/* unused */
        /* Conservatively use larger maxpostingsize than primary */
        state->maxpostingsize = BTMaxItemSize(page);
        state->base = NULL;
        state->baseoff = InvalidOffsetNumber;
        state->basetupsize = 0;
        state->htids = palloc(state->maxpostingsize);
        state->nhtids = 0;
        state->nitems = 0;
        state->phystupsize = 0;
        state->nintervals = 0;

        minoff = P_FIRSTDATAKEY(opaque);
        maxoff = PageGetMaxOffsetNumber(page);
        newpage = PageGetTempPageCopySpecial(page);

        if (!P_RIGHTMOST(opaque))
        {
            ItemId		itemid = PageGetItemId(page, P_HIKEY);
            Size		itemsz = ItemIdGetLength(itemid);
            IndexTuple	item = (IndexTuple) PageGetItem(page, itemid);

            if (PageAddItem(newpage, (Item) item, itemsz, P_HIKEY,
                            false, false) == InvalidOffsetNumber)
                elog(ERROR, "deduplication failed to add highkey");
        }

        intervals = (BTDedupInterval *) ptr;
        for (offnum = minoff;
             offnum <= maxoff;
             offnum = OffsetNumberNext(offnum))
        {
            ItemId		itemid = PageGetItemId(page, offnum);
            IndexTuple	itup = (IndexTuple) PageGetItem(page, itemid);

            if (offnum == minoff)
                _bt_dedup_start_pending(state, itup, offnum);
            else if (state->nintervals < xlrec->nintervals &&
                     state->baseoff == intervals[state->nintervals].baseoff &&
                     state->nitems < intervals[state->nintervals].nitems)
            {
                if (!_bt_dedup_save_htid(state, itup))
                    elog(ERROR, "deduplication failed to add heap tid to pending posting list");
            }
            else
            {
                _bt_dedup_finish_pending(newpage, state);
                _bt_dedup_start_pending(state, itup, offnum);
            }
        }

        _bt_dedup_finish_pending(newpage, state);
        Assert(state->nintervals == xlrec->nintervals);
        Assert(memcmp(state->intervals, intervals,
                      state->nintervals * sizeof(BTDedupInterval)) == 0);

        if (P_HAS_GARBAGE(opaque))
        {
            BTPageOpaque nopaque = (BTPageOpaque) PageGetSpecialPointer(newpage);

            nopaque->btpo_flags &= ~BTP_HAS_GARBAGE;
        }

        PageRestoreTempPage(newpage, page);
        PageSetLSN(page, lsn);
    }

    return action;
}

static XLogRedoAction
polar_btree_xlog_split(bool onleft, bool lhighkey, XLogReaderState *record,
					   BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_btree_split *xlrec = (xl_btree_split *) XLogRecGetData(record);
	bool        isleaf = (xlrec->level == 0);
	XLogRedoAction action = BLK_NOTFOUND;
	Page        rpage = NULL;
	BTPageOpaque ropaque;
	char       *datapos;
	Size        datalen;
	IndexTuple  left_hikey = NULL;
	Size        left_hikeysz = 0;
	BlockNumber rnext;
	BufferTag   tags[4];

	/* leftsib */
	POLAR_GET_LOG_TAG(record, tags[0], 0);
	/* rightsib */
	POLAR_GET_LOG_TAG(record, tags[1], 1);

	if (!XLogRecGetBlockTag(record, 2, NULL, NULL, &rnext))
		rnext = P_NONE;

	if (!isleaf)
	{
		POLAR_GET_LOG_TAG(record, tags[3], 3);

		/*
		 * Clear the incomplete split flag on the left sibling of the child page
		 * this is a downlink for.  (Like in polar_btree_xlog_insert, this can be done
		 * before locking the other pages)
		 */
		if (BUFFERTAGS_EQUAL(tags[3], *tag))
			return polar_bt_clear_incomplete_split(record, 3, buffer);
	}

	if (BUFFERTAGS_EQUAL(*tag, tags[1]))
	{
		POLAR_INIT_BUFFER_FOR_REDO(record, 1, buffer);

		rpage = (Page) BufferGetPage(*buffer);
		polar_restore_right(record, rpage, BufferGetPageSize(*buffer));

		PageSetLSN(rpage, lsn);

		return BLK_NEEDS_REDO;
	}

	if (BUFFERTAGS_EQUAL(*tag, tags[0]))
	{
		action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		/* Now reconstruct left (original) sibling page */
		if (action == BLK_NEEDS_REDO)
		{
			Page fake_rpage = NULL;
			/*
			 * To retain the same physical order of the tuples that they had, we
			 * initialize a temporary empty page for the left page and add all the
			 * items to that in item number order.  This mirrors how _bt_split()
			 * works.  It's not strictly required to retain the same physical
			 * order, as long as the items are in the correct item number order,
			 * but it helps debugging.  See also _bt_restore_page(), which does
			 * the same for the right page.
			 */
			Page        lpage = (Page) BufferGetPage(*buffer);
			BTPageOpaque lopaque = (BTPageOpaque) PageGetSpecialPointer(lpage);
			OffsetNumber off;
			IndexTuple  newitem = NULL;
			Size        newitemsz = 0;
			Page        newlpage;
			OffsetNumber leftoff;

			datapos = XLogRecGetBlockData(record, 0, &datalen);

			if (onleft)
			{
				newitem = (IndexTuple) datapos;
				newitemsz = MAXALIGN(IndexTupleSize(newitem));
				datapos += newitemsz;
				datalen -= newitemsz;
			}

			/* Extract left hikey and its size (assuming 16-bit alignment) */
			if (lhighkey)
			{
				left_hikey = (IndexTuple) datapos;
				left_hikeysz = MAXALIGN(IndexTupleSize(left_hikey));
				datalen -= left_hikeysz;
			}
			else
			{
				ItemId      hiItemId;

				fake_rpage = (Page) palloc0(BLCKSZ);
				/* Restore right page */
				ropaque = polar_restore_right(record, fake_rpage, BLCKSZ);
				hiItemId = PageGetItemId(fake_rpage, P_FIRSTDATAKEY(ropaque));

				Assert(isleaf);
				left_hikey = (IndexTuple) PageGetItem(fake_rpage, hiItemId);
				left_hikeysz = ItemIdGetLength(hiItemId);
			}

			Assert(datalen == 0);

			newlpage = PageGetTempPageCopySpecial(lpage);

			/* Set high key */
			leftoff = P_HIKEY;

			if (PageAddItem(newlpage, (Item) left_hikey, left_hikeysz,
							P_HIKEY, false, false) == InvalidOffsetNumber)
			{
				POLAR_LOG_REDO_INFO(newlpage, record);
				elog(PANIC, "failed to add high key to left page after split");
			}

			leftoff = OffsetNumberNext(leftoff);

			for (off = P_FIRSTDATAKEY(lopaque); off < xlrec->firstrightoff; off++)
			{
				ItemId      itemid;
				Size        itemsz;
				IndexTuple      item;

				/* add the new item if it was inserted on left page */
				if (onleft && off == xlrec->newitemoff)
				{
					if (PageAddItem(newlpage, (Item) newitem, newitemsz, leftoff,
									false, false) == InvalidOffsetNumber)
						elog(ERROR, "failed to add new item to left page after split");

					leftoff = OffsetNumberNext(leftoff);
				}

				itemid = PageGetItemId(lpage, off);
				itemsz = ItemIdGetLength(itemid);
				item = (IndexTuple) PageGetItem(lpage, itemid);

				if (PageAddItem(newlpage, (Item) item, itemsz, leftoff,
								false, false) == InvalidOffsetNumber)
					elog(ERROR, "failed to add old item to left page after split");

				leftoff = OffsetNumberNext(leftoff);
			}

			/* cope with possibility that newitem goes at the end */
			if (onleft && off == xlrec->newitemoff)
			{
				if (PageAddItem(newlpage, (Item) newitem, newitemsz, leftoff,
								false, false) == InvalidOffsetNumber)
					elog(ERROR, "failed to add new item to left page after split");
			}

			PageRestoreTempPage(newlpage, lpage);

			/* Fix opaque fields */
			lopaque->btpo_flags = BTP_INCOMPLETE_SPLIT;

			if (isleaf)
				lopaque->btpo_flags |= BTP_LEAF;

			lopaque->btpo_next = tags[1].blockNum;
			lopaque->btpo_cycleid = 0;

			PageSetLSN(lpage, lsn);

			if (fake_rpage != NULL)
				pfree(fake_rpage);
		}

		return action;
	}

	/*
	 * Fix left-link of the page to the right of the new right sibling.
	 *
	 * Note: in normal operation, we do this while still holding lock on the
	 * two split pages.  However, that's not necessary for correctness in WAL
	 * replay, because no other index update can be in progress, and readers
	 * will cope properly when following an obsolete left-link.
	 */
	if (rnext != P_NONE)
	{
		POLAR_GET_LOG_TAG(record, tags[2], 2);

		if (BUFFERTAGS_EQUAL(*tag, tags[2]))
		{
			action = POLAR_READ_BUFFER_FOR_REDO(record, 2, buffer);

			if (action == BLK_NEEDS_REDO)
			{
				Page        page = (Page) BufferGetPage(*buffer);
				BTPageOpaque pageop = (BTPageOpaque) PageGetSpecialPointer(page);

				pageop->btpo_prev = tags[1].blockNum;

				PageSetLSN(page, lsn);
			}

		}
	}

	return action;
}

static XLogRedoAction
xi_btree_xlog_delete(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_btree_delete *xlrec = (xl_btree_delete *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Page        page;
	BTPageOpaque opaque;
	BufferTag  del_tag;

	POLAR_GET_LOG_TAG(record, del_tag, 0);

	if (!BUFFERTAGS_EQUAL(*tag, del_tag))
		return action;

	/*
	 * We don't need to take a cleanup lock to apply these changes. See
	 * nbtree/README for details.
	 */
	action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

	if (action == BLK_NEEDS_REDO)
	{
        char	   *ptr = XLogRecGetBlockData(record, 0, NULL);

        page = (Page) BufferGetPage(*buffer);

        PageIndexMultiDelete(page, (OffsetNumber *) ptr, xlrec->ndeleted);

        /* Mark the page as not containing any LP_DEAD items */
        opaque = (BTPageOpaque) PageGetSpecialPointer(page);
        opaque->btpo_flags &= ~BTP_HAS_GARBAGE;

        PageSetLSN(page, lsn);
	}

	return action;
}

static XLogRedoAction
polar_btree_xlog_mark_page_halfdead(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_btree_mark_page_halfdead *xlrec = (xl_btree_mark_page_halfdead *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Page        page;
	BTPageOpaque pageop;
	IndexTupleData trunctuple;
	BufferTag   tags[2];

	POLAR_GET_LOG_TAG(record, tags[0], 0);
	POLAR_GET_LOG_TAG(record, tags[1], 1);

	if (BUFFERTAGS_EQUAL(*tag, tags[1]))
	{
		/*
		 * In normal operation, we would lock all the pages this WAL record
		 * touches before changing any of them.  In WAL replay, it should be okay
		 * to lock just one page at a time, since no concurrent index updates can
		 * be happening, and readers should not care whether they arrive at the
		 * target page or not (since it's surely empty).
		 */

		/* parent page */
		action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

		if (action == BLK_NEEDS_REDO)
		{
            OffsetNumber poffset;
            ItemId		itemid;
            IndexTuple	itup;
            OffsetNumber nextoffset;
            BlockNumber rightsib;

            page = (Page) BufferGetPage(*buffer);
            pageop = (BTPageOpaque) PageGetSpecialPointer(page);

            poffset = xlrec->poffset;

            nextoffset = OffsetNumberNext(poffset);
            itemid = PageGetItemId(page, nextoffset);
            itup = (IndexTuple) PageGetItem(page, itemid);
            rightsib = BTreeTupleGetDownLink(itup);

            itemid = PageGetItemId(page, poffset);
            itup = (IndexTuple) PageGetItem(page, itemid);
            BTreeTupleSetDownLink(itup, rightsib);
            nextoffset = OffsetNumberNext(poffset);
            PageIndexTupleDelete(page, nextoffset);

            PageSetLSN(page, lsn);
		}

		return action;
	}

	if (BUFFERTAGS_EQUAL(*tag, tags[0]))
	{
		/* Rewrite the leaf page as a halfdead page */
		POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);

		page = (Page) BufferGetPage(*buffer);

		_bt_pageinit(page, BufferGetPageSize(*buffer));
		pageop = (BTPageOpaque) PageGetSpecialPointer(page);

		pageop->btpo_prev = xlrec->leftblk;
		pageop->btpo_next = xlrec->rightblk;
		pageop->btpo.level = 0;
		pageop->btpo_flags = BTP_HALF_DEAD | BTP_LEAF;
		pageop->btpo_cycleid = 0;

		/*
		 * Construct a dummy hikey item that points to the next parent to be
		 * deleted (if any).
		 */
		MemSet(&trunctuple, 0, sizeof(IndexTupleData));
		trunctuple.t_info = sizeof(IndexTupleData);
		BTreeTupleSetTopParent(&trunctuple, xlrec->topparent);

		if (PageAddItem(page, (Item) &trunctuple, sizeof(IndexTupleData), P_HIKEY,
						false, false) == InvalidOffsetNumber)
			elog(ERROR, "could not add dummy high key to half-dead page");

		PageSetLSN(page, lsn);
	}

	return action;
}

static XLogRedoAction
polar_btree_xlog_unlink_page(uint8 info, XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	BlockNumber leftsib;
	BlockNumber rightsib;
	Page        page;
	BTPageOpaque pageop;
	BufferTag   tags[5];

	leftsib = xlrec->leftsib;
	rightsib = xlrec->rightsib;

	POLAR_GET_LOG_TAG(record, tags[2], 2);

	if (BUFFERTAGS_EQUAL(*tag, tags[2]))
	{
		/*
		 * In normal operation, we would lock all the pages this WAL record
		 * touches before changing any of them.  In WAL replay, it should be okay
		 * to lock just one page at a time, since no concurrent index updates can
		 * be happening, and readers should not care whether they arrive at the
		 * target page or not (since it's surely empty).
		 */

		/* Fix left-link of right sibling */
		action = POLAR_READ_BUFFER_FOR_REDO(record, 2, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			page = (Page) BufferGetPage(*buffer);
			pageop = (BTPageOpaque) PageGetSpecialPointer(page);
			pageop->btpo_prev = leftsib;

			PageSetLSN(page, lsn);
		}

		return action;
	}

	if (leftsib != P_NONE)
	{
		POLAR_GET_LOG_TAG(record, tags[1], 1);

		/* Fix right-link of left sibling, if any */
		if (BUFFERTAGS_EQUAL(*tag, tags[1]))
		{
			action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

			if (action == BLK_NEEDS_REDO)
			{
				page = (Page) BufferGetPage(*buffer);
				pageop = (BTPageOpaque) PageGetSpecialPointer(page);
				pageop->btpo_next = rightsib;

				PageSetLSN(page, lsn);
			}

			return action;
		}
	}

	POLAR_GET_LOG_TAG(record, tags[0], 0);

	if (BUFFERTAGS_EQUAL(*tag, tags[0]))
	{
		/* Rewrite target page as empty deleted page */
		POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
		page = (Page) BufferGetPage(*buffer);

		_bt_pageinit(page, BufferGetPageSize(*buffer));
		pageop = (BTPageOpaque) PageGetSpecialPointer(page);

		pageop->btpo_prev = leftsib;
		pageop->btpo_next = rightsib;
		pageop->btpo.xact = xlrec->btpo_xact;
		pageop->btpo_flags = BTP_DELETED;
		pageop->btpo_cycleid = 0;

		PageSetLSN(page, lsn);

		return BLK_NEEDS_REDO;
	}

	if (XLogRecHasBlockRef(record, 3))
	{
		POLAR_GET_LOG_TAG(record, tags[3], 3);

		/*
		 * If we deleted a parent of the targeted leaf page, instead of the leaf
		 * itself, update the leaf to point to the next remaining child in the
		 * branch.
		 */
		if (BUFFERTAGS_EQUAL(*tag, tags[3]))
		{

			/*
			 * There is no real data on the page, so we just re-create it from
			 * scratch using the information from the WAL record.
			 */
			IndexTupleData trunctuple;

			POLAR_INIT_BUFFER_FOR_REDO(record, 3, buffer);

			page = (Page) BufferGetPage(*buffer);

			_bt_pageinit(page, BufferGetPageSize(*buffer));
			pageop = (BTPageOpaque) PageGetSpecialPointer(page);

			pageop->btpo_flags = BTP_HALF_DEAD | BTP_LEAF;
			pageop->btpo_prev = xlrec->leafleftsib;
			pageop->btpo_next = xlrec->leafrightsib;
			pageop->btpo.level = 0;
			pageop->btpo_cycleid = 0;

			/* Add a dummy hikey item */
			MemSet(&trunctuple, 0, sizeof(IndexTupleData));
			trunctuple.t_info = sizeof(IndexTupleData);
			BTreeTupleSetTopParent(&trunctuple, xlrec->topparent);

			if (PageAddItem(page, (Item) &trunctuple, sizeof(IndexTupleData), P_HIKEY,
							false, false) == InvalidOffsetNumber)
			{
				POLAR_LOG_REDO_INFO(page, record);
				elog(ERROR, "could not add dummy high key to half-dead page");
			}

			PageSetLSN(page, lsn);
			return BLK_NEEDS_REDO;
		}
	}

	if (info == XLOG_BTREE_UNLINK_PAGE_META)
	{
		POLAR_GET_LOG_TAG(record, tags[4], 4);

		if (BUFFERTAGS_EQUAL(*tag, tags[4]))
		{
			/* Update metapage if needed */
			return polar_bt_restore_meta(record, 4, buffer);
		}
	}

	return action;
}

static XLogRedoAction
polar_btree_xlog_newroot(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_btree_newroot *xlrec = (xl_btree_newroot *) XLogRecGetData(record);
	Page        page;
	BTPageOpaque pageop;
	char       *ptr;
	Size        len;
	BufferTag   tags[3];

	POLAR_GET_LOG_TAG(record, tags[0], 0);

	if (BUFFERTAGS_EQUAL(*tag, tags[0]))
	{
		POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
		page = (Page) BufferGetPage(*buffer);

		_bt_pageinit(page, BufferGetPageSize(*buffer));
		pageop = (BTPageOpaque) PageGetSpecialPointer(page);

		pageop->btpo_flags = BTP_ROOT;
		pageop->btpo_prev = pageop->btpo_next = P_NONE;
		pageop->btpo.level = xlrec->level;

		if (xlrec->level == 0)
			pageop->btpo_flags |= BTP_LEAF;

		pageop->btpo_cycleid = 0;

		if (xlrec->level > 0)
		{
			ptr = XLogRecGetBlockData(record, 0, &len);
			_bt_restore_page(page, ptr, len);
		}

		PageSetLSN(page, lsn);
		return BLK_NEEDS_REDO;
	}

	/* move _bt_clear_incomplete_split to here, important by kangxian */
	if (xlrec->level > 0)
	{
		POLAR_GET_LOG_TAG(record, tags[1], 1);

		if (BUFFERTAGS_EQUAL(*tag, tags[1]))
		{
			/* Clear the incomplete-split flag in left child */
			return polar_bt_clear_incomplete_split(record, 1, buffer);
		}
	}

	POLAR_GET_LOG_TAG(record, tags[2], 2);

	if (BUFFERTAGS_EQUAL(*tag, tags[2]))
		return polar_bt_restore_meta(record, 2, buffer);

	return BLK_NOTFOUND;
}

//Updated to PG13.0
void
polar_btree_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_BTREE_INSERT_LEAF:
			polar_btree_xlog_insert_save(instance, true, false, record);
			break;

		case XLOG_BTREE_INSERT_UPPER:
			polar_btree_xlog_insert_save(instance, false, false, record);
			break;

		case XLOG_BTREE_INSERT_META:
			polar_btree_xlog_insert_save(instance, false, true, record);
			break;

		case XLOG_BTREE_SPLIT_L:
		case XLOG_BTREE_SPLIT_R:
			polar_btree_xlog_split_save(instance, record);
			break;

	    case XLOG_BTREE_INSERT_POST:
	    case XLOG_BTREE_DEDUP:
	        polar_logindex_save_block(instance, record, 0);
	        break;

		case XLOG_BTREE_VACUUM:
			polar_logindex_save_block(instance, record, 0);
			break;

		case XLOG_BTREE_DELETE:
			polar_logindex_save_block(instance, record, 0);
			break;

		case XLOG_BTREE_MARK_PAGE_HALFDEAD:
			polar_logindex_save_block(instance, record, 1);
			polar_logindex_save_block(instance, record, 0);
			break;

		case XLOG_BTREE_UNLINK_PAGE:
		case XLOG_BTREE_UNLINK_PAGE_META:
			polar_btree_xlog_unlink_page_save(instance, info, record);
			break;

		case XLOG_BTREE_NEWROOT:
			polar_btree_xlog_newroot_save(instance, record);
			break;

		case XLOG_BTREE_REUSE_PAGE:
			break;

		case XLOG_BTREE_META_CLEANUP:
			polar_logindex_save_block(instance, record, 0);
			break;

		default:
			elog(PANIC, "polar_btree_idx_save: unknown op code %u", info);
			break;
	}
}

//Updated to PG13.0
bool
polar_btree_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_BTREE_INSERT_LEAF:
			polar_btree_xlog_insert_parse(instance, true, false, record);
			break;

		case XLOG_BTREE_INSERT_UPPER:
			polar_btree_xlog_insert_parse(instance, false, false, record);
			break;

		case XLOG_BTREE_INSERT_META:
			polar_btree_xlog_insert_parse(instance, false, true, record);
			break;

		case XLOG_BTREE_SPLIT_L:
		case XLOG_BTREE_SPLIT_R:
			polar_btree_xlog_split_parse(instance, record);
			break;

        case XLOG_BTREE_INSERT_POST:
        case XLOG_BTREE_DEDUP:
            polar_logindex_redo_parse(instance, record, 0);
            break;

		case XLOG_BTREE_VACUUM:
			polar_logindex_cleanup_parse(instance, record, 0);
			break;

		case XLOG_BTREE_DELETE:
			polar_btree_xlog_delete_parse(instance, record);
			break;

		case XLOG_BTREE_MARK_PAGE_HALFDEAD:
			polar_logindex_redo_parse(instance, record, 1);
			polar_logindex_redo_parse(instance, record, 0);
			break;

		case XLOG_BTREE_UNLINK_PAGE:
		case XLOG_BTREE_UNLINK_PAGE_META:
			polar_btree_xlog_unlink_page_parse(instance, info, record);
			break;

		case XLOG_BTREE_NEWROOT:
			polar_btree_xlog_newroot_parse(instance, record);
			break;

		case XLOG_BTREE_REUSE_PAGE:
			polar_btree_xlog_reuse_page_parse(record);
			break;

		case XLOG_BTREE_META_CLEANUP:
			polar_logindex_redo_parse(instance, record, 0);
			break;

		default:
			elog(PANIC, "polar_btree_idx_parse: unknown op code %u", info);
			break;
	}

	return true;
}

XLogRedoAction
polar_btree_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_BTREE_INSERT_LEAF:
			return polar_btree_xlog_insert(true, false, record, tag, buffer);

		case XLOG_BTREE_INSERT_UPPER:
			return polar_btree_xlog_insert(false, false, record, tag, buffer);

		case XLOG_BTREE_INSERT_META:
			return polar_btree_xlog_insert(false, true, record, tag, buffer);

		case XLOG_BTREE_SPLIT_L:
			return polar_btree_xlog_split(true, false, record, tag, buffer);


		case XLOG_BTREE_SPLIT_R:
			return polar_btree_xlog_split(false, false, record, tag, buffer);

        case XLOG_BTREE_INSERT_POST: //XI: Here code not include post logic
            return polar_btree_xlog_insert(false, true, record, tag, buffer);

        case XLOG_BTREE_DEDUP:
            return xi_btree_xlog_dedup(record, tag, buffer);

		case XLOG_BTREE_VACUUM:
			return polar_btree_xlog_vacuum(record, tag, buffer);

		case XLOG_BTREE_DELETE:
			return xi_btree_xlog_delete(record, tag, buffer);

		case XLOG_BTREE_MARK_PAGE_HALFDEAD:
			return polar_btree_xlog_mark_page_halfdead(record, tag, buffer);

		case XLOG_BTREE_UNLINK_PAGE:
		case XLOG_BTREE_UNLINK_PAGE_META:
			return polar_btree_xlog_unlink_page(info, record, tag, buffer);

		case XLOG_BTREE_NEWROOT:
			return polar_btree_xlog_newroot(record, tag, buffer);

		case XLOG_BTREE_REUSE_PAGE:
			/* Never go to here */
			Assert(0);
			break;

		case XLOG_BTREE_META_CLEANUP:
			return polar_bt_restore_meta(record, 0, buffer);

		default:
			elog(PANIC, "polar_btree_idx_redo: unknown op code %u", info);
			break;
	}

	return BLK_NOTFOUND;
}
