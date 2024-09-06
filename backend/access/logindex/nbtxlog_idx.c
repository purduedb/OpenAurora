#include "postgres.h"

#include "access/bufmask.h"
#include "access/nbtxlog.h"
#include "access/nbtree.h"
#include "access/polar_logindex.h"
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/standby.h"
#include "storage/kv_interface.h"
#include "access/xlog.h"

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
polar_btree_xlog_split(bool newitemonleft, XLogReaderState *record,
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

        datapos = XLogRecGetBlockData(record, 1, &datalen);
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
            Page		lpage = (Page) BufferGetPage(*buffer);
            BTPageOpaque lopaque = (BTPageOpaque) PageGetSpecialPointer(lpage);
            OffsetNumber off;
            IndexTuple	newitem = NULL,
                    left_hikey = NULL,
                    nposting = NULL;
            Size		newitemsz = 0,
                    left_hikeysz = 0;
            Page		newlpage;
            OffsetNumber leftoff,
                    replacepostingoff = InvalidOffsetNumber;

            datapos = XLogRecGetBlockData(record, 0, &datalen);

            if (newitemonleft || xlrec->postingoff != 0)
            {
                newitem = (IndexTuple) datapos;
                newitemsz = MAXALIGN(IndexTupleSize(newitem));
                datapos += newitemsz;
                datalen -= newitemsz;

                if (xlrec->postingoff != 0)
                {
                    ItemId		itemid;
                    IndexTuple	oposting;

                    /* Posting list must be at offset number before new item's */
                    replacepostingoff = OffsetNumberPrev(xlrec->newitemoff);

                    /* Use mutable, aligned newitem copy in _bt_swap_posting() */
                    newitem = CopyIndexTuple(newitem);
                    itemid = PageGetItemId(lpage, replacepostingoff);
                    oposting = (IndexTuple) PageGetItem(lpage, itemid);
                    nposting = _bt_swap_posting(newitem, oposting,
                                                xlrec->postingoff);
                }
            }

            /*
             * Extract left hikey and its size.  We assume that 16-bit alignment
             * is enough to apply IndexTupleSize (since it's fetching from a
             * uint16 field).
             */
            left_hikey = (IndexTuple) datapos;
            left_hikeysz = MAXALIGN(IndexTupleSize(left_hikey));
            datapos += left_hikeysz;
            datalen -= left_hikeysz;

            Assert(datalen == 0);

            newlpage = PageGetTempPageCopySpecial(lpage);

            /* Set high key */
            leftoff = P_HIKEY;
            if (PageAddItem(newlpage, (Item) left_hikey, left_hikeysz,
                            P_HIKEY, false, false) == InvalidOffsetNumber)
                elog(PANIC, "failed to add high key to left page after split");
            leftoff = OffsetNumberNext(leftoff);

            for (off = P_FIRSTDATAKEY(lopaque); off < xlrec->firstrightoff; off++)
            {
                ItemId		itemid;
                Size		itemsz;
                IndexTuple	item;

                /* Add replacement posting list when required */
                if (off == replacepostingoff)
                {
                    Assert(newitemonleft ||
                           xlrec->firstrightoff == xlrec->newitemoff);
                    if (PageAddItem(newlpage, (Item) nposting,
                                    MAXALIGN(IndexTupleSize(nposting)), leftoff,
                                    false, false) == InvalidOffsetNumber)
                        elog(ERROR, "failed to add new posting list item to left page after split");
                    leftoff = OffsetNumberNext(leftoff);
                    continue;		/* don't insert oposting */
                }

                    /* add the new item if it was inserted on left page */
                else if (newitemonleft && off == xlrec->newitemoff)
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
            if (newitemonleft && off == xlrec->newitemoff)
            {
                if (PageAddItem(newlpage, (Item) newitem, newitemsz, leftoff,
                                false, false) == InvalidOffsetNumber)
                    elog(ERROR, "failed to add new item to left page after split");
                leftoff = OffsetNumberNext(leftoff);
            }

            PageRestoreTempPage(newlpage, lpage);

            /* Fix opaque fields */
            lopaque->btpo_flags = BTP_INCOMPLETE_SPLIT;
            if (isleaf)
                lopaque->btpo_flags |= BTP_LEAF;
            lopaque->btpo_next = tags[1].blockNum;
            lopaque->btpo_cycleid = 0;

            PageSetLSN(lpage, lsn);
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
polar_bt_restore_meta(XLogReaderState *record, uint8 block_id, Buffer *metabuf)
{
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    XLogRecPtr  lsn = record->EndRecPtr;
    Page        metapg;
    BTMetaPageData *md;
    BTPageOpaque pageop;
    xl_btree_metadata *xlrec;
    char       *ptr;
    Size        len;

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    POLAR_INIT_BUFFER_FOR_REDO(record, block_id, metabuf);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    ptr = XLogRecGetBlockData(record, block_id, &len);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    Assert(len == sizeof(xl_btree_metadata));
    Assert(BufferGetBlockNumber(*metabuf) == BTREE_METAPAGE);
    xlrec = (xl_btree_metadata *) ptr;
    metapg = BufferGetPage(*metabuf);

    _bt_pageinit(metapg, BufferGetPageSize(*metabuf));

    md = BTPageGetMeta(metapg);
    md->btm_magic = BTREE_MAGIC;
    md->btm_version = xlrec->version;
    md->btm_root = xlrec->root;
    md->btm_level = xlrec->level;
    md->btm_fastroot = xlrec->fastroot;
    md->btm_fastlevel = xlrec->fastlevel;
    /* Cannot log BTREE_MIN_VERSION index metapage without upgrade */
    Assert(md->btm_version >= BTREE_NOVAC_VERSION);
    md->btm_oldest_btpo_xact = xlrec->oldest_btpo_xact;
    md->btm_last_cleanup_num_heap_tuples = xlrec->last_cleanup_num_heap_tuples;
    md->btm_allequalimage = xlrec->allequalimage;

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

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

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    return BLK_NEEDS_REDO;
}

static XLogRedoAction
polar_btree_xlog_insert(bool isleaf, bool ismeta, bool posting, XLogReaderState *record,
                        BufferTag *tag, Buffer *buffer) {
    XLogRecPtr lsn = record->EndRecPtr;
    xl_btree_insert *xlrec = (xl_btree_insert *) XLogRecGetData(record);
    XLogRedoAction action = BLK_NOTFOUND;
    Page page;
    BufferTag tags[3];

    if (!isleaf) {
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

    if (BUFFERTAGS_EQUAL(*tag, tags[0])) {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO) {
            Size		datalen;
            char	   *datapos = XLogRecGetBlockData(record, 0, &datalen);

            page = BufferGetPage(*buffer);

            if (!posting)
            {
                /* Simple retail insertion */
                if (PageAddItem(page, (Item) datapos, datalen, xlrec->offnum,
                                false, false) == InvalidOffsetNumber)
                    elog(PANIC, "failed to add new item");
            }
            else
            {
                ItemId		itemid;
                IndexTuple	oposting,
                        newitem,
                        nposting;
                uint16		postingoff;

                /*
                 * A posting list split occurred during leaf page insertion.  WAL
                 * record data will start with an offset number representing the
                 * point in an existing posting list that a split occurs at.
                 *
                 * Use _bt_swap_posting() to repeat posting list split steps from
                 * primary.  Note that newitem from WAL record is 'orignewitem',
                 * not the final version of newitem that is actually inserted on
                 * page.
                 */
                postingoff = *((uint16 *) datapos);
                datapos += sizeof(uint16);
                datalen -= sizeof(uint16);

                itemid = PageGetItemId(page, OffsetNumberPrev(xlrec->offnum));
                oposting = (IndexTuple) PageGetItem(page, itemid);

                /* Use mutable, aligned newitem copy in _bt_swap_posting() */
                Assert(isleaf && postingoff > 0);
                newitem = CopyIndexTuple((IndexTuple) datapos);
                nposting = _bt_swap_posting(newitem, oposting, postingoff);

                /* Replace existing posting list with post-split version */
                memcpy(oposting, nposting, MAXALIGN(IndexTupleSize(nposting)));

                /* Insert "final" new item (not orignewitem from WAL stream) */
                Assert(IndexTupleSize(newitem) == datalen);
                if (PageAddItem(page, (Item) newitem, datalen, xlrec->offnum,
                                false, false) == InvalidOffsetNumber)
                    elog(PANIC, "failed to add posting split new item");
            }

            PageSetLSN(page, lsn);
        }

        return action;
    }

    if (ismeta) {
        POLAR_GET_LOG_TAG(record, tags[2], 2);

        if (BUFFERTAGS_EQUAL(*tag, tags[2])) {
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

static XLogRedoAction
btree_xlog_dedup(XLogReaderState *record, BufferTag *tag, Buffer *buf)
{
    XLogRecPtr	lsn = record->EndRecPtr;
    xl_btree_dedup *xlrec = (xl_btree_dedup *) XLogRecGetData(record);
    XLogRedoAction action = BLK_NOTFOUND;

    BufferTag tag0;
    POLAR_GET_LOG_TAG(record, tag0, 0);

    if (BUFFERTAGS_EQUAL(*tag, tag0)) {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buf);

        if(action == BLK_NEEDS_REDO) {
            char	   *ptr = XLogRecGetBlockData(record, 0, NULL);
            Page		page = (Page) BufferGetPage(*buf);
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
    }

    return action;
}

static XLogRedoAction
polar_btree_xlog_vacuum(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    XLogRedoAction action = BLK_NOTFOUND;
    Page        page;
    BTPageOpaque opaque;
    BufferTag vacuum_tag;
    xl_btree_vacuum *xlrec = (xl_btree_vacuum *) XLogRecGetData(record);

    POLAR_GET_LOG_TAG(record, vacuum_tag, 0);

    if (!BUFFERTAGS_EQUAL(*tag, vacuum_tag))
        return action;

    /*
     * Like in btvacuumpage(), we need to take a cleanup lock on every leaf
     * page. See nbtree/README for details.
     */
    action = XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, buffer);

    if (action == BLK_NEEDS_REDO)
    {
        char	   *ptr = XLogRecGetBlockData(record, 0, NULL);

        page = (Page) BufferGetPage(*buffer);

        if (xlrec->nupdated > 0)
        {
            OffsetNumber *updatedoffsets;
            xl_btree_update *updates;

            updatedoffsets = (OffsetNumber *)
                    (ptr + xlrec->ndeleted * sizeof(OffsetNumber));
            updates = (xl_btree_update *) ((char *) updatedoffsets +
                                           xlrec->nupdated *
                                           sizeof(OffsetNumber));

            for (int i = 0; i < xlrec->nupdated; i++)
            {
                BTVacuumPosting vacposting;
                IndexTuple	origtuple;
                ItemId		itemid;
                Size		itemsz;

                itemid = PageGetItemId(page, updatedoffsets[i]);
                origtuple = (IndexTuple) PageGetItem(page, itemid);

                vacposting = palloc(offsetof(BTVacuumPostingData, deletetids) +
                                    updates->ndeletedtids * sizeof(uint16));
                vacposting->updatedoffset = updatedoffsets[i];
                vacposting->itup = origtuple;
                vacposting->ndeletedtids = updates->ndeletedtids;
                memcpy(vacposting->deletetids,
                       (char *) updates + SizeOfBtreeUpdate,
                       updates->ndeletedtids * sizeof(uint16));

                _bt_update_posting(vacposting);

                /* Overwrite updated version of tuple */
                itemsz = MAXALIGN(IndexTupleSize(vacposting->itup));
                if (!PageIndexTupleOverwrite(page, updatedoffsets[i],
                                             (Item) vacposting->itup, itemsz))
                    elog(PANIC, "failed to update partially dead item");

                pfree(vacposting->itup);
                pfree(vacposting);

                /* advance to next xl_btree_update from array */
                updates = (xl_btree_update *)
                        ((char *) updates + SizeOfBtreeUpdate +
                         updates->ndeletedtids * sizeof(uint16));
            }
        }

        if (xlrec->ndeleted > 0)
            PageIndexMultiDelete(page, (OffsetNumber *) ptr, xlrec->ndeleted);

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
polar_btree_xlog_delete(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
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
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    XLogRecPtr  lsn = record->EndRecPtr;
    xl_btree_newroot *xlrec = (xl_btree_newroot *) XLogRecGetData(record);
    Page        page;
    BTPageOpaque pageop;
    char       *ptr;
    Size        len;
    BufferTag   tags[3];

    POLAR_GET_LOG_TAG(record, tags[0], 0);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    if (BUFFERTAGS_EQUAL(*tag, tags[0]))
    {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif

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
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif

        return BLK_NEEDS_REDO;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    /* move _bt_clear_incomplete_split to here, important by kangxian */
    if (xlrec->level > 0)
    {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif

        POLAR_GET_LOG_TAG(record, tags[1], 1);

        if (BUFFERTAGS_EQUAL(*tag, tags[1]))
        {
#ifdef ENABLE_DEBUG_INFO
            printf("%s %d\n", __func__ , __LINE__);
            fflush(stdout);
#endif

            /* Clear the incomplete-split flag in left child */
            return polar_bt_clear_incomplete_split(record, 1, buffer);
        }
    }

    POLAR_GET_LOG_TAG(record, tags[2], 2);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    if (BUFFERTAGS_EQUAL(*tag, tags[2])){

#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif

        return polar_bt_restore_meta(record, 2, buffer);
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    return BLK_NOTFOUND;
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
    if (InHotStandby)
    {
        xl_btree_reuse_page *xlrec = (xl_btree_reuse_page *) XLogRecGetData(record);
        ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid,
                                            xlrec->node);
    }
}

static void
polar_btree_xlog_insert_save(bool isleaf, bool ismeta, XLogReaderState *record)
{
    if (!isleaf)
        ParseXLogBlocksLsn(record, 1);

    ParseXLogBlocksLsn(record, 0);

    if (ismeta)
        ParseXLogBlocksLsn(record, 2);
}

static void
polar_btree_xlog_insert_get_bufftag_list(bool isleaf, bool ismeta, XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 3);

    if (!isleaf) {
//        ParseXLogBlocksLsn(record, 1);
        XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
    }

    XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;
//    ParseXLogBlocksLsn(record, 0);

    if (ismeta) {
        XLogRecGetBlockTag(record, 2, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
//        ParseXLogBlocksLsn(record, 2);
    }

    *tagNum = tagCount;
}

static void
polar_btree_xlog_split_save(XLogReaderState *record)
{
    if (XLogRecHasBlockRef(record, 3))
        ParseXLogBlocksLsn(record, 3);

    ParseXLogBlocksLsn(record, 1);
    ParseXLogBlocksLsn(record, 0);

    if (XLogRecHasBlockRef(record, 2))
        ParseXLogBlocksLsn(record, 2);
}

static void
polar_btree_xlog_split_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 4);

    if (XLogRecHasBlockRef(record, 3)) {
        XLogRecGetBlockTag(record, 3, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;

//        ParseXLogBlocksLsn(record, 3);
    }

    XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;

    XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;
//    ParseXLogBlocksLsn(record, 1);
//    ParseXLogBlocksLsn(record, 0);

    if (XLogRecHasBlockRef(record, 2)) {
        XLogRecGetBlockTag(record, 2, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;

//        ParseXLogBlocksLsn(record, 2);
    }

    *tagNum = tagCount;
}


static void
polar_btree_xlog_unlink_page_save(uint8 info, XLogReaderState *record)
{
    ParseXLogBlocksLsn(record, 2);

    if (XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn(record, 1);

    ParseXLogBlocksLsn(record, 0);

    if (XLogRecHasBlockRef(record, 3))
        ParseXLogBlocksLsn(record, 3);

    if (info == XLOG_BTREE_UNLINK_PAGE_META)
        ParseXLogBlocksLsn(record, 4);
}

static void
polar_btree_xlog_unlink_page_get_bufftag_list(uint8 info, XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 5);

//    ParseXLogBlocksLsn(record, 2);
    XLogRecGetBlockTag(record, 2, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;

    if (XLogRecHasBlockRef(record, 1)) {
        XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;

//        ParseXLogBlocksLsn(record, 1);
    }

    XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;
//    ParseXLogBlocksLsn(record, 0);

    if (XLogRecHasBlockRef(record, 3)) {
        XLogRecGetBlockTag(record, 3, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;

//        ParseXLogBlocksLsn(record, 3);
    }

    if (info == XLOG_BTREE_UNLINK_PAGE_META) {
        XLogRecGetBlockTag(record, 4, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
//        ParseXLogBlocksLsn(record, 4);
    }

    *tagNum = tagCount;
}

static void
polar_btree_xlog_newroot_save(XLogReaderState *record)
{
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d \n", __func__ , __LINE__);
    fflush(stdout);
#endif

    ParseXLogBlocksLsn(record, 0);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d \n", __func__ , __LINE__);
    fflush(stdout);
#endif

    if (XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn(record, 1);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d \n", __func__ , __LINE__);
    fflush(stdout);
#endif

    ParseXLogBlocksLsn(record, 2);
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d \n", __func__ , __LINE__);
    fflush(stdout);
#endif    

}

static void
polar_btree_xlog_newroot_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 3);

    XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;
//    ParseXLogBlocksLsn(record, 0);

    if (XLogRecHasBlockRef(record, 1)) {
//        ParseXLogBlocksLsn(record, 1);

        XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
    }

    XLogRecGetBlockTag(record, 2, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;
//    ParseXLogBlocksLsn(record, 2);

    *tagNum = tagCount;
}

bool
polar_btree_idx_save(XLogReaderState *record)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info)
    {
        case XLOG_BTREE_INSERT_LEAF:
            polar_btree_xlog_insert_save(true, false, record);
            break;

        case XLOG_BTREE_INSERT_UPPER:
            polar_btree_xlog_insert_save(false, false, record);
            break;

        case XLOG_BTREE_INSERT_META:
            polar_btree_xlog_insert_save(false, true, record);
            break;

        case XLOG_BTREE_SPLIT_L:
        case XLOG_BTREE_SPLIT_R:
            polar_btree_xlog_split_save(record);
            break;

        case XLOG_BTREE_INSERT_POST:
            polar_btree_xlog_insert_save(true, false, record);
            break;
        case XLOG_BTREE_DEDUP:
            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_BTREE_VACUUM:
            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_BTREE_DELETE:
            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_BTREE_MARK_PAGE_HALFDEAD:
            ParseXLogBlocksLsn(record, 1);
            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_BTREE_UNLINK_PAGE:
        case XLOG_BTREE_UNLINK_PAGE_META:
            polar_btree_xlog_unlink_page_save(info, record);
            break;

        case XLOG_BTREE_NEWROOT:
            polar_btree_xlog_newroot_save(record);
            break;

        case XLOG_BTREE_REUSE_PAGE:
            break;

        case XLOG_BTREE_META_CLEANUP:
            ParseXLogBlocksLsn(record, 0);
            break;

        default:
            elog(PANIC, "polar_btree_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}

bool
polar_btree_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    switch (info)
    {
        case XLOG_BTREE_INSERT_LEAF:
            polar_btree_xlog_insert_get_bufftag_list(true, false, record, buffertagList, tagNum);
            break;

        case XLOG_BTREE_INSERT_UPPER:
            polar_btree_xlog_insert_get_bufftag_list(false, false, record, buffertagList, tagNum);
            break;

        case XLOG_BTREE_INSERT_META:
            polar_btree_xlog_insert_get_bufftag_list(false, true, record, buffertagList, tagNum);
            break;

        case XLOG_BTREE_SPLIT_L:
        case XLOG_BTREE_SPLIT_R:
            polar_btree_xlog_split_get_bufftag_list(record, buffertagList, tagNum);
            break;

        case XLOG_BTREE_INSERT_POST:
            polar_btree_xlog_insert_get_bufftag_list(true, false, record, buffertagList, tagNum);
            break;
        case XLOG_BTREE_DEDUP:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 1);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);
            *tagNum = 1;
//            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_BTREE_VACUUM:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 1);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);
            *tagNum = 1;
//            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_BTREE_DELETE:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 1);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);
            *tagNum = 1;
//            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_BTREE_MARK_PAGE_HALFDEAD:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 2);

            XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[1], rnode, forkNumber, blockNumber);
            *tagNum = 2;
//            ParseXLogBlocksLsn(record, 1);
//            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_BTREE_UNLINK_PAGE:
        case XLOG_BTREE_UNLINK_PAGE_META:
            polar_btree_xlog_unlink_page_get_bufftag_list(info, record, buffertagList, tagNum);
            break;

        case XLOG_BTREE_NEWROOT:
            polar_btree_xlog_newroot_get_bufftag_list(record, buffertagList, tagNum);
            break;

        case XLOG_BTREE_REUSE_PAGE:
            break;

        case XLOG_BTREE_META_CLEANUP:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 1);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);
            *tagNum = 1;
//            ParseXLogBlocksLsn(record, 0);
            break;

        default:
            elog(PANIC, "polar_btree_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}


//todo , update parameters
XLogRedoAction
polar_btree_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRedoAction action = BLK_NOTFOUND;

    //todo change context?
    switch (info)
    {
        case XLOG_BTREE_INSERT_LEAF:
            action = polar_btree_xlog_insert(true, false, false, record, tag, buffer);
            break;
        case XLOG_BTREE_INSERT_UPPER:
            action = polar_btree_xlog_insert(false, false, false, record, tag, buffer);
            break;
        case XLOG_BTREE_INSERT_META:
            action = polar_btree_xlog_insert(false, true, false, record, tag, buffer);
            break;
        case XLOG_BTREE_SPLIT_L:
            action = polar_btree_xlog_split(true, record, tag, buffer);
            break;
        case XLOG_BTREE_SPLIT_R:
            action = polar_btree_xlog_split(false, record, tag, buffer);
            break;
        case XLOG_BTREE_INSERT_POST:
            action = polar_btree_xlog_insert(true, false, true, record, tag, buffer);
            break;
        case XLOG_BTREE_DEDUP:
            action = btree_xlog_dedup(record, tag, buffer);
            break;
        case XLOG_BTREE_VACUUM:
            action = polar_btree_xlog_vacuum(record, tag, buffer);
            break;
        case XLOG_BTREE_DELETE:
            action = polar_btree_xlog_delete(record, tag, buffer);
            break;
        case XLOG_BTREE_MARK_PAGE_HALFDEAD:
            action = polar_btree_xlog_mark_page_halfdead(record, tag, buffer);
            break;
        case XLOG_BTREE_UNLINK_PAGE:
        case XLOG_BTREE_UNLINK_PAGE_META:
            action = polar_btree_xlog_unlink_page(info, record, tag, buffer);
            break;
        case XLOG_BTREE_NEWROOT:
            action = polar_btree_xlog_newroot(record, tag, buffer);
            break;
        case XLOG_BTREE_REUSE_PAGE:
//            polar_btree_xlog_reuse_page(record);
            break;
        case XLOG_BTREE_META_CLEANUP:
            action = polar_bt_restore_meta(record, 0, buffer);
            break;
        default:
            elog(PANIC, "btree_redo: unknown op code %u", info);
    }
    return action;
}