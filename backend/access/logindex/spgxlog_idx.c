#include "postgres.h"

#include "access/bufmask.h"
#include "access/polar_logindex.h"
#include "access/spgxlog.h"
#include "access/spgist_private.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "storage/buf_internals.h"
#include "storage/standby.h"
#include "storage/kv_interface.h"
#include "utils/memutils.h"
#include "access/xlog.h"

//static void
//polar_spg_redo_add_node_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
//{
//    if (!XLogRecHasBlockRef(record, 1))
//        polar_logindex_save_block(instance, record, 0);
//    else
//    {
//        polar_logindex_save_block(instance, record, 1);
//        polar_logindex_save_block(instance, record, 0);
//
//        if (XLogRecHasBlockRef(record, 2))
//            polar_logindex_save_block(instance, record, 2);
//    }
//}
//
//static void
//polar_spg_redo_add_node_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
//{
//    if (!XLogRecHasBlockRef(record, 1))
//        polar_logindex_redo_parse(instance, record, 0);
//    else
//    {
//        polar_logindex_redo_parse(instance, record, 1);
//        polar_logindex_redo_parse(instance, record, 0);
//
//        /*
//         * Update parent downlink (if we didn't do it as part of the source or
//         * destination page update already).
//         */
//        if (XLogRecHasBlockRef(record, 2))
//            polar_logindex_redo_parse(instance, record, 2);
//    }
//}
//
//static void
//polar_spg_redo_pick_split_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
//{
//    if (XLogRecHasBlockRef(record, 0))
//        polar_logindex_save_block(instance, record, 0);
//
//    if (XLogRecHasBlockRef(record, 1))
//        polar_logindex_save_block(instance, record, 1);
//
//    polar_logindex_save_block(instance, record, 2);
//
//    if (XLogRecHasBlockRef(record, 3))
//        polar_logindex_save_block(instance, record, 3);
//}
//
//static void
//polar_spg_redo_pick_split_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
//{
//    BufferTag src_tag, dst_tag;
//    polar_page_lock_t src_lock = POLAR_INVALID_PAGE_LOCK,
//            dst_lock = POLAR_INVALID_PAGE_LOCK;
//    Buffer    src_buf = InvalidBuffer,
//            dst_buf = InvalidBuffer;
//
//    if (XLogRecHasBlockRef(record, 0))
//        POLAR_MINI_TRANS_REDO_PARSE(instance, record, 0, src_tag, src_lock, src_buf);
//
//    if (XLogRecHasBlockRef(record, 1))
//        POLAR_MINI_TRANS_REDO_PARSE(instance, record, 1, dst_tag, dst_lock, dst_buf);
//
//    polar_logindex_redo_parse(instance, record, 2);
//
//    if (XLogRecHasBlockRef(record, 0))
//    {
//        if (BufferIsValid(src_buf))
//            UnlockReleaseBuffer(src_buf);
//
//        polar_logindex_mini_trans_unlock(instance->mini_trans, src_lock);
//    }
//
//    if (XLogRecHasBlockRef(record, 1))
//    {
//        if (BufferIsValid(dst_buf))
//            UnlockReleaseBuffer(dst_buf);
//
//        polar_logindex_mini_trans_unlock(instance->mini_trans, dst_lock);
//    }
//
//    if (XLogRecHasBlockRef(record, 3))
//        polar_logindex_redo_parse(instance, record, 3);
//}
//
//static void
//polar_spg_redo_vacuum_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
//{
//    /*
//     * If any redirection tuples are being removed, make sure there are no
//     * live Hot Standby transactions that might need to see them.
//     */
//    if (polar_enable_resolve_conflict && reachedConsistency && InHotStandby)
//    {
//        spgxlogVacuumRedirect *xldata =
//                (spgxlogVacuumRedirect *) XLogRecGetData(record);
//
//        if (TransactionIdIsValid(xldata->newestRedirectXid))
//        {
//            RelFileNode node;
//
//            XLogRecGetBlockTag(record, 0, &node, NULL, NULL);
//            ResolveRecoveryConflictWithSnapshot(xldata->newestRedirectXid,
//                                                node);
//        }
//    }
//
//    polar_logindex_redo_parse(instance, record, 0);
//}

static XLogRedoAction
polar_spg_redo_create_index(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    Page        page;
    BufferTag   meta_tag, leaf_tag;

    POLAR_GET_LOG_TAG(record, meta_tag, 0);

    if (BUFFERTAGS_EQUAL(*tag, meta_tag))
    {
        POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
        Assert(BufferGetBlockNumber(*buffer) == SPGIST_METAPAGE_BLKNO);
        page = (Page) BufferGetPage(*buffer);
        SpGistInitMetapage(page);
        PageSetLSN(page, lsn);

        return BLK_NEEDS_REDO;
    }

    POLAR_GET_LOG_TAG(record, leaf_tag, 1);

    if (BUFFERTAGS_EQUAL(*tag, leaf_tag))
    {
        POLAR_INIT_BUFFER_FOR_REDO(record, 1, buffer);
        Assert(BufferGetBlockNumber(*buffer) == SPGIST_ROOT_BLKNO);
        SpGistInitBuffer(*buffer, SPGIST_LEAF);
        page = (Page) BufferGetPage(*buffer);
        PageSetLSN(page, lsn);

        return BLK_NEEDS_REDO;
    }

    POLAR_GET_LOG_TAG(record, leaf_tag, 2);

    if (BUFFERTAGS_EQUAL(*tag, leaf_tag))
    {
        POLAR_INIT_BUFFER_FOR_REDO(record, 2, buffer);
        Assert(BufferGetBlockNumber(*buffer) == SPGIST_NULL_BLKNO);
        SpGistInitBuffer(*buffer, SPGIST_LEAF | SPGIST_NULLS);
        page = (Page) BufferGetPage(*buffer);
        PageSetLSN(page, lsn);

        return BLK_NEEDS_REDO;
    }

    return BLK_NOTFOUND;
}

static XLogRedoAction
polar_spg_redo_add_leaf(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    char       *ptr = XLogRecGetData(record);
    spgxlogAddLeaf *xldata = (spgxlogAddLeaf *) ptr;
    char       *leafTuple;
    SpGistLeafTupleData leafTupleHdr;
    Page        page;
    XLogRedoAction action = BLK_NOTFOUND;
    BufferTag   leaf_tag, parent_tag;

    ptr += sizeof(spgxlogAddLeaf);
    leafTuple = ptr;
    /* the leaf tuple is unaligned, so make a copy to access its header */
    memcpy(&leafTupleHdr, leafTuple, sizeof(SpGistLeafTupleData));

    POLAR_GET_LOG_TAG(record, leaf_tag, 0);

    if (BUFFERTAGS_EQUAL(*tag, leaf_tag))
    {
        /*
         * In normal operation we would have both current and parent pages locked
         * simultaneously; but in WAL replay it should be safe to update the leaf
         * page before updating the parent.
         */
        if (xldata->newPage)
        {
            POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
            SpGistInitBuffer(*buffer,
                             SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
            action = BLK_NEEDS_REDO;
        }
        else
            action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            page = BufferGetPage(*buffer);

            /* insert new tuple */
            if (xldata->offnumLeaf != xldata->offnumHeadLeaf)
            {
                /* normal cases, tuple was added by SpGistPageAddNewItem */
                addOrReplaceTuple(page, (Item) leafTuple, leafTupleHdr.size,
                                  xldata->offnumLeaf);

                /* update head tuple's chain link if needed */
                if (xldata->offnumHeadLeaf != InvalidOffsetNumber)
                {
                    SpGistLeafTuple head;

                    head = (SpGistLeafTuple) PageGetItem(page,
                                                         PageGetItemId(page, xldata->offnumHeadLeaf));
                    Assert(head->nextOffset == leafTupleHdr.nextOffset);
                    head->nextOffset = xldata->offnumLeaf;
                }
            }
            else
            {
                /* replacing a DEAD tuple */
                PageIndexTupleDelete(page, xldata->offnumLeaf);

                if (PageAddItem(page,
                                (Item) leafTuple, leafTupleHdr.size,
                                xldata->offnumLeaf, false, false) != xldata->offnumLeaf)
                {
                    elog(ERROR, "failed to add item of size %u to SPGiST index page",
                         leafTupleHdr.size);
                }
            }

            PageSetLSN(page, lsn);
        }

        return action;
    }

    /* update parent downlink if necessary */
    if (xldata->offnumParent != InvalidOffsetNumber)
    {
        POLAR_GET_LOG_TAG(record, parent_tag, 1);

        if (BUFFERTAGS_EQUAL(*tag, parent_tag))
        {
            action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

            if (action == BLK_NEEDS_REDO)
            {
                SpGistInnerTuple tuple;
                BlockNumber blknoLeaf;

                XLogRecGetBlockTag(record, 0, NULL, NULL, &blknoLeaf);

                page = BufferGetPage(*buffer);

                tuple = (SpGistInnerTuple) PageGetItem(page,
                                                       PageGetItemId(page, xldata->offnumParent));

                spgUpdateNodeLink(tuple, xldata->nodeI,
                                  blknoLeaf, xldata->offnumLeaf);

                PageSetLSN(page, lsn);
            }
        }
    }

    return action;
}

static XLogRedoAction
polar_spg_redo_move_leafs(XLogReaderState *record, BufferTag  *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    char       *ptr = XLogRecGetData(record);
    spgxlogMoveLeafs *xldata = (spgxlogMoveLeafs *) ptr;
    XLogRedoAction action = BLK_NOTFOUND;
    SpGistState state;
    OffsetNumber *toDelete;
    OffsetNumber *toInsert;
    int         nInsert;
    Page        page;
    BlockNumber blknoDst;
    BufferTag   leaf_tag, src_tag, parent_tag;

    XLogRecGetBlockTag(record, 1, NULL, NULL, &blknoDst);

    fillFakeState(&state, xldata->stateSrc);

    nInsert = xldata->replaceDead ? 1 : xldata->nMoves + 1;

    ptr += SizeOfSpgxlogMoveLeafs;
    toDelete = (OffsetNumber *) ptr;
    ptr += sizeof(OffsetNumber) * xldata->nMoves;
    toInsert = (OffsetNumber *) ptr;
    ptr += sizeof(OffsetNumber) * nInsert;

    /* now ptr points to the list of leaf tuples */
    POLAR_GET_LOG_TAG(record, leaf_tag, 1);

    if (BUFFERTAGS_EQUAL(*tag, leaf_tag))
    {
        /*
         * In normal operation we would have all three pages (source, dest, and
         * parent) locked simultaneously; but in WAL replay it should be safe to
         * update them one at a time, as long as we do it in the right order.
         */

        /* Insert tuples on the dest page (do first, so redirect is valid) */
        if (xldata->newPage)
        {
            POLAR_INIT_BUFFER_FOR_REDO(record, 1, buffer);
            SpGistInitBuffer(*buffer,
                             SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
            action = BLK_NEEDS_REDO;
        }
        else
            action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            int         i;

            page = BufferGetPage(*buffer);

            for (i = 0; i < nInsert; i++)
            {
                char       *leafTuple;
                SpGistLeafTupleData leafTupleHdr;

                /*
                 * the tuples are not aligned, so must copy to access the size
                 * field.
                 */
                leafTuple = ptr;
                memcpy(&leafTupleHdr, leafTuple,
                       sizeof(SpGistLeafTupleData));

                addOrReplaceTuple(page, (Item) leafTuple,
                                  leafTupleHdr.size, toInsert[i]);
                ptr += leafTupleHdr.size;
            }

            PageSetLSN(page, lsn);
        }

        return action;
    }

    POLAR_GET_LOG_TAG(record, src_tag, 0);

    if (BUFFERTAGS_EQUAL(*tag, src_tag))
    {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        /* Delete tuples from the source page, inserting a redirection pointer */
        if (action == BLK_NEEDS_REDO)
        {
            page = BufferGetPage(*buffer);

            spgPageIndexMultiDelete(&state, page, toDelete, xldata->nMoves,
                                    state.isBuild ? SPGIST_PLACEHOLDER : SPGIST_REDIRECT,
                                    SPGIST_PLACEHOLDER,
                                    blknoDst,
                                    toInsert[nInsert - 1]);

            PageSetLSN(page, lsn);
        }

        return action;
    }

    POLAR_GET_LOG_TAG(record, parent_tag, 2);

    if (BUFFERTAGS_EQUAL(*tag, parent_tag))
    {
        /* And update the parent downlink */
        action = POLAR_READ_BUFFER_FOR_REDO(record, 2, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            SpGistInnerTuple tuple;

            page = BufferGetPage(*buffer);

            tuple = (SpGistInnerTuple) PageGetItem(page,
                                                   PageGetItemId(page, xldata->offnumParent));

            spgUpdateNodeLink(tuple, xldata->nodeI,
                              blknoDst, toInsert[nInsert - 1]);

            PageSetLSN(page, lsn);
        }
    }

    return action;
}

static XLogRedoAction
polar_spg_redo_add_node(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    char       *ptr = XLogRecGetData(record);
    spgxlogAddNode *xldata = (spgxlogAddNode *) ptr;
    XLogRedoAction action = BLK_NOTFOUND;
    char       *innerTuple;
    SpGistInnerTupleData innerTupleHdr;
    SpGistState state;
    Page        page;
    BufferTag   old_tag, new_tag;

    ptr += sizeof(spgxlogAddNode);
    innerTuple = ptr;
    /* the tuple is unaligned, so make a copy to access its header */
    memcpy(&innerTupleHdr, innerTuple, sizeof(SpGistInnerTupleData));

    fillFakeState(&state, xldata->stateSrc);

    if (!XLogRecHasBlockRef(record, 1))
    {
        POLAR_GET_LOG_TAG(record, old_tag, 0);

        if (BUFFERTAGS_EQUAL(*tag, old_tag))
        {
            /* update in place */
            Assert(xldata->parentBlk == -1);
            action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

            if (action == BLK_NEEDS_REDO)
            {
                page = BufferGetPage(*buffer);

                PageIndexTupleDelete(page, xldata->offnum);

                if (PageAddItem(page, (Item) innerTuple, innerTupleHdr.size,
                                xldata->offnum,
                                false, false) != xldata->offnum)
                {
                    elog(ERROR, "failed to add item of size %u to SPGiST index page",
                         innerTupleHdr.size);
                }

                PageSetLSN(page, lsn);
            }

            return action;
        }
    }
    else
    {

        POLAR_GET_LOG_TAG(record, old_tag, 0);
        POLAR_GET_LOG_TAG(record, new_tag, 1);

        /*
         * In normal operation we would have all three pages (source, dest,
         * and parent) locked simultaneously; but in WAL replay it should be
         * safe to update them one at a time, as long as we do it in the right
         * order. We must insert the new tuple before replacing the old tuple
         * with the redirect tuple.
         */

        /* Install new tuple first so redirect is valid */
        if (BUFFERTAGS_EQUAL(*tag, new_tag))
        {
            if (xldata->newPage)
            {
                /* AddNode is not used for nulls pages */
                POLAR_INIT_BUFFER_FOR_REDO(record, 1, buffer);
                SpGistInitBuffer(*buffer, 0);
                action = BLK_NEEDS_REDO;
            }
            else
                action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

            if (action == BLK_NEEDS_REDO)
            {
                page = BufferGetPage(*buffer);

                addOrReplaceTuple(page, (Item) innerTuple,
                                  innerTupleHdr.size, xldata->offnumNew);

                /*
                 * If parent is in this same page, update it now.
                 */
                if (xldata->parentBlk == 1)
                {
                    SpGistInnerTuple parentTuple;

                    parentTuple = (SpGistInnerTuple) PageGetItem(page,
                                                                 PageGetItemId(page, xldata->offnumParent));

                    spgUpdateNodeLink(parentTuple, xldata->nodeI,
                                      new_tag.blockNum, xldata->offnumNew);
                }

                PageSetLSN(page, lsn);
            }

            return action;
        }

        if (BUFFERTAGS_EQUAL(*tag, old_tag))
        {
            action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

            /* Delete old tuple, replacing it with redirect or placeholder tuple */
            if (action == BLK_NEEDS_REDO)
            {
                SpGistDeadTuple dt;

                page = BufferGetPage(*buffer);

                if (state.isBuild)
                    dt = spgFormDeadTuple(&state, SPGIST_PLACEHOLDER,
                                          InvalidBlockNumber,
                                          InvalidOffsetNumber);
                else
                    dt = spgFormDeadTuple(&state, SPGIST_REDIRECT,
                                          new_tag.blockNum,
                                          xldata->offnumNew);

                PageIndexTupleDelete(page, xldata->offnum);

                if (PageAddItem(page, (Item) dt, dt->size,
                                xldata->offnum,
                                false, false) != xldata->offnum)
                {
                    elog(ERROR, "failed to add item of size %u to SPGiST index page",
                         dt->size);
                }

                if (state.isBuild)
                    SpGistPageGetOpaque(page)->nPlaceholder++;
                else
                    SpGistPageGetOpaque(page)->nRedirection++;

                /*
                 * If parent is in this same page, update it now.
                 */
                if (xldata->parentBlk == 0)
                {
                    SpGistInnerTuple parentTuple;

                    parentTuple = (SpGistInnerTuple) PageGetItem(page,
                                                                 PageGetItemId(page, xldata->offnumParent));

                    spgUpdateNodeLink(parentTuple, xldata->nodeI,
                                      new_tag.blockNum, xldata->offnumNew);
                }

                PageSetLSN(page, lsn);
            }

            return action;
        }

        /*
         * Update parent downlink (if we didn't do it as part of the source or
         * destination page update already).
         */
        if (xldata->parentBlk == 2)
        {
            BufferTag parent_tag;
            POLAR_GET_LOG_TAG(record, parent_tag, 2);

            if (BUFFERTAGS_EQUAL(*tag, parent_tag))
            {
                action = POLAR_READ_BUFFER_FOR_REDO(record, 2, buffer);

                if (action == BLK_NEEDS_REDO)
                {
                    SpGistInnerTuple parentTuple;

                    page = BufferGetPage(*buffer);

                    parentTuple = (SpGistInnerTuple) PageGetItem(page,
                                                                 PageGetItemId(page, xldata->offnumParent));

                    spgUpdateNodeLink(parentTuple, xldata->nodeI,
                                      new_tag.blockNum, xldata->offnumNew);

                    PageSetLSN(page, lsn);
                }
            }
        }
    }

    return action;
}

static XLogRedoAction
polar_spg_redo_split_tuple(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    char       *ptr = XLogRecGetData(record);
    spgxlogSplitTuple *xldata = (spgxlogSplitTuple *) ptr;
    XLogRedoAction action = BLK_NOTFOUND;
    char       *prefixTuple;
    SpGistInnerTupleData prefixTupleHdr;
    char       *postfixTuple;
    SpGistInnerTupleData postfixTupleHdr;
    Page        page;
    BufferTag   update_tag, orig_tag;

    ptr += sizeof(spgxlogSplitTuple);
    prefixTuple = ptr;
    /* the prefix tuple is unaligned, so make a copy to access its header */
    memcpy(&prefixTupleHdr, prefixTuple, sizeof(SpGistInnerTupleData));
    ptr += prefixTupleHdr.size;
    postfixTuple = ptr;
    /* postfix tuple is also unaligned */
    memcpy(&postfixTupleHdr, postfixTuple, sizeof(SpGistInnerTupleData));

    /*
     * In normal operation we would have both pages locked simultaneously; but
     * in WAL replay it should be safe to update them one at a time, as long
     * as we do it in the right order.
     */

    /* insert postfix tuple first to avoid dangling link */
    if (!xldata->postfixBlkSame)
    {
        POLAR_GET_LOG_TAG(record, update_tag, 1);

        if (BUFFERTAGS_EQUAL(*tag, update_tag))
        {
            if (xldata->newPage)
            {
                POLAR_INIT_BUFFER_FOR_REDO(record, 1, buffer);
                /* SplitTuple is not used for nulls pages */
                SpGistInitBuffer(*buffer, 0);
                action = BLK_NEEDS_REDO;
            }
            else
                action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

            if (action == BLK_NEEDS_REDO)
            {
                page = BufferGetPage(*buffer);

                addOrReplaceTuple(page, (Item) postfixTuple,
                                  postfixTupleHdr.size, xldata->offnumPostfix);

                PageSetLSN(page, lsn);
            }

            return action;
        }
    }

    POLAR_GET_LOG_TAG(record, orig_tag, 0);

    if (BUFFERTAGS_EQUAL(*tag, orig_tag))
    {
        /* now handle the original page */
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            page = BufferGetPage(*buffer);

            PageIndexTupleDelete(page, xldata->offnumPrefix);

            if (PageAddItem(page, (Item) prefixTuple, prefixTupleHdr.size,
                            xldata->offnumPrefix, false, false) != xldata->offnumPrefix)
            {
                elog(ERROR, "failed to add item of size %u to SPGiST index page",
                     prefixTupleHdr.size);
            }

            if (xldata->postfixBlkSame)
                addOrReplaceTuple(page, (Item) postfixTuple,
                                  postfixTupleHdr.size,
                                  xldata->offnumPostfix);

            PageSetLSN(page, lsn);
        }
    }

    return action;
}

static XLogRedoAction
polar_spg_redo_pick_split(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    char       *ptr = XLogRecGetData(record);
    spgxlogPickSplit *xldata = (spgxlogPickSplit *) ptr;
    XLogRedoAction action = BLK_NOTFOUND;
    char       *innerTuple;
    SpGistInnerTupleData innerTupleHdr;
    SpGistState state;
    OffsetNumber *toDelete;
    OffsetNumber *toInsert;
    uint8      *leafPageSelect;
    Page        srcPage = NULL;
    Page        destPage = NULL;
    Page        page = NULL;
    int         i;
    BlockNumber blknoInner;
    BufferTag   src_tag, dst_tag, inner_tag, parent_tag;

    XLogRecGetBlockTag(record, 2, NULL, NULL, &blknoInner);

    fillFakeState(&state, xldata->stateSrc);

    ptr += SizeOfSpgxlogPickSplit;
    toDelete = (OffsetNumber *) ptr;
    ptr += sizeof(OffsetNumber) * xldata->nDelete;
    toInsert = (OffsetNumber *) ptr;
    ptr += sizeof(OffsetNumber) * xldata->nInsert;
    leafPageSelect = (uint8 *) ptr;
    ptr += sizeof(uint8) * xldata->nInsert;

    innerTuple = ptr;
    /* the inner tuple is unaligned, so make a copy to access its header */
    memcpy(&innerTupleHdr, innerTuple, sizeof(SpGistInnerTupleData));
    ptr += innerTupleHdr.size;

    if (!XLogRecHasBlockRef(record, 0))
        srcPage = NULL;
    else
    {
        POLAR_GET_LOG_TAG(record, src_tag, 0);

        if (BUFFERTAGS_EQUAL(*tag, src_tag))
        {
            /* now ptr points to the list of leaf tuples */

            if (xldata->isRootSplit)
            {
                /* when splitting root, we touch it only in the guise of new inner */
                srcPage = NULL;
            }
            else if (xldata->initSrc)
            {
                /* just re-init the source page */
                POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
                srcPage = (Page) BufferGetPage(*buffer);

                SpGistInitBuffer(*buffer,
                                 SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
                /* don't update LSN etc till we're done with it */
                action = BLK_NEEDS_REDO;
            }
            else
            {
                /*
                 * Delete the specified tuples from source page.  (In case we're in
                 * Hot Standby, we need to hold lock on the page till we're done
                 * inserting leaf tuples and the new inner tuple, else the added
                 * redirect tuple will be a dangling link.)
                 */
                srcPage = NULL;
                action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

                if (action == BLK_NEEDS_REDO)
                {
                    srcPage = BufferGetPage(*buffer);

                    /*
                     * We have it a bit easier here than in doPickSplit(), because we
                     * know the inner tuple's location already, so we can inject the
                     * correct redirection tuple now.
                     */
                    if (!state.isBuild)
                        spgPageIndexMultiDelete(&state, srcPage,
                                                toDelete, xldata->nDelete,
                                                SPGIST_REDIRECT,
                                                SPGIST_PLACEHOLDER,
                                                blknoInner,
                                                xldata->offnumInner);
                    else
                        spgPageIndexMultiDelete(&state, srcPage,
                                                toDelete, xldata->nDelete,
                                                SPGIST_PLACEHOLDER,
                                                SPGIST_PLACEHOLDER,
                                                InvalidBlockNumber,
                                                InvalidOffsetNumber);

                    /* don't update LSN etc till we're done with it */
                }
            }
        }
    }

    /* try to access dest page if any */
    if (!XLogRecHasBlockRef(record, 1))
        destPage = NULL;
    else
    {
        POLAR_GET_LOG_TAG(record, dst_tag, 1);

        if (BUFFERTAGS_EQUAL(*tag, dst_tag))
        {
            if (xldata->initDest)
            {
                POLAR_INIT_BUFFER_FOR_REDO(record, 1, buffer);
                /* just re-init the dest page */
                destPage = (Page) BufferGetPage(*buffer);

                SpGistInitBuffer(*buffer,
                                 SPGIST_LEAF | (xldata->storesNulls ? SPGIST_NULLS : 0));
                /* don't update LSN etc till we're done with it */
                action = BLK_NEEDS_REDO;
            }
            else
            {
                /*
                 * We could probably release the page lock immediately in the
                 * full-page-image case, but for safety let's hold it till later.
                 */
                action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

                if (action == BLK_NEEDS_REDO)
                    destPage = (Page) BufferGetPage(*buffer);
                else
                    destPage = NULL;    /* don't do any page updates */
            }
        }
    }

    if (srcPage != NULL || destPage != NULL)
    {
        /* restore leaf tuples to src and/or dest page */
        for (i = 0; i < xldata->nInsert; i++)
        {
            char       *leafTuple;
            SpGistLeafTupleData leafTupleHdr;

            /* the tuples are not aligned, so must copy to access the size field. */
            leafTuple = ptr;
            memcpy(&leafTupleHdr, leafTuple, sizeof(SpGistLeafTupleData));
            ptr += leafTupleHdr.size;

            page = leafPageSelect[i] ? destPage : srcPage;

            if (page == NULL)
                continue;           /* no need to touch this page */

            addOrReplaceTuple(page, (Item) leafTuple, leafTupleHdr.size,
                              toInsert[i]);
        }

        if (srcPage != NULL)
            PageSetLSN(srcPage, lsn);

        if (destPage != NULL)
            PageSetLSN(destPage, lsn);
    }

    POLAR_GET_LOG_TAG(record, inner_tag, 2);

    if (BUFFERTAGS_EQUAL(*tag, inner_tag))
    {
        /* restore new inner tuple */
        if (xldata->initInner)
        {
            POLAR_INIT_BUFFER_FOR_REDO(record, 2, buffer);
            SpGistInitBuffer(*buffer, (xldata->storesNulls ? SPGIST_NULLS : 0));
            action = BLK_NEEDS_REDO;
        }
        else
            action = POLAR_READ_BUFFER_FOR_REDO(record, 2, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            page = BufferGetPage(*buffer);

            addOrReplaceTuple(page, (Item) innerTuple, innerTupleHdr.size,
                              xldata->offnumInner);

            /* if inner is also parent, update link while we're here */
            if (xldata->innerIsParent)
            {
                SpGistInnerTuple parent;

                parent = (SpGistInnerTuple) PageGetItem(page,
                                                        PageGetItemId(page, xldata->offnumParent));
                spgUpdateNodeLink(parent, xldata->nodeI,
                                  blknoInner, xldata->offnumInner);
            }

            PageSetLSN(page, lsn);
        }

        return action;
    }

    /* update parent downlink, unless we did it above */
    if (XLogRecHasBlockRef(record, 3))
    {
        POLAR_GET_LOG_TAG(record, parent_tag, 3);

        if (BUFFERTAGS_EQUAL(*tag, parent_tag))
        {
            action = POLAR_READ_BUFFER_FOR_REDO(record, 3, buffer);

            if (action == BLK_NEEDS_REDO)
            {
                SpGistInnerTuple parent;

                page = BufferGetPage(*buffer);

                parent = (SpGistInnerTuple) PageGetItem(page,
                                                        PageGetItemId(page, xldata->offnumParent));
                spgUpdateNodeLink(parent, xldata->nodeI,
                                  blknoInner, xldata->offnumInner);

                PageSetLSN(page, lsn);
            }
        }
    }
    else
        Assert(xldata->innerIsParent || xldata->isRootSplit);

    return action;
}

static XLogRedoAction
polar_spg_redo_vacuum_leaf(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    char       *ptr = XLogRecGetData(record);
    spgxlogVacuumLeaf *xldata = (spgxlogVacuumLeaf *) ptr;
    XLogRedoAction action = BLK_NOTFOUND;
    OffsetNumber *toDead;
    OffsetNumber *toPlaceholder;
    OffsetNumber *moveSrc;
    OffsetNumber *moveDest;
    OffsetNumber *chainSrc;
    OffsetNumber *chainDest;
    SpGistState state;
    Page        page;
    int         i;
    BufferTag  vacuum_tag;

    POLAR_GET_LOG_TAG(record, vacuum_tag, 0);

    if (!BUFFERTAGS_EQUAL(*tag, vacuum_tag))
        return action;

    fillFakeState(&state, xldata->stateSrc);

    ptr += SizeOfSpgxlogVacuumLeaf;
    toDead = (OffsetNumber *) ptr;
    ptr += sizeof(OffsetNumber) * xldata->nDead;
    toPlaceholder = (OffsetNumber *) ptr;
    ptr += sizeof(OffsetNumber) * xldata->nPlaceholder;
    moveSrc = (OffsetNumber *) ptr;
    ptr += sizeof(OffsetNumber) * xldata->nMove;
    moveDest = (OffsetNumber *) ptr;
    ptr += sizeof(OffsetNumber) * xldata->nMove;
    chainSrc = (OffsetNumber *) ptr;
    ptr += sizeof(OffsetNumber) * xldata->nChain;
    chainDest = (OffsetNumber *) ptr;

    action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

    if (action == BLK_NEEDS_REDO)
    {
        page = BufferGetPage(*buffer);

        spgPageIndexMultiDelete(&state, page,
                                toDead, xldata->nDead,
                                SPGIST_DEAD, SPGIST_DEAD,
                                InvalidBlockNumber,
                                InvalidOffsetNumber);

        spgPageIndexMultiDelete(&state, page,
                                toPlaceholder, xldata->nPlaceholder,
                                SPGIST_PLACEHOLDER, SPGIST_PLACEHOLDER,
                                InvalidBlockNumber,
                                InvalidOffsetNumber);

        /* see comments in vacuumLeafPage() */
        for (i = 0; i < xldata->nMove; i++)
        {
            ItemId      idSrc = PageGetItemId(page, moveSrc[i]);
            ItemId      idDest = PageGetItemId(page, moveDest[i]);
            ItemIdData  tmp;

            tmp = *idSrc;
            *idSrc = *idDest;
            *idDest = tmp;
        }

        spgPageIndexMultiDelete(&state, page,
                                moveSrc, xldata->nMove,
                                SPGIST_PLACEHOLDER, SPGIST_PLACEHOLDER,
                                InvalidBlockNumber,
                                InvalidOffsetNumber);

        for (i = 0; i < xldata->nChain; i++)
        {
            SpGistLeafTuple lt;

            lt = (SpGistLeafTuple) PageGetItem(page,
                                               PageGetItemId(page, chainSrc[i]));
            Assert(lt->tupstate == SPGIST_LIVE);
            lt->nextOffset = chainDest[i];
        }

        PageSetLSN(page, lsn);
    }

    return action;
}

static XLogRedoAction
polar_spg_redo_vacuum_root(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    char       *ptr = XLogRecGetData(record);
    spgxlogVacuumRoot *xldata = (spgxlogVacuumRoot *) ptr;
    XLogRedoAction  action = BLK_NOTFOUND;
    OffsetNumber *toDelete;
    Page        page;
    BufferTag   del_tag;

    POLAR_GET_LOG_TAG(record, del_tag, 0);

    if (!BUFFERTAGS_EQUAL(*tag, del_tag))
        return action;

    toDelete = xldata->offsets;

    action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

    if (action == BLK_NEEDS_REDO)
    {
        page = BufferGetPage(*buffer);

        /* The tuple numbers are in order */
        PageIndexMultiDelete(page, toDelete, xldata->nDelete);

        PageSetLSN(page, lsn);
    }

    return action;
}

static XLogRedoAction
polar_spg_redo_vacuum_redirect(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    char       *ptr = XLogRecGetData(record);
    spgxlogVacuumRedirect *xldata = (spgxlogVacuumRedirect *) ptr;
    XLogRedoAction action = BLK_NOTFOUND;
    OffsetNumber *itemToPlaceholder;
    BufferTag   vacuum_tag;

    POLAR_GET_LOG_TAG(record, vacuum_tag, 0);

    if (!BUFFERTAGS_EQUAL(*tag, vacuum_tag))
        return action;

    itemToPlaceholder = xldata->offsets;

    action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

    if (action == BLK_NEEDS_REDO)
    {
        Page        page = BufferGetPage(*buffer);
        SpGistPageOpaque opaque = SpGistPageGetOpaque(page);
        int         i;

        /* Convert redirect pointers to plain placeholders */
        for (i = 0; i < xldata->nToPlaceholder; i++)
        {
            SpGistDeadTuple dt;

            dt = (SpGistDeadTuple) PageGetItem(page,
                                               PageGetItemId(page, itemToPlaceholder[i]));
            Assert(dt->tupstate == SPGIST_REDIRECT);
            dt->tupstate = SPGIST_PLACEHOLDER;
            ItemPointerSetInvalid(&dt->pointer);
        }

        Assert(opaque->nRedirection >= xldata->nToPlaceholder);
        opaque->nRedirection -= xldata->nToPlaceholder;
        opaque->nPlaceholder += xldata->nToPlaceholder;

        /* Remove placeholder tuples at end of page */
        if (xldata->firstPlaceholder != InvalidOffsetNumber)
        {
            int         max = PageGetMaxOffsetNumber(page);
            OffsetNumber *toDelete;

            toDelete = palloc(sizeof(OffsetNumber) * max);

            for (i = xldata->firstPlaceholder; i <= max; i++)
                toDelete[i - xldata->firstPlaceholder] = i;

            i = max - xldata->firstPlaceholder + 1;
            Assert(opaque->nPlaceholder >= i);
            opaque->nPlaceholder -= i;

            /* The array is sorted, so can use PageIndexMultiDelete */
            PageIndexMultiDelete(page, toDelete, i);

            pfree(toDelete);
        }

        PageSetLSN(page, lsn);
    }

    return action;
}

static void
polar_spg_redo_add_node_save(XLogReaderState *record)
{
    if (!XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn(record, 0);
    else
    {
        ParseXLogBlocksLsn(record, 1);
        ParseXLogBlocksLsn(record, 0);

        if (XLogRecHasBlockRef(record, 2))
            ParseXLogBlocksLsn(record, 2);
    }
}

static void
polar_spg_redo_add_node_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 3);

    if (!XLogRecHasBlockRef(record, 1)) {
//        ParseXLogBlocksLsn(record, 0);
        XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
    }
    else
    {
//        ParseXLogBlocksLsn(record, 1);
//        ParseXLogBlocksLsn(record, 0);
        XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
        XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;


        if (XLogRecHasBlockRef(record, 2)) {
            XLogRecGetBlockTag(record, 2, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
            tagCount++;

//            ParseXLogBlocksLsn(record, 2);
        }
    }

    *tagNum = tagCount;
}

static void
polar_spg_redo_pick_split_save(XLogReaderState *record)
{
    if (XLogRecHasBlockRef(record, 0))
        ParseXLogBlocksLsn(record, 0);

    if (XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn(record, 1);

    ParseXLogBlocksLsn(record, 2);

    if (XLogRecHasBlockRef(record, 3))
        ParseXLogBlocksLsn(record, 3);
}

static void
polar_spg_redo_pick_split_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 4);

    if (XLogRecHasBlockRef(record, 0)) {
        XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
//        ParseXLogBlocksLsn(record, 0);
    }

    if (XLogRecHasBlockRef(record, 1)) {
        XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;

//        ParseXLogBlocksLsn(record, 1);
    }

//    ParseXLogBlocksLsn(record, 2);
    XLogRecGetBlockTag(record, 2, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;

    if (XLogRecHasBlockRef(record, 3)) {
        XLogRecGetBlockTag(record, 3, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;

//        ParseXLogBlocksLsn(record, 3);
    }

    *tagNum = tagCount;
}


bool
polar_spg_idx_save(XLogReaderState *record)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info)
    {
//        case XLOG_SPGIST_CREATE_INDEX:
//            polar_logindex_save_block(instance, record, 0);
//            polar_logindex_save_block(instance, record, 1);
//            polar_logindex_save_block(instance, record, 2);
//            break;

        case XLOG_SPGIST_ADD_LEAF:
            ParseXLogBlocksLsn(record, 0);

            if (XLogRecHasBlockRef(record, 1))
                ParseXLogBlocksLsn(record, 1);

            break;

        case XLOG_SPGIST_MOVE_LEAFS:
            ParseXLogBlocksLsn(record, 1);
            ParseXLogBlocksLsn(record, 0);
            ParseXLogBlocksLsn(record, 2);
            break;

        case XLOG_SPGIST_ADD_NODE:
            polar_spg_redo_add_node_save(record);
            break;

        case XLOG_SPGIST_SPLIT_TUPLE:
            if (XLogRecHasBlockRef(record, 1))
                ParseXLogBlocksLsn(record, 1);

            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_SPGIST_PICKSPLIT:
            polar_spg_redo_pick_split_save(record);
            break;

        case XLOG_SPGIST_VACUUM_LEAF:
        case XLOG_SPGIST_VACUUM_ROOT:
        case XLOG_SPGIST_VACUUM_REDIRECT:
            ParseXLogBlocksLsn(record, 0);
            break;

        default:
            elog(PANIC, "polar_spg_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}

bool
polar_spg_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    switch (info)
    {
//        case XLOG_SPGIST_CREATE_INDEX:
//            polar_logindex_save_block(instance, record, 0);
//            polar_logindex_save_block(instance, record, 1);
//            polar_logindex_save_block(instance, record, 2);
//            break;

        case XLOG_SPGIST_ADD_LEAF:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 2);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
            tagCount++;

//            ParseXLogBlocksLsn(record, 0);
            if (XLogRecHasBlockRef(record, 1)) {
                XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
                INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
                tagCount++;

//                ParseXLogBlocksLsn(record, 1);
            }
            *tagNum = tagCount;

            break;

        case XLOG_SPGIST_MOVE_LEAFS:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 3);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG(*buffertagList[0], rnode, forkNumber, blockNumber);

            XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG(*buffertagList[1], rnode, forkNumber, blockNumber);

            XLogRecGetBlockTag(record, 2, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG(*buffertagList[2], rnode, forkNumber, blockNumber);
            *tagNum = 3;
            break;

        case XLOG_SPGIST_ADD_NODE:
            polar_spg_redo_add_node_get_bufftag_list(record, buffertagList, tagNum);
            break;

        case XLOG_SPGIST_SPLIT_TUPLE:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 2);

            if (XLogRecHasBlockRef(record, 1)) {
                XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
                INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
                tagCount++;

//                ParseXLogBlocksLsn(record, 1);
            }

//            ParseXLogBlocksLsn(record, 0);
            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
            tagCount++;

            *tagNum = tagCount;
            break;

        case XLOG_SPGIST_PICKSPLIT:
            polar_spg_redo_pick_split_get_bufftag_list(record, buffertagList, tagNum);
            break;

        case XLOG_SPGIST_VACUUM_LEAF:
        case XLOG_SPGIST_VACUUM_ROOT:
        case XLOG_SPGIST_VACUUM_REDIRECT:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 1);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG(*buffertagList[0], rnode, forkNumber, blockNumber);
            *tagNum = 1;
//            ParseXLogBlocksLsn(record, 0);
            break;

        default:
            elog(PANIC, "polar_spg_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}

//
//bool
//polar_spg_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
//{
//    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
//
//    switch (info)
//    {
//        case XLOG_SPGIST_CREATE_INDEX:
//            polar_logindex_redo_parse(instance, record, 0);
//            polar_logindex_redo_parse(instance, record, 1);
//            polar_logindex_redo_parse(instance, record, 2);
//            break;
//
//        case XLOG_SPGIST_ADD_LEAF:
//            polar_logindex_redo_parse(instance, record, 0);
//
//            if (XLogRecHasBlockRef(record, 1))
//                polar_logindex_redo_parse(instance, record, 1);
//
//            break;
//
//        case XLOG_SPGIST_MOVE_LEAFS:
//            polar_logindex_redo_parse(instance, record, 1);
//            polar_logindex_redo_parse(instance, record, 0);
//            polar_logindex_redo_parse(instance, record, 2);
//            break;
//
//        case XLOG_SPGIST_ADD_NODE:
//            polar_spg_redo_add_node_parse(instance, record);
//            break;
//
//        case XLOG_SPGIST_SPLIT_TUPLE:
//            if (XLogRecHasBlockRef(record, 1))
//                polar_logindex_redo_parse(instance, record, 1);
//
//            polar_logindex_redo_parse(instance, record, 0);
//            break;
//
//        case XLOG_SPGIST_PICKSPLIT:
//            polar_spg_redo_pick_split_parse(instance, record);
//            break;
//
//        case XLOG_SPGIST_VACUUM_LEAF:
//        case XLOG_SPGIST_VACUUM_ROOT:
//            polar_logindex_redo_parse(instance, record, 0);
//            break;
//
//        case XLOG_SPGIST_VACUUM_REDIRECT:
//            polar_spg_redo_vacuum_parse(instance, record);
//            break;
//
//        default:
//            elog(PANIC, "polar_spg_idx_parse: unknown op code %u", info);
//            break;
//    }
//
//    return true;
//}

XLogRedoAction
polar_spg_idx_redo(XLogReaderState *record,  BufferTag *tag, Buffer *buffer)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRedoAction action = BLK_NOTFOUND;
    MemoryContext old_ctx, redo_ctx;

    redo_ctx = polar_get_redo_context();
    old_ctx = MemoryContextSwitchTo(redo_ctx);

    switch (info)
    {
        //PG 13.0 don't have this CREATE_INDEX xlog anymore, don't know the details
//        case XLOG_SPGIST_CREATE_INDEX:
//        {
//            action = polar_spg_redo_create_index(record, tag, buffer);
//            break;
//        }

        case XLOG_SPGIST_ADD_LEAF:
        {
            action = polar_spg_redo_add_leaf(record, tag, buffer);
            break;
        }

        case XLOG_SPGIST_MOVE_LEAFS:
        {
            action = polar_spg_redo_move_leafs(record, tag, buffer);
            break;
        }

        case XLOG_SPGIST_ADD_NODE:
        {
            action = polar_spg_redo_add_node(record, tag, buffer);
            break;
        }

        case XLOG_SPGIST_SPLIT_TUPLE:
        {
            action = polar_spg_redo_split_tuple(record, tag, buffer);
            break;
        }

        case XLOG_SPGIST_PICKSPLIT:
        {
            action = polar_spg_redo_pick_split(record, tag, buffer);
            break;
        }

        case XLOG_SPGIST_VACUUM_LEAF:
        {
            action = polar_spg_redo_vacuum_leaf(record, tag, buffer);
            break;
        }

        case XLOG_SPGIST_VACUUM_ROOT:
        {
            action = polar_spg_redo_vacuum_root(record, tag, buffer);
            break;
        }

        case XLOG_SPGIST_VACUUM_REDIRECT:
        {
            action = polar_spg_redo_vacuum_redirect(record, tag, buffer);
            break;
        }

        default:
            elog(PANIC, "polar_spg_idx_redo: unknown op code %u", info);
            break;
    }

    MemoryContextSwitchTo(old_ctx);
    MemoryContextReset(redo_ctx);
    return action;
}
