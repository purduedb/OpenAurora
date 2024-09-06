#include "postgres.h"

#include "access/bufmask.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/polar_logindex.h"
#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/freespace.h"
#include "storage/standby.h"
#include "access/polar_logindex.h"
#include "storage/kv_interface.h"
#include "access/xlog.h"

//#define ENABLE_DEBUG_INFO
static XLogRedoAction
polar_heap_clear_vm(XLogReaderState *record, RelFileNode *rnode,
                    BlockNumber heapBlk, Buffer *buffer, uint8 flags)
{
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d \n", __func__ , __LINE__);
    fflush(stdout);
#endif

    Relation    reln = CreateFakeRelcacheEntry(*rnode);
    int         mapByte = HEAPBLK_TO_MAPBYTE(heapBlk);
    int         mapOffset = HEAPBLK_TO_OFFSET(heapBlk);
    uint8       mask = flags << mapOffset;
    char       *map;
    Page        page;

    if (!BufferIsValid(*buffer))
    {
        visibilitymap_pin(reln, heapBlk, buffer);
        LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d \n", __func__ , __LINE__);
    fflush(stdout);
#endif

    map = PageGetContents(BufferGetPage(*buffer));

    if (map[mapByte] & mask)
        map[mapByte] &= ~mask;

    FreeFakeRelcacheEntry(reln);

    page = BufferGetPage(*buffer);

    if (PageGetLSN(page) < record->EndRecPtr)
        PageSetLSN(page, record->EndRecPtr);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d \n", __func__ , __LINE__);
    fflush(stdout);
#endif

    return BLK_NEEDS_REDO;
}

/*
 * Handles MULTI_INSERT record type.
 */
static XLogRedoAction
polar_heap_xlog_multi_insert(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    XLogRedoAction action = BLK_NOTFOUND;
    xl_heap_multi_insert *xlrec;
    Page        page;
    union
    {
        HeapTupleHeaderData hdr;
        char        data[MaxHeapTupleSize];
    }           tbuf;
    HeapTupleHeader htup;
    uint32      newlen;
    int         i;
    bool        isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;
    BufferTag   tag0, tag1;

    POLAR_GET_LOG_TAG(record, tag0, 0);
    /*
     * Insertion doesn't overwrite MVCC data, so no conflict processing is
     * required.
     */
    xlrec = (xl_heap_multi_insert *) XLogRecGetData(record);

    INIT_BUFFERTAG(tag1, tag0.rnode, VISIBILITYMAP_FORKNUM, HEAPBLK_TO_MAPBLOCK(tag0.blockNum));

    if (BUFFERTAGS_EQUAL(tag1, *tag))
    {
        /*
         * The visibility map may need to be fixed even if the heap page is
         * already up-to-date.
         */

        if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
        {
            action = polar_heap_clear_vm(record, &tag0.rnode, tag0.blockNum, buffer,
                                         VISIBILITYMAP_VALID_BITS);
        }
    }

    if (BUFFERTAGS_EQUAL(tag0, *tag))
    {
        if (isinit)
        {
            POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);

            page = BufferGetPage(*buffer);
            PageInit(page, BufferGetPageSize(*buffer), 0);
            action = BLK_NEEDS_REDO;
        }
        else
            action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            char       *tupdata;
            char       *endptr;
            Size        len;

            /* Tuples are stored as block data */
            tupdata = XLogRecGetBlockData(record, 0, &len);
            endptr = tupdata + len;

            page = (Page) BufferGetPage(*buffer);

            for (i = 0; i < xlrec->ntuples; i++)
            {
                OffsetNumber offnum;
                xl_multi_insert_tuple *xlhdr;

                /*
                 * If we're reinitializing the page, the tuples are stored in
                 * order from FirstOffsetNumber. Otherwise there's an array of
                 * offsets in the WAL record, and the tuples come after that.
                 */
                if (isinit)
                    offnum = FirstOffsetNumber + i;
                else
                    offnum = xlrec->offsets[i];

                if (PageGetMaxOffsetNumber(page) + 1 < offnum)
                {
                    elog(PANIC, "invalid max offset number, page_max_off=%ld, offnum=%d",
                         PageGetMaxOffsetNumber(page), offnum);
                }

                xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(tupdata);
                tupdata = ((char *) xlhdr) + SizeOfMultiInsertTuple;

                newlen = xlhdr->datalen;
                Assert(newlen <= MaxHeapTupleSize);
                htup = &tbuf.hdr;
                MemSet((char *) htup, 0, SizeofHeapTupleHeader);
                /* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
                memcpy((char *) htup + SizeofHeapTupleHeader,
                       (char *) tupdata,
                       newlen);
                tupdata += newlen;

                newlen += SizeofHeapTupleHeader;
                htup->t_infomask2 = xlhdr->t_infomask2;
                htup->t_infomask = xlhdr->t_infomask;
                htup->t_hoff = xlhdr->t_hoff;
                HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
                HeapTupleHeaderSetCmin(htup, FirstCommandId);
                ItemPointerSetBlockNumber(&htup->t_ctid, tag0.blockNum);
                ItemPointerSetOffsetNumber(&htup->t_ctid, offnum);

                offnum = PageAddItem(page, (Item) htup, newlen, offnum, true, true);

                if (offnum == InvalidOffsetNumber)
                {
                    elog(PANIC, "failed to add tuple");
                }
            }

            if (tupdata != endptr)
            {
                elog(PANIC, "total tuple length mismatch, gap=%ld", endptr - tupdata);
            }

            PageSetLSN(page, lsn);

            if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
                PageClearAllVisible(page);
        }
    }

    return action;
}



static XLogRedoAction
polar_heap_xlog_insert(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d -- , record lsn = %lu\n", __func__ , __LINE__, lsn);
    fflush(stdout);
#endif

    xl_heap_insert *xlrec = (xl_heap_insert *) XLogRecGetData(record);
    Page        page;
    union
    {
        HeapTupleHeaderData hdr;
        char        data[MaxHeapTupleSize];
    }           tbuf;
    HeapTupleHeader htup;
    xl_heap_header xlhdr;
    uint32      newlen;
    ItemPointerData target_tid;
    BufferTag   tag0, tag1;
    XLogRedoAction action = BLK_NOTFOUND;

    POLAR_GET_LOG_TAG(record, tag0, 0);
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    INIT_BUFFERTAG(tag1, tag0.rnode, VISIBILITYMAP_FORKNUM, HEAPBLK_TO_MAPBLOCK(tag0.blockNum));
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d, parameter tag: spc=%lu, db=%lu, rel=%lu, fork=%d, blk=%lu\n", __func__ , __LINE__,
           tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->forkNum, tag->blockNum);

    printf("%s %d, tag0: spc=%lu, db=%lu, rel=%lu, fork=%d, blk=%lu\n", __func__ , __LINE__,
           tag0.rnode.spcNode, tag0.rnode.dbNode, tag0.rnode.relNode, tag0.forkNum, tag0.blockNum);

    printf("%s %d, tag1: spc=%lu, db=%lu, rel=%lu, fork=%d, blk=%lu\n", __func__ , __LINE__,
           tag1.rnode.spcNode, tag1.rnode.dbNode, tag1.rnode.relNode, tag1.forkNum, tag1.blockNum);
    fflush(stdout);
#endif

    if (BUFFERTAGS_EQUAL(*tag, tag1))
    {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d \n", __func__ , __LINE__);
        fflush(stdout);
#endif

        /*
         * The visibility map may need to be fixed even if the heap page is
         * already up-to-date.
         */
        if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
        {
#ifdef ENABLE_DEBUG_INFO
            printf("%s %d \n", __func__ , __LINE__);
            fflush(stdout);
#endif

            action = polar_heap_clear_vm(record, &tag0.rnode, tag0.blockNum, buffer,
                                         VISIBILITYMAP_VALID_BITS);
        }
    }

    if (BUFFERTAGS_EQUAL(*tag, tag0))
    {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d \n", __func__ , __LINE__);
        fflush(stdout);
#endif

        ItemPointerSetBlockNumber(&target_tid, tag0.blockNum);
        ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

        /*
         * If we inserted the first and only tuple on the page, re-initialize the
         * page from scratch.
         */
        if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
        {
            POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);

            page = BufferGetPage(*buffer);
            PageInit(page, BufferGetPageSize(*buffer), 0);
            action = BLK_NEEDS_REDO;
        }
        else
            action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO)
        {
#ifdef ENABLE_DEBUG_INFO
            printf("%s %d, action need redo\n", __func__ , __LINE__);
            fflush(stdout);
#endif

            Size        datalen;
            char       *data;

            page = BufferGetPage(*buffer);

            if (PageGetMaxOffsetNumber(page) + 1 < xlrec->offnum)
            {
                elog(PANIC, "invalid max offset number: page_max_off=%ld, offnum=%d",
                     PageGetMaxOffsetNumber(page), xlrec->offnum);
            }

            data = XLogRecGetBlockData(record, 0, &datalen);

            newlen = datalen - SizeOfHeapHeader;
            Assert(datalen > SizeOfHeapHeader && newlen <= MaxHeapTupleSize);
            memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
            data += SizeOfHeapHeader;

            htup = &tbuf.hdr;
            MemSet((char *) htup, 0, SizeofHeapTupleHeader);
            /* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
            memcpy((char *) htup + SizeofHeapTupleHeader,
                   data,
                   newlen);
            newlen += SizeofHeapTupleHeader;
            htup->t_infomask2 = xlhdr.t_infomask2;
            htup->t_infomask = xlhdr.t_infomask;
            htup->t_hoff = xlhdr.t_hoff;
            HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
            HeapTupleHeaderSetCmin(htup, FirstCommandId);
            htup->t_ctid = target_tid;

            if (PageAddItem(page, (Item) htup, newlen, xlrec->offnum,
                            true, true) == InvalidOffsetNumber)
            {
                elog(PANIC, "failed to add tuple");
            }

            PageSetLSN(page, lsn);

            if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
                PageClearAllVisible(page);

        }
    }

    return action;
}

static XLogRedoAction
polar_heap_xlog_delete(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    xl_heap_delete *xlrec = (xl_heap_delete *) XLogRecGetData(record);
    Page        page;
    ItemId      lp = NULL;
    HeapTupleHeader htup;
    ItemPointerData target_tid;
    BufferTag tag0, tag1;

    XLogRedoAction action = BLK_NOTFOUND;

    POLAR_GET_LOG_TAG(record, tag0, 0);
    INIT_BUFFERTAG(tag1, tag0.rnode, VISIBILITYMAP_FORKNUM, HEAPBLK_TO_MAPBLOCK(tag0.blockNum));

    if (BUFFERTAGS_EQUAL(*tag, tag1))
    {
        /*
         * The visibility map may need to be fixed even if the heap page is
         * already up-to-date.
         */
        if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
        {
            action = polar_heap_clear_vm(record, &tag0.rnode, tag0.blockNum, buffer,
                                         VISIBILITYMAP_VALID_BITS);
        }

        return action;
    }

    if (BUFFERTAGS_EQUAL(*tag, tag0))
    {
        ItemPointerSetBlockNumber(&target_tid, tag0.blockNum);
        ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            page = BufferGetPage(*buffer);

            if (PageGetMaxOffsetNumber(page) >= xlrec->offnum)
                lp = PageGetItemId(page, xlrec->offnum);

            if (PageGetMaxOffsetNumber(page) < xlrec->offnum || !lp || !ItemIdIsNormal(lp))
            {
                elog(PANIC, "invalid lp: page_max_off=%ld, offnum=%d, lp=%d",
                     PageGetMaxOffsetNumber(page), xlrec->offnum, (lp ? lp->lp_flags : 0));
            }

            htup = (HeapTupleHeader) PageGetItem(page, lp);

            htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
            htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
            HeapTupleHeaderClearHotUpdated(htup);
            fix_infomask_from_infobits(xlrec->infobits_set,
                                       &htup->t_infomask, &htup->t_infomask2);

            if (!(xlrec->flags & XLH_DELETE_IS_SUPER))
                HeapTupleHeaderSetXmax(htup, xlrec->xmax);
            else
                HeapTupleHeaderSetXmin(htup, InvalidTransactionId);

            HeapTupleHeaderSetCmax(htup, FirstCommandId, false);

            /* Mark the page as a candidate for pruning */
            PageSetPrunable(page, XLogRecGetXid(record));

            if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
                PageClearAllVisible(page);

            /* Make sure t_ctid is set correctly */
            if (xlrec->flags & XLH_DELETE_IS_PARTITION_MOVE)
                HeapTupleHeaderSetMovedPartitions(htup);
            else
                htup->t_ctid = target_tid;

            PageSetLSN(page, lsn);
        }
    }

    return action;
}

static XLogRedoAction
polar_heap_xlog_update(XLogReaderState *record, BufferTag *tag, Buffer *buffer, bool hotupdate)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    XLogRecPtr  orig_old_lsn = InvalidXLogRecPtr;
    xl_heap_update *xlrec = (xl_heap_update *) XLogRecGetData(record);
    BlockNumber oldblk;
    BlockNumber newblk;
    ItemPointerData newtid;
    Page        page;
    OffsetNumber offnum;
    ItemId      lp = NULL;
    HeapTupleData oldtup;
    HeapTupleHeader htup;
    uint16      prefixlen = 0,
            suffixlen = 0;
    char       *newp;
    union
    {
        HeapTupleHeaderData hdr;
        char        data[MaxHeapTupleSize];
    }           tbuf;
    xl_heap_header xlhdr;
    uint32      newlen;
    XLogRedoAction action = BLK_NOTFOUND;
    BufferTag   tag0, tag1, tag2, tag3;
    BufferTag   *old_tag = NULL,
            *new_tag = NULL,
            *old_vm_tag = NULL,
            *new_vm_tag = NULL;

    /* initialize to keep the compiler quiet */
    oldtup.t_data = NULL;
    oldtup.t_len = 0;

    POLAR_GET_LOG_TAG(record, tag0, 0);
    newblk = tag0.blockNum;
    new_tag = &tag0;

    CLEAR_BUFFERTAG(tag1);

    if (XLogRecHasBlockRef(record, 1))
    {
        /* HOT updates are never done across pages */
        Assert(!hotupdate);
        POLAR_GET_LOG_TAG(record, tag1, 1);
        oldblk = tag1.blockNum;
        old_tag = &tag1;
    }
    else
    {
        oldblk = newblk;
        old_tag = &tag0;
    }

    if (oldblk != newblk)
    {
        if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
        {
            INIT_BUFFERTAG(tag3, tag1.rnode, VISIBILITYMAP_FORKNUM,
                           HEAPBLK_TO_MAPBLOCK(tag1.blockNum));

            old_vm_tag = &tag3;
        }

        if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
        {
            INIT_BUFFERTAG(tag2, tag0.rnode, VISIBILITYMAP_FORKNUM,
                           HEAPBLK_TO_MAPBLOCK(tag0.blockNum));
            new_vm_tag = &tag2;
        }
    }
    else
    {
        if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
        {
            INIT_BUFFERTAG(tag2, tag0.rnode, VISIBILITYMAP_FORKNUM,
                           HEAPBLK_TO_MAPBLOCK(tag0.blockNum));
            old_vm_tag = &tag2;
        }
    }

    ItemPointerSet(&newtid, newblk, xlrec->new_offnum);

    if (old_vm_tag != NULL && BUFFERTAGS_EQUAL(*old_vm_tag, *tag))
    {
        /*
         * The visibility map may need to be fixed even if the heap page is
         * already up-to-date.
         */
        if ((xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED))
        {
            action = polar_heap_clear_vm(record, &old_tag->rnode, old_tag->blockNum,
                                         buffer, VISIBILITYMAP_VALID_BITS);
        }
    }

    if (new_vm_tag != NULL && BUFFERTAGS_EQUAL(*new_vm_tag, *tag))
    {
        /*
         * The visibility map may need to be fixed even if the heap page is
         * already up-to-date.
         */
        if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
        {
            action = polar_heap_clear_vm(record, &new_tag->rnode, new_tag->blockNum,
                                         buffer, VISIBILITYMAP_VALID_BITS);
        }
    }

    if (BUFFERTAGS_EQUAL(*tag, *old_tag))
    {
        /*
         * In normal operation, it is important to lock the two pages in
         * page-number order, to avoid possible deadlocks against other update
         * operations going the other way.  However, during WAL replay there can
         * be no other update happening, so we don't need to worry about that. But
         * we *do* need to worry that we don't expose an inconsistent state to Hot
         * Standby queries --- so the original page can't be unlocked before we've
         * added the new tuple to the new page.
         */

        /* Deal with old tuple version */
        action = POLAR_READ_BUFFER_FOR_REDO(record, (oldblk == newblk) ? 0 : 1, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            page = BufferGetPage(*buffer);
            offnum = xlrec->old_offnum;
            orig_old_lsn = PageGetLSN(page);

            if (PageGetMaxOffsetNumber(page) >= offnum)
                lp = PageGetItemId(page, offnum);

            if (PageGetMaxOffsetNumber(page) < offnum || !lp || !ItemIdIsNormal(lp))
            {
                elog(PANIC, "invalid lp: page_max_off=%ld, offnum=%d, lp=%d",
                     PageGetMaxOffsetNumber(page), offnum, (lp ? lp->lp_flags : 0));
            }

            htup = (HeapTupleHeader) PageGetItem(page, lp);

            oldtup.t_data = htup;
            oldtup.t_len = ItemIdGetLength(lp);

            htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
            htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;

            if (hotupdate)
                HeapTupleHeaderSetHotUpdated(htup);
            else
                HeapTupleHeaderClearHotUpdated(htup);

            fix_infomask_from_infobits(xlrec->old_infobits_set, &htup->t_infomask,
                                       &htup->t_infomask2);
            HeapTupleHeaderSetXmax(htup, xlrec->old_xmax);
            HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
            /* Set forward chain link in t_ctid */
            htup->t_ctid = newtid;

            /* Mark the page as a candidate for pruning */
            PageSetPrunable(page, XLogRecGetXid(record));

            if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
                PageClearAllVisible(page);

            PageSetLSN(page, lsn);
        }

    }

    /* Maybe old and new tuple all in the same page  */
    if (BUFFERTAGS_EQUAL(*tag, *new_tag))
    {
        /*
         * Read the page the new tuple goes into, if different from old.
         */
        if (oldblk != newblk)
        {
            if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
            {
                POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);

                page = (Page) BufferGetPage(*buffer);
                PageInit(page, BufferGetPageSize(*buffer), 0);
                action = BLK_NEEDS_REDO;
            }
            else
                action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);
        }

        /* Deal with new tuple */
        if (action == BLK_NEEDS_REDO)
        {
            char       *recdata;
            char       *recdata_end;
            Size        datalen;
            Size        tuplen;

            recdata = XLogRecGetBlockData(record, 0, &datalen);
            recdata_end = recdata + datalen;

            page = BufferGetPage(*buffer);

            offnum = xlrec->new_offnum;

            if (PageGetMaxOffsetNumber(page) + 1 < offnum)
            {
                elog(LOG, "Original old page lsn is %lX", orig_old_lsn);

                elog(PANIC, "invalid max offset number: page_max_off=%ld, offnum=%d",
                     PageGetMaxOffsetNumber(page), offnum);
            }

            if (xlrec->flags & XLH_UPDATE_PREFIX_FROM_OLD)
            {
                /* pageTag->blockNum must equal oldblk, see function log_heap_update */
                Assert(newblk == oldblk);
                memcpy(&prefixlen, recdata, sizeof(uint16));
                recdata += sizeof(uint16);
            }

            if (xlrec->flags & XLH_UPDATE_SUFFIX_FROM_OLD)
            {
                /* pageTag->blockNum must equal oldblk, see function log_heap_update */
                Assert(newblk == oldblk);
                memcpy(&suffixlen, recdata, sizeof(uint16));
                recdata += sizeof(uint16);
            }

            memcpy((char *) &xlhdr, recdata, SizeOfHeapHeader);
            recdata += SizeOfHeapHeader;

            tuplen = recdata_end - recdata;
            Assert(tuplen <= MaxHeapTupleSize);

            htup = &tbuf.hdr;
            MemSet((char *) htup, 0, SizeofHeapTupleHeader);

            /*
             * Reconstruct the new tuple using the prefix and/or suffix from the
             * old tuple, and the data stored in the WAL record.
             */
            newp = (char *) htup + SizeofHeapTupleHeader;

            if (prefixlen > 0)
            {
                int         len;

                /* copy bitmap [+ padding] [+ oid] from WAL record */
                len = xlhdr.t_hoff - SizeofHeapTupleHeader;
                memcpy(newp, recdata, len);
                recdata += len;
                newp += len;

                if (!oldtup.t_data)
                {
                    elog(LOG, "Original old page lsn is %lX", orig_old_lsn);
                    elog(PANIC, "Old tuple data is null");
                }

                /* copy prefix from old tuple */
                memcpy(newp, (char *) oldtup.t_data + oldtup.t_data->t_hoff, prefixlen);
                newp += prefixlen;

                /* copy new tuple data from WAL record */
                len = tuplen - (xlhdr.t_hoff - SizeofHeapTupleHeader);
                memcpy(newp, recdata, len);
                recdata += len;
                newp += len;
            }
            else
            {
                /*
                 * copy bitmap [+ padding] [+ oid] + data from record, all in one
                 * go
                 */
                memcpy(newp, recdata, tuplen);
                recdata += tuplen;
                newp += tuplen;
            }

            if (recdata != recdata_end)
            {
                elog(LOG, "Original old page lsn is %lX", orig_old_lsn);
                elog(PANIC, "Failed to decode tuple");
            }

            /* copy suffix from old tuple */
            if (suffixlen > 0)
            {
                if (!oldtup.t_data)
                {
                    elog(LOG, "Original old page lsn is %lX", orig_old_lsn);
                    elog(PANIC, "Old tuple data is null");
                }

                memcpy(newp, (char *) oldtup.t_data + oldtup.t_len - suffixlen, suffixlen);
            }

            newlen = SizeofHeapTupleHeader + tuplen + prefixlen + suffixlen;
            htup->t_infomask2 = xlhdr.t_infomask2;
            htup->t_infomask = xlhdr.t_infomask;
            htup->t_hoff = xlhdr.t_hoff;

            HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
            HeapTupleHeaderSetCmin(htup, FirstCommandId);
            HeapTupleHeaderSetXmax(htup, xlrec->new_xmax);
            /* Make sure there is no forward chain link in t_ctid */
            htup->t_ctid = newtid;

            offnum = PageAddItem(page, (Item) htup, newlen, offnum, true, true);

            if (offnum == InvalidOffsetNumber)
            {
                elog(LOG, "Original old page lsn is %lX", orig_old_lsn);
                elog(PANIC, "failed to add tuple");
            }

            if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
                PageClearAllVisible(page);

            PageSetLSN(page, lsn);
        }
    }

    return action;
}

static XLogRedoAction
polar_heap_xlog_confirm(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    xl_heap_confirm *xlrec = (xl_heap_confirm *) XLogRecGetData(record);
    XLogRedoAction action = BLK_NOTFOUND;
    Page        page;
    OffsetNumber offnum;
    ItemId      lp = NULL;
    HeapTupleHeader htup;
    BufferTag tag0;

    POLAR_GET_LOG_TAG(record, tag0, 0);

    if (!BUFFERTAGS_EQUAL(*tag, tag0))
        return action;

    action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

    if (action == BLK_NEEDS_REDO)
    {
        page = BufferGetPage(*buffer);

        offnum = xlrec->offnum;

        if (PageGetMaxOffsetNumber(page) >= offnum)
            lp = PageGetItemId(page, offnum);

        if (PageGetMaxOffsetNumber(page) < offnum || !lp || !ItemIdIsNormal(lp))
        {
            elog(PANIC, "invalid lp: page_max_off=%ld, offnum=%d, lp=%d",
                 PageGetMaxOffsetNumber(page), offnum, (lp ? lp->lp_flags : 0));
        }

        htup = (HeapTupleHeader) PageGetItem(page, lp);

        /*
         * Confirm tuple as actually inserted
         */
        ItemPointerSet(&htup->t_ctid, BufferGetBlockNumber(*buffer), offnum);

        PageSetLSN(page, lsn);
    }

    return action;
}


static XLogRedoAction
polar_heap_xlog_lock(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    xl_heap_lock *xlrec = (xl_heap_lock *) XLogRecGetData(record);
    XLogRedoAction action = BLK_NOTFOUND;
    Page        page;
    OffsetNumber offnum;
    ItemId      lp = NULL;
    HeapTupleHeader htup;
    BufferTag   tag0, tag1;

    POLAR_GET_LOG_TAG(record, tag0, 0);
    INIT_BUFFERTAG(tag1, tag0.rnode, VISIBILITYMAP_FORKNUM, HEAPBLK_TO_MAPBLOCK(tag0.blockNum));

    if (BUFFERTAGS_EQUAL(*tag, tag1))
    {
        if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
        {
            action = polar_heap_clear_vm(record, &tag0.rnode, tag0.blockNum, buffer,
                                         VISIBILITYMAP_ALL_FROZEN);
        }
    }

    if (BUFFERTAGS_EQUAL(*tag, tag0))
    {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            page = (Page) BufferGetPage(*buffer);

            offnum = xlrec->offnum;

            if (PageGetMaxOffsetNumber(page) >= offnum)
                lp = PageGetItemId(page, offnum);

            if (PageGetMaxOffsetNumber(page) < offnum || !lp || !ItemIdIsNormal(lp))
            {
                elog(PANIC, "invalid lp: page_max_off=%ld, offnum=%d, lp=%d",
                     PageGetMaxOffsetNumber(page), offnum, (lp ? lp->lp_flags : 0));
            }

            htup = (HeapTupleHeader) PageGetItem(page, lp);

            htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
            htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
            fix_infomask_from_infobits(xlrec->infobits_set, &htup->t_infomask,
                                       &htup->t_infomask2);

            /*
             * Clear relevant update flags, but only if the modified infomask says
             * there's no update.
             */
            if (HEAP_XMAX_IS_LOCKED_ONLY(htup->t_infomask))
            {
                HeapTupleHeaderClearHotUpdated(htup);
                /* Make sure there is no forward chain link in t_ctid */
                ItemPointerSet(&htup->t_ctid,
                               BufferGetBlockNumber(*buffer),
                               offnum);
            }

            HeapTupleHeaderSetXmax(htup, xlrec->locking_xid);
            HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
            PageSetLSN(page, lsn);
        }
    }

    return action;
}

static XLogRedoAction
polar_heap_xlog_inplace(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    xl_heap_inplace *xlrec = (xl_heap_inplace *) XLogRecGetData(record);
    XLogRedoAction action = BLK_NOTFOUND;
    Page        page;
    OffsetNumber offnum;
    ItemId      lp = NULL;
    HeapTupleHeader htup;
    uint32      oldlen;
    Size        newlen;
    BufferTag   tag0;

    POLAR_GET_LOG_TAG(record, tag0, 0);

    if (!BUFFERTAGS_EQUAL(*tag, tag0))
        return action;

    action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

    if (action == BLK_NEEDS_REDO)
    {
        char       *newtup = XLogRecGetBlockData(record, 0, &newlen);

        page = BufferGetPage(*buffer);

        offnum = xlrec->offnum;

        if (PageGetMaxOffsetNumber(page) >= offnum)
            lp = PageGetItemId(page, offnum);

        if (PageGetMaxOffsetNumber(page) < offnum || !lp || !ItemIdIsNormal(lp))
        {
            elog(PANIC, "invalid lp: page_max_off=%ld, offnum=%d, lp=%d",
                 PageGetMaxOffsetNumber(page), offnum, (lp ? lp->lp_flags : 0));
        }

        htup = (HeapTupleHeader) PageGetItem(page, lp);

        oldlen = ItemIdGetLength(lp) - htup->t_hoff;

        if (oldlen != newlen)
        {
            elog(PANIC, "wrong tuple length: oldlen=%u, newlen=%ld", oldlen, newlen);
        }

        memcpy((char *) htup + htup->t_hoff, newtup, newlen);

        PageSetLSN(page, lsn);
    }

    return action;
}

/*
 * Handles HEAP2_CLEAN record type
 */
static XLogRedoAction
polar_heap_xlog_clean(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    xl_heap_clean *xlrec = (xl_heap_clean *) XLogRecGetData(record);
    XLogRedoAction action = BLK_NOTFOUND;
    BufferTag tag0;

    POLAR_GET_LOG_TAG(record, tag0, 0);

    if (!BUFFERTAGS_EQUAL(*tag, tag0))
        return action;

    /*
     * If we have a full-page image, restore it (using a cleanup lock) and
     * we're done.
     */
    action = XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, buffer);

    if (action == BLK_NEEDS_REDO)
    {
        Page        page = (Page) BufferGetPage(*buffer);
        OffsetNumber *end;
        OffsetNumber *redirected;
        OffsetNumber *nowdead;
        OffsetNumber *nowunused;
        int         nredirected;
        int         ndead;
        int         nunused;
        Size        datalen;

        redirected = (OffsetNumber *) XLogRecGetBlockData(record, 0, &datalen);

        nredirected = xlrec->nredirected;
        ndead = xlrec->ndead;
        end = (OffsetNumber *)((char *) redirected + datalen);
        nowdead = redirected + (nredirected * 2);
        nowunused = nowdead + ndead;
        nunused = (end - nowunused);
        Assert(nunused >= 0);

        /* Update all item pointers per the record, and repair fragmentation */
        heap_page_prune_execute(*buffer,
                                redirected, nredirected,
                                nowdead, ndead,
                                nowunused, nunused);

        /*
         * Note: we don't worry about updating the page's prunability hints.
         * At worst this will cause an extra prune cycle to occur soon.
         */

        PageSetLSN(page, lsn);
    }

    return action;
}

static XLogRedoAction
polar_heap_xlog_freeze_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    xl_heap_freeze_page *xlrec = (xl_heap_freeze_page *) XLogRecGetData(record);
    XLogRedoAction action = BLK_NOTFOUND;
    int         ntup;
    BufferTag tag0;

    POLAR_GET_LOG_TAG(record, tag0, 0);

    if (!BUFFERTAGS_EQUAL(*tag, tag0))
        return action;

    action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

    if (action == BLK_NEEDS_REDO)
    {
        Page        page = BufferGetPage(*buffer);
        xl_heap_freeze_tuple *tuples;

        tuples = (xl_heap_freeze_tuple *) XLogRecGetBlockData(record, 0, NULL);

        /* now execute freeze plan for each frozen tuple */
        for (ntup = 0; ntup < xlrec->ntuples; ntup++)
        {
            xl_heap_freeze_tuple *xlrec_tp;
            ItemId      lp;
            HeapTupleHeader tuple;

            xlrec_tp = &tuples[ntup];
            lp = PageGetItemId(page, xlrec_tp->offset); /* offsets are one-based */
            tuple = (HeapTupleHeader) PageGetItem(page, lp);

            heap_execute_freeze_tuple(tuple, xlrec_tp);
        }

        PageSetLSN(page, lsn);
    }

    return action;
}

/*
 * Replay XLOG_HEAP2_VISIBLE record.
 *
 * The critical integrity requirement here is that we must never end up with
 * a situation where the visibility map bit is set, and the page-level
 * PD_ALL_VISIBLE bit is clear.  If that were to occur, then a subsequent
 * page modification would fail to clear the visibility map bit.
 */
static XLogRedoAction
polar_heap_xlog_visible(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    XLogRecPtr  lsn = record->EndRecPtr;
    xl_heap_visible *xlrec = (xl_heap_visible *) XLogRecGetData(record);
    XLogRedoAction action = BLK_NOTFOUND;
    Page        page;
    BufferTag   tag0, tag1;

    POLAR_GET_LOG_TAG(record, tag0, 0);
    POLAR_GET_LOG_TAG(record, tag1, 1);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    if (BUFFERTAGS_EQUAL(*tag, tag1))
    {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif

        /*
         * Read the heap page, if it still exists. If the heap file has dropped or
         * truncated later in recovery, we don't need to update the page, but we'd
         * better still update the visibility map.
         */
        action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

        if (action == BLK_NEEDS_REDO)
        {
#ifdef ENABLE_DEBUG_INFO
            printf("%s %d\n", __func__ , __LINE__);
            fflush(stdout);
#endif

            /*
             * We don't bump the LSN of the heap page when setting the visibility
             * map bit (unless checksums or wal_hint_bits is enabled, in which
             * case we must), because that would generate an unworkable volume of
             * full-page writes.  This exposes us to torn page hazards, but since
             * we're not inspecting the existing page contents in any way, we
             * don't care.
             *
             * However, all operations that clear the visibility map bit *do* bump
             * the LSN, and those operations will only be replayed if the XLOG LSN
             * follows the page LSN.  Thus, if the page LSN has advanced past our
             * XLOG record's LSN, we mustn't mark the page all-visible, because
             * the subsequent update won't be replayed to clear the flag.
             */
            page = BufferGetPage(*buffer);

            PageSetAllVisible(page);

            //todo
            /* Update lsn otherwise we would not mark buffer dirty during online promote */
            PageSetLSN(page, lsn);
        }
        else if (action == BLK_RESTORED)
        {
#ifdef ENABLE_DEBUG_INFO
            printf("%s %d\n", __func__ , __LINE__);
            fflush(stdout);
#endif

            /*
             * If heap block was backed up, we already restored it and there's
             * nothing more to do. (This can only happen with checksums or
             * wal_log_hints enabled.)
             */
        }
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    if (BUFFERTAGS_EQUAL(*tag, tag0))
    {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif

        ReadBufferMode mode = BufferIsValid(*buffer) ? RBM_NORMAL : RBM_ZERO_ON_ERROR;
        /*
         * Even if we skipped the heap page update due to the LSN interlock, it's
         * still safe to update the visibility map.  Any WAL record that clears
         * the visibility map bit does so before checking the page LSN, so any
         * bits that need to be cleared will still be cleared.
         */
        action = XLogReadBufferForRedoExtended(record, 0, mode, false, buffer);

#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif

        if (action == BLK_NEEDS_REDO)
        {
#ifdef ENABLE_DEBUG_INFO
            printf("%s %d\n", __func__ , __LINE__);
            fflush(stdout);
#endif

            Page        vmpage = BufferGetPage(*buffer);

            /* initialize the page if it was read as zeros */
            if (PageIsNew(vmpage))
                PageInit(vmpage, BLCKSZ, 0);

            /*
             * Don't set the bit if replay has already passed this point.
             *
             * It might be safe to do this unconditionally; if replay has passed
             * this point, we'll replay at least as far this time as we did
             * before, and if this bit needs to be cleared, the record responsible
             * for doing so should be again replayed, and clear it.  For right
             * now, out of an abundance of conservatism, we use the same test here
             * we did for the heap page.  If this results in a dropped bit, no
             * real harm is done; and the next VACUUM will fix it.
             */
            if (lsn > PageGetLSN(vmpage))
            {
                polar_visibilitymap_set(tag1.blockNum, *buffer, xlrec->flags);
                PageSetLSN(vmpage, lsn);
            }
        }
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    return action;
}

static XLogRedoAction
polar_heap_xlog_lock_updated(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    XLogRedoAction action = BLK_NOTFOUND;
    xl_heap_lock_updated *xlrec;
    Page        page;
    OffsetNumber offnum;
    ItemId      lp = NULL;
    HeapTupleHeader htup;
    BufferTag   tag0, tag1;

    POLAR_GET_LOG_TAG(record, tag0, 0);
    INIT_BUFFERTAG(tag1, tag0.rnode, VISIBILITYMAP_FORKNUM, HEAPBLK_TO_MAPBLOCK(tag0.blockNum));

    xlrec = (xl_heap_lock_updated *) XLogRecGetData(record);

    if (BUFFERTAGS_EQUAL(*tag, tag1))
    {
        /*
         * The visibility map may need to be fixed even if the heap page is
         * already up-to-date.
         */
        if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
        {
            action = polar_heap_clear_vm(record, &tag0.rnode, tag0.blockNum, buffer,
                                         VISIBILITYMAP_ALL_FROZEN);
        }
    }

    if (BUFFERTAGS_EQUAL(*tag, tag0))
    {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            page = BufferGetPage(*buffer);

            offnum = xlrec->offnum;

            if (PageGetMaxOffsetNumber(page) >= offnum)
                lp = PageGetItemId(page, offnum);

            if (PageGetMaxOffsetNumber(page) < offnum || !lp || !ItemIdIsNormal(lp))
            {
                elog(PANIC, "invalid lp: page_max_off=%ld, offnum=%d, lp=%d",
                     PageGetMaxOffsetNumber(page), offnum, (lp ? lp->lp_flags : 0));
            }

            htup = (HeapTupleHeader) PageGetItem(page, lp);

            htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
            htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
            fix_infomask_from_infobits(xlrec->infobits_set, &htup->t_infomask,
                                       &htup->t_infomask2);
            HeapTupleHeaderSetXmax(htup, xlrec->xmax);

            PageSetLSN(page, lsn);
        }
    }

    return action;
}

static void
polar_heap_insert_save(XLogReaderState *record)
{
    xl_heap_insert *xlrec = (xl_heap_insert *)record->main_data;

    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
        ParseXLogBlocksLsn(record, 1);

    ParseXLogBlocksLsn(record, 0);
}

static void
polar_heap_insert_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    xl_heap_insert *xlrec = (xl_heap_insert *)record->main_data;

    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 2);

    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) {
        XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
    }

    XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;

    *tagNum = tagCount;
}

static void
polar_heap_delete_save(XLogReaderState *record)
{
    xl_heap_delete *xlrec = (xl_heap_delete *)record->main_data;

    if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
        ParseXLogBlocksLsn(record, 1);

    ParseXLogBlocksLsn(record, 0);
}

static void
polar_heap_delete_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum) {
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    xl_heap_delete *xlrec = (xl_heap_delete *) record->main_data;

    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 2);

    if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED) {
        XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
    }

    XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;

    *tagNum = tagCount;
}

static void
polar_heap_xlog_update_save(XLogReaderState *record, bool hotupdate)
{
    BlockNumber oldblk, newblk;
    BufferTag old_cleared_vm, new_cleared_vm;
    xl_heap_update *xlrec = (xl_heap_update *)(record->main_data);

    CLEAR_BUFFERTAG(old_cleared_vm);
    CLEAR_BUFFERTAG(new_cleared_vm);

    XLogRecGetBlockTag(record, 0, NULL, NULL, &newblk);

    if (XLogRecGetBlockTag(record, 1, NULL, NULL, &oldblk))
    {
        /* HOT updates are never done across pages */
        Assert(!hotupdate);
    }
    else
        oldblk = newblk;

    if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
    {
        uint8 vm_block = (oldblk == newblk) ? 2 : 3;
        ParseXLogBlocksLsn(record, vm_block);
        POLAR_GET_LOG_TAG(record, old_cleared_vm, vm_block);
    }

    ParseXLogBlocksLsn(record, (oldblk == newblk) ? 0 : 1);

    if (oldblk != newblk)
    {
        ParseXLogBlocksLsn(record, 0);

        if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
        {
            /* Avoid add the same vm page to logindex twice with the same lsn value */
            POLAR_GET_LOG_TAG(record, new_cleared_vm, 2);

            if (!BUFFERTAGS_EQUAL(old_cleared_vm, new_cleared_vm))
                ParseXLogBlocksLsn(record, 2);
        }
    }
}

static void
polar_heap_xlog_update_get_bufftag_list(XLogReaderState *record, bool hotupdate, BufferTag** buffertagList, int* tagNum)
{
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;
    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 4);

    BlockNumber oldblk, newblk;
    BufferTag old_cleared_vm, new_cleared_vm;
    xl_heap_update *xlrec = (xl_heap_update *)(record->main_data);

    CLEAR_BUFFERTAG(old_cleared_vm);
    CLEAR_BUFFERTAG(new_cleared_vm);

    XLogRecGetBlockTag(record, 0, NULL, NULL, &newblk);

    if (XLogRecGetBlockTag(record, 1, NULL, NULL, &oldblk))
    {
        /* HOT updates are never done across pages */
        Assert(!hotupdate);
    }
    else
        oldblk = newblk;

    if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
    {
        uint8 vm_block = (oldblk == newblk) ? 2 : 3;

        XLogRecGetBlockTag(record, vm_block, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;

        POLAR_GET_LOG_TAG(record, old_cleared_vm, vm_block);
    }

//    ParseXLogBlocksLsn(record, (oldblk == newblk) ? 0 : 1);
    XLogRecGetBlockTag(record, (oldblk == newblk) ? 0 : 1, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;

    if (oldblk != newblk)
    {
        XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
//        ParseXLogBlocksLsn(record, 0);

        if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
        {
            /* Avoid add the same vm page to logindex twice with the same lsn value */
            POLAR_GET_LOG_TAG(record, new_cleared_vm, 2);

            if (!BUFFERTAGS_EQUAL(old_cleared_vm, new_cleared_vm)) {
                XLogRecGetBlockTag(record, 2, &rnode, &forkNumber, &blockNumber);
                INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
                tagCount++;
//                ParseXLogBlocksLsn(record, 2);
            }
        }
    }

    *tagNum = tagCount;
}


static void
polar_heap_lock_save(XLogReaderState *record)
{
    xl_heap_lock *xlrec = (xl_heap_lock *)record->main_data;

    if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
        ParseXLogBlocksLsn(record, 1);

    ParseXLogBlocksLsn(record, 0);
}

static void
polar_heap_lock_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;
    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 2);

    xl_heap_lock *xlrec = (xl_heap_lock *)record->main_data;

    if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED) {

//        ParseXLogBlocksLsn(record, 1);
        XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
    }

    XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;
//    ParseXLogBlocksLsn(record, 0);

    *tagNum = tagCount;
}

bool
polar_heap_idx_save(XLogReaderState *record)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK)
    {
        case XLOG_HEAP_INSERT:
            polar_heap_insert_save(record);
            break;

        case XLOG_HEAP_DELETE:
            polar_heap_delete_save(record);
            break;

        case XLOG_HEAP_UPDATE:
            polar_heap_xlog_update_save(record, false);
            break;

        case XLOG_HEAP_TRUNCATE:
            break;

        case XLOG_HEAP_HOT_UPDATE:
            polar_heap_xlog_update_save(record, true);
            break;

        case XLOG_HEAP_CONFIRM:
            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_HEAP_LOCK:
            polar_heap_lock_save(record);
            break;

        case XLOG_HEAP_INPLACE:
            ParseXLogBlocksLsn(record, 0);
            break;

        default:
            elog(PANIC, "polar_heap_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}

bool
polar_heap_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    switch (info & XLOG_HEAP_OPMASK)
    {
        case XLOG_HEAP_INSERT:
            polar_heap_insert_get_bufftag_list(record, buffertagList, tagNum);
            break;

        case XLOG_HEAP_DELETE:
            polar_heap_delete_get_bufftag_list(record, buffertagList, tagNum);
            break;

        case XLOG_HEAP_UPDATE:
            polar_heap_xlog_update_get_bufftag_list(record, false, buffertagList, tagNum);
            break;

        case XLOG_HEAP_TRUNCATE:
            break;

        case XLOG_HEAP_HOT_UPDATE:
            polar_heap_xlog_update_get_bufftag_list(record, true, buffertagList, tagNum);
            break;

        case XLOG_HEAP_CONFIRM:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 1);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);
            *tagNum = 1;
            break;

        case XLOG_HEAP_LOCK:
            polar_heap_lock_get_bufftag_list(record, buffertagList, tagNum);
            break;

        case XLOG_HEAP_INPLACE:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 1);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);
            *tagNum = 1;
            break;

        default:
            elog(PANIC, "polar_heap_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}

XLogRedoAction
polar_heap_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK)
    {
        case XLOG_HEAP_INSERT:
            return polar_heap_xlog_insert(record, tag, buffer);

        case XLOG_HEAP_DELETE:
            return polar_heap_xlog_delete(record, tag, buffer);

        case XLOG_HEAP_UPDATE:
            return polar_heap_xlog_update(record, tag, buffer, false);

        case XLOG_HEAP_TRUNCATE:

            /*
             * TRUNCATE is a no-op because the actions are already logged as
             * SMGR WAL records.  TRUNCATE WAL record only exists for logical
             * decoding.
             */
            break;

        case XLOG_HEAP_HOT_UPDATE:
            return polar_heap_xlog_update(record, tag, buffer, true);

        case XLOG_HEAP_CONFIRM:
            return polar_heap_xlog_confirm(record, tag, buffer);

        case XLOG_HEAP_LOCK:
            return polar_heap_xlog_lock(record, tag, buffer);

        case XLOG_HEAP_INPLACE:
            return polar_heap_xlog_inplace(record, tag, buffer);

        default:
            elog(PANIC, "polar_heap_idx_redo: unknown op code %u", info);
            break;
    }

    return BLK_NOTFOUND;
}

static void
polar_heap_multi_insert_save(XLogReaderState *record)
{
    xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *)record->main_data;

    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
        ParseXLogBlocksLsn(record, 1);

    ParseXLogBlocksLsn(record, 0);
}

static void
polar_heap_multi_insert_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 2);

    xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *)record->main_data;

    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) {
        XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;

//        ParseXLogBlocksLsn(record, 1);
    }

    XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;
//    ParseXLogBlocksLsn(record, 0);

    *tagNum = tagCount;
}

static void
polar_heap_lock_update_save(XLogReaderState *record)
{
    xl_heap_lock_updated *xlrec = (xl_heap_lock_updated *)record->main_data;

    if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
        ParseXLogBlocksLsn(record, 1);

    ParseXLogBlocksLsn(record, 0);
}

static void
polar_heap_lock_update_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 2);

    xl_heap_lock_updated *xlrec = (xl_heap_lock_updated *)record->main_data;

    if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED) {
        XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
//        ParseXLogBlocksLsn(record, 1);
    }

    XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;
//    ParseXLogBlocksLsn(record, 0);

    *tagNum = tagCount;
}

bool
polar_heap2_idx_save(XLogReaderState *record)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK)
    {
        case XLOG_HEAP2_CLEAN:
        case XLOG_HEAP2_FREEZE_PAGE:
            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_HEAP2_CLEANUP_INFO:
            /* don't modify buffer, nothing to do for parse, just do it */
            break;

        case XLOG_HEAP2_VISIBLE:
            ParseXLogBlocksLsn(record, 1);
            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_HEAP2_MULTI_INSERT:
            polar_heap_multi_insert_save(record);
            break;

        case XLOG_HEAP2_LOCK_UPDATED:
            polar_heap_lock_update_save(record);
            break;

        case XLOG_HEAP2_NEW_CID:
            break;

        case XLOG_HEAP2_REWRITE:
            heap_xlog_logical_rewrite(record);
            break;

        default:
            elog(PANIC, "polar_heap2_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}

bool
polar_heap2_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    switch (info & XLOG_HEAP_OPMASK)
    {
        case XLOG_HEAP2_CLEAN:
        case XLOG_HEAP2_FREEZE_PAGE:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 1);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);
            *tagNum = 1;
            break;

        case XLOG_HEAP2_CLEANUP_INFO:
            /* don't modify buffer, nothing to do for parse, just do it */
            break;

        case XLOG_HEAP2_VISIBLE:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 2);

            XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[1], rnode, forkNumber, blockNumber);
            *tagNum = 2;
            break;

        case XLOG_HEAP2_MULTI_INSERT:
            polar_heap_multi_insert_get_bufftag_list(record, buffertagList, tagNum);
            break;

        case XLOG_HEAP2_LOCK_UPDATED:
            polar_heap_lock_update_get_bufftag_list(record, buffertagList, tagNum);
            break;

        case XLOG_HEAP2_NEW_CID:
            break;

        case XLOG_HEAP2_REWRITE:
            heap_xlog_logical_rewrite(record);
            // heap_.._rewrite() will do redo function, no need for more redo
            *tagNum = 0;
            break;

        default:
            elog(PANIC, "polar_heap2_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}

XLogRedoAction
polar_heap2_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK)
    {
        case XLOG_HEAP2_CLEAN:
            return polar_heap_xlog_clean(record, tag, buffer);

        case XLOG_HEAP2_FREEZE_PAGE:
            return polar_heap_xlog_freeze_page(record, tag, buffer);

        case XLOG_HEAP2_CLEANUP_INFO:
            /* nothing to do, don't modify buffer, never here*/
            Assert(0);
            break;

        case XLOG_HEAP2_VISIBLE:
            return polar_heap_xlog_visible(record, tag, buffer);

        case XLOG_HEAP2_MULTI_INSERT:
            return polar_heap_xlog_multi_insert(record, tag, buffer);

        case XLOG_HEAP2_LOCK_UPDATED:
            return polar_heap_xlog_lock_updated(record, tag, buffer);

        case XLOG_HEAP2_NEW_CID:
            break;
        case XLOG_HEAP2_REWRITE:
            break;
        default:
            elog(PANIC, "polar_heap2_idx_redo: unknown op code %u", info);
            break;
    }

    return BLK_NOTFOUND;
}