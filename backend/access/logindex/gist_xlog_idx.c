#include "postgres.h"

#include "access/bufmask.h"
#include "access/gist_private.h"
#include "access/gistxlog.h"
#include "access/polar_logindex.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "storage/buf_internals.h"
#include "storage/standby.h"
#include "utils/memutils.h"

#include "storage/kv_interface.h"
#include "access/xlog.h"


/*
 * Replay the clearing of F_FOLLOW_RIGHT flag on a child page.
 *
 * Even if the WAL record includes a full-page image, we have to update the
 * follow-right flag, because that change is not included in the full-page
 * image.  To be sure that the intermediate state with the wrong flag value is
 * not visible to concurrent Hot Standby queries, this function handles
 * restoring the full-page image as well as updating the flag.  (Note that
 * we never need to do anything else to the child page in the current WAL
 * action.)
 */
static XLogRedoAction
polar_gist_redo_clear_follow_right(XLogReaderState *record, uint8 block_id, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    XLogRedoAction action = BLK_NOTFOUND;
    Page        page;

    /*
     * Note that we still update the page even if it was restored from a full
     * page image, because the updated NSN is not included in the image.
     */
    action = POLAR_READ_BUFFER_FOR_REDO(record, block_id, buffer);

    if (action == BLK_NEEDS_REDO || action == BLK_RESTORED)
    {
        page = BufferGetPage(*buffer);

        GistPageSetNSN(page, lsn);
        GistClearFollowRight(page);

        PageSetLSN(page, lsn);
    }

    return action;
}

/*
 * redo any page update (except page split)
 */
static XLogRedoAction
polar_gist_redo_page_update_record(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    gistxlogPageUpdate *xldata = (gistxlogPageUpdate *) XLogRecGetData(record);
    XLogRedoAction action = BLK_NOTFOUND;
    Page        page;
    BufferTag   update_tag;

    POLAR_GET_LOG_TAG(record, update_tag, 0);

    if (BUFFERTAGS_EQUAL(*tag, update_tag))
    {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            char       *begin;
            char       *data;
            Size        datalen;
            int         ninserted = 0;

            data = begin = XLogRecGetBlockData(record, 0, &datalen);

            page = (Page) BufferGetPage(*buffer);

            if (xldata->ntodelete == 1 && xldata->ntoinsert == 1)
            {
                /*
                 * When replacing one tuple with one other tuple, we must use
                 * PageIndexTupleOverwrite for consistency with gistplacetopage.
                 */
                OffsetNumber offnum = *((OffsetNumber *) data);
                IndexTuple  itup;
                Size        itupsize;

                data += sizeof(OffsetNumber);
                itup = (IndexTuple) data;
                itupsize = IndexTupleSize(itup);

                if (!PageIndexTupleOverwrite(page, offnum, (Item) itup, itupsize))
                {
                    elog(ERROR, "failed to add item to GiST index page, size %d bytes",
                         (int) itupsize);
                }

                data += itupsize;
                /* should be nothing left after consuming 1 tuple */
                Assert(data - begin == datalen);
                /* update insertion count for assert check below */
                ninserted++;
            }
            else if (xldata->ntodelete > 0)
            {
                /* Otherwise, delete old tuples if any */
                OffsetNumber *todelete = (OffsetNumber *) data;

                data += sizeof(OffsetNumber) * xldata->ntodelete;

                PageIndexMultiDelete(page, todelete, xldata->ntodelete);

                if (GistPageIsLeaf(page))
                    GistMarkTuplesDeleted(page);
            }

            /* Add new tuples if any */
            if (data - begin < datalen)
            {
                OffsetNumber off = (PageIsEmpty(page)) ? FirstOffsetNumber :
                                   OffsetNumberNext(PageGetMaxOffsetNumber(page));

                while (data - begin < datalen)
                {
                    IndexTuple  itup = (IndexTuple) data;
                    Size        sz = IndexTupleSize(itup);
                    OffsetNumber l;

                    data += sz;

                    l = PageAddItem(page, (Item) itup, sz, off, false, false);

                    if (l == InvalidOffsetNumber)
                    {
                        elog(ERROR, "failed to add item to GiST index page, size %d bytes",
                             (int) sz);
                    }

                    off++;
                    ninserted++;
                }
            }

            /* Check that XLOG record contained expected number of tuples */
            Assert(ninserted == xldata->ntoinsert);

            PageSetLSN(page, lsn);
        }

        return action;
    }

    /*
     * Fix follow-right data on left child page
     *
     * This must be done while still holding the lock on the target page. Note
     * that even if the target page no longer exists, we still attempt to
     * replay the change on the child page.
     */
    if (XLogRecHasBlockRef(record, 1))
    {
        BufferTag right_tag;
        POLAR_GET_LOG_TAG(record, right_tag, 1);

        if (BUFFERTAGS_EQUAL(*tag, right_tag))
            action = polar_gist_redo_clear_follow_right(record, 1, buffer);
    }

    return action;
}


static XLogRedoAction
polar_gist_redo_page_split_record(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    gistxlogPageSplit *xldata = (gistxlogPageSplit *) XLogRecGetData(record);
    XLogRedoAction action = BLK_NOTFOUND;
    Page        page;
    int         i;
    bool        isrootsplit = false;

    /*
     * We must hold lock on the first-listed page throughout the action,
     * including while updating the left child page (if any).  We can unlock
     * remaining pages in the list as soon as they've been written, because
     * there is no path for concurrent queries to reach those pages without
     * first visiting the first-listed page.
     */

    /* loop around all pages */
    for (i = 0; i < xldata->npage; i++)
    {
        int         flags;
        char       *data;
        Size        datalen;
        int         num;
        IndexTuple *tuples;
        BufferTag   page_tag;
        int         block_id = i + 1;

        POLAR_GET_LOG_TAG(record, page_tag, block_id);

        if (page_tag.blockNum == GIST_ROOT_BLKNO)
        {
            Assert(i == 0);
            isrootsplit = true;
        }

        if (!BUFFERTAGS_EQUAL(*tag, page_tag))
            continue;

        POLAR_INIT_BUFFER_FOR_REDO(record, block_id, buffer);
        page = (Page) BufferGetPage(*buffer);
        data = XLogRecGetBlockData(record, i + 1, &datalen);

        tuples = decodePageSplitRecord(data, datalen, &num);

        /* ok, clear buffer */
        if (xldata->origleaf && page_tag.blockNum != GIST_ROOT_BLKNO)
            flags = F_LEAF;
        else
            flags = 0;

        GISTInitBuffer(*buffer, flags);

        /* and fill it */
        gistfillbuffer(page, tuples, num, FirstOffsetNumber);

        if (page_tag.blockNum == GIST_ROOT_BLKNO)
        {
            GistPageGetOpaque(page)->rightlink = InvalidBlockNumber;
            GistPageSetNSN(page, xldata->orignsn);
            GistClearFollowRight(page);
        }
        else
        {
            if (i < xldata->npage - 1)
            {
                BlockNumber nextblkno;

                XLogRecGetBlockTag(record, i + 2, NULL, NULL, &nextblkno);
                GistPageGetOpaque(page)->rightlink = nextblkno;
            }
            else
                GistPageGetOpaque(page)->rightlink = xldata->origrlink;

            GistPageSetNSN(page, xldata->orignsn);

            if (i < xldata->npage - 1 && !isrootsplit &&
                xldata->markfollowright)
                GistMarkFollowRight(page);
            else
                GistClearFollowRight(page);
        }

        PageSetLSN(page, lsn);

        return BLK_NEEDS_REDO;
    }


    /* Fix follow-right data on left child page, if any */
    if (XLogRecHasBlockRef(record, 0))
    {
        BufferTag right_tag;

        POLAR_GET_LOG_TAG(record, right_tag, 0);

        if (BUFFERTAGS_EQUAL(*tag, right_tag))
            action = polar_gist_redo_clear_follow_right(record, 0, buffer);
    }

    return action;
}

/*
 * redo delete on gist index page to remove tuples marked as DEAD during index
 * tuple insertion
 */
static XLogRedoAction
polar_gistRedoDeleteRecord(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr	lsn = record->EndRecPtr;
    gistxlogDelete *xldata = (gistxlogDelete *) XLogRecGetData(record);
    Page		page;
    BufferTag   delete_tag;
    XLogRedoAction action = BLK_NOTFOUND;

    POLAR_GET_LOG_TAG(record, delete_tag, 0);

    if (BUFFERTAGS_EQUAL(*tag, delete_tag))
    {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO){
            page = (Page) BufferGetPage(*buffer);

            if (XLogRecGetDataLen(record) > SizeOfGistxlogDelete)
            {
                OffsetNumber *todelete;

                todelete = (OffsetNumber *) ((char *) xldata + SizeOfGistxlogDelete);

                PageIndexMultiDelete(page, todelete, xldata->ntodelete);
            }

            GistClearPageHasGarbage(page);
            GistMarkTuplesDeleted(page);

            PageSetLSN(page, lsn);
        }
    }


}


/* redo page deletion */
static XLogRedoAction
polar_gistRedoPageDelete(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr	lsn = record->EndRecPtr;
    gistxlogPageDelete *xldata = (gistxlogPageDelete *) XLogRecGetData(record);
    Buffer		parentBuffer;
    Buffer		leafBuffer;
    XLogRedoAction action = BLK_NOTFOUND;
    BufferTag   tag0;
    BufferTag   tag1;

    POLAR_GET_LOG_TAG(record, tag0, 0);

    if (BUFFERTAGS_EQUAL(*tag, tag0)) {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO) {
            Page		page = (Page) BufferGetPage(*buffer);

            GistPageSetDeleted(page, xldata->deleteXid);

            PageSetLSN(page, lsn);
        }
    }

    POLAR_GET_LOG_TAG(record, tag1, 1);

    if (BUFFERTAGS_EQUAL(*tag, tag1)) {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

        if (action == BLK_NEEDS_REDO) {
            Page		page = (Page) BufferGetPage(*buffer);

            PageIndexTupleDelete(page, xldata->downlinkOffset);

            PageSetLSN(page, lsn);
        }
    }

}


XLogRedoAction
polar_gist_idx_redo(XLogReaderState *record,  BufferTag *tag, Buffer *buffer)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRedoAction action = BLK_NOTFOUND;
    MemoryContext old_ctx, redo_ctx;

    redo_ctx = polar_get_redo_context();
    old_ctx = MemoryContextSwitchTo(redo_ctx);

    /*
     * GiST indexes do not require any conflict processing. NB: If we ever
     * implement a similar optimization we have in b-tree, and remove killed
     * tuples outside VACUUM, we'll need to handle that here.
     */
    switch (info)
    {
        case XLOG_GIST_PAGE_UPDATE:
            action = polar_gist_redo_page_update_record(record, tag, buffer);
            break;
        case XLOG_GIST_DELETE:
            action = polar_gistRedoDeleteRecord(record, tag, buffer);
            break;
        case XLOG_GIST_PAGE_REUSE:
            //todo
//                gistRedoPageReuse(record);
            break;
        case XLOG_GIST_PAGE_SPLIT:
            action = polar_gist_redo_page_split_record(record, tag, buffer);
            break;
        case XLOG_GIST_PAGE_DELETE:
            action = polar_gistRedoPageDelete(record, tag, buffer);
            break;
        case XLOG_GIST_ASSIGN_LSN:
            /* nop. See gistGetFakeLSN(). */
            break;
        default:
            elog(PANIC, "gist_redo: unknown op code %u", info);
    }

//    switch (info)
//    {
//
//        default:
//            elog(PANIC, "polar_gist_idx_redo: unknown op code %u", info);
//    }

    MemoryContextSwitchTo(old_ctx);
    MemoryContextReset(redo_ctx);
    return action;
}

static void
polar_gist_redo_page_update_record_save(XLogReaderState *record)
{
    ParseXLogBlocksLsn(record, 0);

    if (XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn(record, 1);
}

static void
polar_gist_redo_page_update_record_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 2);

    XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;

    if (XLogRecHasBlockRef(record, 1)) {
        XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
    }
    *tagNum = tagCount;
}

static void
polar_gist_redo_page_split_record_save(XLogReaderState *record)
{
    int block_id;

    ParseXLogBlocksLsn(record, 1);

    for (block_id = 2; block_id <= XLR_MAX_BLOCK_ID; block_id++)
    {
        if (XLogRecHasBlockRef(record, block_id))
            ParseXLogBlocksLsn(record, block_id);
        else
            break;
    }

    if (XLogRecHasBlockRef(record, 0))
        ParseXLogBlocksLsn(record, 0);
}

static void
polar_gist_redo_page_split_record_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    int block_id;
    int tagCount = 1; // already have block 1

    for (block_id = 2; block_id <= XLR_MAX_BLOCK_ID; block_id++)
    {
        if (XLogRecHasBlockRef(record, block_id))
            tagCount++;
        else
            break;
    }

    if (XLogRecHasBlockRef(record, 0))
        tagCount++;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * tagCount);
    *tagNum = tagCount;

    tagCount = 0;
    XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;

    for (block_id = 2; block_id <= XLR_MAX_BLOCK_ID; block_id++)
    {
        if (XLogRecHasBlockRef(record, block_id)) {
            XLogRecGetBlockTag(record, block_id, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
            tagCount++;
        }
        else
            break;
    }

    if (XLogRecHasBlockRef(record, 0)) {
        XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG(*buffertagList[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
    }
    *tagNum = tagCount;
}

bool
polar_gist_idx_save(XLogReaderState *record) {
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    switch (info)
    {
        case XLOG_GIST_PAGE_UPDATE:
            polar_gist_redo_page_update_record_save(record);
            break;
        case XLOG_GIST_DELETE:
            ParseXLogBlocksLsn(record, 0);
            break;
        case XLOG_GIST_PAGE_REUSE:
            //todo
//                gistRedoPageReuse(record);
            break;
        case XLOG_GIST_PAGE_SPLIT:
            polar_gist_redo_page_split_record_save(record);
            break;
        case XLOG_GIST_PAGE_DELETE:
            ParseXLogBlocksLsn(record, 0);
            ParseXLogBlocksLsn(record, 1);
            break;
        case XLOG_GIST_ASSIGN_LSN:
            /* nop. See gistGetFakeLSN(). */
            break;
        default:
            elog(PANIC, "gist_redo: unknown op code %u", info);
    }

    return true;
}

bool
polar_gist_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum) {
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    switch (info)
    {
        case XLOG_GIST_PAGE_UPDATE:
            polar_gist_redo_page_update_record_get_bufftag_list(record, buffertagList, tagNum);
            break;
        case XLOG_GIST_DELETE:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 1);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG(*buffertagList[0], rnode, forkNumber, blockNumber);
            *tagNum = 1;
            break;
        case XLOG_GIST_PAGE_REUSE:
            //todo
//                gistRedoPageReuse(record);
            break;
        case XLOG_GIST_PAGE_SPLIT:
            polar_gist_redo_page_split_record_get_bufftag_list(record, buffertagList, tagNum);
            break;
        case XLOG_GIST_PAGE_DELETE:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 2);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG(*buffertagList[0], rnode, forkNumber, blockNumber);

            XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG(*buffertagList[1], rnode, forkNumber, blockNumber);
            *tagNum = 2;
            break;
        case XLOG_GIST_ASSIGN_LSN:
            /* nop. See gistGetFakeLSN(). */
            break;
        default:
            elog(PANIC, "gist_redo: unknown op code %u", info);
    }

    return true;
}
