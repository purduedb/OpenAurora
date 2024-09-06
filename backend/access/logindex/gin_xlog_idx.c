

#include "postgres.h"

#include "access/bufmask.h"
#include "access/gin_private.h"
#include "access/ginxlog.h"
#include "access/polar_logindex.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "storage/buf_internals.h"
#include "storage/kv_interface.h"
#include "utils/memutils.h"
#include "access/xlog.h"



static XLogRedoAction
polar_gin_redo_create_index(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    Page        page;
    BufferTag   meta_tag, root_tag;

    POLAR_GET_LOG_TAG(record, meta_tag, 0);
    POLAR_GET_LOG_TAG(record, root_tag, 1);

    if (BUFFERTAGS_EQUAL(*tag, meta_tag))
    {
        POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);

        Assert(BufferGetBlockNumber(*buffer) == GIN_METAPAGE_BLKNO);
        page = (Page) BufferGetPage(*buffer);

        GinInitMetabuffer(*buffer);

        PageSetLSN(page, lsn);
        return BLK_NEEDS_REDO;
    }

    if (BUFFERTAGS_EQUAL(*tag, root_tag))
    {
        POLAR_INIT_BUFFER_FOR_REDO(record, 1, buffer);
        Assert(BufferGetBlockNumber(*buffer) == GIN_ROOT_BLKNO);
        page = (Page) BufferGetPage(*buffer);

        GinInitBuffer(*buffer, GIN_LEAF);

        PageSetLSN(page, lsn);

        return BLK_NEEDS_REDO;
    }

    return BLK_NOTFOUND;
}

static XLogRedoAction
polar_gin_redo_create_ptree(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    ginxlogCreatePostingTree *data = (ginxlogCreatePostingTree *) XLogRecGetData(record);
    char       *ptr;
    Page        page;
    BufferTag   tag0;

    POLAR_GET_LOG_TAG(record, tag0, 0);

    if (BUFFERTAGS_EQUAL(*tag, tag0))
    {
        POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
        page = (Page) BufferGetPage(*buffer);

        GinInitBuffer(*buffer, GIN_DATA | GIN_LEAF | GIN_COMPRESSED);

        ptr = XLogRecGetData(record) + sizeof(ginxlogCreatePostingTree);

        /* Place page data */
        memcpy(GinDataLeafPageGetPostingList(page), ptr, data->size);

        GinDataPageSetDataSize(page, data->size);

        PageSetLSN(page, lsn);
        return BLK_NEEDS_REDO;
    }

    return BLK_NOTFOUND;
}

static XLogRedoAction
polar_gin_redo_clear_incomplete_split(XLogReaderState *record, uint8 block_id, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    Page        page;
    XLogRedoAction action;

    action = POLAR_READ_BUFFER_FOR_REDO(record, block_id, buffer);

    if (action == BLK_NEEDS_REDO)
    {
        page = (Page) BufferGetPage(*buffer);
        GinPageGetOpaque(page)->flags &= ~GIN_INCOMPLETE_SPLIT;

        PageSetLSN(page, lsn);
    }

    return action;
}

static XLogRedoAction
polar_gin_redo_insert(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    ginxlogInsert *data = (ginxlogInsert *) XLogRecGetData(record);
#ifdef NOT_USED
    BlockNumber leftChildBlkno = InvalidBlockNumber;
#endif
    BlockNumber rightChildBlkno = InvalidBlockNumber;
    bool        isLeaf = (data->flags & GIN_INSERT_ISLEAF) != 0;
    XLogRedoAction action = BLK_NOTFOUND;
    BufferTag   incomp_tag, payload_tag;

    /*
     * First clear incomplete-split flag on child page if this finishes a
     * split.
     */
    if (!isLeaf)
    {
        char       *payload = XLogRecGetData(record) + sizeof(ginxlogInsert);
#ifdef NOT_USED
        leftChildBlkno = BlockIdGetBlockNumber((BlockId) payload);
#endif
        payload += sizeof(BlockIdData);
        rightChildBlkno = BlockIdGetBlockNumber((BlockId) payload);

        POLAR_GET_LOG_TAG(record, incomp_tag, 1);

        if (BUFFERTAGS_EQUAL(*tag, incomp_tag))
            return polar_gin_redo_clear_incomplete_split(record, 1, buffer);
    }

    POLAR_GET_LOG_TAG(record, payload_tag, 0);

    if (BUFFERTAGS_EQUAL(*tag, payload_tag))
    {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            Page        page = BufferGetPage(*buffer);
            Size        len;
            char       *payload = XLogRecGetBlockData(record, 0, &len);

            /* How to insert the payload is tree-type specific */
            if (data->flags & GIN_INSERT_ISDATA)
            {
                Assert(GinPageIsData(page));
                ginRedoInsertData(*buffer, isLeaf, rightChildBlkno, payload);
            }
            else
            {
                Assert(!GinPageIsData(page));
                ginRedoInsertEntry(*buffer, isLeaf, rightChildBlkno, payload);
            }

            PageSetLSN(page, lsn);
        }
    }

    return action;
}

static XLogRedoAction
polar_gin_redo_split(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    ginxlogSplit *data = (ginxlogSplit *) XLogRecGetData(record);
    bool        isLeaf = (data->flags & GIN_INSERT_ISLEAF) != 0;
    bool        isRoot = (data->flags & GIN_SPLIT_ROOT) != 0;
    XLogRedoAction action = BLK_NOTFOUND;
    BufferTag   incom_tag, ltag, rtag, root_tag;

    /*
     * First clear incomplete-split flag on child page if this finishes a
     * split
     */
    if (!isLeaf)
    {
        POLAR_GET_LOG_TAG(record, incom_tag, 3);

        if (BUFFERTAGS_EQUAL(*tag, incom_tag))
            return  polar_gin_redo_clear_incomplete_split(record, 3, buffer);
    }

    POLAR_GET_LOG_TAG(record, ltag, 0);

    if (BUFFERTAGS_EQUAL(*tag, ltag))
    {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        return action;
    }

    POLAR_GET_LOG_TAG(record, rtag, 1);

    if (BUFFERTAGS_EQUAL(*tag, rtag))
    {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

        return action;
    }

    if (isRoot)
    {
        POLAR_GET_LOG_TAG(record, root_tag, 2);

        if (BUFFERTAGS_EQUAL(*tag, root_tag))
            action = POLAR_READ_BUFFER_FOR_REDO(record, 2, buffer);
    }

    return action;
}

/*
 * VACUUM_PAGE record contains simply a full image of the page, similar to
 * an XLOG_FPI record.
 */
static XLogRedoAction
polar_gin_redo_vacuum_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    BufferTag vacuum_tag;
    XLogRedoAction action = BLK_NOTFOUND;

    POLAR_GET_LOG_TAG(record, vacuum_tag, 0);

    if (BUFFERTAGS_EQUAL(*tag, vacuum_tag))
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

    return action;
}

static XLogRedoAction
polar_gin_redo_vacuum_data_leaf_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    XLogRedoAction action = BLK_NOTFOUND;
    BufferTag vacuum_tag;

    POLAR_GET_LOG_TAG(record, vacuum_tag, 0);

    if (BUFFERTAGS_EQUAL(*tag, vacuum_tag))
    {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            Page        page = BufferGetPage(*buffer);
            Size        len;
            ginxlogVacuumDataLeafPage *xlrec;

            xlrec = (ginxlogVacuumDataLeafPage *) XLogRecGetBlockData(record, 0, &len);

            Assert(GinPageIsLeaf(page));
            Assert(GinPageIsData(page));

            ginRedoRecompress(page, &xlrec->data);
            PageSetLSN(page, lsn);
        }
    }

    return action;
}

static XLogRedoAction
polar_gin_redo_delete_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    ginxlogDeletePage *data = (ginxlogDeletePage *) XLogRecGetData(record);
    XLogRedoAction    action = BLK_NOTFOUND;
    Page        page;
    BufferTag   ltag, dtag, ptag;

    POLAR_GET_LOG_TAG(record, ltag, 2);

    if (BUFFERTAGS_EQUAL(*tag, ltag))
    {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 2, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            page = BufferGetPage(*buffer);
            Assert(GinPageIsData(page));
            GinPageGetOpaque(page)->rightlink = data->rightLink;
            PageSetLSN(page, lsn);
        }

        return action;
    }

    POLAR_GET_LOG_TAG(record, dtag, 0);

    if (BUFFERTAGS_EQUAL(*tag, dtag))
    {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            page = BufferGetPage(*buffer);
            Assert(GinPageIsData(page));
            GinPageGetOpaque(page)->flags = GIN_DELETED;
            GinPageSetDeleteXid(page, data->deleteXid);
            PageSetLSN(page, lsn);
        }

        return action;
    }

    POLAR_GET_LOG_TAG(record, ptag, 1);

    if (BUFFERTAGS_EQUAL(*tag, ptag))
    {
        action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

        if (action == BLK_NEEDS_REDO)
        {
            page = BufferGetPage(*buffer);
            Assert(GinPageIsData(page));
            Assert(!GinPageIsLeaf(page));
            GinPageDeletePostingItem(page, data->parentOffset);
            PageSetLSN(page, lsn);
        }
    }

    return action;
}

static XLogRedoAction
polar_gin_redo_update_metapage(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    ginxlogUpdateMeta *data = (ginxlogUpdateMeta *) XLogRecGetData(record);
    XLogRedoAction    action = BLK_NOTFOUND;
    Page        metapage;
    BufferTag   meta_tag;

    POLAR_GET_LOG_TAG(record, meta_tag, 0);

    if (BUFFERTAGS_EQUAL(*tag, meta_tag))
    {
        /*
         * Restore the metapage. This is essentially the same as a full-page
         * image, so restore the metapage unconditionally without looking at the
         * LSN, to avoid torn page hazards.
         */
        POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
        Assert(BufferGetBlockNumber(*buffer) == GIN_METAPAGE_BLKNO);
        metapage = BufferGetPage(*buffer);

        GinInitMetabuffer(*buffer);
        memcpy(GinPageGetMeta(metapage), &data->metadata, sizeof(GinMetaPageData));
        PageSetLSN(metapage, lsn);

        return BLK_NEEDS_REDO;
    }

    if (data->ntuples > 0)
    {
        BufferTag tail_tag;
        POLAR_GET_LOG_TAG(record, tail_tag, 1);

        if (BUFFERTAGS_EQUAL(*tag, tail_tag))
        {
            /*
             * insert into tail page
             */
            action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

            if (action == BLK_NEEDS_REDO)
            {
                Page        page = BufferGetPage(*buffer);
                OffsetNumber off;
                int         i;
                Size        tupsize;
                char       *payload;
                IndexTuple  tuples;
                Size        totaltupsize;

                payload = XLogRecGetBlockData(record, 1, &totaltupsize);
                tuples = (IndexTuple) payload;

                if (PageIsEmpty(page))
                    off = FirstOffsetNumber;
                else
                    off = OffsetNumberNext(PageGetMaxOffsetNumber(page));

                for (i = 0; i < data->ntuples; i++)
                {
                    tupsize = IndexTupleSize(tuples);

                    if (PageAddItem(page, (Item) tuples, tupsize, off,
                                    false, false) == InvalidOffsetNumber)
                    {
//                        POLAR_LOG_REDO_INFO(page, record);
                        elog(ERROR, "failed to add item to index page");
                    }

                    tuples = (IndexTuple)(((char *) tuples) + tupsize);

                    off++;
                }

                Assert(payload + totaltupsize == (char *) tuples);

                /*
                 * Increase counter of heap tuples
                 */
                GinPageGetOpaque(page)->maxoff++;

                PageSetLSN(page, lsn);
            }
        }
    }
    else if (data->prevTail != InvalidBlockNumber)
    {
        BufferTag tail_tag;
        POLAR_GET_LOG_TAG(record, tail_tag, 1);

        if (BUFFERTAGS_EQUAL(*tag, tail_tag))
        {
            /*
             * New tail
             */
            action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

            if (action == BLK_NEEDS_REDO)
            {
                Page        page = BufferGetPage(*buffer);

                GinPageGetOpaque(page)->rightlink = data->newRightlink;

                PageSetLSN(page, lsn);
            }
        }
    }

    return action;
}

static XLogRedoAction
polar_gin_redo_insert_list_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    ginxlogInsertListPage *data = (ginxlogInsertListPage *) XLogRecGetData(record);
    Page        page;
    OffsetNumber l,
            off = FirstOffsetNumber;
    int         i,
            tupsize;
    char       *payload;
    IndexTuple  tuples;
    Size        totaltupsize;
    BufferTag   insert_tag;

    POLAR_GET_LOG_TAG(record, insert_tag, 0);

    if (!BUFFERTAGS_EQUAL(*tag, insert_tag))
        return BLK_NOTFOUND;

    POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
    page = BufferGetPage(*buffer);

    GinInitBuffer(*buffer, GIN_LIST);
    GinPageGetOpaque(page)->rightlink = data->rightlink;

    if (data->rightlink == InvalidBlockNumber)
    {
        /* tail of sublist */
        GinPageSetFullRow(page);
        GinPageGetOpaque(page)->maxoff = 1;
    }
    else
        GinPageGetOpaque(page)->maxoff = 0;

    payload = XLogRecGetBlockData(record, 0, &totaltupsize);

    tuples = (IndexTuple) payload;

    for (i = 0; i < data->ntuples; i++)
    {
        tupsize = IndexTupleSize(tuples);

        l = PageAddItem(page, (Item) tuples, tupsize, off, false, false);

        if (l == InvalidOffsetNumber)
        {
//            POLAR_LOG_REDO_INFO(page, record);
            elog(ERROR, "failed to add item to index page");
        }

        tuples = (IndexTuple)(((char *) tuples) + tupsize);
        off++;
    }

    Assert((char *) tuples == payload + totaltupsize);

    PageSetLSN(page, lsn);

    return BLK_NEEDS_REDO;
}

static XLogRedoAction
polar_gin_redo_delete_list_pages(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    XLogRecPtr  lsn = record->EndRecPtr;
    ginxlogDeleteListPages *data = (ginxlogDeleteListPages *) XLogRecGetData(record);
    Page        metapage;
    uint8           i;
    BufferTag   meta_tag, del_tag;

    POLAR_GET_LOG_TAG(record, meta_tag, 0);

    if (BUFFERTAGS_EQUAL(*tag, meta_tag))
    {
        POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
        Assert(BufferGetBlockNumber(*buffer) == GIN_METAPAGE_BLKNO);
        metapage = BufferGetPage(*buffer);

        GinInitMetabuffer(*buffer);

        memcpy(GinPageGetMeta(metapage), &data->metadata, sizeof(GinMetaPageData));
        PageSetLSN(metapage, lsn);

        return BLK_NEEDS_REDO;
    }

    /*
     * In normal operation, shiftList() takes exclusive lock on all the
     * pages-to-be-deleted simultaneously.  During replay, however, it should
     * be all right to lock them one at a time.  This is dependent on the fact
     * that we are deleting pages from the head of the list, and that readers
     * share-lock the next page before releasing the one they are on. So we
     * cannot get past a reader that is on, or due to visit, any page we are
     * going to delete.  New incoming readers will block behind our metapage
     * lock and then see a fully updated page list.
     *
     * No full-page images are taken of the deleted pages. Instead, they are
     * re-initialized as empty, deleted pages. Their right-links don't need to
     * be preserved, because no new readers can see the pages, as explained
     * above.
     */
    for (i = 0; i < data->ndeleted; i++)
    {
        uint8       block_id = i + 1;
        Page        page;

        POLAR_GET_LOG_TAG(record, del_tag, block_id);

        if (!BUFFERTAGS_EQUAL(*tag, del_tag))
            continue;

        POLAR_INIT_BUFFER_FOR_REDO(record, block_id, buffer);
        page = BufferGetPage(*buffer);
        GinInitBuffer(*buffer, GIN_DELETED);

        PageSetLSN(page, lsn);

        return BLK_NEEDS_REDO;
    }

    return BLK_NOTFOUND;
}


XLogRedoAction
polar_gin_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRedoAction action = BLK_NOTFOUND;
    MemoryContext old_ctx, redo_ctx;

    redo_ctx = polar_get_redo_context();
    old_ctx = MemoryContextSwitchTo(redo_ctx);

    switch (info)
    {
//        case XLOG_GIN_CREATE_INDEX:
//        {
//            action = polar_gin_redo_create_index(record, tag, buffer);
//            break;
//        }

        case XLOG_GIN_CREATE_PTREE:
        {
            action = polar_gin_redo_create_ptree(record, tag, buffer);
            break;
        }

        case XLOG_GIN_INSERT:
        {
            action = polar_gin_redo_insert(record, tag, buffer);
            break;
        }

        case XLOG_GIN_SPLIT:
        {
            action = polar_gin_redo_split(record, tag, buffer);
            break;
        }

        case XLOG_GIN_VACUUM_PAGE:
        {
            action = polar_gin_redo_vacuum_page(record, tag, buffer);
            break;
        }

        case XLOG_GIN_VACUUM_DATA_LEAF_PAGE:
        {
            action = polar_gin_redo_vacuum_data_leaf_page(record, tag, buffer);
            break;
        }

        case XLOG_GIN_DELETE_PAGE:
        {
            action = polar_gin_redo_delete_page(record, tag, buffer);
            break;
        }

        case XLOG_GIN_UPDATE_META_PAGE:
        {
            action = polar_gin_redo_update_metapage(record, tag, buffer);
            break;
        }

        case XLOG_GIN_INSERT_LISTPAGE:
        {
            action = polar_gin_redo_insert_list_page(record, tag, buffer);
            break;
        }

        case XLOG_GIN_DELETE_LISTPAGE:
        {
            action = polar_gin_redo_delete_list_pages(record, tag, buffer);
            break;
        }

        default:
            elog(PANIC, "polar_gin_idx_redo: unknown op code %u", info);
            break;
    }

    MemoryContextSwitchTo(old_ctx);
    MemoryContextReset(redo_ctx);

    return action;
}

static void
polar_gin_redo_insert_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum) {
    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    int tagCount = 0;
    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 2);

    if (XLogRecHasBlockRef(record, 1)) {
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
polar_gin_redo_insert_save(XLogReaderState *record)
{
    if (XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn(record, 1);

    ParseXLogBlocksLsn(record, 0);
}

static void
polar_gin_redo_split_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum) {
    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    int tagCount = 0;
    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 4);

    if (XLogRecHasBlockRef(record, 3)) {
        XLogRecGetBlockTag(record, 3, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
    }

    XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;
    XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;

    if (XLogRecHasBlockRef(record, 2)) {
        XLogRecGetBlockTag(record, 2, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
    }

    *tagNum = tagCount;
}

static void
polar_gin_redo_split_save(XLogReaderState *record)
{
    if (XLogRecHasBlockRef(record, 3))
        ParseXLogBlocksLsn(record, 3);

    ParseXLogBlocksLsn(record, 0);
    ParseXLogBlocksLsn(record, 1);

    if (XLogRecHasBlockRef(record, 2))
        ParseXLogBlocksLsn(record, 2);
}

static void
polar_gin_redo_update_metapage_save(XLogReaderState *record)
{
    ParseXLogBlocksLsn(record, 0);

    if (XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn(record, 1);
}

static void
polar_gin_redo_update_metapage_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    int tagCount = 0;
    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 2);

    XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
    tagCount++;

    if (XLogRecHasBlockRef(record, 1)) {
        XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
        INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
        tagCount++;
    }

    *tagNum = tagCount;
}

static void
polar_gin_redo_delete_list_pages_save(XLogReaderState *record)
{
    int i = 1;

    ParseXLogBlocksLsn(record, 0);

    for (i = 1; i <= GIN_NDELETE_AT_ONCE; i++)
    {
        if (XLogRecHasBlockRef(record, i))
            ParseXLogBlocksLsn(record, i);
        else
            break;
    }
}

static void
polar_gin_redo_delete_list_pages_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum)
{
    // At first, we already have block 0
    int tagCount = 1;
    int i = 1;

    for (i = 1; i <= GIN_NDELETE_AT_ONCE; i++)
    {
        if (XLogRecHasBlockRef(record, i))
            tagCount++;
        else
            break;
    }

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * tagCount);

    XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
    INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);

    for (i = 1; i <= GIN_NDELETE_AT_ONCE; i++)
    {
        if (XLogRecHasBlockRef(record, i)) {
            XLogRecGetBlockTag(record, i, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[i], rnode, forkNumber, blockNumber);
        }
        else
            break;
    }

    *tagNum = tagCount;
}

bool
polar_gin_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum) {
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    switch (info)
    {
//        case XLOG_GIN_CREATE_INDEX:
//            polar_logindex_save_block(instance, record, 0);
//            polar_logindex_save_block(instance, record, 1);
//            break;

        case XLOG_GIN_CREATE_PTREE:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 1);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);
            *tagNum = 1;
            break;

        case XLOG_GIN_INSERT:
            polar_gin_redo_insert_get_bufftag_list(record, buffertagList, tagNum);
            break;

        case XLOG_GIN_SPLIT:
            polar_gin_redo_split_get_bufftag_list(record, buffertagList, tagNum);
            break;

        case XLOG_GIN_VACUUM_PAGE:
        case XLOG_GIN_VACUUM_DATA_LEAF_PAGE:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 1);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);
            *tagNum = 1;
            break;

        case XLOG_GIN_DELETE_PAGE:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 3);

            XLogRecGetBlockTag(record, 2, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[1], rnode, forkNumber, blockNumber);

            XLogRecGetBlockTag(record, 1, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[2], rnode, forkNumber, blockNumber);
            *tagNum = 3;
            break;

        case XLOG_GIN_UPDATE_META_PAGE:
            polar_gin_redo_update_metapage_get_bufftag_list(record, buffertagList, tagNum);
            break;

        case XLOG_GIN_INSERT_LISTPAGE:
            *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) * 1);

            XLogRecGetBlockTag(record, 0, &rnode, &forkNumber, &blockNumber);
            INIT_BUFFERTAG((*buffertagList)[0], rnode, forkNumber, blockNumber);
            *tagNum = 1;
            break;

        case XLOG_GIN_DELETE_LISTPAGE:
            polar_gin_redo_delete_list_pages_get_bufftag_list(record, buffertagList, tagNum);
            break;

        default:
            return false;
            elog(PANIC, "polar_gin_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}
bool
polar_gin_idx_save(XLogReaderState *record)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info)
    {
//        case XLOG_GIN_CREATE_INDEX:
//            polar_logindex_save_block(instance, record, 0);
//            polar_logindex_save_block(instance, record, 1);
//            break;

        case XLOG_GIN_CREATE_PTREE:
            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_GIN_INSERT:
            polar_gin_redo_insert_save(record);
            break;

        case XLOG_GIN_SPLIT:
            polar_gin_redo_split_save(record);
            break;

        case XLOG_GIN_VACUUM_PAGE:
        case XLOG_GIN_VACUUM_DATA_LEAF_PAGE:
            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_GIN_DELETE_PAGE:
            ParseXLogBlocksLsn(record, 2);
            ParseXLogBlocksLsn(record, 0);
            ParseXLogBlocksLsn(record, 1);
            break;

        case XLOG_GIN_UPDATE_META_PAGE:
            polar_gin_redo_update_metapage_save(record);
            break;

        case XLOG_GIN_INSERT_LISTPAGE:
            ParseXLogBlocksLsn(record, 0);
            break;

        case XLOG_GIN_DELETE_LISTPAGE:
            polar_gin_redo_delete_list_pages_save(record);
            break;

        default:
            return false;
            elog(PANIC, "polar_gin_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}