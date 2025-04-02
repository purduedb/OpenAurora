#include "postgres.h"

#include "access/bufmask.h"
#include "access/generic_xlog.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "storage/buf_internals.h"
#include "access/polar_logindex.h"
#include "storage/kv_interface.h"
#include "access/xlog.h"

bool
polar_generic_idx_save(XLogReaderState *record)
{
    int block_id;

    for (block_id = 0; block_id <= MAX_GENERIC_XLOG_PAGES; block_id++)
    {
        if (XLogRecHasBlockRef(record, block_id))
            ParseXLogBlocksLsn(record, block_id);
    }
    return true;
}

bool
polar_generic_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum) {
    int block_id;
    int tagCount = 0;

    RelFileNode rnode;
    ForkNumber forkNumber;
    BlockNumber blockNumber;

    // maximun block_id
    *buffertagList = (BufferTag*) malloc(sizeof(BufferTag) *4);
    for (block_id = 0; block_id <= MAX_GENERIC_XLOG_PAGES; block_id++)
    {
        if (XLogRecHasBlockRef(record, block_id)) {
            XLogRecGetBlockTag(record, block_id, &rnode, &forkNumber, &blockNumber);

            INIT_BUFFERTAG((*buffertagList)[tagCount], rnode, forkNumber, blockNumber);
            tagCount++;
        }
    }
    *tagNum = tagCount;
    return true;
}

XLogRedoAction
polar_generic_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
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
            action = XLogReadBufferForRedo(record, block_id, buffer);

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
