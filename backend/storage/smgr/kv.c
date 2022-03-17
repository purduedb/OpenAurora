#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <string.h>

#include "access/xlog.h"
#include "access/xlogutils.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/kv.h"
#include "storage/kvstore.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

typedef struct _MdfdVec
{
    File		mdfd_vfd;		/* fd number in fd.c's pool */
    BlockNumber mdfd_segno;		/* segment number, from 0 */
} MdfdVec;

static MemoryContext MdCxt;		/* context for all MdfdVec objects */

/* Populate a file tag describing an md.c segment file. */
#define INIT_KV_FILETAG(a,xx_rnode,xx_forknum,xx_segno) \
( \
	memset(&(a), 0, sizeof(FileTag)), \
	(a).handler = SYNC_HANDLER_KV, \
	(a).rnode = (xx_rnode), \
	(a).forknum = (xx_forknum), \
	(a).segno = (xx_segno) \
)

/* ereport if segment not present */
#define EXTENSION_FAIL				(1 << 0)
/* return NULL if segment not present */
#define EXTENSION_RETURN_NULL		(1 << 1)
/* create new segments as needed */
#define EXTENSION_CREATE			(1 << 2)
/* create new segments if needed during recovery */
#define EXTENSION_CREATE_RECOVERY	(1 << 3)
/*
 * Allow opening segments which are preceded by segments smaller than
 * RELSEG_SIZE, e.g. inactive segments (see above). Note that this breaks
 * mdnblocks() and related functionality henceforth - which currently is ok,
 * because this is only required in the checkpointer which never uses
 * mdnblocks().
 */
#define EXTENSION_DONT_CHECK_SIZE	(1 << 4)

/* local routines */
static void kvunlinkfork(RelFileNodeBackend rnode, ForkNumber forkNum,
                         bool isRedo);
static void kvregister_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum,
                                    BlockNumber segno);
static void kvregister_forget_request(RelFileNodeBackend rnode, ForkNumber forknum,
                                    BlockNumber segno);



const char * KV_FILE_PAGE_NUM = "kv_file_page_num_%s";//filename -> how many pages
const char * KV_GET_FILE_PAGE = "kv_get_%s_%d"; //(filename, pageid) -> page
const char * KV_FILE_PAGE_NUM_PREFIX = "kv_file_page_num_\0";//filename -> how many pages
const char * KV_GET_FILE_PAGE_PREFIX = "kv_get_\0"; //(filename, pageid) -> page
//! Input BlockNum is index no, start from 0
//! KV_PAGE_NUM is real page number, start from 1

void
kvinit(void)
{
    MdCxt = AllocSetContextCreate(TopMemoryContext,
                                  "KvSmgr",
                                  ALLOCSET_DEFAULT_SIZES);
}


BlockNumber
kvnblocks(SMgrRelation reln, ForkNumber forknum)
{
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("kvnblocks start\n")));
//    printf("[kvnblocks] dbNum = %d, relNum = %d, forkNum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum);

    char *path;
    int totalPageNum = 0;
    char kvNumKey[MAXPGPATH];
    int err = 0;

    path = relpath(reln->smgr_rnode, forknum);
    snprintf(kvNumKey, sizeof(kvNumKey), KV_FILE_PAGE_NUM, path);
    pfree(path);

    err = KvGetInt(kvNumKey, &totalPageNum);
    if (err == -1) { // err==-1 means $kvNumKey doesn't exist in kv_store
        return -1;
    }
//    if (err != 0) {
//        ereport(ERROR,
//                (errcode(ERRCODE_WINDOWING_ERROR),
//                        errmsg("[kvnblocks] KvGetInt failed, db=%d, rel=%d, fork=%d", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum)));
//        return 0;
//    }

    return totalPageNum;
}

void
kvcopydb(char *srcPath, char*dstPath) {
    KvPrefixCopyDir(srcPath, dstPath, KV_FILE_PAGE_NUM_PREFIX);
    KvPrefixCopyDir(srcPath, dstPath, KV_GET_FILE_PAGE_PREFIX);
}

void
kvopen(SMgrRelation reln)
{
    /* mark it not open */
    for (int forknum = 0; forknum <= MAX_FORKNUM; forknum++)
        reln->md_num_open_segs[forknum] = 0;
}

bool
kvexists(SMgrRelation reln, ForkNumber forkNum)
{
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("kvexists start\n")));
    /*
     * Close it first, to ensure that we notice if the fork has been unlinked
     * since we opened it.
     */
    kvclose(reln, forkNum);
    int pageNum = kvnblocks(reln, forkNum);
    if(pageNum == -1) {
        return false;
    }  else {
        reln->md_num_open_segs[forkNum] = pageNum;
        return true;
    }

//
//
//    char *filename = relpath(reln->smgr_rnode, forkNum);
//    char kvKey[MAXPGPATH];
//    snprintf(kvKey, sizeof(kvKey), KV_FILE_PAGE_NUM, filename);
//    pfree(filename);
//
//    int pageNum = 0;
//    int err = KvGetInt(kvKey, &pageNum);
//
//    // if that file has no page stored in KV
//    // err==1 means key doesn't exist
//    if (err == 1) {
//        return false;
//    }
//
//
//
//    // maybe truncate
//    if (pageNum == 0) {
//        return true;
//    }
//
//    reln->md_num_open_segs[forkNum] = pageNum;
//
//    return true;
}

void
kvclose(SMgrRelation reln, ForkNumber forknum)
{
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("kvclose start\n")));
    // here close means to delete info stored in memory
    // As if this relation fork has never opened since system started
    reln->md_num_open_segs[forknum] = 0;
}

void
kvcreate(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{

//    printf("[kvcreate] dbNum = %d, relNum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode);
    MdfdVec    *mdfd;
    char	   *filename;
    File		fd;

    if (isRedo && reln->md_num_open_segs[forkNum] > 0)
        return;					/* created and opened already... */

    Assert(reln->md_num_open_segs[forkNum] == 0);

    // No need to create dir for this relation, kv will handle it
    /*TablespaceCreateDbspace(reln->smgr_rnode.node.spcNode,
                            reln->smgr_rnode.node.dbNode,
                            isRedo);*/

    filename = relpath(reln->smgr_rnode, forkNum);

    char kvKey[MAXPGPATH];
    snprintf(kvKey, sizeof(kvKey), KV_FILE_PAGE_NUM, filename);
    pfree(filename);
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("[kvcreate] create key = %s\n", kvKey)));
    //! creating a new file, just assign its page number as 1
    KvPutInt(kvKey, 0);
    reln->md_num_open_segs[forkNum] = 1;
    return;
}

void
kvunlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("kvunlink start\n")));
//    printf("[kvunlinkfork] dbNum = %d, relNum = %d, forkNum = %d\n", rnode.node.dbNode, rnode.node.relNode, forkNum);

    /* Now do the per-fork work */
    if (forkNum == InvalidForkNumber)
    {
        for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
            kvunlinkfork(rnode, forkNum, isRedo);
    }
    else
        kvunlinkfork(rnode, forkNum, isRedo);
}

static void
kvunlinkfork(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("kvunlinkfork\n")));
//    printf("[kvunlinkfork] dbNum = %d, relNum = %d\n", rnode.node.dbNode, rnode.node.relNode);

    char	   *path;
    int			ret;

    path = relpath(rnode, forkNum);

    /*
     * Delete or truncate the first segment.
     */
    /* First, forget any pending sync requests for the first segment */
    if (!RelFileNodeBackendIsTemp(rnode))
        kvregister_forget_request(rnode, forkNum, 0 /* first seg */ );


    // Delete all the page, but leave pageNumKey to be processed later
    char kvNumKey[MAXPGPATH];
    snprintf(kvNumKey, sizeof(kvNumKey), KV_FILE_PAGE_NUM, path);



    SMgrRelation reln;
    reln = malloc(sizeof(SMgrRelationData));
    reln->smgr_rnode = rnode;
    int pageNum = kvnblocks(reln, forkNum);
    if (pageNum < 0) {
        ereport(WARNING,
                (errcode_for_file_access(),
                        errmsg("kvunlinkfork get key %s failed", kvNumKey)));
    }
    free(reln);



//
//    int pageNum = 0;
//    int err = KvGetInt(kvNumKey, &pageNum);
//    if (err != 0) {
//        ereport(WARNING,
//                (errcode_for_file_access(),
//                        errmsg("get key failed", kvNumKey)));
//    }

    char kvPageKey[MAXPGPATH];
    for (int pageno = 0; pageno < pageNum; pageno++) {
        snprintf(kvPageKey, sizeof(kvPageKey), KV_GET_FILE_PAGE, path);
        KvDelete(kvPageKey);
    }

    // pageNum key be deleted right now
    if (isRedo || forkNum != MAIN_FORKNUM || RelFileNodeBackendIsTemp(rnode))
    {
        KvDelete(kvNumKey);
    }
    else // pageNum key should be deleted in the next checkpoint
    {
        KvPutInt(kvNumKey, 0);
        /* Register request to unlink first segment later */
        //! Does it works??
        kvregister_unlink_segment(rnode, forkNum, 0 /* first seg */ );
    }

    pfree(path);
}

//todo Here may need a lock
void
kvextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
         char *buffer, bool skipFsync)
{
//    printf("[kvextend] dbNum = %d, relNum = %d, blocknum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, blocknum);
    char *path;
    path = relpath(reln->smgr_rnode, forknum);

    // first get how many pages are in this fork, currently.

    int currPageNum = 0;
//    int err = 0;
    char kvNumKey[MAXPGPATH];
    snprintf(kvNumKey, sizeof(kvNumKey), KV_FILE_PAGE_NUM, path);
//    err = KvGetInt(kvNumKey, &currPageNum);
//    if (err != 0) {
//        ereport(ERROR,
//                (errcode(ERRCODE_WINDOWING_ERROR),
//                        errmsg("[kvextend] KvGetInt failed")));
//        return;
//    }
    currPageNum = kvnblocks(reln, forknum);
    if (currPageNum < 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[kvextend] kvnblocks get minus result")));
    }

    // No need to extend
    if (currPageNum >= blocknum+1) {
        return;
    }

    // extend from currPageNum to blocknum-1 with zeroPage
    // extend blocknum with buffer
    //todo lock start
    char kvPageKey[MAXPGPATH];
    char *zerobuf = palloc0(BLCKSZ);
    for (int i = currPageNum; i < blocknum; i++) {
        snprintf(kvPageKey, sizeof(kvPageKey), KV_GET_FILE_PAGE, path, i);
        int err = KvPut(kvPageKey, zerobuf, BLCKSZ);
        if (err != 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_WINDOWING_ERROR),
                            errmsg("[kvextend] KvPut failed")));
        }
    }
    pfree(zerobuf);

    snprintf(kvPageKey, sizeof(kvPageKey), KV_GET_FILE_PAGE, path, blocknum);
    if ( 0 != KvPut(kvPageKey, buffer, BLCKSZ) ) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[kvextend] KvPut failed")));
    }

    // update the pageNum
    if ( 0 != KvPutInt(kvNumKey, blocknum+1) ) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[kvextend] KvPut failed")));
    }

    pfree(path);

    // todo unlock

//    if (!skipFsync && !SmgrIsTemp(reln))
//        register_dirty_segment(reln, forknum, v);

}

static void
kvregister_forget_request(RelFileNodeBackend rnode, ForkNumber forknum,
                        BlockNumber segno)
{
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("kvregister_forget_request start\n")));
    FileTag		tag;

    INIT_KV_FILETAG(tag, rnode.node, forknum, segno);

    RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true /* retryOnError */ );
}

static void
kvregister_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum,
                        BlockNumber segno)
{
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("kvregister_unlink_segment start\n")));
    FileTag		tag;

    INIT_KV_FILETAG(tag, rnode.node, forknum, segno);

    /* Should never be used with temp relations */
    Assert(!RelFileNodeBackendIsTemp(rnode));

    RegisterSyncRequest(&tag, SYNC_UNLINK_REQUEST, true /* retryOnError */ );
}

// Currently, nothing will be done
bool
kvprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("kvprefetch start\n")));
#ifdef USE_PREFETCH
//    off_t		seekpos;
//    MdfdVec    *v;
//
//    v = _mdfd_getseg(reln, forknum, blocknum, false,
//                     InRecovery ? EXTENSION_RETURN_NULL : EXTENSION_FAIL);
//    if (v == NULL)
//        return false;
//
//    seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
//
//    Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);
//
//    (void) FilePrefetch(v->mdfd_vfd, seekpos, BLCKSZ, WAIT_EVENT_DATA_FILE_PREFETCH);
#endif							/* USE_PREFETCH */

    return true;
}

void
kvread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
       char *buffer)
{
//    printf("[kvread] dbNum = %d, relNum = %d, blocknum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, blocknum);
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("[kvread ]  start\n")));

    char *path;
    int totalPageNum = 0;
    char kvNumKey[MAXPGPATH];
    char kvPageKey[MAXPGPATH];
    int err = 0;

    TRACE_POSTGRESQL_SMGR_MD_READ_START(forknum, blocknum,
                                        reln->smgr_rnode.node.spcNode,
                                        reln->smgr_rnode.node.dbNode,
                                        reln->smgr_rnode.node.relNode,
                                        reln->smgr_rnode.backend);

    path = relpath(reln->smgr_rnode, forknum);
//    snprintf(kvNumKey, sizeof(kvNumKey), KV_FILE_PAGE_NUM, path);
//
//    err = KvGetInt(kvNumKey, &totalPageNum);
//    if (err != 0) {
//        ereport(ERROR,
//                (errcode(ERRCODE_WINDOWING_ERROR),
//                        errmsg("[kvread] KvGetInt failed")));
//        return;
//    }
    totalPageNum = kvnblocks(reln, forknum);
    if (totalPageNum < 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[kvread] kvnblocks failed")));
        return;
    }

    if (totalPageNum < blocknum+1) {
        if (zero_damaged_pages || InRecovery)
            MemSet(buffer, 0, BLCKSZ);
        else
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("[kvread] read page corrupted.")));
        return;
    }

    TRACE_POSTGRESQL_SMGR_MD_READ_DONE(forknum, blocknum,
                                       reln->smgr_rnode.node.spcNode,
                                       reln->smgr_rnode.node.dbNode,
                                       reln->smgr_rnode.node.relNode,
                                       reln->smgr_rnode.backend,
                                       nbytes,
                                       BLCKSZ);

    // Get page from kv
    snprintf(kvPageKey, sizeof(kvPageKey), KV_GET_FILE_PAGE, path, blocknum);
    if ( KvGet(kvPageKey, &buffer) ) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[kvread] KvGet failed")));
    }

    pfree(path);

}

void
kvwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
        char *buffer, bool skipFsync)
{
//    printf("[kvwrite] dbNum = %d   relNum = %d  blockNum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, blocknum);

    char *path;
    int totalPageNum = 0;
    char kvNumKey[MAXPGPATH];
    char kvPageKey[MAXPGPATH];
    int err = 0;

    TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum,
                                         reln->smgr_rnode.node.spcNode,
                                         reln->smgr_rnode.node.dbNode,
                                         reln->smgr_rnode.node.relNode,
                                         reln->smgr_rnode.backend);

    path = relpath(reln->smgr_rnode, forknum);
    totalPageNum = kvnblocks(reln, forknum);
    if (totalPageNum < 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[kvwrite] kvnblocks failed")));
        return;
    }
//
//
//    snprintf(kvNumKey, sizeof(kvNumKey), KV_FILE_PAGE_NUM, path);
//
//    err = KvGetInt(kvNumKey, &totalPageNum);
//    if (err != 0) {
//        ereport(ERROR,
//                (errcode(ERRCODE_WINDOWING_ERROR),
//                        errmsg("[kvwrite] KvGetInt failed")));
//        return;
//    }

    if (totalPageNum < blocknum+1) {
        // if inRecovery == true, then extend page
        if (InRecovery) {
            char *zerobuf = palloc0(BLCKSZ);
            kvextend(reln, forknum, blocknum, zerobuf, skipFsync);
            pfree(zerobuf);
        } else { // report err
            ereport(ERROR,
                    (errcode_for_file_access(),
                            errmsg("[kvwrite] write to a non-exist page")));
            return;
        }
    }

    snprintf(kvPageKey, sizeof(kvPageKey), KV_GET_FILE_PAGE, path, blocknum);
    if ( KvPut(kvPageKey, buffer, BLCKSZ) ) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[kvwrite] KvPut failed")));
    }
    pfree(path);
}

/*
 * mdwriteback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
void
kvwriteback(SMgrRelation reln, ForkNumber forknum,
            BlockNumber blocknum, BlockNumber nblocks)
{
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("kvwriteback start\n")));
    // do nothing
    // let kvstore do sync by itself
}

/*
 *	kvtruncate() -- Truncate relation to specified number of blocks.
 */
void
kvtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
//    printf("[kvtruncate] dbNum = %d, relNum = %d, nBlocks = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, nblocks);

    char *path;
    int totalPageNum = 0;
    char kvNumKey[MAXPGPATH];
    int err = 0;

    path = relpath(reln->smgr_rnode, forknum);
    snprintf(kvNumKey, sizeof(kvNumKey), KV_FILE_PAGE_NUM, path);

    totalPageNum = kvnblocks(reln, forknum);
    if (totalPageNum < 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[kvtruncate] kvnblocks failed")));
        return;
    }
//
//    err = KvGetInt(kvNumKey, &totalPageNum);
//    if (err != 0) {
//        ereport(ERROR,
//                (errcode(ERRCODE_WINDOWING_ERROR),
//                        errmsg("[kvtruncate] KvGetInt failed")));
//        return;
//    }

    if (nblocks > totalPageNum) {
        /* Bogus request ... but no complaint if InRecovery */
        if (InRecovery)
            return;
        ereport(ERROR,
                (errmsg("could not truncate file \"%s\" to %u blocks: it's only %u blocks now",
                        relpath(reln->smgr_rnode, forknum),
                        nblocks, totalPageNum)));
    }

    // Delete pages from nblocks to totalPageNum-1
    // todo need a lock
    char kvPageKey[MAXPGPATH];
    for (unsigned int i = nblocks; i < totalPageNum; i++) {
        snprintf(kvPageKey, sizeof(kvPageKey), KV_GET_FILE_PAGE, path, i);
        if (KvDelete(kvPageKey) ) {
            ereport(ERROR,
                    (errcode(ERRCODE_WINDOWING_ERROR),
                            errmsg("[kvtruncate] KvDelete failed")));
        }
    }
    pfree(path);

    if (KvPutInt(kvNumKey, nblocks) != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[kvtruncate] KvPutInt failed")));
    }

    // todo need unlock
    return;
}

/*
 *	mdimmedsync() -- Immediately sync a relation to stable storage.
 *
 * Note that only writes already issued are synced; this routine knows
 * nothing of dirty buffers that may exist inside the buffer manager.  We
 * sync active and inactive segments; smgrDoPendingSyncs() relies on this.
 * Consider a relation skipping WAL.  Suppose a checkpoint syncs blocks of
 * some segment, then mdtruncate() renders that segment inactive.  If we
 * crash before the next checkpoint syncs the newly-inactive segment, that
 * segment may survive recovery, reintroducing unwanted data into the table.
 */
void
kvimmedsync(SMgrRelation reln, ForkNumber forknum)
{
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("kvimmedsync start\n")));
    // do nothing with sync in this layer
}

/*
 * Sync a file to disk, given a file tag.  Write the path into an output
 * buffer so the caller can use it in error messages.
 *
 * Return 0 on success, -1 on failure, with errno set.
 */
int
kvsyncfiletag(const FileTag *ftag, char *path)
{
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("kvsyncfiletag start\n")));
    // do nothing
}

/*
 * Unlink a file, given a file tag.  Write the path into an output
 * buffer so the caller can use it in error messages.
 *
 * Return 0 on success, -1 on failure, with errno set.
 */
int
kvunlinkfiletag(const FileTag *ftag, char *path)
{
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("kvunlinkfiletag start\n")));
    char	   *p;

    /* Compute the path. */
    p = relpathperm(ftag->rnode, MAIN_FORKNUM);
    strlcpy(path, p, MAXPGPATH);
    pfree(p);

    char kvNumKey[MAXPGPATH];
    snprintf(kvNumKey, sizeof(kvNumKey), KV_FILE_PAGE_NUM, path);

    /*
     * First, get this relation's current page num.
     * Normally, the relation will contain 0 page
     *      If there are still some pages, delete it
     * Finally, delete page number key
     */
//    int resultPageNum = -1;
//    if (KvGetInt(kvNumKey, &resultPageNum) != 0) {
//        ereport(ERROR,
//                (errcode(ERRCODE_WINDOWING_ERROR),
//                        errmsg("[kvunlinkfiletag] KvGetInt failed")));
//        return -1;
//    }

    SMgrRelation reln = malloc(sizeof(SMgrRelationData));
    reln->smgr_rnode.node = ftag->rnode;

    int resultPageNum = 0;
    resultPageNum = kvnblocks(reln, MAIN_FORKNUM);
    free(reln);

    if(resultPageNum < 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[kvunlinkfiletag] kvnblocks failed")));
        return -1;
    }

    char kvPageKey[MAXPGPATH];
    for (int i = 0; i < resultPageNum; i++) {
        snprintf(kvPageKey, sizeof(kvPageKey), KV_GET_FILE_PAGE, path, i);
        if( KvDelete(kvPageKey)!= 0 ) {
            ereport(ERROR,
                    (errcode(ERRCODE_WINDOWING_ERROR),
                            errmsg("[kvunlinkfiletag] KvDelete failed")));
        }
    }

    if (KvDelete(kvNumKey) != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[kvunlinkfiletag] KvDelete failed")));
        return -1;
    }

    return 0;
}

/*
 * Check if a given candidate request matches a given tag, when processing
 * a SYNC_FILTER_REQUEST request.  This will be called for all pending
 * requests to find out whether to forget them.
 */
bool
kvfiletagmatches(const FileTag *ftag, const FileTag *candidate)
{
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("kvfiletagmatches start\n")));
    /*
     * For now we only use filter requests as a way to drop all scheduled
     * callbacks relating to a given database, when dropping the database.
     * We'll return true for all candidates that have the same database OID as
     * the ftag from the SYNC_FILTER_REQUEST request, so they're forgotten.
     */
    return ftag->rnode.dbNode == candidate->rnode.dbNode;
}

