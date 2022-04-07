#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <string.h>

#include "access/xlog.h"
#include "access/xlogrecord.h"
#include "access/xlogutils.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/storage_kv.h"
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
//todo
#define INIT_STORAGE_KV_FILETAG(a,xx_rnode,xx_forknum,xx_segno) \
( \
	memset(&(a), 0, sizeof(FileTag)), \
	(a).handler = SYNC_HANDLER_KV, \
	(a).rnode = (xx_rnode), \
	(a).forknum = (xx_forknum), \
	(a).segno = (xx_segno) \
)
#define DEBUG_INFO

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
static void storage_kvunlinkfork(RelFileNodeBackend rnode, ForkNumber forkNum,
                         bool isRedo);
static void storage_kvregister_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum,
                                      BlockNumber segno);
static void storage_kvregister_forget_request(RelFileNodeBackend rnode, ForkNumber forknum,
                                      BlockNumber segno);

// Here remember all the xlog that didn't been redo
const char * KV_XLOG_PAGE_ASYNC_REDO_LIST = "kv_xlog_aync_page_redo_list_%s_%d"; // (filename, pageid) -> [ ]LSN
const char * KV_XLOG_REL_ASYNC_REDO_LIST = "kv_xlog_aync_rel_redo_list_%s"; // filename -> [ ]LSN
const char * KV_XLOG_LIST = "kv_xlog_list_%lld"; // lsn -> xlog_record

char * KV_FILE_PAGE_NUM_VERSION_LIST = "kv_file_page_num_version_list_%s"; // filename -> [ ]LSN
char * KV_FILE_VERSION_PAGE_NUM = "kv_file_page_num_version_%s_%lu"; // (filename, lsn) -> how many pages
char * KV_GET_PAGE_VERSION_LIST = "kv_get_page_version_list_%s_%d"; // (filename, pageid) -> [ ]LSN
char * KV_GET_VERSION_PAGE = "kv_get_version_page_%s_%d_%lu"; // (filename, pageid, lsn) -> page

char * KV_FILE_PAGE_NUM_VERSION_LIST_PREFIX = "kv_file_page_num_version_list_\0"; // filename -> [ ]LSN
char * KV_FILE_VERSION_PAGE_NUM_PREFIX = "kv_file_page_num_version_\0"; // (filename, lsn) -> how many pages
char * KV_GET_PAGE_VERSION_LIST_PREFIX = "kv_get_page_version_list_\0"; // (filename, pageid) -> [ ]LSN
char * KV_GET_VERSION_PAGE_PREFIX = "kv_get_version_page_\0"; // (filename, pageid, lsn) -> page
const char * KV_XLOG_PAGE_ASYNC_REDO_LIST_PREFIX = "kv_xlog_aync_page_redo_list_\0"; // (filename, pageid) -> [ ]LSN
const char * KV_XLOG_REL_ASYNC_REDO_LIST_PREFIX = "kv_xlog_aync_rel_redo_list_\0"; // filename -> [ ]LSN
const char * KV_XLOG_LIST_PREFIX = "kv_xlog_list_\0"; // lsn -> xlog_record

//! Input BlockNum is index no, start from 0
//! KV_PAGE_NUM is real page number, start from 1

unsigned long CurrentRedoLsn = 0;

void
storage_kvinit(void)
{
    char		path[MAXPGPATH];
    snprintf(path, sizeof(path), "%s/client.signal", DataDir);
    if (access(path, F_OK) == 0) {
        KV_FILE_PAGE_NUM_VERSION_LIST = "cli_kv_file_page_num_version_list_%s"; // filename -> [ ]LSN
        KV_FILE_VERSION_PAGE_NUM = "cli_kv_file_page_num_version_%s_%s"; // (filename, lsn) -> how many pages
        KV_GET_PAGE_VERSION_LIST = "cli_kv_get_page_version_list_%s"; // (filename, pageid) -> [ ]LSN
        KV_GET_VERSION_PAGE = "cli_kv_get_version_page_%s"; // (filename, pageid, lsn) -> page

        KV_FILE_PAGE_NUM_VERSION_LIST_PREFIX = "cli_kv_file_page_num_version_list_\0"; // filename -> [ ]LSN
        KV_FILE_VERSION_PAGE_NUM_PREFIX = "cli_kv_file_page_num_version_\0"; // (filename, lsn) -> how many pages
        KV_GET_PAGE_VERSION_LIST_PREFIX = "cli_kv_get_page_version_list_\0"; // (filename, pageid) -> [ ]LSN
        KV_GET_VERSION_PAGE_PREFIX = "cli_kv_get_version_page_\0"; // (filename, pageid, lsn) -> page

    }
    MdCxt = AllocSetContextCreate(TopMemoryContext,
                                  "KvSmgr",
                                  ALLOCSET_DEFAULT_SIZES);
}


BlockNumber
storage_kvnblocks(SMgrRelation reln, ForkNumber forknum) {
    return storage_kvnblocks_internal(reln, forknum, -1);
}



// Update Page number version without redo
// When received a xlog and try to put it into kv, first check whether this page
// exists in this file. If not, create a new page number version
BlockNumber
storage_kvnblocks_internal(SMgrRelation reln, ForkNumber forknum, unsigned long lsn) {
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_kvnblocks_internal START\n")));
    printf("[storage_kvnblocks_internal] START dbNum = %d, relNum = %d, forkNum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum);
#endif
    if (lsn == -1) {
        lsn = CurrentRedoLsn;
    }

    char *path;
    int totalPageNum = 0;
    char TempKey[MAXPGPATH];
    int err = 0;

    // First, redo all relation related xlog, which smaller/equal than lsn
    RedoRelXlogForAsync(reln->smgr_rnode.node, forknum, lsn);

    path = relpath(reln->smgr_rnode, forknum);
    snprintf(TempKey, sizeof(TempKey), KV_FILE_PAGE_NUM_VERSION_LIST, path);
    pfree(path);

    char *kvGetResult = malloc(sizeof(unsigned long)*128);
    char *backupP = kvGetResult;
    err = KvGet(TempKey, &kvGetResult);
    if (err != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvnblocks_internal] KvGet failed, db=%d, rel=%d, fork=%d",
                               reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum)));
        return 0;
    }
    printf("[storage_kvnblocks_internal] FILE PAGE NUMBER VERSION LIST KEY = %s\n", TempKey);
    // This relation doesn't exist in kvStore
    if (kvGetResult == NULL) {
#ifdef DEBUG_INFO
        ereport(NOTICE,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("storage_kvnblocks_internal END\n")));
        printf("[storage_kvnblocks_internal] END, relation doesn't exist\n");
#endif
        free(backupP);
        return -1;
    }

#ifdef DEBUG_INFO
    printf("[storage_kvnblocks_internal] FILE PAGE NUMBER has %d VERSIONs\n", UnmarshalUnsignedLongListGetSize(kvGetResult));
    for(int i = 0; i < UnmarshalUnsignedLongListGetSize(kvGetResult); i++) {
        printf("[storage_kvnblocks_internal] Version %d -> LSN %lu\n", UnmarshalUnsignedLongListGetList(kvGetResult)[i]);
    }
#endif
    int targetVersionIndex = FindLowerBound_UnsignedLong(UnmarshalUnsignedLongListGetList(kvGetResult),
                                UnmarshalUnsignedLongListGetSize(kvGetResult),
                                lsn);
    if (targetVersionIndex < 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvnblocks_internal] No version in version list, db=%d, rel=%d, fork=%d",
                               reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum)));
        return 0;
    }

    unsigned long targetVersion = UnmarshalUnsignedLongListGetList(kvGetResult)[targetVersionIndex];
    printf("[storage_kvnblocks_internal] Use %d File Page number version, and the lsn = %lu\n", targetVersionIndex,
        targetVersion);
    free(kvGetResult);

    // Then we get the target version PageNum
    path = relpath(reln->smgr_rnode, forknum);
    snprintf(TempKey, sizeof(TempKey), KV_FILE_VERSION_PAGE_NUM, path, lsn);
    pfree(path);
    printf("[storage_kvnblocks_internal] Get target page number version, key = %s\n", TempKey);
    err = KvGetInt(TempKey, &totalPageNum);
    if (err == -1) { // err==-1 means $kvNumKey doesn't exist in kv_store
        // It's impossible, that lsn index exists in version list, but doesn't really exist.
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvnblocks_internal] KvGetInt, can not find target version, db=%d, rel=%d, fork=%d",
                               reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum)));
        return 0;
    }

#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_kvnblocks_internal end\n")));
    printf("[storage_kvnblocks_internal] END dbNum = %d, relNum = %d, forkNum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum);
#endif
    return totalPageNum;
}

void
storage_kvcopydb(char *srcPath, char*dstPath) {
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_kvcopydb start\n")));
    printf("Start COPY!! \n\n\n");
#endif
    KvPrefixCopyDir(srcPath, dstPath, KV_FILE_PAGE_NUM_VERSION_LIST_PREFIX);
    KvPrefixCopyDir(srcPath, dstPath, KV_FILE_VERSION_PAGE_NUM_PREFIX);
    KvPrefixCopyDir(srcPath, dstPath, KV_GET_PAGE_VERSION_LIST_PREFIX);
    KvPrefixCopyDir(srcPath, dstPath, KV_GET_VERSION_PAGE_PREFIX);

    KvPrefixCopyDir(srcPath, dstPath, KV_XLOG_PAGE_ASYNC_REDO_LIST_PREFIX);
    KvPrefixCopyDir(srcPath, dstPath, KV_XLOG_REL_ASYNC_REDO_LIST_PREFIX);
    KvPrefixCopyDir(srcPath, dstPath, KV_XLOG_LIST_PREFIX);

}

void
storage_kvopen(SMgrRelation reln)
{
    /* mark it not open */
    for (int forknum = 0; forknum <= MAX_FORKNUM; forknum++)
        reln->md_num_open_segs[forknum] = 0;
}

bool
storage_kvexists(SMgrRelation reln, ForkNumber forkNum)
{
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_kvexists start\n")));
    printf("[storage_kvexists] dbNum = %d, relNum = %d, forkNum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forkNum);
#endif
    /*
     * Close it first, to ensure that we notice if the fork has been unlinked
     * since we opened it.
     */
    storage_kvclose(reln, forkNum);
    int pageNum = storage_kvnblocks(reln, forkNum);
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
storage_kvclose(SMgrRelation reln, ForkNumber forknum)
{
    // here close means to delete info stored in memory
    // As if this relation fork has never opened since system started
    reln->md_num_open_segs[forknum] = 0;
}

void
storage_kvcreate(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_create start\n")));
#endif

    char	   *filename;


    if (isRedo && reln->md_num_open_segs[forkNum] > 0)
        return;					/* created and opened already... */

    filename = relpath(reln->smgr_rnode, forkNum);

    char kvKey[MAXPGPATH];
    snprintf(kvKey, sizeof(kvKey), KV_FILE_PAGE_NUM_VERSION_LIST, filename);

#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[storage_kvcreate] create key = %s\n", kvKey)));
    printf("[storage_kvcreate] dbNum = %d, relNum = %d, forkNum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forkNum);
#endif

    unsigned long initList[] = {CurrentRedoLsn};

    char * result;
    MarshalUnsignedLongList(initList, 1, &result);
    KvPut(kvKey, result, sizeof(unsigned long)*2);


    snprintf(kvKey, sizeof(kvKey), KV_FILE_VERSION_PAGE_NUM, filename, CurrentRedoLsn);
    KvPutInt(kvKey, 0);

    pfree(filename);

    return;
}

void
storage_kvunlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("kvunlink start\n")));
    printf("[storage_kvunlink] dbNum = %d, relNum = %d, forkNum = %d\n", rnode.node.dbNode, rnode.node.relNode, forkNum);
#endif
    /* Now do the per-fork work */
    if (forkNum == InvalidForkNumber)
    {
        for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
            storage_kvunlinkfork(rnode, forkNum, isRedo);
    }
    else
        storage_kvunlinkfork(rnode, forkNum, isRedo);
}

// todo here didn't delete the page_number key. Don't know how to delete it
// One possible solution: first, set the page_number as 0. When doing checkpoint,
// set the page_number as -1.
static void
storage_kvunlinkfork(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_kvunlinkfork start\n")));
#endif

    char	   *path;
    int			ret;

    path = relpath(rnode, forkNum);

    /*
     * Delete or truncate the first segment.
     */
    /* First, forget any pending sync requests for the first segment */
    if (!RelFileNodeBackendIsTemp(rnode))
        storage_kvregister_forget_request(rnode, forkNum, 0 /* first seg */ );


    // 1. Get the relation page number version list
    // 2. Set newest version page number as 0

    char kvNumListKey[MAXPGPATH];

    snprintf(kvNumListKey, sizeof(kvNumListKey), KV_FILE_PAGE_NUM_VERSION_LIST, path);

#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[storage_kvunlinkfork] extend key = %s\n", kvNumListKey)));
#endif

    char * pageNumList = malloc(sizeof(unsigned long) * 128);
    char * backupPointer = pageNumList;

    int err = KvGet(kvNumListKey, &pageNumList);
    if (err != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvunlinkfork] KvGet failed")));
    }

    if (pageNumList == NULL) {
        free(backupPointer);
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvunlinkfork] KvGet has NULL result")));
    }

    AddLargestElem2OrderedKVList((unsigned long **)&pageNumList, CurrentRedoLsn);

    KvPut(kvNumListKey, pageNumList, (UnmarshalUnsignedLongListGetSize(pageNumList) +1));

    snprintf(kvNumListKey, sizeof(kvNumListKey), KV_FILE_VERSION_PAGE_NUM, path, CurrentRedoLsn);
    KvPutInt(kvNumListKey, 0);

//    // pageNum key be deleted right now
//    if (isRedo || forkNum != MAIN_FORKNUM || RelFileNodeBackendIsTemp(rnode))
//    {
//        KvDelete(kvNumKey);
//    }
//    else // pageNum key should be deleted in the next checkpoint
//    {
//        KvPutInt(kvNumKey, 0);
//        /* Register request to unlink first segment later */
//        //! Does it works??
//        storage_kvregister_unlink_segment(rnode, forkNum, 0 /* first seg */ );
//    }

    free(pageNumList);
    pfree(path);
}

//todo Here may need a lock
/*
 *  1. Get the Relation page number version list
 *  2. Find the right page_number version in list with CurrRedoLSN
 *  3. Insert a new version page_number version into page_number version list
 *  4. Create some new version pages (both page and page version list)
 */
void
storage_kvextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
         char *buffer, bool skipFsync)
{
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_kvextend start\n")));
    printf("[storage_kvextend] Start dbNum = %d, relNum = %d, forkNum = %d, blocknum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, blocknum);
#endif

    int err = 0;
    int currPageNum = storage_kvnblocks(reln, forknum);
    if (currPageNum < 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[kvextend] kvnblocks get minus result")));
    }
    printf("[extend] current page number = %d\n", currPageNum);
    // No need to extend
    if (currPageNum >= blocknum+1) {
        return;
    }

    // Update Relation Data: page number version list
    char *path;
    path = relpath(reln->smgr_rnode, forknum);

    char kvNumListKey[MAXPGPATH];

    snprintf(kvNumListKey, sizeof(kvNumListKey), KV_FILE_PAGE_NUM_VERSION_LIST, path);
    char * pageNumList = malloc(sizeof(unsigned long) * 128);
    char * backupPointer = pageNumList;

    err = KvGet(kvNumListKey, &pageNumList);
    if (err != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvextend] KvGet failed")));
    }
    if (pageNumList == NULL) {
        free(backupPointer);
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvextend] KvGet has NULL result")));
    }

#ifdef DEBUG_INFO
    printf("[kvExtend] pageNumList size = %d\n", UnmarshalUnsignedLongListGetSize(pageNumList));
    for (int i = 0; i < UnmarshalUnsignedLongListGetSize(pageNumList); ++i) {
        printf("%d => %lu\n", i, UnmarshalUnsignedLongListGetList(pageNumList)[i]);
    }
    printf("[kvextend] add ele %lu to list\n", CurrentRedoLsn);
#endif
    AddLargestElem2OrderedKVList((unsigned long **)&pageNumList, CurrentRedoLsn);
#ifdef DEBUG_INFO
    printf("[kvExtend] pageNumList size = %d\n", UnmarshalUnsignedLongListGetSize(pageNumList));
    for (int i = 0; i < UnmarshalUnsignedLongListGetSize(pageNumList); ++i) {
        printf("%d => %lu\n", i, UnmarshalUnsignedLongListGetList(pageNumList)[i]);
    }
#endif

    KvPut(kvNumListKey, pageNumList, sizeof(unsigned long)*(UnmarshalUnsignedLongListGetSize(pageNumList) +1));

    free(pageNumList);

    // Update relation page number version
    snprintf(kvNumListKey, sizeof(kvNumListKey), KV_FILE_VERSION_PAGE_NUM, path, CurrentRedoLsn);
    KvPutInt(kvNumListKey, blocknum+1);

#ifdef DEBUG_INFO
    printf("[kvExtend] Put page number , key = %s and number = %d\n", kvNumListKey, blocknum+1);
#endif


    // extend from currPageNum to blocknum-1 with zeroPage
    // extend blocknum with buffer
    //todo lock start
    char kvPageKey[MAXPGPATH];
    char kvPageVersionListKey[MAXPGPATH];
    char *zerobuf = palloc0(BLCKSZ);
    unsigned long newVersionList[] = {CurrentRedoLsn};
    char * versionList;
    MarshalUnsignedLongList(newVersionList, 1, &versionList);

    char * resultVersionList = malloc(sizeof(unsigned long) *128);
    for (int i = currPageNum; i < blocknum; i++) {
        // create a new version list
        snprintf(kvPageVersionListKey, sizeof(kvPageVersionListKey), KV_GET_PAGE_VERSION_LIST, path, i);
        // first, try to get the old intended extend page version list
        // maybe exist, because of deleting fork
        char *backupP = resultVersionList;
        KvGet(kvPageVersionListKey, &backupP);
        if(backupP != NULL) { // the list exists
            AddLargestElem2OrderedKVList((unsigned long**)&resultVersionList, CurrentRedoLsn);
            KvPut(kvPageVersionListKey, resultVersionList, sizeof(unsigned long) * (UnmarshalUnsignedLongListGetSize(resultVersionList)+1) );
        } else {
            KvPut(kvPageVersionListKey, versionList, sizeof(unsigned long)*2);
        }

        snprintf(kvPageKey, sizeof(kvPageKey), KV_GET_VERSION_PAGE, path, i, CurrentRedoLsn);
        int err = KvPut(kvPageKey, zerobuf, BLCKSZ);
        if (err != 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_WINDOWING_ERROR),
                            errmsg("[storage_kvextend] KvPut failed")));
        }
    }
    pfree(zerobuf);


    snprintf(kvPageVersionListKey, sizeof(kvPageVersionListKey), KV_GET_PAGE_VERSION_LIST, path, blocknum);
    char *backupP = resultVersionList;
    KvGet(kvPageVersionListKey, &backupP);
    if(backupP != NULL) { // the list exists
        AddLargestElem2OrderedKVList((unsigned long**)&resultVersionList, CurrentRedoLsn);
        KvPut(kvPageVersionListKey, resultVersionList, sizeof(unsigned long) * (UnmarshalUnsignedLongListGetSize(resultVersionList)+1) );
    } else {
        KvPut(kvPageVersionListKey, versionList, sizeof(unsigned long)*2);
    }

    snprintf(kvPageKey, sizeof(kvPageKey), KV_GET_VERSION_PAGE, path, blocknum, CurrentRedoLsn);
    err = KvPut(kvPageKey, buffer, BLCKSZ);
    if (err != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvextend] KvPut failed")));
    }


    free(resultVersionList);
    free(versionList);

    pfree(path);

//    printf("2Start KVNBLOCKS!!! \n\n\n");
//    storage_kvnblocks(reln, forknum);
//    printf("END KVNBLOCKS!!! \n\n\n");
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_kvextend end\n")));
    printf("[storage_kvextend] End dbNum = %d, relNum = %d, forkNum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum);
#endif
    // todo unlock
}

static void
storage_kvregister_forget_request(RelFileNodeBackend rnode, ForkNumber forknum,
                          BlockNumber segno)
{
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_kvregister_forget request start\n")));
#endif

    FileTag		tag;

    INIT_STORAGE_KV_FILETAG(tag, rnode.node, forknum, segno);

    RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true /* retryOnError */ );
}

static void
storage_kvregister_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum,
                          BlockNumber segno)
{
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("kvregister_unlink_segment start\n")));
#endif
    FileTag		tag;

    INIT_STORAGE_KV_FILETAG(tag, rnode.node, forknum, segno);

    /* Should never be used with temp relations */
    Assert(!RelFileNodeBackendIsTemp(rnode));

    RegisterSyncRequest(&tag, SYNC_UNLINK_REQUEST, true /* retryOnError */ );
}

// Currently, nothing will be done
bool
storage_kvprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
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
storage_kvread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
               char *buffer) {
    return storage_kvread_internal(reln, forknum, blocknum, buffer, -1);
}


void
storage_kvread_internal(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
       char *buffer, int lsn)
{
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_read start\n")));
    printf("[storage_read] Start dbNum = %d, relNum = %d, forkNum = %d, blocknum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, blocknum);
#endif
    if(lsn == -1) {
        lsn = CurrentRedoLsn;
    }


    char *path;
    int totalPageNum = 0;
    char kvPageKey[MAXPGPATH];
    char kvPageListKey[MAXPGPATH];
    int err = 0;


    TRACE_POSTGRESQL_SMGR_MD_READ_START(forknum, blocknum,
                                        reln->smgr_rnode.node.spcNode,
                                        reln->smgr_rnode.node.dbNode,
                                        reln->smgr_rnode.node.relNode,
                                        reln->smgr_rnode.backend);

    totalPageNum = storage_kvnblocks_internal(reln, forknum, lsn);
    if (totalPageNum < 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvread_internal] kvnblocks failed")));
        return;
    }

    if (totalPageNum < blocknum+1) {
        if (zero_damaged_pages || InRecovery)
            MemSet(buffer, 0, BLCKSZ);
        else
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("[storage_kvread_internal] read page corrupted.")));
        return;
    }


    path = relpath(reln->smgr_rnode, forknum);
    snprintf(kvPageListKey, sizeof(kvPageListKey), KV_GET_PAGE_VERSION_LIST, path, blocknum);
    printf("[storage_kvread] get page version list, key = %s\n", kvPageListKey);


    char *kvGetResult = malloc(sizeof(unsigned long) *128);
    char *backupP = kvGetResult;
    err = KvGet(kvPageListKey, &kvGetResult);
    if (err != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvread_internal] KvGet list failed, db=%d, rel=%d, fork=%d",
                               reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum)));
        return;
    }

    // This relation doesn't exist in kvStore
    if (kvGetResult == NULL) {
#ifdef DEBUG_INFO
        printf("[storage_kvread_internal] This relation doesn't exist\n");
#endif
        free(backupP);
        return;
    }


#ifdef DEBUG_INFO
    printf("[storage_kvread_internal] FILE PAGE NUMBER has %d VERSIONs\n", UnmarshalUnsignedLongListGetSize(kvGetResult));
    for(int i = 0; i < UnmarshalUnsignedLongListGetSize(kvGetResult); i++) {
        printf("[storage_kvread_internal] Version %d -> LSN %lu\n", UnmarshalUnsignedLongListGetList(kvGetResult)[i]);
    }
#endif

    int targetVersionIndex = FindLowerBound_UnsignedLong(UnmarshalUnsignedLongListGetList(kvGetResult),
                                                         UnmarshalUnsignedLongListGetSize(kvGetResult),
                                                         lsn);
    if (targetVersionIndex < 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvread_internal] No version in version list, db=%d, rel=%d, fork=%d",
                               reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum)));
        return;
    }

    unsigned long targetVersion = UnmarshalUnsignedLongListGetList(kvGetResult)[targetVersionIndex];

    free(kvGetResult);
    // Then we get the target version PageNum
    snprintf(kvPageKey, sizeof(kvPageKey), KV_GET_VERSION_PAGE, path, blocknum, targetVersion);
    pfree(path);
#ifdef DEBUG_INFO
    printf("[storage_kvread_internal] targetVersionIndex = %d, targetVersion = %lu\n", targetVersionIndex, targetVersion);
    printf("[storage_kvread_internal] Get version page, key = %s\n", kvPageKey);
#endif
    err = KvGet(kvPageKey, &buffer);
    if (err == -1) { // err==-1 means $kvNumKey doesn't exist in kv_store
        // It's impossible, that lsn index exists in version list, but doesn't really exist.
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvnblocks_internal] KvGet, can not find target version, db=%d, rel=%d, fork=%d",
                               reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum)));
        return;
    }

}

void
storage_kvwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
        char *buffer, bool skipFsync)
{
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_kvwrite start\n")));
    printf("[storage_kvwrite] Start dbNum = %d, relNum = %d, forkNum = %d, blocknum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, blocknum);
#endif

    char *path;
    int totalPageNum = 0;

    int err = 0;

    TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum,
                                         reln->smgr_rnode.node.spcNode,
                                         reln->smgr_rnode.node.dbNode,
                                         reln->smgr_rnode.node.relNode,
                                         reln->smgr_rnode.backend);

    path = relpath(reln->smgr_rnode, forknum);
    totalPageNum = storage_kvnblocks(reln, forknum);
    if (totalPageNum < 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvwrite] kvnblocks failed")));
        return;
    }

    if (totalPageNum < blocknum+1) {
        // if inRecovery == true, then extend page
        if (InRecovery) {
#ifdef DEBUG_INFO
            printf("[storage_kvwrite] write unexists page, call kvextend\n");
#endif
            char *zerobuf = palloc0(BLCKSZ);
            storage_kvextend(reln, forknum, blocknum, zerobuf, skipFsync);
            pfree(zerobuf);
        } else { // report err
            ereport(ERROR,
                    (errcode_for_file_access(),
                            errmsg("[storage_kvwrite] write to a non-exist page")));
            return;
        }
    }

    // Get Page Version List and insert a elem
    // Put a new version page
    char * versionList = malloc (sizeof(unsigned long) * 128);
    char * tempP = versionList;
    char kvPageVersionListKey[MAXPGPATH];
    char kvPageKey[MAXPGPATH];
    snprintf(kvPageVersionListKey, sizeof(kvPageVersionListKey), KV_GET_PAGE_VERSION_LIST, path, blocknum);
#ifdef DEBUG_INFO
    printf("[storage_kvwrite] Get PageVersionList, key = %s\n", kvPageVersionListKey);
#endif
    err = KvGet(kvPageVersionListKey, &versionList);
    if (err != 0 || versionList == NULL) {
        free(tempP);
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvwrite] kvGet failed")));
    }
    AddLargestElem2OrderedKVList((unsigned long **)&versionList, CurrentRedoLsn);
    KvPut(kvPageVersionListKey, versionList,
          sizeof(unsigned long) * (UnmarshalUnsignedLongListGetSize(versionList)+1));

    snprintf(kvPageKey, sizeof(kvPageKey), KV_GET_VERSION_PAGE, path, blocknum, CurrentRedoLsn);
#ifdef DEBUG_INFO
    printf("[storage_kvwrite] Add elem to PageVersionList, key = %s\n", kvPageVersionListKey);
    printf("[storage_kvwrite] Put a page version, key = %s\n", kvPageKey);
#endif

    if ( KvPut(kvPageKey, buffer, BLCKSZ) ) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvwrite] KvPut failed")));
    }
    pfree(path);
    free(versionList);

#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_kvwrite END\n")));
#endif
}

/*
 * mdwriteback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
void
storage_kvwriteback(SMgrRelation reln, ForkNumber forknum,
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
storage_kvtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_kvtruncate start\n")));
    printf("[storage_kvtruncate] Start dbNum = %d, relNum = %d, forkNum = %d, blocknum = %d\n", reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, nblocks);
#endif
    char *path;
    int totalPageNum = 0;
    int err = 0;


    totalPageNum = storage_kvnblocks(reln, forknum);
    if (totalPageNum < 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvtruncate] kvnblocks failed")));
        return;
    }

    if (nblocks > totalPageNum) {
        /* Bogus request ... but no complaint if InRecovery */
        if (InRecovery)
            return;
        ereport(ERROR,
                (errmsg("could not truncate file \"%s\" to %u blocks: it's only %u blocks now",
                        relpath(reln->smgr_rnode, forknum),
                        nblocks, totalPageNum)));
    }

    char * versionList = malloc (sizeof(unsigned long) * 128);
    char * tempP = versionList;
    char kvRelPageNumVersionListKey[MAXPGPATH];
    char kvRelPageNumKey[MAXPGPATH];
    snprintf(kvRelPageNumVersionListKey, sizeof(kvRelPageNumVersionListKey), KV_FILE_PAGE_NUM_VERSION_LIST, path);
    err = KvGet(kvRelPageNumVersionListKey, &versionList);
    if (err != 0 || versionList == NULL) {
        free(tempP);
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvtruncate] kvGet failed")));
    }

    AddLargestElem2OrderedKVList((unsigned long **)&versionList, CurrentRedoLsn);
    KvPut(kvRelPageNumVersionListKey, versionList,
          sizeof(unsigned long) * (UnmarshalUnsignedLongListGetSize(versionList)+1));

    snprintf(kvRelPageNumKey, sizeof(kvRelPageNumKey), KV_FILE_VERSION_PAGE_NUM, path, CurrentRedoLsn);
    if ( KvPutInt(kvRelPageNumKey, nblocks) ) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvtruncate] KvPut failed")));
    }
    pfree(path);
    pfree(versionList);
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
storage_kvimmedsync(SMgrRelation reln, ForkNumber forknum)
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
storage_kvsyncfiletag(const FileTag *ftag, char *path)
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
/*
 * Find the file page number list, if doesn't exist, just return
 * Add a new version to list. And set the page_number version as -1
 */
int
storage_kvunlinkfiletag(const FileTag *ftag, char *return_path)
{
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_kvunlinkfiletag start\n")));
#endif
    char	   *path;

    /* Compute the path. */
    path = relpathperm(ftag->rnode, MAIN_FORKNUM);
    strlcpy(path, return_path, MAXPGPATH);

    int totalPageNum = 0;
    int err = 0;


    char * versionList = malloc (sizeof(unsigned long) * 128);
    char * tempP = versionList;
    char kvRelPageNumVersionListKey[MAXPGPATH];
    char kvRelPageNumKey[MAXPGPATH];
    snprintf(kvRelPageNumVersionListKey, sizeof(kvRelPageNumVersionListKey), KV_FILE_PAGE_NUM_VERSION_LIST, path);
    err = KvGet(kvRelPageNumVersionListKey, &versionList);
    if (err != 0 || versionList == NULL) {
        free(tempP);
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvunlinkfiletag] kvGet failed")));
    }

    AddLargestElem2OrderedKVList((unsigned long **)&versionList, CurrentRedoLsn);
    KvPut(kvRelPageNumVersionListKey, versionList,
          sizeof(unsigned long) * (UnmarshalUnsignedLongListGetSize(versionList)+1));

    snprintf(kvRelPageNumKey, sizeof(kvRelPageNumKey), KV_FILE_VERSION_PAGE_NUM, path, CurrentRedoLsn);
    if ( KvPutInt(kvRelPageNumKey, -1) ) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_kvunlinkfiletag] KvPut failed")));
    }
    pfree(path);
    pfree(versionList);
    return 0;
}

/*
 * Check if a given candidate request matches a given tag, when processing
 * a SYNC_FILTER_REQUEST request.  This will be called for all pending
 * requests to find out whether to forget them.
 */
bool
storage_kvfiletagmatches(const FileTag *ftag, const FileTag *candidate)
{
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_kvfiletagmatches start\n")));
#endif
    /*
     * For now we only use filter requests as a way to drop all scheduled
     * callbacks relating to a given database, when dropping the database.
     * We'll return true for all candidates that have the same database OID as
     * the ftag from the SYNC_FILTER_REQUEST request, so they're forgotten.
     */
    return ftag->rnode.dbNode == candidate->rnode.dbNode;
}

void
storage_savexlog(unsigned long lsn, XLogRecord *record) {

    char pageXlogKey[MAXPGPATH];
    snprintf(pageXlogKey, sizeof(pageXlogKey), KV_XLOG_LIST, lsn);

    KvPut(pageXlogKey, (char *)record, record->xl_tot_len);

    return;
}

void
storage_deletexlog(unsigned long lsn) {
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_deletexlog start\n")));
#endif
    char pageXlogKey[MAXPGPATH];
    snprintf(pageXlogKey, sizeof(pageXlogKey), KV_XLOG_LIST, lsn);

    KvDelete(pageXlogKey);

    return;
}


void
storage_getxlog(unsigned long lsn, char **buffer) {
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_getxlog start\n")));
#endif
    char pageXlogKey[MAXPGPATH];
    snprintf(pageXlogKey, sizeof(pageXlogKey), KV_XLOG_LIST, lsn);
    int err = KvGet(pageXlogKey, buffer);
    if (err != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_getxlog] KvGet failed")));
    }
    return;
}

// Save a xlog for a page that wait redo
void
storage_async_pagexlog(RelFileNode rnode, ForkNumber forknum, BlockNumber blocknum, unsigned long lsn) {
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_async_pagexlog start\n")));
#endif
    // first, check whether this list exist?
    char asyncXlogKey_page[MAXPGPATH];
    char * path = relpathperm(rnode, forknum);
    snprintf(asyncXlogKey_page, sizeof(asyncXlogKey_page), KV_XLOG_PAGE_ASYNC_REDO_LIST, path, blocknum);
    pfree(path);

    char * result = malloc(sizeof(unsigned long) * 64);
    char * backupPointer = result;
    int err = KvGet(path, &result);
    // if the list doesn't exist, create new one, and add this to the list
    if (result == NULL) {
        // why free: in the later Marshal function, the result will be assigned a new space
        free(backupPointer);
        unsigned long tempList[1] = {lsn};
        MarshalUnsignedLongList(tempList, lsn, &result);
    }
    // if the lsn isn's bigger than largest element of list, ignore it
    AddLargestElem2OrderedKVList((unsigned long **)&result, lsn);

    err = KvPut(asyncXlogKey_page, result,
          sizeof(unsigned long) * (UnmarshalUnsignedLongListGetSize(result)+1));

    if(err != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_async_pagexlog] KvPut failed")));
    }

    free(result);
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_async_pagexlog end\n")));
#endif
}

void
storage_async_relxlog(RelFileNode rnode, ForkNumber forknum, unsigned long lsn) {
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_async_relxlog start\n")));
#endif
    // first, check whether this list exist?
    char asyncXlogKey_rel[MAXPGPATH];
    char * path = relpathperm(rnode, forknum);
    snprintf(asyncXlogKey_rel, sizeof(asyncXlogKey_rel), KV_XLOG_REL_ASYNC_REDO_LIST, path);
    pfree(path);

    char * result = malloc(sizeof(unsigned long) * 128);
    char * backupPointer = result;
    int err = KvGet(path, &result);
    // if the list doesn't exist, create new one, and add this to the list
    if (result == NULL) {
        // why free: in the later Marshal function, the result will be assigned a new space
        free(backupPointer);
        unsigned long tempList[1] = {lsn};
        MarshalUnsignedLongList(tempList, lsn, &result);
    }
    // if the lsn isn's bigger than largest element of list, ignore it
    AddLargestElem2OrderedKVList((unsigned long **)&result, lsn);

    err = KvPut(asyncXlogKey_rel, result,
                sizeof(unsigned long) * (UnmarshalUnsignedLongListGetSize(result)+1));

    if(err != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_async_relxlog] KvPut failed")));
    }

    free(result);
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_async_relxlog end\n")));
#endif
}

// todo I think this funcion is wrong, kvnblocks should always shows the pages already exists
// todo otherwise, when kvread() call nblocks(), it will wrongly think this page already exists
// When received xlog, this xlog may create a new page in a relation
// This function aims to add a new page_number_version
void
UpdateRelPageNumVersion(char * filePath, BlockNumber blocknum, unsigned long lsn) {
//    char keyPageNumList[MAXPGPATH];
//    char * resultList = malloc(sizeof(unsigned long)*128);
//    char * backupPointer = resultList;
//    snprintf(keyPageNumList, sizeof(keyPageNumList), KV_XLOG_PAGE_ASYNC_REDO_LIST, filePath, blocknum);
//    int err = KvGet(keyPageNumList, &resultList);
//    if (err != 0) {
//        ereport(ERROR,
//                (errcode(ERRCODE_WINDOWING_ERROR),
//                        errmsg("[UpdateRelPageNumVersion] KvPut failed")));
//    }
//
//    // doesn't exist any page
//    if(resultList == NULL) {
//
//    }
}

// 1. get a page's xlog list
// 2. delete all the list elem than smaller than / euqal to $lsn
// 3. put the list back
void
storage_delete_from_page_xlog_list(RelFileNode rnode, ForkNumber forknum, BlockNumber blocknum, unsigned long lsn) {
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_delete_from_page_xlog_list start\n")));
    printf("[storage_delete_from_page_xlog_list] Start dbNum = %d, relNum = %d, forkNum = %d, blocknum = %d\n", rnode.dbNode, rnode.relNode, forknum, blocknum);
#endif
    char asyncXlogKey[MAXPGPATH];
    char * path = relpathperm(rnode, forknum);
    snprintf(asyncXlogKey, sizeof(asyncXlogKey), KV_XLOG_PAGE_ASYNC_REDO_LIST, path, blocknum);
    pfree(path);
    char * result = malloc(sizeof(unsigned long) * 64);
    char * backupPointer = result;
    int err = KvGet(path, &result);
    // if the list doesn't exist, ignore the request
    if (result == NULL) {
        free(backupPointer);
        return;
    }

    //todo  check the logic here
    unsigned long listSize = UnmarshalUnsignedLongListGetSize(result);
    for (int i = 1; i<=listSize; i++) {
        if( ((unsigned long *) result) [i] > lsn) {
            // set the listSize
            ((unsigned long *) result) [i-1] = listSize-(i-1);

            KvPut(asyncXlogKey,  (char*)&(((unsigned long *) result)[i-1]),  (listSize-(i-1)+1)*sizeof(unsigned long) );
            return;
        }
    }

    free(result);
    // if reach here, all elem has been deleted. So, just delete this kv-key
    KvDelete(asyncXlogKey);

    return;
}



void
storage_delete_from_rel_xlog_list(RelFileNode rnode, ForkNumber forknum, unsigned long lsn) {
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_delete_from_rel_xlog_list start\n")));
    printf("[storage_delete_from_rel_xlog_list] Start dbNum = %d, relNum = %d, forkNum = %d\n", rnode.dbNode, rnode.relNode, forknum);
#endif
    char asyncXlogKey[MAXPGPATH];
    char * path = relpathperm(rnode, forknum);
    snprintf(asyncXlogKey, sizeof(asyncXlogKey), KV_XLOG_REL_ASYNC_REDO_LIST, path);
    pfree(path);
    char * result = malloc(sizeof(unsigned long) * 64);
    char * backupPointer = result;
    int err = KvGet(path, &result);
    // if the list doesn't exist, ignore the request
    if (result == NULL) {
        free(backupPointer);
        return;
    }

    //todo  check the logic here
    unsigned long listSize = UnmarshalUnsignedLongListGetSize(result);
    for (int i = 1; i<=listSize; i++) {
        if( ((unsigned long *) result) [i] > lsn) {
            // set the listSize
            ((unsigned long *) result) [i-1] = listSize-(i-1);

            KvPut(asyncXlogKey,  (char*)&(((unsigned long *) result)[i-1]),  (listSize-(i-1)+1)*sizeof(unsigned long) );
            return;
        }
    }

    // if reach here, all elem has been deleted. So, just delete this kv-key
    KvDelete(asyncXlogKey);
    free(result);

    return;
}

// Get lsn list for a page that didn't do xlog_redo
// Caller needs to free "targetResult" space
// The result value $targetResult is KvList, the first element is _size
void
storage_get_page_asynxlog_list(RelFileNode rnode, ForkNumber forknum, BlockNumber blocknum, unsigned long **targetResult) {
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_get_page_asynxlog_list start\n")));
    printf("[storage_get_page_asynxlog_list] Start dbNum = %d, relNum = %d, forkNum = %d, blocknum = %d\n", rnode.dbNode, rnode.relNode, forknum, blocknum);
#endif
    // KV_XLOG_PAGE_ASYNC_REDO_LIST
    char asyncListKey[MAXPGPATH];
    char * path = relpathperm(rnode, forknum);
    snprintf(asyncListKey, sizeof(asyncListKey), KV_XLOG_PAGE_ASYNC_REDO_LIST, path, blocknum);
    pfree(path);

    int err = 0;
    *targetResult = malloc(sizeof(unsigned long) * 64);
    unsigned long *result = *targetResult;
    err = KvGet(asyncListKey, (char**) targetResult);
    if (err != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_get_asynxlog_list] KvGet failed")));
    }
    // if there is no such list, return null
    if (*targetResult == NULL) {
        free(result);
    }

    return;
}

void
storage_get_rel_asynxlog_list(RelFileNode rnode, ForkNumber forknum, unsigned long **targetResult) {
    // KV_XLOG_PAGE_ASYNC_REDO_LIST
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_get_rel_asynxlog_list start\n")));
#endif

    char asyncListKey[MAXPGPATH];

    char * path = relpathperm(rnode, forknum);
    snprintf(asyncListKey, sizeof(asyncListKey), KV_XLOG_REL_ASYNC_REDO_LIST, path);
    pfree(path);

    int err = 0;

    *targetResult = malloc(sizeof(unsigned long) * 64);
    unsigned long *result = *targetResult;

    err = KvGet(asyncListKey, (char**) targetResult);

    if (err != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("[storage_get_rel_asynxlog_list] KvGet failed")));
    }
    // if there is no such list, return null
    if (*targetResult == NULL) {
        free(result);
    }
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("storage_get_rel_asynxlog_list end\n")));
#endif
    return;
}