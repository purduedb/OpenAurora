/*-------------------------------------------------------------------------
 *
 * md.c
 *	  This code manages relations that reside on magnetic disk.
 *
 * Or at least, that was what the Berkeley folk had in mind when they named
 * this file.  In reality, what this code provides is an interface from
 * the smgr API to Unix-like filesystem APIs, so it will work with any type
 * of device for which the operating system provides filesystem support.
 * It doesn't matter whether the bits are on spinning rust or some other
 * storage technology.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/md.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>

#include "access/xlog.h"
#include "access/xlogutils.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/rpcmd.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "storage/rpcclient.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "storage/rel_cache.h"

#define ENABLE_REL_SIZE_CACHE
#define ENABLE_REL_SIZE_CACHE2
/*
 *	The magnetic disk storage manager keeps track of open file
 *	descriptors in its own descriptor pool.  This is done to make it
 *	easier to support relations that are larger than the operating
 *	system's file size limit (often 2GBytes).  In order to do that,
 *	we break relations up into "segment" files that are each shorter than
 *	the OS file size limit.  The segment size is set by the RELSEG_SIZE
 *	configuration constant in pg_config.h.
 *
 *	On disk, a relation must consist of consecutively numbered segment
 *	files in the pattern
 *		-- Zero or more full segments of exactly RELSEG_SIZE blocks each
 *		-- Exactly one partial segment of size 0 <= size < RELSEG_SIZE blocks
 *		-- Optionally, any number of inactive segments of size 0 blocks.
 *	The full and partial segments are collectively the "active" segments.
 *	Inactive segments are those that once contained data but are currently
 *	not needed because of an mdtruncate() operation.  The reason for leaving
 *	them present at size zero, rather than unlinking them, is that other
 *	backends and/or the checkpointer might be holding open file references to
 *	such segments.  If the relation expands again after mdtruncate(), such
 *	that a deactivated segment becomes active again, it is important that
 *	such file references still be valid --- else data might get written
 *	out to an unlinked old copy of a segment file that will eventually
 *	disappear.
 *
 *	File descriptors are stored in the per-fork md_seg_fds arrays inside
 *	SMgrRelation. The length of these arrays is stored in md_num_open_segs.
 *	Note that a fork's md_num_open_segs having a specific value does not
 *	necessarily mean the relation doesn't have additional segments; we may
 *	just not have opened the next segment yet.  (We could not have "all
 *	segments are in the array" as an invariant anyway, since another backend
 *	could extend the relation while we aren't looking.)  We do not have
 *	entries for inactive segments, however; as soon as we find a partial
 *	segment, we assume that any subsequent segments are inactive.
 *
 *	The entire MdfdVec array is palloc'd in the MdCxt memory context.
 */

typedef struct _MdfdVec
{
	File		mdfd_vfd;		/* fd number in fd.c's pool */
	BlockNumber mdfd_segno;		/* segment number, from 0 */
} MdfdVec;

static MemoryContext RpcMdCxt;		/* context for all MdfdVec objects */


/*** behavior for mdopen & _mdfd_getseg ***/
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
static void rpcmdunlinkfork(RelFileNodeBackend rnode, ForkNumber forkNum,
						 bool isRedo);
static MdfdVec *rpcmdopenfork(SMgrRelation reln, ForkNumber forknum, int behavior);
static void rpc_register_dirty_segment(SMgrRelation reln, ForkNumber forknum,
								   MdfdVec *seg);
static void rpc_register_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum,
									BlockNumber segno);
static void rpcregister_forget_request(RelFileNodeBackend rnode, ForkNumber forknum,
									BlockNumber segno);
static void _rpc_fdvec_resize(SMgrRelation reln,
						  ForkNumber forknum,
						  int nseg);
static char *_rpc_mdfd_segpath(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber segno);
static MdfdVec *_rpc_mdfd_openseg(SMgrRelation reln, ForkNumber forkno,
							  BlockNumber segno, int oflags);
static MdfdVec *_rpc_mdfd_getseg(SMgrRelation reln, ForkNumber forkno,
							 BlockNumber blkno, bool skipFsync, int behavior);
static BlockNumber _rpc_mdnblocks(SMgrRelation reln, ForkNumber forknum,
							  MdfdVec *seg);


#define CHECK_EQUAL(a, b)  \
do {                         \
    if(a != b) printf("%s %d, %lu, %lu doesn't equal\n", __func__, __LINE__, a, b); \
} while(0);

#define CHECK_REL_EQUAL(a, b)  \
do {                         \
    if(a.spcNode != b.spcNode) printf("%s %d, %lu, %lu spc doesn't equal\n", __func__, __LINE__, a.spcNode, b.spcNode); \
    if(a.dbNode != b.dbNode) printf("%s %d, %lu, %lu db doesn't equal\n", __func__, __LINE__, a.dbNode, b.dbNode); \
    if(a.relNode != b.relNode) printf("%s %d, %lu, %lu rel doesn't equal\n", __func__, __LINE__, a.relNode, b.relNode); \
} while(0);

#define CHECK_PARA_CONSIST


static void TransRelNode2RelKey(SMgrRelation relSmgr, RelKey *relKey, ForkNumber forkNumber) {
    relKey->SpcId = relSmgr->smgr_rnode.node.spcNode;
    relKey->DbId = relSmgr->smgr_rnode.node.dbNode;
    relKey->RelId = relSmgr->smgr_rnode.node.relNode;

    relKey->forkNum = forkNumber;
    return;
}
/*
 *	mdinit() -- Initialize private state for magnetic disk storage manager.
 */
void
rpcmdinit(void)
{
}

/*
 *	mdexists() -- Does the physical file exist?
 *
 * Note: this will return true for lingering files, with pending deletions
 */
bool
rpcmdexists(SMgrRelation reln, ForkNumber forkNum)
{

#ifdef ENABLE_REL_SIZE_CACHE2
    uint32_t result=-1;
    RelKey relKey;

    TransRelNode2RelKey(reln, &relKey, forkNum);


    //! TODO After add these locks, sysbench will crash
    RelSizeSharedLock(relKey);
    if(GetRelSizeCache(relKey, &result)) {
        // printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
        RelSizeReleaseLock(relKey);
        return result >= 0;
    }
    RelSizeReleaseLock(relKey);
#endif

    // If not cached locally, get from remote
    uint32_t rpcResult = RpcMdExists(reln, forkNum);
#ifdef ENABLE_REL_SIZE_CACHE
    if(result != -1 && (result>=0) != rpcResult) {
        printf("%s %d, %lu_%lu_%lu_%d cache=%u, rpc=%u\n", __func__ , __LINE__, relKey.SpcId, relKey.DbId, relKey.RelId, relKey.forkNum, (result>=0), rpcResult);
        fflush(stdout);
    }
#endif

    return rpcResult;
}

void
rpcmdcreate(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{

#ifdef ENABLE_REL_SIZE_CACHE2
    uint32_t result;
    RelKey relKey;

    TransRelNode2RelKey(reln, &relKey, forkNum);

    RelSizeExclusiveLock(relKey);
    if(GetRelSizeCache(relKey, &result)) {
        if(result < 0) { // If it was deleted?
//            printf("%s %d, %lu_%lu_%lu_%d = %u\n", __func__ , __LINE__, relKey.SpcId, relKey.DbId, relKey.RelId, relKey.forkNum, 0);
//            fflush(stdout);
            InsertRelSizeCache(relKey, 0);
        }
    } else { // Not exist in buffer, init blkNum as 0
//        printf("%s %d, %lu_%lu_%lu_%d = %u\n", __func__ , __LINE__, relKey.SpcId, relKey.DbId, relKey.RelId, relKey.forkNum, 0);
//        fflush(stdout);
        InsertRelSizeCache(relKey, 0);
    }
    RelSizeReleaseLock(relKey);
#endif

    // Secondary compute node won't alter the shared relation
    if(!InRecovery)
        RpcMdCreate(reln, forkNum, isRedo);
}

void
rpcmdunlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
}

static void
rpcmdunlinkfork(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
}

void
rpcmdextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer, bool skipFsync)
{

#ifdef ENABLE_REL_SIZE_CACHE
    uint32_t result;
    RelKey relKey;

    TransRelNode2RelKey(reln, &relKey, forknum);

//    printf("%s %d\n", __func__ , __LINE__);
//    fflush(stdout);
    RelSizeExclusiveLock(relKey);
    if(GetRelSizeCache(relKey, &result)) { //After extend, blkNum increased
        if(blocknum+1 > result) {
//            printf("%s %d, %lu_%lu_%lu_%d = %u\n", __func__ , __LINE__, relKey.SpcId, relKey.DbId, relKey.RelId, relKey.forkNum, blocknum+1);
//            fflush(stdout);
            InsertRelSizeCache(relKey, blocknum+1);
        }
    } else { // Not exist
//        printf("%s %d, %lu_%lu_%lu_%d = %u\n", __func__ , __LINE__, relKey.SpcId, relKey.DbId, relKey.RelId, relKey.forkNum, blocknum+1);
//        fflush(stdout);
        InsertRelSizeCache(relKey, blocknum+1);
    }
    RelSizeReleaseLock(relKey);
#endif

    // Secondary compute node won't alter the shared relation
    if(!InRecovery)
        RpcMdExtend(reln, forknum, blocknum, buffer, skipFsync);
}

static MdfdVec *
rpcmdopenfork(SMgrRelation reln, ForkNumber forknum, int behavior)
{
}

void
rpcmdopen(SMgrRelation reln)
{
}

void
rpcmdclose(SMgrRelation reln, ForkNumber forknum)
{
}

bool
rpcmdprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
    return true;
}

void
rpcmdwriteback(SMgrRelation reln, ForkNumber forknum,
			BlockNumber blocknum, BlockNumber nblocks)
{
}

void
rpcmdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
	   char *buffer)
{
    RpcMdRead(buffer, reln, forknum, blocknum);
}

void
rpcmdwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		char *buffer, bool skipFsync)
{
}

BlockNumber
rpcmdnblocks(SMgrRelation reln, ForkNumber forknum)
{
#ifdef ENABLE_REL_SIZE_CACHE
    uint32_t result = -1;
    RelKey relKey;

    TransRelNode2RelKey(reln, &relKey, forknum);
//    printf("%s %d\n", __func__ , __LINE__);
//    fflush(stdout);
    RelSizeSharedLock(relKey);
    if(GetRelSizeCache(relKey, &result)) { //After extend, blkNum increased
        RelSizeReleaseLock(relKey);
        // printf("%s %d\n", __func__ , __LINE__);
        // fflush(stdout);
        return result;
    }
    RelSizeReleaseLock(relKey);
#endif

    uint32_t blckNum = RpcMdNblocks(reln, forknum);


#ifdef ENABLE_REL_SIZE_CACHE
    if(result != -1 && result != blckNum) {
        printf("%s %d, %lu_%lu_%lu_%d cache=%u, rpc=%u\n", __func__ , __LINE__, relKey.SpcId, relKey.DbId, relKey.RelId, relKey.forkNum, result, blckNum);
        fflush(stdout);
    }
//    printf("%s %d, %lu_%lu_%lu_%d = %u\n", __func__ , __LINE__, relKey.SpcId, relKey.DbId, relKey.RelId, relKey.forkNum, blckNum);
//    fflush(stdout);
    RelSizeExclusiveLock(relKey);

    // Before we create a new relSize entry, check whether other process has already created
    bool found = GetRelSizeCache(relKey, &result);
    if(!found)
        InsertRelSizeCache(relKey, blckNum);
    RelSizeReleaseLock(relKey);
#endif
    return blckNum;
}

void
rpcmdtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
#ifdef ENABLE_REL_SIZE_CACHE
    RelKey relKey;

    TransRelNode2RelKey(reln, &relKey, forknum);
//    printf("%s %d, %lu_%lu_%lu_%d = %u\n", __func__ , __LINE__, relKey.SpcId, relKey.DbId, relKey.RelId, relKey.forkNum, nblocks);
//    fflush(stdout);

    RelSizeExclusiveLock(relKey);
    InsertRelSizeCache(relKey, nblocks);
    RelSizeReleaseLock(relKey);
#endif

    // Secondary compute node won't alter the shared relation
    if(!InRecovery)
        return RpcMdTruncate(reln, forknum, nblocks);
}

void
rpcmdimmedsync(SMgrRelation reln, ForkNumber forknum)
{
}

static void
rpc_register_dirty_segment(SMgrRelation reln, ForkNumber forknum, MdfdVec *seg)
{
}

static void
rpc_register_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum,
						BlockNumber segno)
{
}

static void
rpc_register_forget_request(RelFileNodeBackend rnode, ForkNumber forknum,
						BlockNumber segno)
{
}

void
rpcForgetDatabaseSyncRequests(Oid dbid)
{
}

void
rpcDropRelationFiles(RelFileNode *delrels, int ndelrels, bool isRedo)
{
}


static void
_rpc_fdvec_resize(SMgrRelation reln,
			  ForkNumber forknum,
			  int nseg)
{
}

static char *
_rpc_mdfd_segpath(SMgrRelation reln, ForkNumber forknum, BlockNumber segno)
{
}

static MdfdVec *
_rpc_mdfd_openseg(SMgrRelation reln, ForkNumber forknum, BlockNumber segno,
			  int oflags)
{
}

static MdfdVec *
_rpc_mdfd_getseg(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno,
			 bool skipFsync, int behavior)
{
}

static BlockNumber
_rpc_mdnblocks(SMgrRelation reln, ForkNumber forknum, MdfdVec *seg)
{
}

int
rpcmdsyncfiletag(const FileTag *ftag, char *path)
{
	return 0;
}

int
rpcmdunlinkfiletag(const FileTag *ftag, char *path)
{
	return 0;
}

bool
rpcmdfiletagmatches(const FileTag *ftag, const FileTag *candidate)
{
	/*
	 * For now we only use filter requests as a way to drop all scheduled
	 * callbacks relating to a given database, when dropping the database.
	 * We'll return true for all candidates that have the same database OID as
	 * the ftag from the SYNC_FILTER_REQUEST request, so they're forgotten.
	 */
	return ftag->rnode.dbNode == candidate->rnode.dbNode;
}
