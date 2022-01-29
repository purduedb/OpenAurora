/*-------------------------------------------------------------------------
 *
 * rpcclient.c
 *	  This code manages gRPC interfaces that read and write data pages to storage nodes.
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/rpc/rpcclient.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>

#include "access/xlog.h"
#include "access/xlogutils.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/md.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "storage/rpcclient.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/elog.h"

#include <iostream>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include "DataPageAccess.h"

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

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace tutorial;

std::shared_ptr<TTransport> rpcsocket(new TSocket("localhost", 9090));
std::shared_ptr<TTransport> rpctransport(new TBufferedTransport(rpcsocket));
std::shared_ptr<TProtocol> rpcprotocol(new TBinaryProtocol(rpctransport));
DataPageAccessClient client(rpcprotocol);

/*
 *	rpcinit() -- Initialize private state for magnetic disk storage manager.
 */
void
rpcinit(void)
{
	rpctransport->open();
}

void
rpcshutdown(void)
{
	rpctransport->close();
}

/*
 *  rpcopen() -- Initialize newly-opened relation.
 */
void
rpcopen(SMgrRelation reln)
{
    /* mark it not open */
	for (int forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		reln->md_num_open_segs[forknum] = 0;
}

/*
 *	rpcclose() -- Close the specified relation, if it isn't closed already.
 */
void
rpcclose(SMgrRelation reln, ForkNumber forknum)
{
    int			nopensegs = reln->md_num_open_segs[forknum];

	/* No work if already closed */
	if (nopensegs == 0)
		return;

	/* close segments starting from the end */
	while (nopensegs > 0)
	{
		MdfdVec    *v = &reln->md_seg_fds[forknum][nopensegs - 1];

        /*TODO 
        rpc interface that close the file with a virtual file despcriptor that allocated from storage node*/
		client.RpcFileClose(v->mdfd_vfd);
		_fdvec_resize(reln, forknum, nopensegs - 1);
		nopensegs--;
	}
}

/*
 *	rpccreate() -- Create a new relation.
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
void
rpccreate(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
    File		fd;
	MdfdVec    *mdfd;
	_Oid spcnode = reln->smgr_rnode.node.spcNode;
	_Oid dbnode = reln->smgr_rnode.node.dbNode;
	_Oid relnode = reln->smgr_rnode.node.relNode;
	_RelFileNode _node;

	_node.spcNode = spcnode;
	_node.dbNode = dbnode;
	_node.relNode = relnode;

    if (isRedo && reln->md_num_open_segs[forkNum] > 0)
		return;					/* created and opened already... but in compute node isRedo should be false*/

	Assert(reln->md_num_open_segs[forkNum] == 0);

    /*TODO rpc interface create a file in storage nodes with given RelFileNode and ForkNumber*/

    fd = client.RpcFileCreate(_node, static_cast<_ForkNumber::type>(forkNum));

    _fdvec_resize(reln, forkNum, 1);
	mdfd = &reln->md_seg_fds[forkNum][0];
	mdfd->mdfd_vfd = fd;
	mdfd->mdfd_segno = 0;
}

/*
 *	rpcexists() -- Does the physical file exist?
 *
 * Note: this will return true for lingering files, with pending deletions
 */
bool
rpcexists(SMgrRelation reln, ForkNumber forkNum)
{}

/*
 *	rpcunlink() -- Unlink a relation.
 *
 * Note that we're passed a RelFileNodeBackend --- by the time this is called,
 * there won't be an SMgrRelation hashtable entry anymore.
 *
 * forkNum can be a fork number to delete a specific fork, or InvalidForkNumber
 * to delete all forks.
 *
 * For regular relations, we don't unlink the first segment file of the rel,
 * but just truncate it to zero length, and record a request to unlink it after
 * the next checkpoint.  Additional segments can be unlinked immediately,
 * however.  Leaving the empty file in place prevents that relfilenode
 * number from being reused.  The scenario this protects us from is:
 * 1. We delete a relation (and commit, and actually remove its file).
 * 2. We create a new relation, which by chance gets the same relfilenode as
 *	  the just-deleted one (OIDs must've wrapped around for that to happen).
 * 3. We crash before another checkpoint occurs.
 * During replay, we would delete the file and then recreate it, which is fine
 * if the contents of the file were repopulated by subsequent WAL entries.
 * But if we didn't WAL-log insertions, but instead relied on fsyncing the
 * file after populating it (as we do at wal_level=minimal), the contents of
 * the file would be lost forever.  By leaving the empty file until after the
 * next checkpoint, we prevent reassignment of the relfilenode number until
 * it's safe, because relfilenode assignment skips over any existing file.
 *
 * We do not need to go through this dance for temp relations, though, because
 * we never make WAL entries for temp rels, and so a temp rel poses no threat
 * to the health of a regular rel that has taken over its relfilenode number.
 * The fact that temp rels and regular rels have different file naming
 * patterns provides additional safety.
 *
 * All the above applies only to the relation's main fork; other forks can
 * just be removed immediately, since they are not needed to prevent the
 * relfilenode number from being recycled.  Also, we do not carefully
 * track whether other forks have been created or not, but just attempt to
 * unlink them unconditionally; so we should never complain about ENOENT.
 *
 * If isRedo is true, it's unsurprising for the relation to be already gone.
 * Also, we should remove the file immediately instead of queuing a request
 * for later, since during redo there's no possibility of creating a
 * conflicting relation.
 *
 * Note: any failure should be reported as WARNING not ERROR, because
 * we are usually not in a transaction anymore when this is called.
 */
void
rpcunlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	_Oid spcnode = rnode.node.spcNode;
	_Oid dbnode = rnode.node.dbNode;
	_Oid relnode = rnode.node.relNode;
	_RelFileNode _node;

	_node.spcNode = spcnode;
	_node.dbNode = dbnode;
	_node.relNode = relnode;

	/*TODO rpc interface that unlink the file in storage nodes*/
	client.RpcFileUnlink(_node, static_cast<_ForkNumber::type>(forkNum));
}

/*
 *	rpcextend() -- Add a block to the specified relation.
 *
 *		The semantics are nearly the same as mdwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void
rpcextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer, bool skipFsync)
{
	_Oid spcnode = reln->smgr_rnode.node.spcNode;
	_Oid dbnode = reln->smgr_rnode.node.dbNode;
	_Oid relnode = reln->smgr_rnode.node.relNode;
	_RelFileNode _node;

	_node.spcNode = spcnode;
	_node.dbNode = dbnode;
	_node.relNode = relnode;
	/*TODO send a block in buffer to storage nodes*/

	client.RpcFileExtend(_node, static_cast<_ForkNumber::type>(forknum), blocknum);
}

/*
 *	rpcread() -- Read the specified block from a relation.
 */
void
rpcread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
	   char *buffer)
{
	off_t		seekpos;
	int			nbytes;
	_Page 		_page;
	_BlockNumber _blocknum = blocknum;
	MdfdVec    *v;

	TRACE_POSTGRESQL_SMGR_MD_READ_START(forknum, blocknum,
										reln->smgr_rnode.node.spcNode,
										reln->smgr_rnode.node.dbNode,
										reln->smgr_rnode.node.relNode,
										reln->smgr_rnode.backend);

	v = _mdfd_getseg(reln, forknum, blocknum, false,
					 EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	client.RpcFileRead(_page, v->mdfd_vfd, _blocknum);

	nbytes = _page.content.size();

	_page.content.copy(buffer, nbytes);

	TRACE_POSTGRESQL_SMGR_MD_READ_DONE(forknum, blocknum,
									   reln->smgr_rnode.node.spcNode,
									   reln->smgr_rnode.node.dbNode,
									   reln->smgr_rnode.node.relNode,
									   reln->smgr_rnode.backend,
									   nbytes,
									   BLCKSZ);

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));

		/*
		 * Short read: we are at or past EOF, or we read a partial block at
		 * EOF.  Normally this is an error; upper levels should never try to
		 * read a nonexistent block.  However, if zero_damaged_pages is ON or
		 * we are InRecovery, we should instead return zeroes without
		 * complaining.  This allows, for example, the case of trying to
		 * update a block that was later truncated away.
		 */
		if (zero_damaged_pages || InRecovery)
			MemSet(buffer, 0, BLCKSZ);
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read block %u in file \"%s\": read only %d of %d bytes",
							blocknum, FilePathName(v->mdfd_vfd),
							nbytes, BLCKSZ)));
	}


}

/*
 *	rpcwrite() -- Write the supplied block at the appropriate location.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
void
rpcwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		char *buffer, bool skipFsync)
{
	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;
	_BlockNumber _blocknum = blocknum;
	_Page		_page;

	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum < mdnblocks(reln, forknum));
#endif

	TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum,
										 reln->smgr_rnode.node.spcNode,
										 reln->smgr_rnode.node.dbNode,
										 reln->smgr_rnode.node.relNode,
										 reln->smgr_rnode.backend);

	v = _mdfd_getseg(reln, forknum, blocknum, skipFsync,
					 EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	_page.content.assign(buffer, BLCKSZ);

	client.RpcFileWrite(v->mdfd_vfd, _page, _blocknum);

	nbytes = BLCKSZ;

	TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum,
										reln->smgr_rnode.node.spcNode,
										reln->smgr_rnode.node.dbNode,
										reln->smgr_rnode.node.relNode,
										reln->smgr_rnode.backend,
										nbytes,
										BLCKSZ);

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write block %u in file \"%s\": %m",
							blocknum, FilePathName(v->mdfd_vfd))));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write block %u in file \"%s\": wrote only %d of %d bytes",
						blocknum,
						FilePathName(v->mdfd_vfd),
						nbytes, BLCKSZ),
				 errhint("Check free disk space.")));
	}

	if (!skipFsync && !SmgrIsTemp(reln))
		register_dirty_segment(reln, forknum, v);
}

/*
 *	rpcnblocks() -- Get the number of blocks stored in a relation.
 *
 *		Important side effect: all active segments of the relation are opened
 *		and added to the md_seg_fds array.  If this routine has not been
 *		called, then only segments up to the last one actually touched
 *		are present in the array.
 */
BlockNumber
rpcnblocks(SMgrRelation reln, ForkNumber forknum)
{
	_Oid spcnode = reln->smgr_rnode.node.spcNode;
	_Oid dbnode = reln->smgr_rnode.node.dbNode;
	_Oid relnode = reln->smgr_rnode.node.relNode;
	_RelFileNode _node;

	_node.spcNode = spcnode;
	_node.dbNode = dbnode;
	_node.relNode = relnode;
	/*TODO*/
	return (BlockNumber)client.RpcFileNblocks(_node, static_cast<_ForkNumber::type>(forknum));
}

/*
 *	rpctruncate() -- Truncate relation to specified number of blocks.
 */
void
rpctruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	_Oid spcnode = reln->smgr_rnode.node.spcNode;
	_Oid dbnode = reln->smgr_rnode.node.dbNode;
	_Oid relnode = reln->smgr_rnode.node.relNode;
	_RelFileNode _node;

	_node.spcNode = spcnode;
	_node.dbNode = dbnode;
	_node.relNode = relnode;
	/*TODO*/
	client.RpcFileTruncate(_node, static_cast<_ForkNumber::type>(forknum), nblocks);
}

