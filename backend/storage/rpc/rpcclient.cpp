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
#include "utils/palloc.h"

#include <iostream>
#include <fstream>

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

std::shared_ptr<TTransport> rpcsocket;
std::shared_ptr<TTransport> rpctransport;
std::shared_ptr<TProtocol> rpcprotocol;
DataPageAccessClient *client;

MdfdVec *
_rpcfd_openseg(SMgrRelation reln, ForkNumber forknum, BlockNumber segno,
			  int oflags);
MdfdVec *
_rpcfd_getseg(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno,
			 bool skipFsync, int behavior);
BlockNumber
_rpcnblocks(SMgrRelation reln, ForkNumber forknum, MdfdVec *seg);
MdfdVec *
rpcopenfork(SMgrRelation reln, ForkNumber forknum, int behavior);

void TryRpcFileClose(_File _fd);

void TryRpcTablespaceCreateDbspace(_Oid _spcnode, _Oid _dbnode, bool isRedo);

_File TryRpcPathNameOpenFile(_Path& _path, _Flag _flag);

int32_t TryRpcFileWrite(_File _fd, _Page& _page, _Off_t _seekpos);

void TryRpcFilePathName(_Path& _return, _File _fd);

void TryRpcFileRead(_Page& _return, _File _fd, _Off_t _seekpos);

int32_t TryRpcFileTruncate(_File _fd, _Off_t _offset);

_Off_t TryRpcFileSize(_File _fd);

void TryRpcInitFile(_Page& _return, _Path& _path);

/*
 *	rpcinit() -- Initialize private state for magnetic disk storage manager.
 */
void
rpcinit(void)
{
	char		path[MAXPGPATH];

	snprintf(path, sizeof(path), "%s/client.signal", DataDir);

	if (access(path, F_OK) == 0);
	{
		rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
		rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
		rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
		client = new DataPageAccessClient(rpcprotocol);
	}
}

void
rpcshutdown(void)
{
	char		path[MAXPGPATH];

	snprintf(path, sizeof(path), "%s/client->signal", DataDir);

	if (access(path, F_OK) == 0)
	{
		delete client;
	}
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
		TryRpcFileClose(v->mdfd_vfd);
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
	
    MdfdVec    *mdfd;
	char	   *path;
	File		fd;
	_Path		_path;
	_Oid spcnode = reln->smgr_rnode.node.spcNode;
	_Oid dbnode = reln->smgr_rnode.node.dbNode;

	if (isRedo && reln->md_num_open_segs[forkNum] > 0)
		return;					/* created and opened already... */

	Assert(reln->md_num_open_segs[forkNum] == 0);

	

	/*
	 * We may be using the target table space for the first time in this
	 * database, so create a per-database subdirectory if needed.
	 *
	 * XXX this is a fairly ugly violation of module layering, but this seems
	 * to be the best place to put the check.  Maybe TablespaceCreateDbspace
	 * should be here and not in commands/tablespace.c?  But that would imply
	 * importing a lot of stuff that smgr.c oughtn't know, either.
	 */
	TryRpcTablespaceCreateDbspace(spcnode,
							dbnode,
							isRedo);

	path = relpath(reln->smgr_rnode, forkNum);

	_path.assign(path);

	fd = TryRpcPathNameOpenFile(_path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);

	if (fd < 0)
	{
		int			save_errno = errno;

		if (isRedo)
			fd = TryRpcPathNameOpenFile(_path, O_RDWR | PG_BINARY);
		if (fd < 0)
		{
			/* be sure to report the error reported by create, not open */
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m", path)));
		}
	}

	pfree(path);

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
{
	/*
	 * Close it first, to ensure that we notice if the fork has been unlinked
	 * since we opened it.
	 */
	rpcclose(reln, forkNum);

	return (rpcopenfork(reln, forkNum, EXTENSION_RETURN_NULL) != NULL);
}

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
{}

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

	off_t		seekpos;
	int			nbytes;
	MdfdVec    *v;
	_Page 		_page;

	/* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
	Assert(blocknum >= mdnblocks(reln, forknum));
#endif

	/*
	 * If a relation manages to grow to 2^32-1 blocks, refuse to extend it any
	 * more --- we mustn't create a block whose number actually is
	 * InvalidBlockNumber.
	 */
	if (blocknum == InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot extend file \"%s\" beyond %u blocks",
						relpath(reln->smgr_rnode, forknum),
						InvalidBlockNumber)));

	v = _rpcfd_getseg(reln, forknum, blocknum, skipFsync, EXTENSION_CREATE);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	_page.assign(buffer, BLCKSZ);

	if ((nbytes = TryRpcFileWrite(v->mdfd_vfd, _page, seekpos)) != BLCKSZ)
	{
		char		path[MAXPGPATH];
		_Path 		_path;
		TryRpcFilePathName(_path, v->mdfd_vfd);
		std::size_t length = _path.copy(path, _path.size());
		path[length] = '\0';

		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not extend file \"%s\": %m",
							path),
					 errhint("Check free disk space.")));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
						path,
						nbytes, BLCKSZ, blocknum),
				 errhint("Check free disk space.")));
	}

	Assert(_rpcnblocks(reln, forknum, v) <= ((BlockNumber) RELSEG_SIZE));
	
	
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
	MdfdVec    *v;
	_Page		_page;

	TRACE_POSTGRESQL_SMGR_MD_READ_START(forknum, blocknum,
										reln->smgr_rnode.node.spcNode,
										reln->smgr_rnode.node.dbNode,
										reln->smgr_rnode.node.relNode,
										reln->smgr_rnode.backend);

	v = _rpcfd_getseg(reln, forknum, blocknum, false,
					 EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);
	
	TryRpcFileRead(_page, v->mdfd_vfd, seekpos);
	
	nbytes = _page.copy(buffer, BLCKSZ);

	TRACE_POSTGRESQL_SMGR_MD_READ_DONE(forknum, blocknum,
									   reln->smgr_rnode.node.spcNode,
									   reln->smgr_rnode.node.dbNode,
									   reln->smgr_rnode.node.relNode,
									   reln->smgr_rnode.backend,
									   nbytes,
									   BLCKSZ);
	printf("%ld %ld rpc read finish make sure?\n", (long)getpid(), (long)getppid());
	if (nbytes != BLCKSZ)
	{
		char		path[MAXPGPATH];
		_Path 		_path;
		TryRpcFilePathName(_path, v->mdfd_vfd);
		std::size_t length = _path.copy(path, _path.size());
		path[length] = '\0';

		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read block %u in file \"%s\": %m",
							blocknum, path)));

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
							blocknum, path,
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

	v = _rpcfd_getseg(reln, forknum, blocknum, skipFsync,
					 EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY);

	seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));

	Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);

	nbytes = TryRpcFileWrite(v->mdfd_vfd, _page, seekpos);

	TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum,
										reln->smgr_rnode.node.spcNode,
										reln->smgr_rnode.node.dbNode,
										reln->smgr_rnode.node.relNode,
										reln->smgr_rnode.backend,
										nbytes,
										BLCKSZ);

	if (nbytes != BLCKSZ)
	{
		char		path[MAXPGPATH];
		_Path 		_path;
		TryRpcFilePathName(_path, v->mdfd_vfd);
		std::size_t length = _path.copy(path, _path.size());
		path[length] = '\0';

		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write block %u in file \"%s\": %m",
							blocknum, path)));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write block %u in file \"%s\": wrote only %d of %d bytes",
						blocknum,
						path,
						nbytes, BLCKSZ),
				 errhint("Check free disk space.")));
	}
	
	
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
	MdfdVec    *v = rpcopenfork(reln, forknum, EXTENSION_FAIL);
	BlockNumber nblocks;
	BlockNumber segno = 0;

	/* mdopen has opened the first segment */
	Assert(reln->md_num_open_segs[forknum] > 0);

	/*
	 * Start from the last open segments, to avoid redundant seeks.  We have
	 * previously verified that these segments are exactly RELSEG_SIZE long,
	 * and it's useless to recheck that each time.
	 *
	 * NOTE: this assumption could only be wrong if another backend has
	 * truncated the relation.  We rely on higher code levels to handle that
	 * scenario by closing and re-opening the md fd, which is handled via
	 * relcache flush.  (Since the checkpointer doesn't participate in
	 * relcache flush, it could have segment entries for inactive segments;
	 * that's OK because the checkpointer never needs to compute relation
	 * size.)
	 */
	segno = reln->md_num_open_segs[forknum] - 1;
	v = &reln->md_seg_fds[forknum][segno];

	for (;;)
	{
		nblocks = _rpcnblocks(reln, forknum, v);
		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");
		if (nblocks < ((BlockNumber) RELSEG_SIZE))
			return (segno * ((BlockNumber) RELSEG_SIZE)) + nblocks;

		/*
		 * If segment is exactly RELSEG_SIZE, advance to next one.
		 */
		segno++;

		/*
		 * We used to pass O_CREAT here, but that has the disadvantage that it
		 * might create a segment which has vanished through some operating
		 * system misadventure.  In such a case, creating the segment here
		 * undermines _mdfd_getseg's attempts to notice and report an error
		 * upon access to a missing segment.
		 */
		v = _rpcfd_openseg(reln, forknum, segno, 0);
		if (v == NULL)
			return segno * ((BlockNumber) RELSEG_SIZE);
	}
}

/*
 *	rpctruncate() -- Truncate relation to specified number of blocks.
 */
void
rpctruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{

	BlockNumber curnblk;
	BlockNumber priorblocks;
	int			curopensegs;

	/*
	 * NOTE: mdnblocks makes sure we have opened all active segments, so that
	 * truncation loop will get them all!
	 */
	curnblk = rpcnblocks(reln, forknum);
	if (nblocks > curnblk)
	{
		/* Bogus request ... but no complaint if InRecovery */
		if (InRecovery)
			return;
		ereport(ERROR,
				(errmsg("could not truncate file \"%s\" to %u blocks: it's only %u blocks now",
						relpath(reln->smgr_rnode, forknum),
						nblocks, curnblk)));
	}
	if (nblocks == curnblk)
		return;					/* no work */

	

	/*
	 * Truncate segments, starting at the last one. Starting at the end makes
	 * managing the memory for the fd array easier, should there be errors.
	 */
	curopensegs = reln->md_num_open_segs[forknum];
	while (curopensegs > 0)
	{
		MdfdVec    *v;

		priorblocks = (curopensegs - 1) * RELSEG_SIZE;

		v = &reln->md_seg_fds[forknum][curopensegs - 1];

		if (priorblocks > nblocks)
		{
			char		path[MAXPGPATH];
			_Path 		_path;
			TryRpcFilePathName(_path, v->mdfd_vfd);
			std::size_t length = _path.copy(path, _path.size());
			path[length] = '\0';
			/*
			 * This segment is no longer active. We truncate the file, but do
			 * not delete it, for reasons explained in the header comments.
			 */
			if (TryRpcFileTruncate(v->mdfd_vfd, 0) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\": %m",
								path)));


			/* we never drop the 1st segment */
			Assert(v != &reln->md_seg_fds[forknum][0]);

			TryRpcFileClose(v->mdfd_vfd);
			_fdvec_resize(reln, forknum, curopensegs - 1);
		}
		else if (priorblocks + ((BlockNumber) RELSEG_SIZE) > nblocks)
		{
			char		path[MAXPGPATH];
			_Path 		_path;
			TryRpcFilePathName(_path, v->mdfd_vfd);
			std::size_t length = _path.copy(path, _path.size());
			path[length] = '\0';
			/*
			 * This is the last segment we want to keep. Truncate the file to
			 * the right length. NOTE: if nblocks is exactly a multiple K of
			 * RELSEG_SIZE, we will truncate the K+1st segment to 0 length but
			 * keep it. This adheres to the invariant given in the header
			 * comments.
			 */
			BlockNumber lastsegblocks = nblocks - priorblocks;

			if (TryRpcFileTruncate(v->mdfd_vfd, (off_t) lastsegblocks * BLCKSZ) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\" to %u blocks: %m",
								path,
								nblocks)));
		}
		else
		{
			/*
			 * We still need this segment, so nothing to do for this and any
			 * earlier segment.
			 */
			break;
		}
		curopensegs--;
	}
	
	
}

void
rpcinitfile(char * db_dir_raw, char * fp)
{
	char		mpath[MAXPGPATH];

	rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
	rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
	rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
	client = new DataPageAccessClient(rpcprotocol);
	
	_Page _page;
	_Path _path;
	_path.assign(fp);

	TryRpcInitFile(_page, _path);
	
	snprintf(mpath, sizeof(mpath), "%s/%s", db_dir_raw, fp);	

	std::ofstream ofile(mpath);

	ofile.write(&_page[0], _page.size());

	ofile.close();

	
	delete client;
}

/*
 * Open the specified segment of the relation,
 * and make a MdfdVec object for it.  Returns NULL on failure.
 */
MdfdVec *
_rpcfd_openseg(SMgrRelation reln, ForkNumber forknum, BlockNumber segno,
			  int oflags)
{
	MdfdVec    *v;
	File		fd;
	char	   *fullpath;
	_Path		_fullpath;

	fullpath = _mdfd_segpath(reln, forknum, segno);

	_fullpath.assign(fullpath);

	/* open the file */
	fd = TryRpcPathNameOpenFile(_fullpath, O_RDWR | PG_BINARY | oflags);	

	pfree(fullpath);

	if (fd < 0)
		return NULL;

	/*
	 * Segments are always opened in order from lowest to highest, so we must
	 * be adding a new one at the end.
	 */
	Assert(segno == reln->md_num_open_segs[forknum]);

	_fdvec_resize(reln, forknum, segno + 1);

	/* fill the entry */
	v = &reln->md_seg_fds[forknum][segno];
	v->mdfd_vfd = fd;
	v->mdfd_segno = segno;

	Assert(_rpcnblocks(reln, forknum, v) <= ((BlockNumber) RELSEG_SIZE));

	/* all done */
	return v;
}

/*
 *	_rpcfd_getseg() -- Find the segment of the relation holding the
 *		specified block.
 *
 * If the segment doesn't exist, we ereport, return NULL, or create the
 * segment, according to "behavior".  Note: skipFsync is only used in the
 * EXTENSION_CREATE case.
 */
MdfdVec *
_rpcfd_getseg(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno,
			 bool skipFsync, int behavior)
{
	MdfdVec    *v;
	BlockNumber targetseg;
	BlockNumber nextsegno;

	/* some way to handle non-existent segments needs to be specified */
	Assert(behavior &
		   (EXTENSION_FAIL | EXTENSION_CREATE | EXTENSION_RETURN_NULL));

	targetseg = blkno / ((BlockNumber) RELSEG_SIZE);

	/* if an existing and opened segment, we're done */
	if (targetseg < reln->md_num_open_segs[forknum])
	{
		v = &reln->md_seg_fds[forknum][targetseg];
		return v;
	}

	/*
	 * The target segment is not yet open. Iterate over all the segments
	 * between the last opened and the target segment. This way missing
	 * segments either raise an error, or get created (according to
	 * 'behavior'). Start with either the last opened, or the first segment if
	 * none was opened before.
	 */
	if (reln->md_num_open_segs[forknum] > 0)
		v = &reln->md_seg_fds[forknum][reln->md_num_open_segs[forknum] - 1];
	else
	{
		v = rpcopenfork(reln, forknum, behavior);
		if (!v)
			return NULL;		/* if behavior & EXTENSION_RETURN_NULL */
	}

	for (nextsegno = reln->md_num_open_segs[forknum];
		 nextsegno <= targetseg; nextsegno++)
	{
		BlockNumber nblocks = _rpcnblocks(reln, forknum, v);
		int			flags = 0;

		Assert(nextsegno == v->mdfd_segno + 1);

		if (nblocks > ((BlockNumber) RELSEG_SIZE))
			elog(FATAL, "segment too big");

		if ((behavior & EXTENSION_CREATE) ||
			(InRecovery && (behavior & EXTENSION_CREATE_RECOVERY)))
		{
			/*
			 * Normally we will create new segments only if authorized by the
			 * caller (i.e., we are doing mdextend()).  But when doing WAL
			 * recovery, create segments anyway; this allows cases such as
			 * replaying WAL data that has a write into a high-numbered
			 * segment of a relation that was later deleted. We want to go
			 * ahead and create the segments so we can finish out the replay.
			 *
			 * We have to maintain the invariant that segments before the last
			 * active segment are of size RELSEG_SIZE; therefore, if
			 * extending, pad them out with zeroes if needed.  (This only
			 * matters if in recovery, or if the caller is extending the
			 * relation discontiguously, but that can happen in hash indexes.)
			 */
			if (nblocks < ((BlockNumber) RELSEG_SIZE))
			{
				char	   *zerobuf = static_cast<char *>(palloc0(BLCKSZ));

				rpcextend(reln, forknum,
						 nextsegno * ((BlockNumber) RELSEG_SIZE) - 1,
						 zerobuf, skipFsync);
				pfree(zerobuf);
			}
			flags = O_CREAT;
		}
		else if (!(behavior & EXTENSION_DONT_CHECK_SIZE) &&
				 nblocks < ((BlockNumber) RELSEG_SIZE))
		{
			/*
			 * When not extending (or explicitly including truncated
			 * segments), only open the next segment if the current one is
			 * exactly RELSEG_SIZE.  If not (this branch), either return NULL
			 * or fail.
			 */
			if (behavior & EXTENSION_RETURN_NULL)
			{
				/*
				 * Some callers discern between reasons for _mdfd_getseg()
				 * returning NULL based on errno. As there's no failing
				 * syscall involved in this case, explicitly set errno to
				 * ENOENT, as that seems the closest interpretation.
				 */
				errno = ENOENT;
				return NULL;
			}

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): previous segment is only %u blocks",
							_mdfd_segpath(reln, forknum, nextsegno),
							blkno, nblocks)));
		}

		v = _rpcfd_openseg(reln, forknum, nextsegno, flags);

		if (v == NULL)
		{
			if ((behavior & EXTENSION_RETURN_NULL) &&
				FILE_POSSIBLY_DELETED(errno))
				return NULL;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" (target block %u): %m",
							_mdfd_segpath(reln, forknum, nextsegno),
							blkno)));
		}
	}

	return v;
}

/*
 * Get number of blocks present in a single disk file
 */
BlockNumber
_rpcnblocks(SMgrRelation reln, ForkNumber forknum, MdfdVec *seg)
{
	off_t		len;

	len = TryRpcFileSize(seg->mdfd_vfd);
	if (len < 0)
	{
		char		path[MAXPGPATH];
		_Path 		_path;
		TryRpcFilePathName(_path, seg->mdfd_vfd);
		std::size_t length = _path.copy(path, _path.size());
		path[length] = '\0';

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek to end of file \"%s\": %m",
						path)));
	}
	
	
	/* note that this calculation will ignore any partial block at EOF */
	return (BlockNumber) (len / BLCKSZ);
}

/*
 *	rpcopenfork() -- Open one fork of the specified relation.
 *
 * Note we only open the first segment, when there are multiple segments.
 *
 * If first segment is not present, either ereport or return NULL according
 * to "behavior".  We treat EXTENSION_CREATE the same as EXTENSION_FAIL;
 * EXTENSION_CREATE means it's OK to extend an existing relation, not to
 * invent one out of whole cloth.
 */
MdfdVec *
rpcopenfork(SMgrRelation reln, ForkNumber forknum, int behavior)
{
	
	MdfdVec    *mdfd;
	char	   *path;
	File		fd;
	_Path 		_path;

	/* No work if already open */
	if (reln->md_num_open_segs[forknum] > 0)
		return &reln->md_seg_fds[forknum][0];
	
	path = relpath(reln->smgr_rnode, forknum);

	_path.assign(path);

	fd = TryRpcPathNameOpenFile(_path, O_RDWR | PG_BINARY);
	
	if (fd < 0)
	{
		if ((behavior & EXTENSION_RETURN_NULL))
		{
			pfree(path);
			return NULL;
		}
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}

	pfree(path);

	_fdvec_resize(reln, forknum, 1);
	mdfd = &reln->md_seg_fds[forknum][0];
	mdfd->mdfd_vfd = fd;
	mdfd->mdfd_segno = 0;

	Assert(_rpcnblocks(reln, forknum, mdfd) <= ((BlockNumber) RELSEG_SIZE));

	return mdfd;
}

void TryRpcFileClose(_File _fd)
{
	int trycount=0;
	int maxcount=3;
	do{
		try{
			rpctransport->open();
			client->RpcFileClose(_fd);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);

			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
};

void TryRpcTablespaceCreateDbspace(_Oid _spcnode, _Oid _dbnode, bool isRedo)
{
	int trycount=0;
	int maxcount=3;
	do{
		try{
			rpctransport->open();
			client->RpcTablespaceCreateDbspace(_spcnode, _dbnode, isRedo);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);

			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
};

_File TryRpcPathNameOpenFile(_Path& _path, _Flag _flag)
{
	int trycount=0;
	int maxcount=3;
	_File result;
	do{
		try{
			rpctransport->open();
			result = client->RpcPathNameOpenFile(_path, _flag);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);

			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
	return result;
};

int32_t TryRpcFileWrite(_File _fd, _Page& _page, _Off_t _seekpos)
{
	int trycount=0;
	int maxcount=3;
	int32_t result;
	do{
		try{
			rpctransport->open();
			result = client->RpcFileWrite(_fd, _page, _seekpos);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);

			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
	return result;
};

void TryRpcFilePathName(_Path& _return, _File _fd)
{
	int trycount=0;
	int maxcount=3;
	do{
		try{
			rpctransport->open();
			client->RpcFilePathName(_return, _fd);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);

			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
};

void TryRpcFileRead(_Page& _return, _File _fd, _Off_t _seekpos)
{
	int trycount=0;
	int maxcount=3;
	do{
		try{
			rpctransport->open();
			client->RpcFileRead(_return, _fd, _seekpos);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);
			
			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
};

int32_t TryRpcFileTruncate(_File _fd, _Off_t _offset)
{
	int trycount=0;
	int maxcount=3;
	int32_t result;
	do{
		try{
			rpctransport->open();
			result = client->RpcFileTruncate(_fd, _offset);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);

			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
	return result;
};

_Off_t TryRpcFileSize(_File _fd)
{
	int trycount=0;
	int maxcount=3;
	_Off_t result;
	do{
		try{
			rpctransport->open();
			result = client->RpcFileSize(_fd);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);

			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
	return result;
};

void TryRpcInitFile(_Page& _return, _Path& _path)
{
	int trycount=0;
	int maxcount=3;
	do{
		try{
			rpctransport->open();
			client->RpcInitFile(_return, _path);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);
			
			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
};
