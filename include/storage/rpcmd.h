/*-------------------------------------------------------------------------
 *
 * md.h
 *	  magnetic disk storage manager public interface declarations.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/md.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RPCMD_H
#define RPCMD_H

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"

/* md storage manager functionality */
extern void rpcmdinit(void);
extern void rpcmdopen(SMgrRelation reln);
extern void rpcmdclose(SMgrRelation reln, ForkNumber forknum);
extern void rpcmdcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool rpcmdexists(SMgrRelation reln, ForkNumber forknum);
extern void rpcmdunlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
extern void rpcmdextend(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, char *buffer, bool skipFsync);
extern bool rpcmdprefetch(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum);
extern void rpcmdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				   char *buffer);
extern void rpcmdwrite(SMgrRelation reln, ForkNumber forknum,
					BlockNumber blocknum, char *buffer, bool skipFsync);
extern void rpcmdwriteback(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber rpcmdnblocks(SMgrRelation reln, ForkNumber forknum);
extern void rpcmdtruncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber nblocks);
extern void rpcmdimmedsync(SMgrRelation reln, ForkNumber forknum);

extern void rpcForgetDatabaseSyncRequests(Oid dbid);
extern void rpcDropRelationFiles(RelFileNode *delrels, int ndelrels, bool isRedo);

/* md sync callbacks */
extern int	rpcmdsyncfiletag(const FileTag *ftag, char *path);
extern int	rpcmdunlinkfiletag(const FileTag *ftag, char *path);
extern bool rpcmdfiletagmatches(const FileTag *ftag, const FileTag *candidate);

#endif							/* MD_H */
