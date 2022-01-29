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
#ifndef MD_H
#define MD_H

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"

#ifdef __cplusplus
extern "C" {
#elif
extern {
#endif

/* md storage manager functionality */
void mdinit(void);
void mdopen(SMgrRelation reln);
void mdclose(SMgrRelation reln, ForkNumber forknum);
void mdcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
bool mdexists(SMgrRelation reln, ForkNumber forknum);
void mdunlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
void mdextend(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, char *buffer, bool skipFsync);
bool mdprefetch(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum);
void mdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				   char *buffer);
void mdwrite(SMgrRelation reln, ForkNumber forknum,
					BlockNumber blocknum, char *buffer, bool skipFsync);
void mdwriteback(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, BlockNumber nblocks);
BlockNumber mdnblocks(SMgrRelation reln, ForkNumber forknum);
void mdtruncate(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber nblocks);
void mdimmedsync(SMgrRelation reln, ForkNumber forknum);

void ForgetDatabaseSyncRequests(Oid dbid);
void DropRelationFiles(RelFileNode *delrels, int ndelrels, bool isRedo);

/* md sync callbacks */
int	mdsyncfiletag(const FileTag *ftag, char *path);
int	mdunlinkfiletag(const FileTag *ftag, char *path);
bool mdfiletagmatches(const FileTag *ftag, const FileTag *candidate);


#ifdef __cplusplus
}
#elif
}
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* local routines */
void mdunlinkfork(RelFileNodeBackend rnode, ForkNumber forkNum,
						 bool isRedo);
MdfdVec *mdopenfork(SMgrRelation reln, ForkNumber forknum, int behavior);
void register_dirty_segment(SMgrRelation reln, ForkNumber forknum,
								   MdfdVec *seg);
void register_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum,
									BlockNumber segno);
void register_forget_request(RelFileNodeBackend rnode, ForkNumber forknum,
									BlockNumber segno);
void _fdvec_resize(SMgrRelation reln,
						  ForkNumber forknum,
						  int nseg);
char *_mdfd_segpath(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber segno);
MdfdVec *_mdfd_openseg(SMgrRelation reln, ForkNumber forkno,
							  BlockNumber segno, int oflags);
MdfdVec *_mdfd_getseg(SMgrRelation reln, ForkNumber forkno,
							 BlockNumber blkno, bool skipFsync, int behavior);
BlockNumber _mdnblocks(SMgrRelation reln, ForkNumber forknum,
							  MdfdVec *seg);

#ifdef __cplusplus
}
#endif

#endif							/* MD_H */
