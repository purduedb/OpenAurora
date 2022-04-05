#ifndef SRC_KV_H
#define SRC_KV_H

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "postgres_ext.h"

/* md storage manager functionality */
extern void kvinit(void);
extern void kvopen(SMgrRelation reln);
extern void kvclose(SMgrRelation reln, ForkNumber forknum);
extern void kvcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool kvexists(SMgrRelation reln, ForkNumber forknum);
extern void kvunlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
extern void kvextend(SMgrRelation reln, ForkNumber forknum,
                     BlockNumber blocknum, char *buffer, bool skipFsync);
extern bool kvprefetch(SMgrRelation reln, ForkNumber forknum,
                       BlockNumber blocknum);
extern void kvread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                   char *buffer);
extern void kvwrite(SMgrRelation reln, ForkNumber forknum,
                    BlockNumber blocknum, char *buffer, bool skipFsync);
extern void kvwriteback(SMgrRelation reln, ForkNumber forknum,
                        BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber kvnblocks(SMgrRelation reln, ForkNumber forknum);
extern void kvtruncate(SMgrRelation reln, ForkNumber forknum,
                       BlockNumber nblocks);
extern void kvimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void kvcopydb(char *srcPath, char*dstPath);


/* md sync callbacks */
extern int	kvsyncfiletag(const FileTag *ftag, char *path);
extern int	kvunlinkfiletag(const FileTag *ftag, char *path);
extern bool kvfiletagmatches(const FileTag *ftag, const FileTag *candidate);

#endif //SRC_KV_H

