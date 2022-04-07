#ifndef SRC_STORAGE_KV_H
#define SRC_STORAGE_KV_H

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "postgres_ext.h"
#include "access/xlogrecord.h"

/* md storage manager functionality */
extern void storage_kvinit(void);
extern void storage_kvopen(SMgrRelation reln);
extern void storage_kvclose(SMgrRelation reln, ForkNumber forknum);
extern void storage_kvcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool storage_kvexists(SMgrRelation reln, ForkNumber forknum);
extern void storage_kvunlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
extern void storage_kvextend(SMgrRelation reln, ForkNumber forknum,
                     BlockNumber blocknum, char *buffer, bool skipFsync);
extern bool storage_kvprefetch(SMgrRelation reln, ForkNumber forknum,
                       BlockNumber blocknum);
extern void storage_kvread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                   char *buffer);
extern void storage_kvread_internal(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                           char *buffer, int lsn);
extern void storage_kvwrite(SMgrRelation reln, ForkNumber forknum,
                    BlockNumber blocknum, char *buffer, bool skipFsync);
extern void storage_kvwriteback(SMgrRelation reln, ForkNumber forknum,
                        BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber storage_kvnblocks(SMgrRelation reln, ForkNumber forknum);
extern BlockNumber storage_kvnblocks_internal(SMgrRelation reln, ForkNumber forknum, unsigned long lsn);
extern void storage_kvtruncate(SMgrRelation reln, ForkNumber forknum,
                       BlockNumber nblocks);
extern void storage_kvimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void storage_kvcopydb(char *srcPath, char*dstPath);



extern void storage_async_pagexlog(RelFileNode rnode, ForkNumber forknum, BlockNumber blocknum, unsigned long lsn);
extern void storage_async_relxlog(RelFileNode rnode, ForkNumber forknum, unsigned long lsn);

extern void storage_get_page_asynxlog_list(RelFileNode rnode, ForkNumber forknum, BlockNumber blocknum, unsigned long **targetResult);
extern void storage_get_rel_asynxlog_list(RelFileNode rnode, ForkNumber forknum, unsigned long **targetResult);

extern void storage_getxlog(unsigned long lsn, char **buffer);
extern void storage_savexlog(unsigned long lsn, XLogRecord *record);
extern void storage_deletexlog(unsigned long lsn);

extern void storage_delete_from_page_xlog_list(RelFileNode rnode, ForkNumber forknum, BlockNumber blocknum, unsigned long lsn);
extern void storage_delete_from_rel_xlog_list(RelFileNode rnode, ForkNumber forknum, unsigned long lsn);

/* md sync callbacks */
extern int	storage_kvsyncfiletag(const FileTag *ftag, char *path);
extern int	storage_kvunlinkfiletag(const FileTag *ftag, char *path);
extern bool storage_kvfiletagmatches(const FileTag *ftag, const FileTag *candidate);

#endif //SRC_STORAGE_KV_H
