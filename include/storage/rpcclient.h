/*-------------------------------------------------------------------------
 *
 * rpcclient.h
 *	
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/rpcclient.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RPCCLIENT_H
#define RPCCLIENT_H

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void rpcinit(void);

extern void rpcshutdown(void);

extern void rpcopen(SMgrRelation reln);

extern void rpcclose(SMgrRelation reln, ForkNumber forknum);

extern void rpccreate(SMgrRelation reln, ForkNumber forkNum, bool isRedo);

extern bool rpcexists(SMgrRelation reln, ForkNumber forkNum);

extern void rpcunlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo);

extern void rpcextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer, bool skipFsync);

extern void rpcread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
	   char *buffer);

extern void rpcwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		char *buffer, bool skipFsync);

extern BlockNumber rpcnblocks(SMgrRelation reln, ForkNumber forknum);

extern void rpctruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks);

extern void rpcinitfile(char * db_dir_raw, char * fp);

extern File TryRpcOpenTransientFile(const char * filename, int fileflags);

extern int TryRpcCloseTransientFile(const File fd);

extern int TryRpcread(File fd,void * buf,  int size);

#ifdef __cplusplus
}
#endif

#endif							/* RPCCLIENT_H */