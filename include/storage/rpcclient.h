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

void
rpcinit(void);

void
rpcshutdown(void);

void
rpcopen(SMgrRelation reln);

void
rpcclose(SMgrRelation reln, ForkNumber forknum);

void
rpccreate(SMgrRelation reln, ForkNumber forkNum, bool isRedo);

bool
rpcexists(SMgrRelation reln, ForkNumber forkNum);

void
rpcunlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo);

void
rpcextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer, bool skipFsync);

void
rpcread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
	   char *buffer);

void
rpcwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		char *buffer, bool skipFsync);

BlockNumber
rpcnblocks(SMgrRelation reln, ForkNumber forknum);

void
rpctruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks);

#ifdef __cplusplus
}
#endif

#endif							/* RPCCLIENT_H */