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

extern void RpcInit();

extern int64_t TryRpcKvNblocks(Oid spcNode, Oid dbNode,
 Oid relNode, ForkNumber forknum, XLogRecPtr LSN);

extern void TryRpcKvRead(char * buf, Oid spcNode, 
Oid dbNode, Oid relNode, ForkNumber forknum, 
BlockNumber blocknum, XLogRecPtr LSN);

extern void rpcinitfile(char * db_dir_raw, char * fp);

extern File TryRpcOpenTransientFile(const char * filename, int fileflags);

extern int TryRpcCloseTransientFile(const File fd);

extern int TryRpcread(File fd,void * buf,  int size);

#ifdef __cplusplus
}
#endif

#endif							/* RPCCLIENT_H */