#ifndef DB2_PG_STORAGE_SERVER_H
#define DB2_PG_STORAGE_SERVER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common/relpath.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#include "access/xlogdefs.h"

extern void
SyncReplayProcess();


extern void
GetPageByLsn(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, XLogRecPtr lsn, char *buffer);

extern void
RpcServerMain(int argc, char *argv[],
              const char *dbname,
              const char *username);

#ifdef __cplusplus
}
#endif

#endif //DB2_PG_STORAGE_SERVER_H
