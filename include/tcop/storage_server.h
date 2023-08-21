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
extern int
SyncGetRelSize(RelFileNode relFileNode, ForkNumber forkNumber, XLogRecPtr lsn);
extern void
ApplyOneLsn(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, XLogRecPtr lsn, char* origPage, char* targetPage);
extern void
GetBasePage(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, char* buffer);
extern void
ApplyOneLsnWithoutBasePage(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, XLogRecPtr lsn, char* targetPage);
extern void
ApplyLsnList(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, XLogRecPtr* lsnList, int listSize, char* origPage, char* targetPage);
extern void
ApplyLsnListAndGetUpdatedPage(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, XLogRecPtr* lsnList, int listSize,  char* targetPage);
extern void
WalRedoExtendRel(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, char * content);
extern void
WalRedoCreateRel(RelFileNode relFileNode, ForkNumber forkNumber);



extern void
RpcServerMain(int argc, char *argv[],
              const char *dbname,
              const char *username);

#ifdef __cplusplus
}
#endif

#endif //DB2_PG_STORAGE_SERVER_H
