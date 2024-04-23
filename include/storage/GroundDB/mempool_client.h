#ifndef DB2_PG_MEMPOOL_CLIENT_H
#define DB2_PG_MEMPOOL_CLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <infiniband/verbs.h>
#include "c.h"
#include "access/logindex_hashmap.h"
#include "access/logindex_func.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
#include "storage/GroundDB/mempool_shmem.h"

#define SyncPAT_Interval_ms 1000

struct RDMAReadPageInfo{
	struct ibv_mr remote_pa_mr, remote_pida_mr;
	size_t pa_ofs;
};
typedef struct RDMAReadPageInfo RDMAReadPageInfo;

extern bool PageExistsInMemPool(KeyType PageID, RDMAReadPageInfo* rdma_read_info);

extern bool FetchPageFromMemoryPool(char* des, KeyType PageID, RDMAReadPageInfo* rdma_read_info);

extern bool LsnIsSatisfied(XLogRecPtr PageLSN);

extern bool ReplayXLog(KeyType PageID, BufferDesc* bufHdr, char* block, XLogRecPtr current_lsn, XLogRecPtr target_lsn);

extern void AsyncAccessPageOnMemoryPool(KeyType PageID);

extern void AsyncGetNewestPageAddressTable();

extern void SyncFlushPageToMemoryPool(char* src, KeyType PageID);

extern void UpdateVersionMap(XLogRecData* rdata, XLogRecPtr lsn);

extern void MemPoolSyncMain();


#ifdef __cplusplus
}
#endif

#endif //DB2_PG_MEMPOOL_CLIENT_H