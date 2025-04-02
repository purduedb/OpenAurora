#ifndef DB2_PG_MEMPOOL_CLIENT_H
#define DB2_PG_MEMPOOL_CLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <infiniband/verbs.h>
#include "c.h"
#include "access/brin_xlog.h"
#include "access/generic_xlog.h"
#include "access/ginxlog.h"
#include "access/gistxlog.h"
#include "access/hash_xlog.h"
#include "access/heapam_xlog.h"
#include "access/logindex_hashmap.h"
#include "access/logindex_func.h"
#include "access/nbtxlog.h"
#include "access/polar_logindex.h"
#include "access/spgxlog.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "catalog/pg_control.h"
#include "commands/sequence.h"
#include "replication/origin.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
#include "storage/GroundDB/mempool_shmem.h"

// #define USE_MEMPOOL_STAT
// #define MEMPOOL_CACHE_POLICY_COVERING
// #define MEMPOOL_CACHE_POLICY_DISJOINT
#define SyncPAT_Interval_us 1000000
#define CheckSyncPAT_Interval_us (SyncPAT_Interval_us / 100)
#define SyncXLogInfo_Interval_us 1000
#define SyncUpdateVersionMapInfo_Interval_us 500

#define TryReconnectionToMemPool_Interval_us 1000000

struct RDMAReadPageInfo{
	struct ibv_mr remote_pa_mr, remote_pida_mr;
	size_t memnode_id, pa_ofs;
};
typedef struct RDMAReadPageInfo RDMAReadPageInfo;

extern bool MempoolClientReplaying;

extern void proc_exit_MemPool();

extern void ReportStatForMemPool();
extern void ResetStatForMemPool();

extern bool PageExistsInMemPool(KeyType PageID, RDMAReadPageInfo* rdma_read_info);

extern bool FetchPageFromMemoryPool(char* des, KeyType PageID, RDMAReadPageInfo* rdma_read_info);

extern bool LsnIsSatisfied(XLogRecPtr PageLSN, XLogRecPtr TargetLSN);

extern bool ReplayXLog(KeyType PageID, BufferDesc* bufHdr, char* block, XLogRecPtr current_lsn, XLogRecPtr target_lsn);

extern void AsyncAccessPageOnMemoryPool(KeyType PageID);

extern void AsyncRemovePageOnMemoryPool(KeyType PageID);

extern void AsyncGetNewestPageAddressTable();

extern void MemPoolmdwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool skipFsync);
extern void ASyncFlushPageToMemoryPool(char* src, KeyType PageID);;
extern void SyncFlushPageToMemoryPool(char* src, KeyType PageID);

extern void InsertIntoVersionMap(KeyType page_id, XLogRecPtr lsn);
extern void UpdateVersionMap(XLogRecData* rdata, XLogRecPtr lsn);

extern void MemPoolSyncMain();


#ifdef __cplusplus
}
#endif

#endif //DB2_PG_MEMPOOL_CLIENT_H