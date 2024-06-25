#ifndef DB2_PG_MEMPOOL_SHMEM_H
#define DB2_PG_MEMPOOL_SHMEM_H

#ifdef __cplusplus
extern "C" {
#endif

#include <infiniband/verbs.h>
#include "postgres.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/GroundDB/mempool_client.h"

#define NUMBER_OF_mempool_client_lw_lock 4
#define mempool_client_connection_lock (&mempool_client_lw_lock[0])
#define mempool_client_version_map_lock (&mempool_client_lw_lock[1])
#define mempool_client_pat_lock (&mempool_client_lw_lock[2])
#define mempool_client_sync_pat_lock (&mempool_client_lw_lock[3])

extern PGDLLIMPORT LWLock* mempool_client_lw_lock;

#define MAX_PAGE_ARRAY_COUNT 20ull
#define MAX_TOTAL_PAGE_ARRAY_SIZE (1ull << 24)
#define PAGE_ARRAY_TABLE_PARTITION_NUM 128

extern PGDLLIMPORT size_t *mpc_pa_cnt, *mpc_pa_size;
extern PGDLLIMPORT KeyType *mpc_idx_to_pid;
extern PGDLLIMPORT struct ibv_mr *mpc_idx_to_mr;
extern PGDLLIMPORT HTAB *mpc_pid_to_idx;

typedef struct{
	KeyType page_id;
	size_t pa_idx, pa_ofs;
} PATLookupEntry;

extern PGDLLIMPORT bool *is_first_mpc;
extern PGDLLIMPORT HTAB_VM *version_map;

extern void MemPoolClientShmemInit();

extern Size MemPoolClientShmemSize();

size_t get_MemPoolClient_node_id();

bool whetherSyncPAT();

#ifdef __cplusplus
}
#endif

#endif //DB2_PG_MEMPOOL_SHMEM_H