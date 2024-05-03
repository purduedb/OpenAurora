#include <chrono>
#include "postgres.h"
#include "storage/GroundDB/mempool_shmem.h"

LWLock *mempool_client_lw_lock;
size_t *node_id_cnt;

std::chrono::steady_clock::time_point *last_sync_pat;

// For PAT
size_t *mpc_pa_cnt, *mpc_pa_size;
KeyType *mpc_idx_to_pid;
ibv_mr *mpc_idx_to_mr;
HTAB *mpc_pid_to_idx;

HTAB_VM *version_map;

bool *is_first_mpc;

void* ShmemInitStruct(char* name, size_t size, bool& found_any, bool& found_all){
	bool found;
	auto ptr = ShmemInitStruct(name, size, &found);
	found_any |= found, found_all &= found;
	return ptr;
}
void MemPoolClientShmemInit(){
	bool found_any = false, found_all = true;

	mempool_client_lw_lock = (LWLock *)
		ShmemInitStruct("MemPool Client lwlock",
						NUMBER_OF_mempool_client_lw_lock * sizeof(LWLock),
						found_any, found_all);
	node_id_cnt = (size_t*)
		ShmemInitStruct("MemPool Client node ID counter",
						sizeof(size_t),
						found_any, found_all);
	last_sync_pat = (std::chrono::steady_clock::time_point*)
		ShmemInitStruct("MemPool Client Last SyncPAT",
						sizeof(std::chrono::steady_clock::time_point),
						found_any, found_all);
	mpc_pa_cnt = (size_t*)
		ShmemInitStruct("MemPool Client PA count",
						sizeof(size_t),
						found_any, found_all);
	mpc_pa_size = (size_t*)
		ShmemInitStruct("MemPool Client PA size",
						(MAX_PAGE_ARRAY_COUNT + 1) * sizeof(size_t),
						found_any, found_all);
	mpc_idx_to_pid = (KeyType*)
		ShmemInitStruct("MemPool Client index-to-PageID map",
						MAX_TOTAL_PAGE_ARRAY_SIZE * sizeof(KeyType),
						found_any, found_all);
	mpc_idx_to_mr = (ibv_mr*)
		ShmemInitStruct("MemPool Client index-to-ibv_mr map",
						MAX_PAGE_ARRAY_COUNT * sizeof(ibv_mr) * 2,
						found_any, found_all);
	is_first_mpc = (bool*)
		ShmemInitStruct("MemPool Client first client flag",
						sizeof(bool),
						found_any, found_all);
    HASHCTL info;
    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(KeyType);
    info.entrysize = sizeof(PATLookupEntry);
    info.num_partitions = PAGE_ARRAY_TABLE_PARTITION_NUM;
	mpc_pid_to_idx = 
		ShmemInitHash("MemPool Client PageID-to-index map",
						MAX_TOTAL_PAGE_ARRAY_SIZE, MAX_TOTAL_PAGE_ARRAY_SIZE,
						&info, HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
	version_map =
		ShmemInitVersionMap("MemPool Client VersionMap",
						1 << 18, 1 << 20);

	if (found_any){
		/* should find all of these, or none of them */
		Assert(found_all);
	}
	else{
		for(int i = 0; i < NUMBER_OF_mempool_client_lw_lock; i++)
			LWLockInitialize(&mempool_client_lw_lock[i], LWTRANCHE_MEMPOOL_CLIENT);
		*node_id_cnt = 0;
		*last_sync_pat = std::chrono::steady_clock::now();
		*mpc_pa_cnt = 0;
		*mpc_pa_size = 0;
		*is_first_mpc = true;
	}
}

Size MemPoolClientShmemSize(void)
{
	Size size = 0;

	size = add_size(size, mul_size(NUMBER_OF_mempool_client_lw_lock, sizeof(LWLock)));

	size = add_size(size, sizeof(size_t));

	size = add_size(size, sizeof(std::chrono::steady_clock::time_point));

	size = add_size(size, sizeof(size_t));

	size = add_size(size, mul_size(MAX_PAGE_ARRAY_COUNT + 1, sizeof(size_t)));

	size = add_size(size, mul_size(MAX_TOTAL_PAGE_ARRAY_SIZE, sizeof(KeyType)));

	size = add_size(size, mul_size(MAX_PAGE_ARRAY_COUNT, sizeof(ibv_mr) * 2));

	size = add_size(size, sizeof(bool));

	size = add_size(size, hash_estimate_size(MAX_TOTAL_PAGE_ARRAY_SIZE, sizeof(PATLookupEntry)));
	
	size = add_size(size, hash_estimate_size_vm(1 << 18, 1 << 22));

	return size;
}

size_t get_MemPoolClient_node_id(){
	size_t id = *node_id_cnt;
	*node_id_cnt += 2;
	return id;
}

void AsyncGetNewestPageAddressTable(){
	LWLockAcquire(mempool_client_sync_pat_lock, LW_EXCLUSIVE);
	*last_sync_pat = std::chrono::steady_clock::now() - std::chrono::duration<int, std::milli>(SyncPAT_Interval_ms);
	LWLockRelease(mempool_client_sync_pat_lock);
}
bool whetherSyncPAT(){
	LWLockAcquire(mempool_client_sync_pat_lock, LW_EXCLUSIVE);
    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
	std::chrono::steady_clock::duration elapsed = now - *last_sync_pat;
	long long elapsed_seconds = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
	if(elapsed_seconds >= SyncPAT_Interval_ms * 0.95){
		*last_sync_pat = now;
		LWLockRelease(mempool_client_sync_pat_lock);
		return true;
	}
	else{
		LWLockRelease(mempool_client_sync_pat_lock);
		return false;
	}
}
