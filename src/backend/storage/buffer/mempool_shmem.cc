#include <chrono>
#include "postgres.h"
#include "storage/GroundDB/mempool_shmem.h"
#include "utils/DSMEngine/hash.h"

LWLock *mempool_client_lw_lock;
size_t *node_id_cnt;

std::chrono::steady_clock::time_point *last_sync_pat;
int64 *mpLocalCnt, *mpMemCnt, *mpStoCnt;

// For PAT
size_t *mpc_pa_cnt, *mpc_pa_size, *mpc_pa_cnt_per_memnode, *mpc_pa_to_memnode, *mpc_memnode_to_pa;
KeyType *mpc_idx_to_pid;
ibv_mr *mpc_idx_to_mr;
HTAB *mpc_pid_to_idx;

HTAB_VM *version_map;
size_t *update_vm_info_ptr;

bool *is_first_mpc, *is_first_mpc_connection;

void* ShmemInitStruct(char* name, size_t size, bool& found_any, bool& found_all){
	bool found;
	auto ptr = ShmemInitStruct(name, size, &found);
	found_any |= found, found_all &= found;
	return ptr;
}
void MemPoolClientShmemInit(){
	bool found_any = false, found_all = true;

	mpLocalCnt = (int64 *)ShmemInitStruct("mpLocalCnt",sizeof(int64),found_any, found_all);
	mpMemCnt = (int64 *)ShmemInitStruct("mpMemCnt",sizeof(int64),found_any, found_all);
	mpStoCnt = (int64 *)ShmemInitStruct("mpStoCnt",sizeof(int64),found_any, found_all);
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
	mpc_pa_cnt_per_memnode = (size_t*)
		ShmemInitStruct("MemPool Client PA count for each MemNode",
						MAX_MEMNODE_NODE * sizeof(size_t),
						found_any, found_all);
	mpc_pa_to_memnode = (size_t*)
		ShmemInitStruct("MemPool Client PA-index-to-MemNode-index map",
						MAX_PAGE_ARRAY_COUNT * sizeof(size_t) * 2,
						found_any, found_all);
	mpc_memnode_to_pa = (size_t*)
		ShmemInitStruct("MemPool Client MemNode-index-to-PA-index map",
						MAX_MEMNODE_NODE * MAX_PAGE_ARRAY_COUNT_PER_MEMNODE * sizeof(size_t),
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
	is_first_mpc_connection = (bool*)
		ShmemInitStruct("MemPool Client first client flag per MemNode connection",
						sizeof(bool) * MAX_MEMNODE_NODE,
						found_any, found_all);
    HASHCTL info;
    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(KeyType);
    info.entrysize = sizeof(PATLookupEntry);
    info.num_partitions = PAGE_ARRAY_TABLE_PARTITION_NUM;
	info.hash = [](const void *key, Size keysize)->uint32 {
		return DSMEngine::Hash((KeyType*)key, 0);
	};
	info.match = [](const void *key1, const void *key2, Size keysize)->int {
		auto k1 = *(KeyType*)key1, k2 = *(KeyType*)key2;
		return k1.SpcID != k2.SpcID || k1.DbID != k2.DbID || k1.RelID != k2.RelID || k1.ForkNum != k2.ForkNum || k1.BlkNum != k2.BlkNum;
	};
	mpc_pid_to_idx = 
		ShmemInitHash("MemPool Client PageID-to-index map",
						MAX_TOTAL_PAGE_ARRAY_SIZE, MAX_TOTAL_PAGE_ARRAY_SIZE,
						&info, HASH_ELEM | HASH_BLOBS | HASH_PARTITION | HASH_FUNCTION | HASH_COMPARE);
    HASHCTL_VM info_vm;
    MemSet(&info_vm, 0, sizeof(info_vm));
	info_vm.hash = info.hash;
	info_vm.match = info.match;
	version_map =
		ShmemInitVersionMap("MemPool Client VersionMap",
						1 << 18, 1 << 20,
						&info_vm, HASH_ELEM | HASH_BLOBS | HASH_FUNCTION | HASH_COMPARE);
	update_vm_info_ptr = (size_t*)
		ShmemInitStruct("MemPool Client VersionMap Info Pointer",
						sizeof(size_t),
						found_any, found_all);

	if (found_any){
		/* should find all of these, or none of them */
		Assert(found_all);
	}
	else{
		for(int i = 0; i < NUMBER_OF_mempool_client_lw_lock; i++)
			LWLockInitialize(&mempool_client_lw_lock[i], LWTRANCHE_MEMPOOL_CLIENT);
		*node_id_cnt = 0;
		*last_sync_pat = std::chrono::steady_clock::now();
		*is_first_mpc = true;
		*mpLocalCnt = *mpMemCnt = *mpStoCnt = 0;
	}
}

Size MemPoolClientShmemSize(void)
{
	Size size = 0;

	size = add_size(size, mul_size(3, sizeof(int64)));

	size = add_size(size, mul_size(NUMBER_OF_mempool_client_lw_lock, sizeof(LWLock)));

	size = add_size(size, sizeof(size_t));

	size = add_size(size, sizeof(std::chrono::steady_clock::time_point));

	size = add_size(size, sizeof(size_t));

	size = add_size(size, mul_size(MAX_PAGE_ARRAY_COUNT + 1, sizeof(size_t)));

	size = add_size(size, mul_size(MAX_MEMNODE_NODE, sizeof(size_t)));

	size = add_size(size, mul_size(MAX_PAGE_ARRAY_COUNT, sizeof(size_t) * 2));

	size = add_size(size, mul_size(mul_size(MAX_MEMNODE_NODE, MAX_PAGE_ARRAY_COUNT_PER_MEMNODE), sizeof(size_t)));

	size = add_size(size, mul_size(MAX_TOTAL_PAGE_ARRAY_SIZE, sizeof(KeyType)));

	size = add_size(size, mul_size(MAX_PAGE_ARRAY_COUNT, sizeof(ibv_mr) * 2));

	size = add_size(size, sizeof(bool));

	size = add_size(size, mul_size(MAX_MEMNODE_NODE, sizeof(bool)));

	size = add_size(size, hash_estimate_size(MAX_TOTAL_PAGE_ARRAY_SIZE, sizeof(PATLookupEntry)));
	
	size = add_size(size, hash_estimate_size_vm(1 << 18, 1 << 22));

	size = add_size(size, sizeof(size_t));

	return size;
}

size_t get_MemPoolClient_node_id(){
	size_t id = *node_id_cnt;
	*node_id_cnt += 2;
	return id;
}

void AsyncGetNewestPageAddressTable(){
	LWLockAcquire(mempool_client_sync_pat_lock, LW_EXCLUSIVE);
	*last_sync_pat = std::chrono::steady_clock::now() - std::chrono::duration<int, std::micro>(SyncPAT_Interval_us);
	LWLockRelease(mempool_client_sync_pat_lock);
}
bool whetherSyncPAT(){
	LWLockAcquire(mempool_client_sync_pat_lock, LW_EXCLUSIVE);
    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
	std::chrono::steady_clock::duration elapsed = now - *last_sync_pat;
	long long elapsed_seconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
	if(elapsed_seconds >= SyncPAT_Interval_us * 0.95){
		*last_sync_pat = now;
		LWLockRelease(mempool_client_sync_pat_lock);
		return true;
	}
	else{
		LWLockRelease(mempool_client_sync_pat_lock);
		return false;
	}
}
