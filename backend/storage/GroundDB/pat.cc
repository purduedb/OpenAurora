#include "storage/GroundDB/rdma.hh"
#include "storage/GroundDB/mempool_shmem.h"

namespace mempool
{

bool KeyTypeEqualFunction::operator() (const KeyType &key1, const KeyType &key2) const {
    return key1.SpcID == key2.SpcID
        && key1.DbID == key2.DbID
        && key1.RelID == key2.RelID
        && key1.ForkNum == key2.ForkNum
        && key1.BlkNum == key2.BlkNum;
}

// todo (te): optimize locking mechanism
size_t PageAddressTable::page_array_count(){
	LWLockAcquire(mempool_client_pat_lock, LW_SHARED);
	auto res = *mpc_pa_cnt;
	LWLockRelease(mempool_client_pat_lock);
	return res;
}
size_t PageAddressTable::page_array_size(size_t pa_idx){
	LWLockAcquire(mempool_client_pat_lock, LW_SHARED);
	auto res = mpc_pa_size[pa_idx + 1] - mpc_pa_size[pa_idx];
	LWLockRelease(mempool_client_pat_lock);
	return res;
}
void PageAddressTable::append_page_array(size_t pa_size, const ibv_mr& pa_mr, const ibv_mr& pida_mr){
	LWLockAcquire(mempool_client_pat_lock, LW_EXCLUSIVE);
	mpc_pa_size[*mpc_pa_cnt + 1] = mpc_pa_size[*mpc_pa_cnt] + pa_size;
	for(size_t i = mpc_pa_size[*mpc_pa_cnt]; i < mpc_pa_size[*mpc_pa_cnt+ 1]; i++)
		mpc_idx_to_pid[i] = nullKeyType;
	mpc_idx_to_mr[*mpc_pa_cnt << 1] = pa_mr;
	mpc_idx_to_mr[*mpc_pa_cnt << 1 | 1] = pida_mr;
	(*mpc_pa_cnt)++;
	LWLockRelease(mempool_client_pat_lock);
}
void PageAddressTable::at(KeyType pid, RDMAReadPageInfo& info){
	LWLockAcquire(mempool_client_pat_lock, LW_SHARED);
    auto *result = (PATLookupEntry*)
		hash_search_with_hash_value(mpc_pid_to_idx,
									&pid,
									get_hash_value(mpc_pid_to_idx, &pid),
									HASH_FIND,
									NULL);
	if(result != NULL){
		info.remote_pa_mr = mpc_idx_to_mr[result->pa_idx << 1];
		info.remote_pida_mr = mpc_idx_to_mr[result->pa_idx << 1 | 1];
		info.pa_ofs = result->pa_ofs;
	}
	else
		info.pa_ofs = -1;
	LWLockRelease(mempool_client_pat_lock);
}
void PageAddressTable::update(size_t pa_idx, size_t pa_ofs, KeyType pid){
	LWLockAcquire(mempool_client_pat_lock, LW_EXCLUSIVE);
	auto& page_id = mpc_idx_to_pid[mpc_pa_size[pa_idx] + pa_ofs];
	if(KeyTypeEqualFunction()(page_id, pid)){
		if(!KeyTypeEqualFunction()(page_id, nullKeyType)){
			auto *result = (PATLookupEntry*)
				hash_search_with_hash_value(mpc_pid_to_idx,
											&page_id,
											get_hash_value(mpc_pid_to_idx, &page_id),
											HASH_REMOVE,
											NULL);
			Assert(result != NULL);
		}
		page_id = pid;
		if(!KeyTypeEqualFunction()(pid, nullKeyType)){
			auto result = (PATLookupEntry*)
				hash_search_with_hash_value(mpc_pid_to_idx,
											&pid,
											get_hash_value(mpc_pid_to_idx, &pid),
											HASH_ENTER,
											NULL);
			result->pa_idx = pa_idx;
			result->pa_ofs = pa_ofs;
		}
	}
	LWLockRelease(mempool_client_pat_lock);
}

} // namespace mempool