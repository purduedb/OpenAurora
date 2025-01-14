#include <mutex>
#include "storage/GroundDB/mempool_client.h"
#include "storage/GroundDB/rdma.hh"
#include "storage/DSMEngine/rdma_manager.h"
#include "storage/rpcclient.h"
#include "utils/DSMEngine/hash.h"
#include "utils/version_map.h"

extern int IsRpcClient;
extern uint64_t RpcXLogFlushedLsn;
bool MempoolClientReplaying;

namespace mempool{

class MemPoolClient{
public:
    MemPoolClient();
    static MemPoolClient* Get_Instance();
    bool AppendToPAT(size_t memnode_id, size_t pa_idx);
    void Disconnect();
	int AccessPageOnMemoryPool(KeyType PageID);
	int RemovePageOnMemoryPool(KeyType PageID);
	void GetNewestPageAddressTable();
	int AsyncFlushPageToMemoryPool(char* src, KeyType PageID);
	int SyncFlushPageToMemoryPool(char* src, KeyType PageID);
	void FlushXLogInfoToMemoryPool();
	void FetchXLogInfoFromMemoryPool();
    void FlushUpdateVersionMapInfoToMemoryPool(KeyType page_id, XLogRecPtr lsn);
    int FetchUpdateVersionMapInfoFromMemoryPool(size_t info_idx);
    size_t GetFirstUpdateVersionMapInfoIndex();
    static void Clear_Instance(bool disconnect);
    static void Clear_Instance_If_Failed();

    DSMEngine::RDMA_Manager* rdma_mg;
    PageAddressTable pat;
    // todo (te): asyncly do it with multiprocessing
    // DSMEngine::ThreadPool* thrd_pool;

    size_t memnode_cnt;
    std::vector<int> has_failed;
};

static bool first_time_to_connect_to_mempool_server = true;
static std::chrono::steady_clock::time_point last_time_try_connecting_to_mempool_server;

bool time_to_reconnect(){
    return std::chrono::steady_clock::now() - last_time_try_connecting_to_mempool_server > std::chrono::duration<int, std::micro>(TryReconnectionToMemPool_Interval_us);
}

MemPoolClient::MemPoolClient(){
	LWLockAcquire(mempool_client_connection_lock, LW_EXCLUSIVE);
    struct DSMEngine::config_t config = {
            NULL,  /* dev_name */
            122189, /* tcp_port */
            1,	 /* ib_port */
            1, /* gid_idx */
            0,
            0 << 16 | get_MemPoolClient_node_id()};
    rdma_mg = DSMEngine::RDMA_Manager::Get_Instance(&config);
    if (rdma_mg == NULL){
        has_failed.push_back(true);
        goto exit;
    }
    memnode_cnt = rdma_mg->GetMemoryNodeNum();
    for(int i = 0; i < memnode_cnt; i++)
        has_failed.push_back(!rdma_mg->memory_node_status[2 * i + 1]);
    rdma_mg->Mempool_initialize(DSMEngine::PageArray, BLCKSZ, RECEIVE_OUTSTANDING_SIZE * BLCKSZ);
    rdma_mg->Mempool_initialize(DSMEngine::PageIDArray, sizeof(KeyType), RECEIVE_OUTSTANDING_SIZE * sizeof(KeyType));

    // todo (te): asyncly do it with multiprocessing
	// thrd_pool = new DSMEngine::ThreadPool();
    // thrd_pool->SetBackgroundThreads(5);

	if(*is_first_mpc){
        pat.init(memnode_cnt);
        for(int i = 0; i < memnode_cnt; i++)
            is_first_mpc_connection[i] = true;
        for(*update_vm_info_ptr = GetFirstUpdateVersionMapInfoIndex(); !has_failed[0];){ // todo (te): for secondary nodes, need to loop to fetch until reaching the lsn where StartupXLog() starts
            if(FetchUpdateVersionMapInfoFromMemoryPool(*update_vm_info_ptr))
                (*update_vm_info_ptr)++;
            else
                break;
        }
        if(has_failed[0])
            goto exit;
		*is_first_mpc = false;
	}
    for(int i = 0; i < memnode_cnt; i++)
        if(!has_failed[i] && is_first_mpc_connection[i]){
            if(AppendToPAT(i, 0))
                is_first_mpc_connection[i] = false;
        }
exit:
	LWLockRelease(mempool_client_connection_lock);
}

bool MemPoolClient::AppendToPAT(size_t memnode_id, size_t pa_idx){
	ibv_mr recv_mr, send_mr;

	rdma_mg->Allocate_Local_RDMA_Slot(recv_mr, DSMEngine::Message);
	has_failed[memnode_id] |= rdma_mg->post_receive<DSMEngine::RDMA_Reply>(&recv_mr, 2 * memnode_id + 1);
    if(has_failed[memnode_id]) return false;
	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.mr_info;
	send_pointer->command = DSMEngine::mr_info_;
	send_pointer->buffer = recv_mr.addr;
	send_pointer->rkey = recv_mr.rkey;
	req->pa_idx = pa_idx;
	has_failed[memnode_id] |= rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 2 * memnode_id + 1);
    if(has_failed[memnode_id]) return false;

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	has_failed[memnode_id] |= rdma_mg->poll_completion(wc, 1, qp_type, true, 2 * memnode_id + 1);
    if(has_failed[memnode_id]) return false;
	has_failed[memnode_id] |= rdma_mg->poll_completion(wc, 1, qp_type, false, 2 * memnode_id + 1);
    if(has_failed[memnode_id]) return false;

	auto res = &((DSMEngine::RDMA_Reply*)recv_mr.addr)->content.mr_info;
	pat.append_page_array(memnode_id, pa_idx, res->pa_mr.length / BLCKSZ, res->pa_mr, res->pida_mr);

	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
	rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, DSMEngine::Message);
    return true;
}

static pid_t pid = -1;
static MemPoolClient* client = nullptr;
static std::mutex get_instance_lock;
MemPoolClient* MemPoolClient::Get_Instance(){
    Clear_Instance_If_Failed();
    get_instance_lock.lock();
    if (client == nullptr && (first_time_to_connect_to_mempool_server || time_to_reconnect()) || pid != getpid()){
        first_time_to_connect_to_mempool_server = false;
        last_time_try_connecting_to_mempool_server = std::chrono::steady_clock::now();
        client = new MemPoolClient();
		pid = getpid();
        if(client->has_failed[0]){
            DSMEngine::RDMA_Manager::Delete_Instance();
            delete client;
            client = nullptr;
        }
	}
    else if (client != nullptr && time_to_reconnect()){
        last_time_try_connecting_to_mempool_server = std::chrono::steady_clock::now();
        for(int i = 0; i < client->memnode_cnt; i++)
            if(client->has_failed[i]){
                if(client->rdma_mg->Client_Set_Up_One_Connection(2 * i + 1)){
                    client->has_failed[i] = false;
                    if(is_first_mpc_connection[i]){
                        if(client->AppendToPAT(i, 0))
                            is_first_mpc_connection[i] = false;
                    }
                }
                else
                    client->rdma_mg->ClearOneConnection(2 * i + 1);
            }
    }
    get_instance_lock.unlock();
    return client;
}
void MemPoolClient::Clear_Instance(bool disconnect){
    get_instance_lock.lock();
    if (client != nullptr && pid == getpid()){
        if(disconnect)
            client->Disconnect();
        else
            last_time_try_connecting_to_mempool_server = std::chrono::steady_clock::now() - std::chrono::duration<int, std::micro>(TryReconnectionToMemPool_Interval_us);
        DSMEngine::RDMA_Manager::Delete_Instance();
        delete client;
        client = nullptr;
	}
    get_instance_lock.unlock();
}
void MemPoolClient::Clear_Instance_If_Failed(){
    get_instance_lock.lock();
    if(client != nullptr){
        if(client->has_failed[0]){
            get_instance_lock.unlock();
            MemPoolClient::Clear_Instance(false);
            return;
        }
        for(int i = 1; i < client->memnode_cnt; i++)
            if(client->has_failed[i] && !is_first_mpc_connection[i]){
                client->rdma_mg->ClearOneConnection(2 * i + 1);
                is_first_mpc_connection[i] = true;
            }
    }
    get_instance_lock.unlock();
}

} // namespace mempool

void proc_exit_MemPool(){
    mempool::MemPoolClient::Clear_Instance(true);
#ifdef USE_MEMPOOL_STAT
    ReportStatForMemPool();
#endif
}

void ReportStatForMemPool(){
    LWLockAcquire(mempool_client_stat_lock, LW_EXCLUSIVE);
    int64_t tmpLocalCnt = *mpLocalCnt, tmpMemCnt = *mpMemCnt, tmpStoCnt = *mpStoCnt;
    int64_t totalCnt = tmpLocalCnt + tmpMemCnt + tmpStoCnt;
    if(totalCnt != 0)
        fprintf(stderr, "pg_stat: %lld | %lld | %lld | %lld | %.3lf | %.3lf | %.3lf\n",
            totalCnt, tmpLocalCnt, tmpMemCnt, tmpStoCnt,
            (double)tmpLocalCnt / totalCnt,
            (double)tmpMemCnt / totalCnt,
            (double)tmpStoCnt / totalCnt
        );
    LWLockRelease(mempool_client_stat_lock);
}
void ResetStatForMemPool(){
    LWLockAcquire(mempool_client_stat_lock, LW_EXCLUSIVE);
    *mpLocalCnt = *mpMemCnt = *mpStoCnt = 0;
    LWLockRelease(mempool_client_stat_lock);
}

bool PageExistsInMemPool(KeyType PageID, RDMAReadPageInfo* rdma_read_info) {
	auto client = mempool::MemPoolClient::Get_Instance();
    if(client == NULL) return false;
    size_t memnode_id = DSMEngine::Hash(&PageID, 0) % client->memnode_cnt;
    if(client->has_failed[memnode_id]) return false;
	client->pat.at(PageID, *rdma_read_info);
	return rdma_read_info->pa_ofs != -1;
}

bool FetchPageFromMemoryPool(char* des, KeyType PageID, RDMAReadPageInfo* rdma_read_info){
	auto client = mempool::MemPoolClient::Get_Instance();
    if(client == NULL || client->has_failed[rdma_read_info->memnode_id]) return false;
	auto rdma_mg = client->rdma_mg;

    bool failed = false, ret = false;
	ibv_mr pa_mr, pida_mr;
	rdma_mg->Allocate_Local_RDMA_Slot(pa_mr, DSMEngine::PageArray);
	rdma_mg->Allocate_Local_RDMA_Slot(pida_mr, DSMEngine::PageIDArray);
	failed = rdma_mg->RDMA_Read(&rdma_read_info->remote_pa_mr, &pa_mr, rdma_read_info->pa_ofs * BLCKSZ, BLCKSZ, IBV_SEND_SIGNALED, 1, rdma_read_info->memnode_id * 2 + 1, "main");
	if(failed) goto exit;
	failed = rdma_mg->RDMA_Read(&rdma_read_info->remote_pida_mr, &pida_mr, rdma_read_info->pa_ofs * sizeof(KeyType), sizeof(KeyType), IBV_SEND_SIGNALED, 1, rdma_read_info->memnode_id * 2 + 1, "main");
	if(failed) goto exit;

    {
	auto res_page = (uint8_t*)pa_mr.addr;
	auto res_id = (KeyType*)pida_mr.addr;
    ret = mempool::KeyTypeEqualFunction()(*res_id, PageID);
	if (ret)
	    memcpy(des, res_page, BLCKSZ);
    }

exit:
	rdma_mg->Deallocate_Local_RDMA_Slot(pa_mr.addr, DSMEngine::PageArray);
	rdma_mg->Deallocate_Local_RDMA_Slot(pida_mr.addr, DSMEngine::PageIDArray);
    if(failed){
        client->has_failed[rdma_read_info->memnode_id] = true;
        return false;
    }
	return ret;
}

bool LsnIsSatisfied(XLogRecPtr PageLSN, XLogRecPtr TargetLSN){
	return PageLSN <= TargetLSN;
}

void GetLSNListfromVersionMap(KeyType PageID, XLogRecPtr current_lsn, XLogRecPtr target_lsn, std::vector<XLogRecPtr>& lsn_list){
	lsn_list.clear();
	bool found, head;
	auto result = 
		hash_search_vm(version_map, &PageID, HASH_FIND, &found, &head);
	if(!found)
		return;
	while(result != NULL){
		if(head){
			auto item_head = (ITEMHEAD_VM*)result;
			for(int i = 0; i < ITEMHEAD_SLOT_CNT_VM; i++)
				if(item_head->lsn[i] == InvalidXLogRecPtr)
					break;
				else if(current_lsn < item_head->lsn[i] && item_head->lsn[i] <= target_lsn)
					lsn_list.push_back(item_head->lsn[i]);
			result = hash_next_segment_vm(item_head, true);
			head = false;
		}
		else{
			auto item_seg = (ITEMSEG_VM*)result;
			for(int i = 0; i < ITEMSEG_SLOT_CNT_VM; i++)
				if(item_seg->lsn[i] == InvalidXLogRecPtr)
					break;
				else if(current_lsn < item_seg->lsn[i] && item_seg->lsn[i] <= target_lsn)
					lsn_list.push_back(item_seg->lsn[i]);
			result = hash_next_segment_vm(item_seg, false);
		}
	}
	sort(lsn_list.begin(), lsn_list.end());
	return;
}

void ApplyLSNListToPage(KeyType PageID, char* block, std::vector<XLogRecPtr>& lsn_list){
	static bool initialized = false;
	if(!initialized){
    	ReadControlFileTimeLine();
		for(int rmid = 0; rmid <= RM_MAX_ID; rmid++){
			if (RmgrTable[rmid].rm_startup != NULL)
				RmgrTable[rmid].rm_startup();
		}
		initialized = true;
	}
	XLogReaderState *reader_state = NULL;
    void* xlog_reader_private;
    reader_state = XLogReaderAllocateForMemPool(&xlog_reader_private);
    XLogBeginRead(reader_state, InvalidXLogRecPtr);

	Buffer buf;
    BufferTag bufferTag;
	XLogRecord* record;
    INIT_BUFFERTAG(bufferTag, ((RelFileNode){PageID.SpcID, PageID.DbID, PageID.RelID}), (ForkNumber)PageID.ForkNum, PageID.BlkNum);
    for(int i = 0; i < lsn_list.size(); i++) {
		char* err_msg;
        XLogBeginRead(reader_state, lsn_list[i]);
        record = XLogReadRecord(reader_state, &err_msg);
		polar_xlog_decode_data(reader_state);
        buf = InvalidBuffer;

        XLogRedoAction action = BLK_NOTFOUND;
        switch (record->xl_rmid) {
            case RM_XLOG_ID:
                action = polar_xlog_idx_redo(reader_state, &bufferTag, &buf);
                break;
            case RM_HEAP2_ID:
                action = polar_heap2_idx_redo(reader_state, &bufferTag, &buf);
                break;
            case RM_HEAP_ID:
                action = polar_heap_idx_redo(reader_state, &bufferTag, &buf);
                break;
            case RM_BTREE_ID:
                action = polar_btree_redo(reader_state, &bufferTag, &buf);
                break;
            case RM_HASH_ID:
                action = polar_hash_idx_redo(reader_state, &bufferTag, &buf);
                break;
            case RM_GIN_ID:
                action = polar_gin_idx_redo(reader_state, &bufferTag, &buf);
                break;
            case RM_GIST_ID:
                action = polar_gist_idx_redo(reader_state, &bufferTag, &buf);
                break;
            case RM_SEQ_ID:
                action = polar_seq_idx_redo(reader_state, &bufferTag, &buf);
                break;
            case RM_SPGIST_ID:
                action = polar_spg_idx_redo(reader_state, &bufferTag, &buf);
                break;
            case RM_BRIN_ID:
                action = polar_brin_idx_redo(reader_state, &bufferTag, &buf);
                break;
            case RM_GENERIC_ID:
                action = polar_generic_idx_redo(reader_state, &bufferTag, &buf);
                break;
            default:
                printf("%s  didn't find any corresponding polar redo function\n", __func__);
                break;
        }
        if(action == BLK_NOTFOUND) {
            RmgrTable[record->xl_rmid].rm_redo(reader_state);
        } else {
            UnlockReleaseBuffer(buf);
        }
	}
    free(xlog_reader_private);
    XLogReaderFree(reader_state);
}

bool ReplayXLog(KeyType PageID, BufferDesc* bufHdr, char* block, XLogRecPtr current_lsn, XLogRecPtr target_lsn){
    MempoolClientReplaying = true;
	std::vector<XLogRecPtr> lsn_list;
	LWLockAcquire(mempool_client_version_map_lock, LW_SHARED);
	GetLSNListfromVersionMap(PageID, current_lsn, target_lsn, lsn_list);
	LWLockRelease(mempool_client_version_map_lock);
	if(lsn_list.size() > 0){
		ApplyLSNListToPage(PageID, block, lsn_list);
        MempoolClientReplaying = false;
		return true;
	}
	else{
        MempoolClientReplaying = false;
		return false;
    }
}

void mempool::MemPoolClient::Disconnect(){
	auto rdma_mg = this->rdma_mg;
	ibv_mr send_mr;

	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	send_pointer->command = DSMEngine::disconnect_;
	rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
	
	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
}

int mempool::MemPoolClient::AccessPageOnMemoryPool(KeyType PageID){
    int rc = 0;
	auto rdma_mg = this->rdma_mg;
	ibv_mr send_mr;
    size_t memnode_id = DSMEngine::Hash(&PageID, 0) % memnode_cnt;
    if(has_failed[memnode_id]) return -1;

	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.access_page;
	send_pointer->command = DSMEngine::access_page_;
	req->page_id = PageID;
	rc |= rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, memnode_id * 2 + 1);

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	rc |= rdma_mg->poll_completion(wc, 1, qp_type, true, memnode_id * 2 + 1);
	
	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    if(rc) has_failed[memnode_id] = true;
    return rc;
}
int mempool::MemPoolClient::RemovePageOnMemoryPool(KeyType PageID){
    int rc = 0;
	auto rdma_mg = this->rdma_mg;
	ibv_mr send_mr;
    size_t memnode_id = DSMEngine::Hash(&PageID, 0) % memnode_cnt;
    if(has_failed[memnode_id])
        return -1;

	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.remove_page;
	send_pointer->command = DSMEngine::async_remove_page_;
	req->page_id = PageID;
	rc |= rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, memnode_id * 2 + 1);

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	rc |= rdma_mg->poll_completion(wc, 1, qp_type, true, memnode_id * 2 + 1);
	
	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    if(rc) has_failed[memnode_id] = true;
    return rc;
}
void AsyncAccessPageOnMemoryPool(KeyType PageID){
	std::function<void(void *args)> handler = [](void *args){
		auto PageID = (KeyType*)args;
        auto client = mempool::MemPoolClient::Get_Instance();
        if(client == NULL) goto exit;
		client->AccessPageOnMemoryPool(*PageID);
    exit:
		delete (KeyType*)args;
	};
	auto a = new KeyType;
	*a = PageID;
    handler((void*)a); // todo (te): asyncly do it with multiprocessing
}
void AsyncRemovePageOnMemoryPool(KeyType PageID){
	std::function<void(void *args)> handler = [](void *args){
		auto PageID = (KeyType*)args;
        auto client = mempool::MemPoolClient::Get_Instance();
        if(client == NULL) goto exit;
		client->RemovePageOnMemoryPool(*PageID);
    exit:
		delete (KeyType*)args;
	};
	auto a = new KeyType;
	*a = PageID;
    handler((void*)a); // todo (te): asyncly do it with multiprocessing
}

void mempool::MemPoolClient::GetNewestPageAddressTable(){
	auto& pat = this->pat;
	auto rdma_mg = this->rdma_mg;
	ibv_mr recv_mr, send_mr;
	for(size_t i = 0, max_i = pat.page_array_count(); i < max_i; i++){
        size_t memnode_id, memnode_pa_idx;
        pat.get_memnode_id(i, memnode_id, memnode_pa_idx);
        if(has_failed[memnode_id])
            continue;
		for(size_t j = 0, max_j = pat.page_array_size(i); j < max_j; j += SYNC_PAT_SIZE){
			rdma_mg->Allocate_Local_RDMA_Slot(recv_mr, DSMEngine::Message);
			has_failed[memnode_id] |= rdma_mg->post_receive<DSMEngine::RDMA_Reply>(&recv_mr, memnode_id * 2 + 1);
            if(has_failed[memnode_id]) break;
			rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
			auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
			auto req = &send_pointer->content.sync_pat;
			send_pointer->command = DSMEngine::sync_pat_;
			send_pointer->buffer = recv_mr.addr;
			send_pointer->rkey = recv_mr.rkey;
			req->pa_idx = memnode_pa_idx;
			req->pa_ofs = j;
			has_failed[memnode_id] |= rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, memnode_id * 2 + 1);
            if(has_failed[memnode_id]) break;

			ibv_wc wc[3] = {};
			std::string qp_type("main");
			has_failed[memnode_id] |= rdma_mg->poll_completion(wc, 1, qp_type, true, memnode_id * 2 + 1);
            if(has_failed[memnode_id]) break;
			has_failed[memnode_id] |= rdma_mg->poll_completion(wc, 1, qp_type, false, memnode_id * 2 + 1);
            if(has_failed[memnode_id]) break;
			
			auto res = &((DSMEngine::RDMA_Reply*)recv_mr.addr)->content.sync_pat;
			for(size_t k = 0; j + k < max_j && k < SYNC_PAT_SIZE; k++)
				pat.update(i, j + k, res->page_id_array[k]);

			rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
			rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, DSMEngine::Message);
		}
    }
}

int mempool::MemPoolClient::AsyncFlushPageToMemoryPool(char* src, KeyType PageID){
    int rc = 0;
	auto rdma_mg = this->rdma_mg;
	ibv_mr send_mr;
    size_t memnode_id = DSMEngine::Hash(&PageID, 0) % memnode_cnt;
    if(has_failed[memnode_id])
        return -1;

	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.flush_page;
	send_pointer->command = DSMEngine::async_flush_page_;
	req->page_id = PageID;
	memcpy(req->page_data, src, BLCKSZ);
	rc |= rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, memnode_id * 2 + 1);

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	rc |= rdma_mg->poll_completion(wc, 1, qp_type, true, memnode_id * 2 + 1);
	
	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    if(rc) has_failed[memnode_id] = true;
    return rc;
}
int mempool::MemPoolClient::SyncFlushPageToMemoryPool(char* src, KeyType PageID){
    int rc = 0;
	auto rdma_mg = this->rdma_mg;
	ibv_mr recv_mr, send_mr;
    size_t memnode_id = DSMEngine::Hash(&PageID, 0) % memnode_cnt;
    if(has_failed[memnode_id])
        return -1;

	rdma_mg->Allocate_Local_RDMA_Slot(recv_mr, DSMEngine::Message);
	rc |= rdma_mg->post_receive<DSMEngine::RDMA_Reply>(&recv_mr, memnode_id * 2 + 1);
	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.flush_page;
	send_pointer->command = DSMEngine::sync_flush_page_;
	send_pointer->buffer = recv_mr.addr;
	send_pointer->rkey = recv_mr.rkey;
	req->page_id = PageID;
	memcpy(req->page_data, src, BLCKSZ);
	rc |= rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, memnode_id * 2 + 1);

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	rc |= rdma_mg->poll_completion(wc, 1, qp_type, true, memnode_id * 2 + 1);
	rc |= rdma_mg->poll_completion(wc, 1, qp_type, false, memnode_id * 2 + 1);
	
	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
	rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, DSMEngine::Message);
    if(rc) has_failed[memnode_id] = true;
    return rc;
}
void AsyncFlushPageToMemoryPool(char* src, KeyType PageID){
    auto client = mempool::MemPoolClient::Get_Instance();
    if(client == NULL) return;
    client->AsyncFlushPageToMemoryPool(src, PageID);
}
void SyncFlushPageToMemoryPool(char* src, KeyType PageID){
    auto client = mempool::MemPoolClient::Get_Instance();
    if(client == NULL) return;
    client->SyncFlushPageToMemoryPool(src, PageID);
}
void mempool::MemPoolClient::FlushXLogInfoToMemoryPool(){
	ibv_mr recv_mr, send_mr;

	rdma_mg->Allocate_Local_RDMA_Slot(recv_mr, DSMEngine::Message);
	has_failed[0] |= rdma_mg->post_receive<DSMEngine::RDMA_Reply>(&recv_mr, 1);
    if(has_failed[0]) return;
	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.flush_xlog_info;
	send_pointer->command = DSMEngine::flush_xlog_info_;
	send_pointer->buffer = recv_mr.addr;
	send_pointer->rkey = recv_mr.rkey;
    req->xlog_info.valid = true;
	req->xlog_info.RpcXLogFlushedLsn = RpcXLogFlushedLsn;
	req->xlog_info.ProcLastRecPtr = ProcLastRecPtr;
	req->xlog_info.XactLastRecEnd = XactLastRecEnd;
	req->xlog_info.XactLastCommitEnd = XactLastCommitEnd;
    GetLogWrtResult(&req->xlog_info.LogwrtResult_Write, &req->xlog_info.LogwrtResult_Flush);
	has_failed[0] |= rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);
    if(has_failed[0]) return;

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	has_failed[0] |= rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
    if(has_failed[0]) return;
	has_failed[0] |= rdma_mg->poll_completion(wc, 1, qp_type, false, 1);
    if(has_failed[0]) return;

	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
	rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, DSMEngine::Message);
}
void mempool::MemPoolClient::FetchXLogInfoFromMemoryPool(){
	ibv_mr recv_mr, send_mr;

	rdma_mg->Allocate_Local_RDMA_Slot(recv_mr, DSMEngine::Message);
	has_failed[0] |= rdma_mg->post_receive<DSMEngine::RDMA_Reply>(&recv_mr, 1);
    if(has_failed[0]) return;
	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	send_pointer->command = DSMEngine::fetch_xlog_info_;
	send_pointer->buffer = recv_mr.addr;
	send_pointer->rkey = recv_mr.rkey;
	has_failed[0] |= rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);
    if(has_failed[0]) return;

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	has_failed[0] |= rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
    if(has_failed[0]) return;
	has_failed[0] |= rdma_mg->poll_completion(wc, 1, qp_type, false, 1);
    if(has_failed[0]) return;

	auto res = &((DSMEngine::RDMA_Reply*)recv_mr.addr)->content.fetch_xlog_info;
    if(res->xlog_info.valid){
        RpcXLogFlushedLsn = res->xlog_info.RpcXLogFlushedLsn;
        ProcLastRecPtr = res->xlog_info.ProcLastRecPtr;
        XactLastRecEnd = res->xlog_info.XactLastRecEnd;
        XactLastCommitEnd = res->xlog_info.XactLastCommitEnd;
        UpdateLogWrtResult(res->xlog_info.LogwrtResult_Write, res->xlog_info.LogwrtResult_Flush);
    }

	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
	rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, DSMEngine::Message);
}
void mempool::MemPoolClient::FlushUpdateVersionMapInfoToMemoryPool(KeyType page_id, XLogRecPtr lsn){
	ibv_mr recv_mr, send_mr;

	rdma_mg->Allocate_Local_RDMA_Slot(recv_mr, DSMEngine::Message);
	has_failed[0] |= rdma_mg->post_receive<DSMEngine::RDMA_Reply>(&recv_mr, 1);
    if(has_failed[0]) return;
	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.flush_update_vm_info;
	send_pointer->command = DSMEngine::flush_update_vm_info_;
	send_pointer->buffer = recv_mr.addr;
	send_pointer->rkey = recv_mr.rkey;
	req->info.page_id = page_id;
	req->info.lsn = lsn;
	has_failed[0] |= rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);
    if(has_failed[0]) return;

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	has_failed[0] |= rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
    if(has_failed[0]) return;
	has_failed[0] |= rdma_mg->poll_completion(wc, 1, qp_type, false, 1);
    if(has_failed[0]) return;

	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
	rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, DSMEngine::Message);
}
int mempool::MemPoolClient::FetchUpdateVersionMapInfoFromMemoryPool(size_t info_idx){
    int ret = 0;
	ibv_mr recv_mr, send_mr;

	rdma_mg->Allocate_Local_RDMA_Slot(recv_mr, DSMEngine::Message);
	has_failed[0] |= rdma_mg->post_receive<DSMEngine::RDMA_Reply>(&recv_mr, 1);
    if(has_failed[0]) return ret;
	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.fetch_update_vm_info;
	send_pointer->command = DSMEngine::fetch_update_vm_info_;
	send_pointer->buffer = recv_mr.addr;
	send_pointer->rkey = recv_mr.rkey;
    req->ptr = info_idx;
	has_failed[0] |= rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);
    if(has_failed[0]) return ret;

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	has_failed[0] |= rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
    if(has_failed[0]) return ret;
	has_failed[0] |= rdma_mg->poll_completion(wc, 1, qp_type, false, 1);
    if(has_failed[0]) return ret;

	auto res = &((DSMEngine::RDMA_Reply*)recv_mr.addr)->content.fetch_update_vm_info;
	if(!KeyTypeEqualFunction()(res->info.page_id, nullKeyType)){
        ret = 1;
        InsertIntoVersionMap(res->info.page_id, res->info.lsn);
    }

	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
	rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, DSMEngine::Message);
    return ret;
}
size_t mempool::MemPoolClient::GetFirstUpdateVersionMapInfoIndex(){
    int ret = 0;
	ibv_mr recv_mr, send_mr;

	rdma_mg->Allocate_Local_RDMA_Slot(recv_mr, DSMEngine::Message);
	has_failed[0] |= rdma_mg->post_receive<DSMEngine::RDMA_Reply>(&recv_mr, 1);
    if(has_failed[0]) return ret;
	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	send_pointer->command = DSMEngine::get_first_update_vm_info_idx_;
	send_pointer->buffer = recv_mr.addr;
	send_pointer->rkey = recv_mr.rkey;
	has_failed[0] |= rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);
    if(has_failed[0]) return ret;

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	has_failed[0] |= rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
    if(has_failed[0]) return ret;
	has_failed[0] |= rdma_mg->poll_completion(wc, 1, qp_type, false, 1);
    if(has_failed[0]) return ret;

	auto res = &((DSMEngine::RDMA_Reply*)recv_mr.addr)->content.get_first_update_vm_info_idx;
    *update_vm_info_ptr = res->idx;

	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
	rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, DSMEngine::Message);
    return ret;
}
void MemPoolmdwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool skipFsync){
#ifndef MEMPOOL_CACHE_POLICY_DISJOINT
    AsyncFlushPageToMemoryPool(buffer, (KeyType){
        reln->smgr_rnode.node.spcNode,
        reln->smgr_rnode.node.dbNode,
        reln->smgr_rnode.node.relNode,
        forknum,
        blocknum,
    });
#endif
}

void InsertIntoVersionMap(KeyType page_id, XLogRecPtr lsn){
	LWLockAcquire(mempool_client_version_map_lock, LW_EXCLUSIVE);
	bool found, head;
	auto result = 
		hash_search_vm(version_map, &page_id, HASH_ENTER, &found, &head);
	if(head){
		auto item_head = (ITEMHEAD_VM*)result;
		for(int i = 0; i < ITEMHEAD_SLOT_CNT_VM; i++)
			if(item_head->lsn[i] == InvalidXLogRecPtr){
				item_head->lsn[i] = lsn;
				break;
			}
	}
	else{
		auto item_seg = (ITEMSEG_VM*)result;
		for(int i = 0; i < ITEMSEG_SLOT_CNT_VM; i++)
			if(item_seg->lsn[i] == InvalidXLogRecPtr){
				item_seg->lsn[i] = lsn;
				break;
			}
	}
	// can only insert to the end of list
	LWLockRelease(mempool_client_version_map_lock);
}
void ParseXLogBlocksLsn_vm(XLogReaderState *record, int recordBlockId, XLogRecPtr lsn){
	auto& blk = record->blocks[recordBlockId];
	KeyType page_id = {
		blk.rnode.spcNode,
		blk.rnode.dbNode,
		blk.rnode.relNode,
		blk.forknum,
		blk.blkno
	};
    InsertIntoVersionMap(page_id, lsn);
    auto client = mempool::MemPoolClient::Get_Instance();
    if(client == NULL) return;
    client->FlushUpdateVersionMapInfoToMemoryPool(page_id, lsn);
}
void
ResetDecoder(XLogReaderState *state)
{
	int			block_id;

	state->decoded_record = NULL;

	state->main_data_len = 0;

	for (block_id = 0; block_id <= state->max_block_id; block_id++)
	{
		state->blocks[block_id].in_use = false;
		state->blocks[block_id].has_image = false;
		state->blocks[block_id].has_data = false;
		state->blocks[block_id].apply_image = false;
	}
	state->max_block_id = -1;
}
bool vm_xlog_idx_save(XLogReaderState *record, XLogRecPtr lsn);
bool vm_heap2_idx_save(XLogReaderState *record, XLogRecPtr lsn);
bool vm_heap_idx_save(XLogReaderState *record, XLogRecPtr lsn);
bool vm_btree_idx_save(XLogReaderState *record, XLogRecPtr lsn);
bool vm_hash_idx_save(XLogReaderState *record, XLogRecPtr lsn);
bool vm_gin_idx_save(XLogReaderState *record, XLogRecPtr lsn);
bool vm_gist_idx_save(XLogReaderState *record, XLogRecPtr lsn);
bool vm_seq_idx_save(XLogReaderState *record, XLogRecPtr lsn);
bool vm_spg_idx_save(XLogReaderState *record, XLogRecPtr lsn);
bool vm_brin_idx_save(XLogReaderState *record, XLogRecPtr lsn);
bool vm_generic_idx_save(XLogReaderState *record, XLogRecPtr lsn);
void UpdateVersionMap(XLogRecData* rdata, XLogRecPtr lsn){
#define MIN(a, b) ((a) <= (b) ? (a) : (b))
#define COPY_HEADER_FIELD(_dst, _size)								\
	do {															\
		Assert (remaining >= _size);								\
		for(size_t size = _size, ofs = 0; size > 0;){				\
			while(ptr == rdata->data + rdata->len){					\
				rdata = rdata->next;								\
				ptr = rdata->data;									\
			}														\
			size_t s = MIN(size, rdata->data + rdata->len - ptr);	\
			memcpy(_dst + ofs, ptr, s);								\
			ofs += s; 												\
			ptr += s;												\
			size -= s;												\
		}															\
		remaining -= _size;											\
	} while(0)
#define SKIP_HEADER_FIELD(_size)									\
	do {															\
		Assert (remaining >= _size);								\
		for(size_t size = _size; size > 0;){						\
			while(ptr == rdata->data + rdata->len){					\
				rdata = rdata->next;								\
				ptr = rdata->data;									\
			}														\
			size_t s = MIN(size, rdata->data + rdata->len - ptr);	\
			ptr += s;												\
			size -= s;												\
		}															\
		remaining -= _size;											\
	} while(0)

	auto ptr = (char*)rdata->data;
	uint32		remaining;
	uint32		datatotal;
	RelFileNode *rnode = NULL;
	uint8		block_id;
	static XLogReaderState state_;
	XLogReaderState* state = &state_;

	ResetDecoder(state);

	/* we assume that all of the record header is in the first chunk */
	remaining = ((XLogRecord*)rdata->data)->xl_tot_len;
	XLogRecord record;
	memcpy(&record, ptr, sizeof(XLogRecord));
	SKIP_HEADER_FIELD(SizeOfXLogRecord);

	state->decoded_record = &record;
	state->record_origin = InvalidRepOriginId;

	/* Decode the headers */
	datatotal = 0;
	while (remaining > datatotal)
	{
		COPY_HEADER_FIELD(&block_id, sizeof(uint8));

		if (block_id == XLR_BLOCK_ID_DATA_SHORT)
		{
			uint8		main_data_len;
			COPY_HEADER_FIELD(&main_data_len, sizeof(uint8));
			state->main_data_len = main_data_len;
			datatotal += main_data_len;
			break;
		}
		else if (block_id == XLR_BLOCK_ID_DATA_LONG)
		{
			uint32		main_data_len;
			COPY_HEADER_FIELD(&main_data_len, sizeof(uint32));
			state->main_data_len = main_data_len;
			datatotal += main_data_len;
			break;
		}
		else if (block_id == XLR_BLOCK_ID_ORIGIN)
		{
			COPY_HEADER_FIELD(&state->record_origin, sizeof(RepOriginId));
		}
		else if (block_id <= XLR_MAX_BLOCK_ID)
		{
			/* XLogRecordBlockHeader */
			DecodedBkpBlock *blk;
			uint8		fork_flags;

			if (block_id <= state->max_block_id) Assert(false);
			state->max_block_id = block_id;

			blk = &state->blocks[block_id];
			blk->in_use = true;
			blk->apply_image = false;

			COPY_HEADER_FIELD(&fork_flags, sizeof(uint8));
			blk->forknum = (ForkNumber)(fork_flags & BKPBLOCK_FORK_MASK);
			blk->flags = fork_flags;
			blk->has_image = ((fork_flags & BKPBLOCK_HAS_IMAGE) != 0);
			blk->has_data = ((fork_flags & BKPBLOCK_HAS_DATA) != 0);

			COPY_HEADER_FIELD(&blk->data_len, sizeof(uint16));
			/* cross-check that the HAS_DATA flag is set iff data_length > 0 */
			if (blk->has_data && blk->data_len == 0) Assert(false);
			if (!blk->has_data && blk->data_len != 0) Assert(false);
			datatotal += blk->data_len;

			if (blk->has_image)
			{
				COPY_HEADER_FIELD(&blk->bimg_len, sizeof(uint16));
				COPY_HEADER_FIELD(&blk->hole_offset, sizeof(uint16));
				COPY_HEADER_FIELD(&blk->bimg_info, sizeof(uint8));

				blk->apply_image = ((blk->bimg_info & BKPIMAGE_APPLY) != 0);

				if (blk->bimg_info & BKPIMAGE_IS_COMPRESSED)
				{
					if (blk->bimg_info & BKPIMAGE_HAS_HOLE)
						COPY_HEADER_FIELD(&blk->hole_length, sizeof(uint16));
					else
						blk->hole_length = 0;
				}
				else
					blk->hole_length = BLCKSZ - blk->bimg_len;
				datatotal += blk->bimg_len;

				if ((blk->bimg_info & BKPIMAGE_HAS_HOLE) && (blk->hole_offset == 0 || blk->hole_length == 0 || blk->bimg_len == BLCKSZ))
					Assert(false);
				if (!(blk->bimg_info & BKPIMAGE_HAS_HOLE) && (blk->hole_offset != 0 || blk->hole_length != 0))
					Assert(false);
				if ((blk->bimg_info & BKPIMAGE_IS_COMPRESSED) && blk->bimg_len == BLCKSZ)
					Assert(false);
				if (!(blk->bimg_info & BKPIMAGE_HAS_HOLE) && !(blk->bimg_info & BKPIMAGE_IS_COMPRESSED) && blk->bimg_len != BLCKSZ)
					Assert(false);
			}
			if (!(fork_flags & BKPBLOCK_SAME_REL))
			{
				COPY_HEADER_FIELD(&blk->rnode, sizeof(RelFileNode));
				rnode = &blk->rnode;
			}
			else
			{
				if (rnode == NULL) Assert(false);
				blk->rnode = *rnode;
			}
			COPY_HEADER_FIELD(&blk->blkno, sizeof(BlockNumber));
			// ParseXLogBlocksLsn_vm(state, block_id, lsn); // todo (te): remove
		}
		else
			Assert(false);
	}
	Assert(remaining == datatotal);
	// return; // todo (te): remove

	for (block_id = 0; block_id <= state->max_block_id; block_id++)
	{
		DecodedBkpBlock *blk = &state->blocks[block_id];

		if (!blk->in_use)
			continue;

		Assert(blk->has_image || !blk->apply_image);

		if (blk->has_image)
		{
			blk->bkp_image = ptr;
			ptr += blk->bimg_len;
		}
		if (blk->has_data)
		{
			if (!blk->data || blk->data_len > blk->data_bufsz)
			{
				if (blk->data)
					delete blk->data;
				blk->data_bufsz = MAXALIGN(Max(blk->data_len, BLCKSZ));
				blk->data = new char[blk->data_bufsz]();
			}
			COPY_HEADER_FIELD(blk->data, blk->data_len);
		}
		// ParseXLogBlocksLsn_vm(state, block_id, lsn);
	}

	if (state->main_data_len > 0)
	{
		if (!state->main_data || state->main_data_len > state->main_data_bufsz)
		{
			if (state->main_data)
				delete state->main_data;
			state->main_data_bufsz = MAXALIGN(Max(state->main_data_len,
												  BLCKSZ / 2));
			state->main_data = new char[state->main_data_bufsz]();
		}
		COPY_HEADER_FIELD(state->main_data, state->main_data_len);
	}

    polar_xlog_decode_data(state);
    // switch (record->xl_rmid) {
    //     case RM_XLOG_ID:
    //     case RM_SMGR_ID:
    //     case RM_HEAP2_ID:
    //     case RM_HEAP_ID:
    //     case RM_BTREE_ID:
    //     case RM_HASH_ID:
    //     case RM_GIN_ID:
    //     case RM_GIST_ID:
    //     case RM_SEQ_ID:
    //     case RM_SPGIST_ID:
    //     case RM_BRIN_ID:
    //     case RM_GENERIC_ID:
    //         if(state->max_block_id >= 0) {
    //             for(int i = 0; i <= state->max_block_id; i++) {
    //                 if(!state->blocks[i].in_use)
    //                     continue;
    //                 RelKey relKey;
    //                 TransRelNode2RelKey(state->blocks[i].rnode, &relKey, state->blocks[i].forknum);
    //                 uint64_t foundLsn;
    //                 int foundPageNum;
    //                 uint32_t result = -1;
    //                 bool found = GetRelSizeCache(relKey, &result);
    //                 if(found && result<state->blocks[i].blkno+1) {
    //                     InsertRelSizeCache(relKey, state->blocks[i].blkno+1);
    //                 } else if(!found) {
    //                     int baseRelSize = SyncGetRelSize(state->blocks[i].rnode, state->blocks[i].forknum, state->ReadRecPtr);
    //                     if(baseRelSize > state->blocks[i].blkno+1) {
    //                         InsertRelSizeCache(relKey, baseRelSize);
    //                     } else {
    //                         InsertRelSizeCache(relKey, state->blocks[i].blkno+1);
    //                     }
    //                 }
    //             }
    //         }
    //         break;
    //     default:
    //         break;
    // }
	bool parsed = false;
	switch (record.xl_rmid) {
		case RM_XLOG_ID:
			parsed = vm_xlog_idx_save(state, lsn);
			break;
		case RM_HEAP2_ID:
			parsed = vm_heap2_idx_save(state, lsn);
			break;
		case RM_HEAP_ID:
			parsed = vm_heap_idx_save(state, lsn);
			break;
		case RM_BTREE_ID:
			parsed = vm_btree_idx_save(state, lsn);
			break;
		case RM_HASH_ID:
			parsed = vm_hash_idx_save(state, lsn);
			break;
		case RM_GIN_ID:
			parsed = vm_gin_idx_save(state, lsn);
			break;
		case RM_GIST_ID:
			parsed = vm_gist_idx_save(state, lsn);
			break;
		case RM_SEQ_ID:
			parsed = vm_seq_idx_save(state, lsn);
			break;
		case RM_SPGIST_ID:
			parsed = vm_spg_idx_save(state, lsn);
			break;
		case RM_BRIN_ID:
			parsed = vm_brin_idx_save(state, lsn);
			break;
		case RM_GENERIC_ID:
			parsed = vm_generic_idx_save(state, lsn);
			break;
		default:
			break;
	}
}
bool vm_xlog_idx_save(XLogReaderState *record, XLogRecPtr lsn)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	uint8		block_id;

	if (info != XLOG_FPI &&
			info != XLOG_FPI_FOR_HINT)
		return false;

	switch (info)
	{
		case XLOG_FPI:
		case XLOG_FPI_FOR_HINT:
			for (block_id = 0; block_id <= record->max_block_id; block_id++)
                ParseXLogBlocksLsn_vm(record, block_id, lsn);
			break;

		default:
            return false;
	}
    return true;
}
void vm_heap_multi_insert_save(XLogReaderState *record, XLogRecPtr lsn)
{
    xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *)record->main_data;

    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
        ParseXLogBlocksLsn_vm(record, 1, lsn);

    ParseXLogBlocksLsn_vm(record, 0, lsn);
}
void vm_heap_lock_update_save(XLogReaderState *record, XLogRecPtr lsn)
{
    xl_heap_lock_updated *xlrec = (xl_heap_lock_updated *)record->main_data;

    if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
        ParseXLogBlocksLsn_vm(record, 1, lsn);

    ParseXLogBlocksLsn_vm(record, 0, lsn);
}
bool vm_heap2_idx_save(XLogReaderState *record, XLogRecPtr lsn)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK)
    {
        case XLOG_HEAP2_CLEAN:
        case XLOG_HEAP2_FREEZE_PAGE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_HEAP2_CLEANUP_INFO:
            /* don't modify buffer, nothing to do for parse, just do it */
            break;

        case XLOG_HEAP2_VISIBLE:
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_HEAP2_MULTI_INSERT:
            vm_heap_multi_insert_save(record, lsn);
            break;

        case XLOG_HEAP2_LOCK_UPDATED:
            vm_heap_lock_update_save(record, lsn);
            break;

        case XLOG_HEAP2_NEW_CID:
            break;

        case XLOG_HEAP2_REWRITE:
            heap_xlog_logical_rewrite(record); // ?
            break;

        default:
            elog(PANIC, "vm_heap2_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}
void vm_heap_insert_save(XLogReaderState *record, XLogRecPtr lsn)
{
    xl_heap_insert *xlrec = (xl_heap_insert *)record->main_data;

    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
        ParseXLogBlocksLsn_vm(record, 1, lsn);

    ParseXLogBlocksLsn_vm(record, 0, lsn);
}
void vm_heap_delete_save(XLogReaderState *record, XLogRecPtr lsn)
{
    xl_heap_delete *xlrec = (xl_heap_delete *)record->main_data;

    if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
        ParseXLogBlocksLsn_vm(record, 1, lsn);

    ParseXLogBlocksLsn_vm(record, 0, lsn);
}
void vm_heap_xlog_update_save(XLogReaderState *record, bool hotupdate, XLogRecPtr lsn)
{
    BlockNumber oldblk, newblk;
    BufferTag old_cleared_vm, new_cleared_vm;
    xl_heap_update *xlrec = (xl_heap_update *)(record->main_data);

    CLEAR_BUFFERTAG(old_cleared_vm);
    CLEAR_BUFFERTAG(new_cleared_vm);

    XLogRecGetBlockTag(record, 0, NULL, NULL, &newblk);

    if (XLogRecGetBlockTag(record, 1, NULL, NULL, &oldblk))
    {
        /* HOT updates are never done across pages */
        Assert(!hotupdate);
    }
    else
        oldblk = newblk;

    if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
    {
        uint8 vm_block = (oldblk == newblk) ? 2 : 3;
        ParseXLogBlocksLsn_vm(record, vm_block, lsn);
        POLAR_GET_LOG_TAG(record, old_cleared_vm, vm_block);
    }

    ParseXLogBlocksLsn_vm(record, (oldblk == newblk) ? 0 : 1, lsn);

    if (oldblk != newblk)
    {
        ParseXLogBlocksLsn_vm(record, 0, lsn);

        if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
        {
            /* Avoid add the same vm page to logindex twice with the same lsn value */
            POLAR_GET_LOG_TAG(record, new_cleared_vm, 2);

            if (!BUFFERTAGS_EQUAL(old_cleared_vm, new_cleared_vm))
                ParseXLogBlocksLsn_vm(record, 2, lsn);
        }
    }
}
void vm_heap_lock_save(XLogReaderState *record, XLogRecPtr lsn)
{
    xl_heap_lock *xlrec = (xl_heap_lock *)record->main_data;

    if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
        ParseXLogBlocksLsn_vm(record, 1, lsn);

    ParseXLogBlocksLsn_vm(record, 0, lsn);
}
bool vm_heap_idx_save(XLogReaderState *record, XLogRecPtr lsn)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK)
    {
        case XLOG_HEAP_INSERT:
            vm_heap_insert_save(record, lsn);
            break;

        case XLOG_HEAP_DELETE:
            vm_heap_delete_save(record, lsn);
            break;

        case XLOG_HEAP_UPDATE:
            vm_heap_xlog_update_save(record, false, lsn);
            break;

        case XLOG_HEAP_TRUNCATE:
            break;

        case XLOG_HEAP_HOT_UPDATE:
            vm_heap_xlog_update_save(record, true, lsn);
            break;

        case XLOG_HEAP_CONFIRM:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_HEAP_LOCK:
            vm_heap_lock_save(record, lsn);
            break;

        case XLOG_HEAP_INPLACE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        default:
            elog(PANIC, "vm_heap_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}
void vm_btree_xlog_insert_save(bool isleaf, bool ismeta, XLogReaderState *record, XLogRecPtr lsn)
{
    if (!isleaf)
        ParseXLogBlocksLsn_vm(record, 1, lsn);

    ParseXLogBlocksLsn_vm(record, 0, lsn);

    if (ismeta)
        ParseXLogBlocksLsn_vm(record, 2, lsn);
}
void vm_btree_xlog_split_save(XLogReaderState *record, XLogRecPtr lsn)
{
    if (XLogRecHasBlockRef(record, 3))
        ParseXLogBlocksLsn_vm(record, 3, lsn);

    ParseXLogBlocksLsn_vm(record, 1, lsn);
    ParseXLogBlocksLsn_vm(record, 0, lsn);

    if (XLogRecHasBlockRef(record, 2))
        ParseXLogBlocksLsn_vm(record, 2, lsn);
}
void vm_btree_xlog_unlink_page_save(uint8 info, XLogReaderState *record, XLogRecPtr lsn)
{
    ParseXLogBlocksLsn_vm(record, 2, lsn);

    if (XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn_vm(record, 1, lsn);

    ParseXLogBlocksLsn_vm(record, 0, lsn);

    if (XLogRecHasBlockRef(record, 3))
        ParseXLogBlocksLsn_vm(record, 3, lsn);

    if (info == XLOG_BTREE_UNLINK_PAGE_META)
        ParseXLogBlocksLsn_vm(record, 4, lsn);
}
void vm_btree_xlog_newroot_save(XLogReaderState *record, XLogRecPtr lsn)
{
    ParseXLogBlocksLsn_vm(record, 0, lsn);
    if (XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn_vm(record, 1, lsn);
    ParseXLogBlocksLsn_vm(record, 2, lsn);
}
bool vm_btree_idx_save(XLogReaderState *record, XLogRecPtr lsn)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info)
    {
        case XLOG_BTREE_INSERT_LEAF:
            vm_btree_xlog_insert_save(true, false, record, lsn);
            break;

        case XLOG_BTREE_INSERT_UPPER:
            vm_btree_xlog_insert_save(false, false, record, lsn);
            break;

        case XLOG_BTREE_INSERT_META:
            vm_btree_xlog_insert_save(false, true, record, lsn);
            break;

        case XLOG_BTREE_SPLIT_L:
        case XLOG_BTREE_SPLIT_R:
            vm_btree_xlog_split_save(record, lsn);
            break;

        case XLOG_BTREE_INSERT_POST:
            vm_btree_xlog_insert_save(true, false, record, lsn);
            break;
        case XLOG_BTREE_DEDUP:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_BTREE_VACUUM:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_BTREE_DELETE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_BTREE_MARK_PAGE_HALFDEAD:
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_BTREE_UNLINK_PAGE:
        case XLOG_BTREE_UNLINK_PAGE_META:
            vm_btree_xlog_unlink_page_save(info, record, lsn);
            break;

        case XLOG_BTREE_NEWROOT:
            vm_btree_xlog_newroot_save(record, lsn);
            break;

        case XLOG_BTREE_REUSE_PAGE:
            break;

        case XLOG_BTREE_META_CLEANUP:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        default:
            elog(PANIC, "vm_btree_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}
void
vm_hash_xlog_add_ovfl_page_save(XLogReaderState *record, XLogRecPtr lsn)
{
    ParseXLogBlocksLsn_vm(record, 0, lsn);
    ParseXLogBlocksLsn_vm(record, 1, lsn);

    if (XLogRecHasBlockRef(record, 2))
        ParseXLogBlocksLsn_vm(record, 2, lsn);

    if (XLogRecHasBlockRef(record, 3))
        ParseXLogBlocksLsn_vm(record, 3, lsn);

    ParseXLogBlocksLsn_vm(record, 4, lsn);
}
void
vm_hash_xlog_move_page_contents_save(XLogReaderState *record, XLogRecPtr lsn)
{
    if (XLogRecHasBlockRef(record, 0))
    {
        ParseXLogBlocksLsn_vm(record, 0, lsn);
        ParseXLogBlocksLsn_vm(record, 1, lsn);
    }
    else
        ParseXLogBlocksLsn_vm(record, 1, lsn);

    ParseXLogBlocksLsn_vm(record, 2, lsn);
}
void
vm_hash_xlog_squeeze_page_save(XLogReaderState *record, XLogRecPtr lsn)
{
    if (XLogRecHasBlockRef(record, 0))
    {
        ParseXLogBlocksLsn_vm(record, 0, lsn);
        ParseXLogBlocksLsn_vm(record, 1, lsn);
    }
    else
        ParseXLogBlocksLsn_vm(record, 1, lsn);

    ParseXLogBlocksLsn_vm(record, 2, lsn);

    if (XLogRecHasBlockRef(record, 3))
        ParseXLogBlocksLsn_vm(record, 3, lsn);

    if (XLogRecHasBlockRef(record, 4))
        ParseXLogBlocksLsn_vm(record, 4, lsn);

    ParseXLogBlocksLsn_vm(record, 5, lsn);

    if (XLogRecHasBlockRef(record, 6))
        ParseXLogBlocksLsn_vm(record, 6, lsn);
}
void
vm_hash_xlog_delete_save(XLogReaderState *record, XLogRecPtr lsn)
{
    if (XLogRecHasBlockRef(record, 0))
    {
        ParseXLogBlocksLsn_vm(record, 0, lsn);
        ParseXLogBlocksLsn_vm(record, 1, lsn);
    }
    else
        ParseXLogBlocksLsn_vm(record, 1, lsn);
}
bool
vm_hash_idx_save(XLogReaderState *record, XLogRecPtr lsn)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info)
    {
        case XLOG_HASH_INIT_META_PAGE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_HASH_INIT_BITMAP_PAGE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            break;

        case XLOG_HASH_INSERT:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            break;

        case XLOG_HASH_ADD_OVFL_PAGE:
            vm_hash_xlog_add_ovfl_page_save(record, lsn);
            break;

        case XLOG_HASH_SPLIT_ALLOCATE_PAGE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            ParseXLogBlocksLsn_vm(record, 2, lsn);
            break;

        case XLOG_HASH_SPLIT_PAGE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_HASH_SPLIT_COMPLETE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            break;

        case XLOG_HASH_MOVE_PAGE_CONTENTS:
            vm_hash_xlog_move_page_contents_save(record, lsn);
            break;

        case XLOG_HASH_SQUEEZE_PAGE:
            vm_hash_xlog_squeeze_page_save(record, lsn);
            break;

        case XLOG_HASH_DELETE:
            vm_hash_xlog_delete_save(record, lsn);
            break;

        case XLOG_HASH_SPLIT_CLEANUP:
        case XLOG_HASH_UPDATE_META_PAGE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_HASH_VACUUM_ONE_PAGE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            break;

        default:
            elog(PANIC, "vm_hash_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}
void
vm_gin_redo_insert_save(XLogReaderState *record, XLogRecPtr lsn)
{
    if (XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn_vm(record, 1, lsn);

    ParseXLogBlocksLsn_vm(record, 0, lsn);
}
void
vm_gin_redo_split_save(XLogReaderState *record, XLogRecPtr lsn)
{
    if (XLogRecHasBlockRef(record, 3))
        ParseXLogBlocksLsn_vm(record, 3, lsn);

    ParseXLogBlocksLsn_vm(record, 0, lsn);
    ParseXLogBlocksLsn_vm(record, 1, lsn);

    if (XLogRecHasBlockRef(record, 2))
        ParseXLogBlocksLsn_vm(record, 2, lsn);
}
void
vm_gin_redo_update_metapage_save(XLogReaderState *record, XLogRecPtr lsn)
{
    ParseXLogBlocksLsn_vm(record, 0, lsn);

    if (XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn_vm(record, 1, lsn);
}
void
vm_gin_redo_delete_list_pages_save(XLogReaderState *record, XLogRecPtr lsn)
{
    int i = 1;

    ParseXLogBlocksLsn_vm(record, 0, lsn);

    for (i = 1; i <= GIN_NDELETE_AT_ONCE; i++)
    {
        if (XLogRecHasBlockRef(record, i))
            ParseXLogBlocksLsn_vm(record, i, lsn);
        else
            break;
    }
}
bool
vm_gin_idx_save(XLogReaderState *record, XLogRecPtr lsn)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info)
    {
        case XLOG_GIN_CREATE_PTREE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_GIN_INSERT:
            vm_gin_redo_insert_save(record, lsn);
            break;

        case XLOG_GIN_SPLIT:
            vm_gin_redo_split_save(record, lsn);
            break;

        case XLOG_GIN_VACUUM_PAGE:
        case XLOG_GIN_VACUUM_DATA_LEAF_PAGE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_GIN_DELETE_PAGE:
            ParseXLogBlocksLsn_vm(record, 2, lsn);
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            break;

        case XLOG_GIN_UPDATE_META_PAGE:
            vm_gin_redo_update_metapage_save(record, lsn);
            break;

        case XLOG_GIN_INSERT_LISTPAGE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_GIN_DELETE_LISTPAGE:
            vm_gin_redo_delete_list_pages_save(record, lsn);
            break;

        default:
            return false;
            elog(PANIC, "vm_gin_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}
void
vm_gist_redo_page_update_record_save(XLogReaderState *record, XLogRecPtr lsn)
{
    ParseXLogBlocksLsn_vm(record, 0, lsn);

    if (XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn_vm(record, 1, lsn);
}
void
vm_gist_redo_page_split_record_save(XLogReaderState *record, XLogRecPtr lsn)
{
    int block_id;

    ParseXLogBlocksLsn_vm(record, 1, lsn);

    for (block_id = 2; block_id <= XLR_MAX_BLOCK_ID; block_id++)
    {
        if (XLogRecHasBlockRef(record, block_id))
            ParseXLogBlocksLsn_vm(record, block_id, lsn);
        else
            break;
    }

    if (XLogRecHasBlockRef(record, 0))
        ParseXLogBlocksLsn_vm(record, 0, lsn);
}
bool
vm_gist_idx_save(XLogReaderState *record, XLogRecPtr lsn) {
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    switch (info)
    {
        case XLOG_GIST_PAGE_UPDATE:
            vm_gist_redo_page_update_record_save(record, lsn);
            break;
        case XLOG_GIST_DELETE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;
        case XLOG_GIST_PAGE_REUSE:
            // todo
			// gistRedoPageReuse(record);
            break;
        case XLOG_GIST_PAGE_SPLIT:
            vm_gist_redo_page_split_record_save(record, lsn);
            break;
        case XLOG_GIST_PAGE_DELETE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            break;
        case XLOG_GIST_ASSIGN_LSN:
            /* nop. See gistGetFakeLSN(). */
            break;
        default:
            elog(PANIC, "gist_redo: unknown op code %u", info);
    }

    return true;
}

bool
vm_seq_idx_save( XLogReaderState *record, XLogRecPtr lsn)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_SEQ_LOG:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
			break;

		default:
			elog(PANIC, "vm_seq_idx_save: unknown op code %u", info);
			break;
	}
    return true;
}
void
vm_spg_redo_pick_split_save(XLogReaderState *record, XLogRecPtr lsn)
{
    if (XLogRecHasBlockRef(record, 0))
        ParseXLogBlocksLsn_vm(record, 0, lsn);

    if (XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn_vm(record, 1, lsn);

    ParseXLogBlocksLsn_vm(record, 2, lsn);

    if (XLogRecHasBlockRef(record, 3))
        ParseXLogBlocksLsn_vm(record, 3, lsn);
}
void
vm_spg_redo_add_node_save(XLogReaderState *record, XLogRecPtr lsn)
{
    if (!XLogRecHasBlockRef(record, 1))
        ParseXLogBlocksLsn_vm(record, 0, lsn);
    else
    {
        ParseXLogBlocksLsn_vm(record, 1, lsn);
        ParseXLogBlocksLsn_vm(record, 0, lsn);

        if (XLogRecHasBlockRef(record, 2))
            ParseXLogBlocksLsn_vm(record, 2, lsn);
    }
}
bool
vm_spg_idx_save(XLogReaderState *record, XLogRecPtr lsn)
{
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info)
    {
        case XLOG_SPGIST_ADD_LEAF:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            if (XLogRecHasBlockRef(record, 1))
                ParseXLogBlocksLsn_vm(record, 1, lsn);
            break;

        case XLOG_SPGIST_MOVE_LEAFS:
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            ParseXLogBlocksLsn_vm(record, 2, lsn);
            break;

        case XLOG_SPGIST_ADD_NODE:
            vm_spg_redo_add_node_save(record, lsn);
            break;

        case XLOG_SPGIST_SPLIT_TUPLE:
            if (XLogRecHasBlockRef(record, 1))
                ParseXLogBlocksLsn_vm(record, 1, lsn);

            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        case XLOG_SPGIST_PICKSPLIT:
            vm_spg_redo_pick_split_save(record, lsn);
            break;

        case XLOG_SPGIST_VACUUM_LEAF:
        case XLOG_SPGIST_VACUUM_ROOT:
        case XLOG_SPGIST_VACUUM_REDIRECT:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            break;

        default:
            elog(PANIC, "vm_spg_idx_save: unknown op code %u", info);
            break;
    }
    return true;
}
bool
vm_brin_idx_save(XLogReaderState *record, XLogRecPtr lsn) {
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info & XLOG_BRIN_OPMASK)
    {
        case XLOG_BRIN_CREATE_INDEX:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            return true;

        case XLOG_BRIN_INSERT:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            return true;

        case XLOG_BRIN_UPDATE:
            ParseXLogBlocksLsn_vm(record, 2, lsn);
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            return true;

        case XLOG_BRIN_SAMEPAGE_UPDATE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            return true;

        case XLOG_BRIN_REVMAP_EXTEND:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            return true;

        case XLOG_BRIN_DESUMMARIZE:
            ParseXLogBlocksLsn_vm(record, 0, lsn);
            ParseXLogBlocksLsn_vm(record, 1, lsn);
            return true;

        default:
            return false;
            elog(PANIC, "vm_brin_idx_save: unknown op code %u", info);
    }
    return false;
}
bool
vm_generic_idx_save(XLogReaderState *record, XLogRecPtr lsn)
{
    int block_id;

    for (block_id = 0; block_id <= MAX_GENERIC_XLOG_PAGES; block_id++)
    {
        if (XLogRecHasBlockRef(record, block_id))
            ParseXLogBlocksLsn_vm(record, block_id, lsn);
    }
    return true;
}

void MemPoolSyncMain(){
    int SyncToStorageHashMapId = RpcRegisterSecondaryNode(IsRpcClient == 2, GetLogWrtResultLsn());

    size_t interval_us[4] = {CheckSyncPAT_Interval_us, SyncXLogInfo_Interval_us, SyncUpdateVersionMapInfo_Interval_us, HashMapComputeNodeHearbeatInterval_us};
    size_t min_interval_us = interval_us[0];
    for(int i = 0; i < 4; i++)
        min_interval_us = std::min(min_interval_us, interval_us[i]);
	std::chrono::steady_clock::duration interval[3];
    for(int i = 0; i < 4; i++)
        interval[i] = std::chrono::duration<int, std::micro>(interval_us[i]);

    std::chrono::steady_clock::time_point last[3];
    for(int i = 0; i < 4; i++)
        last[i] = std::chrono::steady_clock::now() - interval[i];
        
    std::chrono::steady_clock::time_point now;
    while(true){
        now = std::chrono::steady_clock::now();
        if(now - last[0] >= interval[0]){
            last[0] = now;
            if(whetherSyncPAT()){
                auto client = mempool::MemPoolClient::Get_Instance();
                if(client == NULL) goto skip_mempool_sync;
                client->GetNewestPageAddressTable();
            }
        }

        if(IsRpcClient == 2){
            now = std::chrono::steady_clock::now();
            if(now - last[1] >= interval[1]){
                last[1] = now;
                auto client = mempool::MemPoolClient::Get_Instance();
                if(client == NULL) goto skip_mempool_sync;
                client->FlushXLogInfoToMemoryPool();
            }
        }

        if(IsRpcClient == 3){
            now = std::chrono::steady_clock::now();
            if(now - last[1] >= interval[1]){
                last[1] = now;
                auto client = mempool::MemPoolClient::Get_Instance();
                if(client == NULL) goto skip_mempool_sync;
                client->FetchXLogInfoFromMemoryPool();
            }

            // todo (te): to remove; update interval_us
            // now = std::chrono::steady_clock::now();
            // if(now - last[2] >= interval[2]){
            //     last[2] = now;
            //     while(true){
            //         auto client = mempool::MemPoolClient::Get_Instance();
            //         if(client == NULL) goto skip_mempool_sync;
            //         if(client->FetchUpdateVersionMapInfoFromMemoryPool(*update_vm_info_ptr))
            //             (*update_vm_info_ptr)++;
            //         else
            //             break;
            //     }
            // }
        }

skip_mempool_sync:
        now = std::chrono::steady_clock::now();
        if(now - last[3] >= interval[3]){
            last[3] = now;
            RpcSecondaryNodeUpdatesLsn(SyncToStorageHashMapId, GetLogWrtResultLsn());
        }
        usleep(min_interval_us);
    }
}