#include <mutex>
#include "storage/GroundDB/mempool_client.h"
#include "storage/GroundDB/rdma.hh"
#include "storage/DSMEngine/rdma_manager.h"
#include "utils/version_map.h"

bool MempoolClientReplaying;

namespace mempool{

class MemPoolClient{
public:
    MemPoolClient();
    static MemPoolClient* Get_Instance();
    void AppendToPAT(size_t pa_idx);
    void Disconnect();
	int AccessPageOnMemoryPool(KeyType PageID);
	void GetNewestPageAddressTable();
	int AsyncFlushPageToMemoryPool(char* src, KeyType PageID);
	int SyncFlushPageToMemoryPool(char* src, KeyType PageID);
    static void Clear_Instance(bool disconnect);

    DSMEngine::RDMA_Manager* rdma_mg;
    PageAddressTable pat;
    DSMEngine::ThreadPool* thrd_pool;
};

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
    rdma_mg->Mempool_initialize(DSMEngine::PageArray, BLCKSZ, RECEIVE_OUTSTANDING_SIZE * BLCKSZ);
    rdma_mg->Mempool_initialize(DSMEngine::PageIDArray, sizeof(KeyType), RECEIVE_OUTSTANDING_SIZE * sizeof(KeyType));

	thrd_pool = new DSMEngine::ThreadPool();
    thrd_pool->SetBackgroundThreads(5);

	if(*is_first_mpc){
		*is_first_mpc = false;
    	AppendToPAT(0);
	}
	LWLockRelease(mempool_client_connection_lock);
}

void MemPoolClient::AppendToPAT(size_t pa_idx){
	ibv_mr recv_mr, send_mr;

	rdma_mg->Allocate_Local_RDMA_Slot(recv_mr, DSMEngine::Message);
	rdma_mg->post_receive<DSMEngine::RDMA_Reply>(&recv_mr, 1);
	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.mr_info;
	send_pointer->command = DSMEngine::mr_info_;
	send_pointer->buffer = recv_mr.addr;
	send_pointer->rkey = recv_mr.rkey;
	req->pa_idx = pa_idx;
	rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
	rdma_mg->poll_completion(wc, 1, qp_type, false, 1);

	auto res = &((DSMEngine::RDMA_Reply*)recv_mr.addr)->content.mr_info;
	pat.append_page_array(res->pa_mr.length / BLCKSZ, res->pa_mr, res->pida_mr);

	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
	rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, DSMEngine::Message);
}

static pid_t pid = -1;
static MemPoolClient* client = nullptr;
static std::mutex get_instance_lock;
MemPoolClient* MemPoolClient::Get_Instance(){
    get_instance_lock.lock();
    if (client == nullptr || pid != getpid()){
        client = new MemPoolClient();
		pid = getpid();
	}
    get_instance_lock.unlock();
    return client;
}
void MemPoolClient::Clear_Instance(bool disconnect){
    get_instance_lock.lock();
    if (client != nullptr && pid == getpid()){
        if(disconnect)
            client->Disconnect();
        DSMEngine::RDMA_Manager::Delete_Instance();
        delete client;
        client = nullptr;
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
	client->pat.at(PageID, *rdma_read_info);
	return rdma_read_info->pa_ofs != -1;
}

bool FetchPageFromMemoryPool(char* des, KeyType PageID, RDMAReadPageInfo* rdma_read_info){
	auto rdma_mg = mempool::MemPoolClient::Get_Instance()->rdma_mg;

	ibv_mr pa_mr, pida_mr;
	rdma_mg->Allocate_Local_RDMA_Slot(pa_mr, DSMEngine::PageArray);
	rdma_mg->Allocate_Local_RDMA_Slot(pida_mr, DSMEngine::PageIDArray);
	rdma_mg->RDMA_Read(&rdma_read_info->remote_pa_mr, &pa_mr, rdma_read_info->pa_ofs * BLCKSZ, BLCKSZ, IBV_SEND_SIGNALED, 1, 1, "main");
	rdma_mg->RDMA_Read(&rdma_read_info->remote_pida_mr, &pida_mr, rdma_read_info->pa_ofs * sizeof(KeyType), sizeof(KeyType), IBV_SEND_SIGNALED, 1, 1, "main");
	
	auto res_page = (uint8_t*)pa_mr.addr;
	auto res_id = (KeyType*)pida_mr.addr;
	if(!mempool::KeyTypeEqualFunction()(*res_id, PageID))
		return false;
	memcpy(des, res_page, BLCKSZ);

	rdma_mg->Deallocate_Local_RDMA_Slot(pa_mr.addr, DSMEngine::PageArray);
	rdma_mg->Deallocate_Local_RDMA_Slot(pida_mr.addr, DSMEngine::PageIDArray);
	return true;
}

bool LsnIsSatisfied(XLogRecPtr PageLSN){
	return true;
	// todo (te): consider secondary compute nodes
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
					return;
				else if(current_lsn < item_head->lsn[i] && item_head->lsn[i] <= target_lsn)
					lsn_list.push_back(item_head->lsn[i]);
			result = item_head->next_seg;
			head = false;
		}
		else{
			auto item_head = (ITEMSEG_VM*)result;
			for(int i = 0; i < ITEMSEG_SLOT_CNT_VM; i++)
				if(item_head->lsn[i] == InvalidXLogRecPtr)
					return;
				else if(current_lsn < item_head->lsn[i] && item_head->lsn[i] <= target_lsn)
					lsn_list.push_back(item_head->lsn[i]);
			result = item_head->next_seg;
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
		bool already_locked = LWLockHeldByMeInMode(BufferDescriptorGetContentLock(bufHdr), LW_EXCLUSIVE);
		if(already_locked) LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
		ApplyLSNListToPage(PageID, block, lsn_list);
		if(already_locked) LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_EXCLUSIVE);
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

	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.access_page;
	send_pointer->command = DSMEngine::access_page_;
	req->page_id = PageID;
	rc = rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	rc = rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
	
	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    return rc;
}
void AsyncAccessPageOnMemoryPool(KeyType PageID){
	std::function<void(void *args)> handler = [](void *args){
		auto PageID = (KeyType*)args;
		while(mempool::MemPoolClient::Get_Instance()->AccessPageOnMemoryPool(*PageID))
            mempool::MemPoolClient::Clear_Instance(false);
		delete (KeyType*)args;
	};
	auto a = new KeyType;
	*a = PageID;
    handler((void*)a); // todo (te): asyncly do it with multiprocessing
}
void AsyncRemovePageOnMemoryPool(KeyType PageID){
	std::function<void(void *args)> handler = [](void *args){
		auto PageID = (KeyType*)args;
		while(mempool::MemPoolClient::Get_Instance()->RemovePageOnMemoryPool(*PageID))
            mempool::MemPoolClient::Clear_Instance(false);
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
	for(size_t i = 0, max_i = pat.page_array_count(); i < max_i; i++)
		for(size_t j = 0, max_j = pat.page_array_size(i); j < max_j; j += SYNC_PAT_SIZE){
			rdma_mg->Allocate_Local_RDMA_Slot(recv_mr, DSMEngine::Message);
			rdma_mg->post_receive<DSMEngine::RDMA_Reply>(&recv_mr, 1);
			rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
			auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
			auto req = &send_pointer->content.sync_pat;
			send_pointer->command = DSMEngine::sync_pat_;
			send_pointer->buffer = recv_mr.addr;
			send_pointer->rkey = recv_mr.rkey;
			req->pa_idx = i;
			req->pa_ofs = j;
			rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);

			ibv_wc wc[3] = {};
			std::string qp_type("main");
			rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
			rdma_mg->poll_completion(wc, 1, qp_type, false, 1);
			
			auto res = &((DSMEngine::RDMA_Reply*)recv_mr.addr)->content.sync_pat;
			for(size_t k = 0; j + k < max_j && k < SYNC_PAT_SIZE; k++)
				pat.update(i, j + k, res->page_id_array[k]);

			rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
			rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, DSMEngine::Message);
		}
}

int mempool::MemPoolClient::AsyncFlushPageToMemoryPool(char* src, KeyType PageID){
    int rc = 0;
	auto rdma_mg = this->rdma_mg;
	ibv_mr send_mr;

	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.flush_page;
	send_pointer->command = DSMEngine::async_flush_page_;
	req->page_id = PageID;
	memcpy(req->page_data, src, BLCKSZ);
	rc = rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	rc = rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
	
	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    return rc;
}
int mempool::MemPoolClient::SyncFlushPageToMemoryPool(char* src, KeyType PageID){
    int rc = 0;
	auto rdma_mg = this->rdma_mg;
	ibv_mr recv_mr, send_mr;

	rdma_mg->Allocate_Local_RDMA_Slot(recv_mr, DSMEngine::Message);
	rdma_mg->post_receive<DSMEngine::RDMA_Reply>(&recv_mr, 1);
	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.flush_page;
	send_pointer->command = DSMEngine::sync_flush_page_;
	send_pointer->buffer = recv_mr.addr;
	send_pointer->rkey = recv_mr.rkey;
	req->page_id = PageID;
	memcpy(req->page_data, src, BLCKSZ);
	rc = rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	rc = rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
	rdma_mg->poll_completion(wc, 1, qp_type, false, 1);
	
	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
	rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, DSMEngine::Message);
    return rc;
}
void AsyncFlushPageToMemoryPool(char* src, KeyType PageID){
	while(mempool::MemPoolClient::Get_Instance()->AsyncFlushPageToMemoryPool(src, PageID))
        mempool::MemPoolClient::Clear_Instance(false);
}
void SyncFlushPageToMemoryPool(char* src, KeyType PageID){
	while(mempool::MemPoolClient::Get_Instance()->SyncFlushPageToMemoryPool(src, PageID))
        mempool::MemPoolClient::Clear_Instance(false);
}
void MemPoolmdwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool skipFsync){
    SyncFlushPageToMemoryPool(buffer, (KeyType){
        reln->smgr_rnode.node.spcNode,
        reln->smgr_rnode.node.dbNode,
        reln->smgr_rnode.node.relNode,
        forknum,
        blocknum,
    });
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
			ParseXLogBlocksLsn_vm(state, block_id, lsn);
		}
		else
			Assert(false);
	}
	Assert(remaining == datatotal);
	return;

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
					pfree(blk->data);
				blk->data_bufsz = MAXALIGN(Max(blk->data_len, BLCKSZ));
				blk->data = (char*)palloc(blk->data_bufsz);
			}
			COPY_HEADER_FIELD(blk->data, blk->data_len);
		}
		ParseXLogBlocksLsn_vm(state, block_id, lsn);
	}

	if (state->main_data_len > 0)
	{
		if (!state->main_data || state->main_data_len > state->main_data_bufsz)
		{
			if (state->main_data)
				pfree(state->main_data);
			state->main_data_bufsz = MAXALIGN(Max(state->main_data_len,
												  BLCKSZ / 2));
			state->main_data = (char*)palloc(state->main_data_bufsz);
		}
		COPY_HEADER_FIELD(state->main_data, state->main_data_len);
	}


	// bool parsed = false;
	// switch (record->xl_rmid) {
	// 	case RM_XLOG_ID:
	// 		parsed = vm_xlog_idx_save(state, lsn);
	// 		break;
	// 	case RM_HEAP2_ID:
	// 		parsed = vm_heap2_idx_save(state, lsn);
	// 		break;
	// 	case RM_HEAP_ID:
	// 		parsed = vm_heap_idx_save(state, lsn);
	// 		break;
	// 	case RM_BTREE_ID:
	// 		parsed = vm_btree_idx_save(state, lsn);
	// 		break;
	// 	case RM_HASH_ID:
	// 		parsed = vm_hash_idx_save(state, lsn);
	// 		break;
	// 	case RM_GIN_ID:
	// 		parsed = vm_gin_idx_save(state, lsn);
	// 		break;
	// 	case RM_GIST_ID:
	// 		parsed = vm_gist_idx_save(state, lsn);
	// 		break;
	// 	case RM_SEQ_ID:
	// 		parsed = vm_seq_idx_save(state, lsn);
	// 		break;
	// 	case RM_SPGIST_ID:
	// 		parsed = vm_spg_idx_save(state, lsn);
	// 		break;
	// 	case RM_BRIN_ID:
	// 		parsed = vm_brin_idx_save(state, lsn);
	// 		break;
	// 	case RM_GENERIC_ID:
	// 		parsed = vm_generic_idx_save(state, lsn);
	// 		break;
	// 	default:
	// 		break;
	// }
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
	auto client = mempool::MemPoolClient::Get_Instance();
	while(true){
		if(whetherSyncPAT())
			client->GetNewestPageAddressTable();
		usleep(SyncPAT_Interval_ms / 100);
	}
}