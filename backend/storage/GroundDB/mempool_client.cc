#include <mutex>
#include "storage/GroundDB/mempool_client.h"
#include "storage/GroundDB/rdma.hh"
#include "storage/DSMEngine/rdma_manager.h"
#include "utils/version_map.h"

namespace mempool{

class MemPoolClient{
public:
    MemPoolClient();
    static MemPoolClient* Get_Instance();
    void AppendToPAT(size_t pa_idx);
	void AccessPageOnMemoryPool(KeyType PageID);
	void GetNewestPageAddressTable();
	void FlushPageToMemoryPool(char* src, KeyType PageID);

    std::shared_ptr<DSMEngine::RDMA_Manager> rdma_mg;
    std::vector<std::thread> threads;
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
    rdma_mg = std::shared_ptr<DSMEngine::RDMA_Manager>(DSMEngine::RDMA_Manager::Get_Instance(&config));
    rdma_mg->Mempool_initialize(DSMEngine::PageArray, BLCKSZ, RECEIVE_OUTSTANDING_SIZE * BLCKSZ);
    rdma_mg->Mempool_initialize(DSMEngine::PageIDArray, sizeof(KeyType), RECEIVE_OUTSTANDING_SIZE * sizeof(KeyType));

	thrd_pool = new DSMEngine::ThreadPool();
    thrd_pool->SetBackgroundThreads(5);

	if(*is_first_mpc){
		*is_first_mpc = false;
    	AppendToPAT(0);
	}
	LWLockRelease(mempool_client_connection_lock);

	// todo (te): switch to another process instead of thread.
	threads.emplace_back([this]{
		while(true){
			std::function<void(void *args)> handler = [this](void *args){
				if(whetherSyncPAT())
					this->GetNewestPageAddressTable();
			};
			thrd_pool->Schedule(std::move(handler), (void*)nullptr);
			usleep(SyncPAT_Interval_ms);
		}
	});
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

MemPoolClient* MemPoolClient::Get_Instance(){
	static pid_t pid = -1;
    static MemPoolClient* client = nullptr;
    static std::mutex lock;
    lock.lock();
    if (client == nullptr || pid != getpid()){
        client = new MemPoolClient();
		pid = getpid();
	}
    lock.unlock();
    return client;
}

} // namespace mempool


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
	rdma_mg->RDMA_Read(&rdma_read_info->remote_pa_mr, &pa_mr, rdma_read_info->pa_ofs * sizeof(BLCKSZ), sizeof(BLCKSZ), IBV_SEND_SIGNALED, 1, 1, "main");
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

bool LsnIsSatisfied(PageXLogRecPtr PageLSN){
	uint64_t pagelsn = PageXLogRecPtrGet(PageLSN);
	return true;
	// todo (te): consider secondary compute nodes
}

void ReplayXLog(){
	// todo (te):
}

void mempool::MemPoolClient::AccessPageOnMemoryPool(KeyType PageID){
	auto rdma_mg = this->rdma_mg;
	ibv_mr send_mr;

	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.access_page;
	send_pointer->command = DSMEngine::access_page_;
	req->page_id = PageID;
	rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
	
	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
}
void AsyncAccessPageOnMemoryPool(KeyType PageID){
	std::function<void(void *args)> handler = [](void *args){
		auto PageID = (KeyType*)args;
		mempool::MemPoolClient::Get_Instance()->AccessPageOnMemoryPool(*PageID);
	};
	mempool::MemPoolClient::Get_Instance()->thrd_pool->Schedule(std::move(handler), (void*)&PageID);
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
			for(size_t k = 0; j + k < pat.page_array_size(i) && k < SYNC_PAT_SIZE; k++)
				pat.update(i, j + k, res->page_id_array[k]);

			rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
			rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr.addr, DSMEngine::Message);
		}
}
void AsyncGetNewestPageAddressTable(){
	std::function<void(void *args)> handler = [](void *args){
		mempool::MemPoolClient::Get_Instance()->GetNewestPageAddressTable();
	};
	mempool::MemPoolClient::Get_Instance()->thrd_pool->Schedule(std::move(handler), (void*)nullptr);
}

void mempool::MemPoolClient::FlushPageToMemoryPool(char* src, KeyType PageID){
	auto rdma_mg = this->rdma_mg;
	ibv_mr send_mr;

	rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
	auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
	auto req = &send_pointer->content.flush_page;
	send_pointer->command = DSMEngine::flush_page_;
	req->page_id = PageID;
	memcpy(req->page_data, src, BLCKSZ);
	rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);

	ibv_wc wc[3] = {};
	std::string qp_type("main");
	rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
	
	rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
}
void AsyncFlushPageToMemoryPool(char* src, KeyType PageID){
	struct Args{char src[BLCKSZ]; KeyType PageID;};
	std::function<void(void *args)> handler = [](void *args){
		auto a = (struct Args*)args;
		mempool::MemPoolClient::Get_Instance()->FlushPageToMemoryPool(a->src, a->PageID);
	};
	struct Args a = {.PageID = PageID};
	memcpy(a.src, src, BLCKSZ);
	mempool::MemPoolClient::Get_Instance()->thrd_pool->Schedule(std::move(handler), (void*)&a);
}

void UpdateVersionMap(XLogRecData* rdata, XLogRecPtr lsn){
	return; // todo (te): debug
#define MIN(a, b) ((a) <= (b) ? (a) : (b))
#define COPY_HEADER_FIELD(_dst, _size)								\
	do {															\
		Assert (remaining >= _size);								\
		for(size_t size = _size; size > 0;){						\
			while(ptr == rdata->data + rdata->len){					\
				rdata = rdata->next;								\
				ptr = rdata->data;									\
			}														\
			size_t s = MIN(size, rdata->data + rdata->len - ptr);	\
			memcpy(_dst, ptr, s);									\
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
	int max_block_id = -1;
	DecodedBkpBlock blk[0];

	/* we assume that all of the record header is in the first chunk */
	remaining = ((XLogRecord*)rdata->data)->xl_tot_len;
	SKIP_HEADER_FIELD(SizeOfXLogRecord);

	/* Decode the headers */
	datatotal = 0;
	while (remaining > datatotal)
	{
		COPY_HEADER_FIELD(&block_id, sizeof(uint8));

		if (block_id == XLR_BLOCK_ID_DATA_SHORT)
		{
			uint8		main_data_len;
			COPY_HEADER_FIELD(&main_data_len, sizeof(uint8));
			datatotal += main_data_len;
			break;
		}
		else if (block_id == XLR_BLOCK_ID_DATA_LONG)
		{
			uint32		main_data_len;
			COPY_HEADER_FIELD(&main_data_len, sizeof(uint32));
			datatotal += main_data_len;
			break;
		}
		else if (block_id == XLR_BLOCK_ID_ORIGIN)
		{
			SKIP_HEADER_FIELD(sizeof(RepOriginId));
		}
		else if (block_id <= XLR_MAX_BLOCK_ID)
		{
			/* XLogRecordBlockHeader */
			uint8		fork_flags;

			if (block_id <= max_block_id) Assert(false);
			max_block_id = block_id;

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

			KeyType page_id = {
				blk->rnode.spcNode,
				blk->rnode.dbNode,
				blk->rnode.relNode,
				blk->forknum,
				blk->blkno
			};
			LWLockAcquire(mempool_client_version_map_lock, LW_EXCLUSIVE);
			bool found, head;
			auto result = 
				hash_search_vm(version_map, &page_id, HASH_ENTER, &found, &head);
			if(head){
				auto item_head = (ITEMHEAD_VM*)result;
				for(int i = 0; i < SLOT_CNT_VM; i++)
					if(item_head->lsn[i] == InvalidXLogRecPtr){
						item_head->lsn[i] = lsn;
						break;
					}
			}
			else{
				auto item_head = (ITEMSEG_VM*)result;
				for(int i = 0; i < SLOT_CNT_VM; i++)
					if(item_head->lsn[i] == InvalidXLogRecPtr){
						item_head->lsn[i] = lsn;
						break;
					}
			}
			// can only insert to the end of list
			LWLockRelease(mempool_client_version_map_lock);
		}
		else
			Assert(false);
	}
	Assert(remaining == datatotal);
}