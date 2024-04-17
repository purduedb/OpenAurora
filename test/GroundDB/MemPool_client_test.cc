#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "storage/GroundDB/rdma_server.hh"

int main(int argc, char** argv) {
    uint32_t tcp_port = 19843;
    struct DSMEngine::config_t config = {
            NULL,  /* dev_name */
            tcp_port, /* tcp_port */
            1,	 /* ib_port */
            1, /* gid_idx */
            0,
            0};
    auto rdma_mg = DSMEngine::RDMA_Manager::Get_Instance(&config);
    rdma_mg->Mempool_initialize(DSMEngine::PageArray, BLCKSZ, RECEIVE_OUTSTANDING_SIZE * BLCKSZ);
    rdma_mg->Mempool_initialize(DSMEngine::PageIDArray, sizeof(KeyType), RECEIVE_OUTSTANDING_SIZE * sizeof(KeyType));

    ibv_mr recv_mr[RECEIVE_OUTSTANDING_SIZE] = {};
    for(int i = 0; i < RECEIVE_OUTSTANDING_SIZE; i++)
        rdma_mg->Allocate_Local_RDMA_Slot(recv_mr[i], DSMEngine::Message);

	uint8_t page_data[BLCKSZ];
    int buffer_position = 0;
    
    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
    for(int i=0; i<100000; i++){
        rdma_mg->post_receive<DSMEngine::RDMA_Reply>(&recv_mr[buffer_position], 1);
        
        ibv_mr send_mr;
        rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
        auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
        auto req = &send_pointer->content.flush_page;
        send_pointer->command = DSMEngine::flush_page_;
        send_pointer->buffer = recv_mr[buffer_position].addr;
        send_pointer->rkey = recv_mr[buffer_position].rkey;
        req->page_id = KeyType{0, 0, 0, 0, i};
        memcpy(req->page_data, page_data, BLCKSZ);
        rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);

        ibv_wc wc[3] = {};
        std::string qp_type("main");
        rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
        rdma_mg->poll_completion(wc, 1, qp_type, false, 1);
        
        auto res = (DSMEngine::RDMA_Reply*)recv_mr[buffer_position].addr;
        buffer_position = (buffer_position + 1) % RECEIVE_OUTSTANDING_SIZE;
        rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        std::chrono::steady_clock::duration elapsed = end - start;
        long long elapsed_seconds = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
        if(i%10000 == 0)printf("Flushed %d pages;  Throughput: %lf pages/ms\n",i,(double)i/elapsed_seconds);
    }
    ibv_mr remote_pa_mr, remote_pida_mr;
    for(int i=0; i<1; i++){
        rdma_mg->post_receive<DSMEngine::RDMA_Reply>(&recv_mr[buffer_position], 1);
        
        ibv_mr send_mr;
        rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
        auto send_pointer = (DSMEngine::RDMA_Request*)send_mr.addr;
        auto req = &send_pointer->content.mr_info;
        send_pointer->command = DSMEngine::mr_info_;
        send_pointer->buffer = recv_mr[buffer_position].addr;
        send_pointer->rkey = recv_mr[buffer_position].rkey;
        req->pa_idx = 0;
        rdma_mg->post_send<DSMEngine::RDMA_Request>(&send_mr, 1);

        ibv_wc wc[3] = {};
        std::string qp_type("main");
        rdma_mg->poll_completion(wc, 1, qp_type, true, 1);
        rdma_mg->poll_completion(wc, 1, qp_type, false, 1);
        
        auto res = &((DSMEngine::RDMA_Reply*)recv_mr[buffer_position].addr)->content.mr_info;
        memcpy(&remote_pa_mr, &res->pa_mr, sizeof(ibv_mr));
        memcpy(&remote_pida_mr, &res->pida_mr, sizeof(ibv_mr));
        buffer_position = (buffer_position + 1) % RECEIVE_OUTSTANDING_SIZE;
        rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    }
    start = std::chrono::steady_clock::now();
    for(int i=0; i<100000; i++){
        ibv_mr pa_mr, pida_mr;
        rdma_mg->Allocate_Local_RDMA_Slot(pa_mr, DSMEngine::PageArray);
        rdma_mg->Allocate_Local_RDMA_Slot(pida_mr, DSMEngine::PageIDArray);
        rdma_mg->RDMA_Read(&remote_pa_mr, &pa_mr, (i%(1<<15)) * sizeof(BLCKSZ), sizeof(BLCKSZ), IBV_SEND_SIGNALED, 1, 1, "main");
        rdma_mg->RDMA_Read(&remote_pida_mr, &pida_mr, (i%(1<<15)) * sizeof(KeyType), sizeof(KeyType), IBV_SEND_SIGNALED, 1, 1, "main");
        
        auto res_page = (uint8_t*)pa_mr.addr;
        auto res_id = (KeyType*)pida_mr.addr;
        memcpy(page_data, res_page, BLCKSZ);
        // if(i%1000 == 0)printf("One-Sided Read Page ID: %ld %ld %ld %ld %ld\n", res_id->SpcID, res_id->DbID, res_id->RelID, res_id->ForkNum, res_id->BlkNum);
        rdma_mg->Deallocate_Local_RDMA_Slot(pa_mr.addr, DSMEngine::PageArray);
        rdma_mg->Deallocate_Local_RDMA_Slot(pida_mr.addr, DSMEngine::PageIDArray);
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        std::chrono::steady_clock::duration elapsed = end - start;
        long long elapsed_seconds = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
        if(i%1000 == 0)printf("RDMA-Read %d pages;  Throughput: %lf pages/ms\n",i,(double)i/elapsed_seconds);
    }
    return 0;
}