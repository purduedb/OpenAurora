#include <fstream>
#include <thread>
#include "c.h"
#include "storage/checksum_impl.h"
#include "storage/bufpage.h"
#include "storage/GroundDB/mempool_server.h"
#include "storage/GroundDB/rdma_server.hh"
#include "storage/DSMEngine/ThreadPool.h"
#include "storage/DSMEngine/cache.h"
#include "storage/GroundDB/request_buffer.h"

namespace mempool {

typedef struct request_handler_args{
	DSMEngine::RDMA_Request request;
	std::string client_ip;
	uint16_t compute_node_id;
} request_handler_args;

void MemPoolManager::init_rdma_manager(int pr_s, DSMEngine::config_t &config){
    pr_size = pr_s;
    rdma_mg = std::make_shared<DSMEngine::RDMA_Manager>(config);

    // Set up the connection information.
    rdma_mg->compute_nodes.insert({0, ""});

    if (rdma_mg->resources_create())
        fprintf(stderr, "failed to create resources\n");
    int rc;
    if (rdma_mg->rdma_config.gid_idx >= 0) {
        rc = ibv_query_gid(rdma_mg->res->ib_ctx, rdma_mg->rdma_config.ib_port,
            rdma_mg->rdma_config.gid_idx,
            &(rdma_mg->res->my_gid));
        if (rc) {
            fprintf(stderr, "could not get gid for port %d, index %d\n",
                rdma_mg->rdma_config.ib_port, rdma_mg->rdma_config.gid_idx);
            return;
        }
    }
    else
        memset(&(rdma_mg->res->my_gid), 0, sizeof rdma_mg->res->my_gid);
}

void MemPoolManager::Server_to_Client_Communication() {
    server_sock_connect(nullptr, rdma_mg->rdma_config.tcp_port);
}
int MemPoolManager::server_sock_connect(const char* servername, int port) {
    struct addrinfo* resolved_addr = NULL;
    struct addrinfo* iterator;
    char service[10];
    int sockfd = -1;
    int listenfd = 0;
    struct sockaddr address;
    socklen_t len = sizeof(struct sockaddr);
    struct addrinfo hints = {
        .ai_flags = AI_PASSIVE, .ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
    if (sprintf(service, "%d", port) < 0) goto sock_connect_exit;
    /* Resolve DNS address, use sockfd as temp storage */
    sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
    if (sockfd < 0) {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
        goto sock_connect_exit;
    }

    /* Search through results and find the one we want */
    for (iterator = resolved_addr; iterator; iterator = iterator->ai_next) {
        sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
        int option = 1;
        setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,&option,sizeof(int));
        if (sockfd >= 0) {
            /* Server mode. Set up listening socket an accept a connection */
            listenfd = sockfd;
            sockfd = -1;
            if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
                goto sock_connect_exit;
            fprintf(stderr, "MemPool Server is ready...\n");
            listen(listenfd, 20);
            while (!exit_all_threads_) {
                sockfd = accept(listenfd, &address, &len);
                std::string client_id = std::string(inet_ntoa(((struct sockaddr_in*)(&address))->sin_addr))
                    + ":" + std::to_string(((struct sockaddr_in*)(&address))->sin_port);
                // Client id must be composed of ip address and port number.
                std::cout << std::endl << "connection built up from " << client_id << std::endl;
                // std::cout << "connection family is " << address.sa_family << std::endl;
                if (sockfd < 0) {
                    fprintf(stderr, "Connection accept error, erron: %d\n", errno);
                    break;
                }
                main_comm_threads.emplace_back(
                    [this](std::string client_ip, int socketfd) {
                        this->server_communication_thread(client_ip, socketfd);
                    },
                    std::string(address.sa_data), sockfd);
                // No need to detach, because the main_comm_threads will not be destroyed.
            }
            usleep(1000);
        }
    }
    sock_connect_exit:

    if (listenfd) close(listenfd);
    if (resolved_addr) freeaddrinfo(resolved_addr);
    if (sockfd < 0)
        if (servername)
            fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        else {
            perror("server accept");
            fprintf(stderr, "accept() failed\n");
        }
    return sockfd;
}

void MemPoolManager::server_communication_thread(std::string client_ip, int socket_fd) {
    printf("A new shared memory thread start\n");
    char temp_receive[3*sizeof(ibv_mr)];
    char temp_send[3*sizeof(ibv_mr)] = "Q";
    int rc = 0;
    uint16_t compute_node_id;
    rdma_mg->ConnectQPThroughSocket(client_ip, socket_fd, compute_node_id);

    printf("The connected compute node's id is %d\n", compute_node_id);
    rdma_mg->res->sock_map.insert({compute_node_id, socket_fd});
    ibv_mr recv_mr[RECEIVE_OUTSTANDING_SIZE] = {};
    for(int i = 0; i < RECEIVE_OUTSTANDING_SIZE; i++)
        rdma_mg->Allocate_Local_RDMA_Slot(recv_mr[i], DSMEngine::Message);
    for(int i = 0; i < RECEIVE_OUTSTANDING_SIZE; i++)
        rdma_mg->post_receive<DSMEngine::RDMA_Request>(&recv_mr[i], compute_node_id, client_ip);
    // rdma_mg->local_mem_regions.reserve(100);
    // if(rdma_mg->pre_allocated_pool.size() < pr_size){
    //     std::unique_lock<std::shared_mutex> lck(rdma_mg->local_mem_mutex);
    //     rdma_mg->Preregister_Memory(pr_size);
    // }
    // ibv_mr* mr_data = rdma_mg->preregistered_region;
    // assert(mr_data->length == (uint64_t)pr_size*1024*1024*1024);
    // memcpy(temp_send, mr_data, sizeof(ibv_mr));

    // rdma_mg->global_lock_table = rdma_mg->create_lock_table();
    // memcpy(temp_send + sizeof(ibv_mr), rdma_mg->global_lock_table, sizeof(ibv_mr));
    // // If this is the node 0 then it need to broad cast the index table mr.
    // if (rdma_mg->node_id == 1){
    //     rdma_mg->global_index_table = rdma_mg->create_index_table();
    //     memcpy(temp_send+ 2*sizeof(ibv_mr), rdma_mg->global_index_table, sizeof(ibv_mr));
    // }

    if (rdma_mg->sock_sync_data(socket_fd, 3*sizeof(ibv_mr), temp_send, temp_receive)) /* just send a dummy char back and forth */
    {
        fprintf(stderr, "sync error after QPs are were moved to RTS\n");
        rc = 1;
    }

    rdma_mg->memory_connection_counter.fetch_add(1);
    // Computing node and share memory connection succeed.
    // Now is the communication through rdma.

    ibv_wc wc[3] = {};
    int buffer_position = 0;
    int miss_poll_counter = 0;
    while (true) {
        if (rdma_mg->try_poll_completions(wc, 1, client_ip, false, compute_node_id) == 0){
            // exponetial back off to save cpu cycles.
            if(++miss_poll_counter < 256){
                continue;
            }
            else if(miss_poll_counter < 512){
                usleep(16);
                continue;
            }
            else if(miss_poll_counter < 1024){
                usleep(256);
                continue;
            }
            else{
                usleep(1024);
                continue;
            }
        }
        miss_poll_counter = 0;
        auto* req_args = new request_handler_args();
        auto& receive_msg_buf = req_args->request;
        req_args->request = *(DSMEngine::RDMA_Request*)recv_mr[buffer_position].addr;
        req_args->client_ip = client_ip;
        req_args->compute_node_id = compute_node_id;

        // copy the pointer of receive buf to a new place because
        // it is the same with send buff pointer.
        rdma_mg->post_receive<DSMEngine::RDMA_Request>(&recv_mr[buffer_position], compute_node_id, client_ip);
        if (receive_msg_buf.command == DSMEngine::async_flush_page_) {
            std::function<void(void *args)> handler = [this](void *args){this->async_flush_page_handler(args);};
            thrd_pool->Schedule(std::move(handler), (void*)req_args);
        } else if (receive_msg_buf.command == DSMEngine::sync_flush_page_) {
            std::function<void(void *args)> handler = [this](void *args){this->sync_flush_page_handler(args);};
            thrd_pool->Schedule(std::move(handler), (void*)req_args);
        } else if (receive_msg_buf.command == DSMEngine::access_page_) {
            std::function<void(void *args)> handler = [this](void *args){this->access_page_handler(args);};
            thrd_pool->Schedule(std::move(handler), (void*)req_args);
        } else if (receive_msg_buf.command == DSMEngine::async_remove_page_) {
            std::function<void(void *args)> handler = [this](void *args){this->async_remove_page_handler(args);};
            thrd_pool->Schedule(std::move(handler), (void*)req_args);
        } else if (receive_msg_buf.command == DSMEngine::sync_pat_) {
            std::function<void(void *args)> handler = [this](void *args){this->sync_pat_handler(args);};
            thrd_pool->Schedule(std::move(handler), (void*)req_args);
        } else if (receive_msg_buf.command == DSMEngine::mr_info_) {
            std::function<void(void *args)> handler = [this](void *args){this->mr_info_handler(args);};
            thrd_pool->Schedule(std::move(handler), (void*)req_args);
        } else if (receive_msg_buf.command == DSMEngine::flush_xlog_info_) {
            std::function<void(void *args)> handler = [this](void *args){this->flush_xlog_info_handler(args);};
            thrd_pool->Schedule(std::move(handler), (void*)req_args);
        } else if (receive_msg_buf.command == DSMEngine::fetch_xlog_info_) {
            std::function<void(void *args)> handler = [this](void *args){this->fetch_xlog_info_handler(args);};
            thrd_pool->Schedule(std::move(handler), (void*)req_args);
        } else if (receive_msg_buf.command == DSMEngine::flush_update_vm_info_) {
            std::function<void(void *args)> handler = [this](void *args){this->flush_update_vm_info_handler(args);};
            thrd_pool->Schedule(std::move(handler), (void*)req_args);
        } else if (receive_msg_buf.command == DSMEngine::fetch_update_vm_info_) {
            std::function<void(void *args)> handler = [this](void *args){this->fetch_update_vm_info_handler(args);};
            thrd_pool->Schedule(std::move(handler), (void*)req_args);
        } else if (receive_msg_buf.command == DSMEngine::get_first_update_vm_info_idx_) {
            std::function<void(void *args)> handler = [this](void *args){this->get_first_update_vm_info_idx_handler(args);};
            thrd_pool->Schedule(std::move(handler), (void*)req_args);
        } else if (receive_msg_buf.command == DSMEngine::disconnect_) {
            break;
        } else {
            printf("corrupt message from client (node %d). %d\n", compute_node_id, receive_msg_buf.command);
            assert(false);
            break;
        }
        // increase the buffer index
        if (buffer_position == RECEIVE_OUTSTANDING_SIZE - 1 )
            buffer_position = 0;
        else
            buffer_position++;
    }
    if (!rdma_mg->res->sock_map.count(compute_node_id)){
        close(rdma_mg->res->sock_map[compute_node_id]);
        rdma_mg->res->sock_map.erase(compute_node_id);
    }
    if (!rdma_mg->res->cq_map.count(compute_node_id)){
        ibv_destroy_cq(rdma_mg->res->cq_map[compute_node_id].first);
        if(rdma_mg->res->cq_map[compute_node_id].second != nullptr)
            ibv_destroy_cq(rdma_mg->res->cq_map[compute_node_id].second);
        rdma_mg->res->cq_map.erase(compute_node_id);
    }
    if (!rdma_mg->res->qp_map.count(compute_node_id)){
        ibv_destroy_qp(rdma_mg->res->qp_map[compute_node_id]);
        rdma_mg->res->qp_map.erase(compute_node_id);
    }
    return;
    assert(false);
    // TODO: Build up a exit method for shared memory side, don't forget to destroy all the RDMA resourses.
}

void MemPoolManager::init_thread_pool(size_t thrd_num){
    thrd_pool = new DSMEngine::ThreadPool();
    thrd_pool->SetBackgroundThreads(thrd_num);
}

void MemPoolManager::allocate_page_array(size_t pa_size){
    rdma_mg->Mempool_initialize(DSMEngine::PageArray, BLCKSZ, BLCKSZ * pa_size);
    rdma_mg->Mempool_initialize(DSMEngine::PageIDArray, sizeof(KeyType), sizeof(KeyType) * pa_size);

    ibv_mr *pa_mr, *pida_mr;
    char *pa_buf, *pida_buf;
    rdma_mg->Local_Memory_Register(&pa_buf, &pa_mr, BLCKSZ * pa_size, DSMEngine::PageArray);
    rdma_mg->Local_Memory_Register(&pida_buf, &pida_mr, sizeof(KeyType) * pa_size, DSMEngine::PageIDArray);
    
    freelist.init();
    lru = DSMEngine::NewLRUCache(pa_size, &freelist);

    page_arrays.push_back((struct page_array){.pa_mr = pa_mr, .pida_mr = pida_mr, .pa_buf = pa_buf, .pida_buf = pida_buf, .size = pa_size});
    for(size_t i = 0; i < pa_size; i++){
        auto pagemeta = new struct PageMeta;
        *pagemeta = (struct PageMeta){
            .page_addr = pa_buf + i * BLCKSZ,
            .page_id_addr = pida_buf + i * sizeof(KeyType)
        };
        freelist.push_back(pagemeta);
        *(KeyType*)(pagemeta->page_id_addr) = nullKeyType;
    }
    // todo (te): multiple page_array
}

void MemPoolManager::init_xlog_info(){
    xlog_info.valid = false;
}

void MemPoolManager::init_vminfo_ring(size_t ring_size){
    assert(ring_size >= 128);
    vminfo_ring.ring = new UpdateVersionMapInfo[ring_size]();
    for(size_t i = 0; i < ring_size; i++)
        vminfo_ring.ring[i].page_id = nullKeyType;
    vminfo_ring.size = ring_size;
    vminfo_ring.ptr = 0;
}

void MemPoolManager::async_flush_page_handler(void* args){
    auto Args = (request_handler_args*)args;
    auto request = &Args->request;
    auto client_ip = Args->client_ip;
    auto target_node_id = Args->compute_node_id;
    auto req = &request->content.flush_page;


    auto e = lru->LookupInsert(req->page_id, nullptr, 1, nullptr);
    auto pagemeta = (PageMeta*)e->value;
    std::unique_lock<std::shared_mutex> lk(e->rw_mtx);
    memcpy(pagemeta->page_addr, req->page_data, BLCKSZ);
    memcpy(pagemeta->page_id_addr, &req->page_id, sizeof(KeyType));
    lk.unlock();
    lru->Release(e);

    delete Args;
}

void MemPoolManager::sync_flush_page_handler(void* args){
    auto Args = (request_handler_args*)args;
    auto request = &Args->request;
    auto client_ip = Args->client_ip;
    auto target_node_id = Args->compute_node_id;
    auto req = &request->content.flush_page;

    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
    auto send_pointer = (DSMEngine::RDMA_Reply*)send_mr.addr;

    auto e = lru->LookupInsert(req->page_id, nullptr, 1, nullptr);
    auto pagemeta = (PageMeta*)e->value;
    std::unique_lock<std::shared_mutex> lk(e->rw_mtx);
    memcpy(pagemeta->page_addr, req->page_data, BLCKSZ);
    memcpy(pagemeta->page_id_addr, &req->page_id, sizeof(KeyType));
    lk.unlock();
    lru->Release(e);

    send_pointer->received = true;
    rdma_mg->post_send<DSMEngine::RDMA_Reply>(&send_mr, target_node_id);
    ibv_wc wc[3] = {};
    rdma_mg->poll_completion(wc, 1, client_ip, true, target_node_id);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    delete Args;
}

void MemPoolManager::access_page_handler(void* args){
    auto Args = (request_handler_args*)args;
    auto request = &Args->request;
    auto client_ip = Args->client_ip;
    auto target_node_id = Args->compute_node_id;
    auto req = &request->content.access_page;


    auto e = lru->Lookup(req->page_id);
    if (e != nullptr)
        lru->Release(e);

    delete Args;
}

void MemPoolManager::async_remove_page_handler(void* args){
    auto Args = (request_handler_args*)args;
    auto request = &Args->request;
    auto client_ip = Args->client_ip;
    auto target_node_id = Args->compute_node_id;
    auto req = &request->content.remove_page;

    auto e = lru->LookupErase(req->page_id);
    if (e != nullptr){
        auto pagemeta = (PageMeta*)e->value;
        std::unique_lock<std::shared_mutex> lk(e->rw_mtx);
        *(KeyType*)(pagemeta->page_id_addr) = nullKeyType;
        lk.unlock();
        lru->Release(e);
    }

    delete Args;
}

void MemPoolManager::sync_pat_handler(void* args){
    auto Args = (request_handler_args*)args;
    auto request = &Args->request;
    auto client_ip = Args->client_ip;
    auto target_node_id = Args->compute_node_id;
    auto req = &request->content.sync_pat;

    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
    auto send_pointer = (DSMEngine::RDMA_Reply*)send_mr.addr;
    auto res = &send_pointer->content.sync_pat;

    auto&page_array = page_arrays[req->pa_idx];
    for(int i = 0; req->pa_ofs + i < page_array.size && i < SYNC_PAT_SIZE; i++)
        res->page_id_array[i] = *(KeyType*)(page_array.pida_buf + (req->pa_ofs + i) * sizeof(KeyType));

    send_pointer->received = true;
    rdma_mg->post_send<DSMEngine::RDMA_Reply>(&send_mr, target_node_id);
    ibv_wc wc[3] = {};
    rdma_mg->poll_completion(wc, 1, client_ip, true, target_node_id);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    delete Args;
}

void MemPoolManager::mr_info_handler(void* args){
    auto Args = (request_handler_args*)args;
    auto request = &Args->request;
    auto client_ip = Args->client_ip;
    auto target_node_id = Args->compute_node_id;
    auto req = &request->content.mr_info;

    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
    auto send_pointer = (DSMEngine::RDMA_Reply*)send_mr.addr;
    auto res = &send_pointer->content.mr_info;

    memcpy(&res->pa_mr, page_arrays[req->pa_idx].pa_mr, sizeof(ibv_mr));
    memcpy(&res->pida_mr, page_arrays[req->pa_idx].pida_mr, sizeof(ibv_mr));

    send_pointer->received = true;
    rdma_mg->post_send<DSMEngine::RDMA_Reply>(&send_mr, target_node_id);
    ibv_wc wc[3] = {};
    rdma_mg->poll_completion(wc, 1, client_ip, true, target_node_id);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    delete Args;
}

void MemPoolManager::flush_xlog_info_handler(void* args){
    auto Args = (request_handler_args*)args;
    auto request = &Args->request;
    auto client_ip = Args->client_ip;
    auto target_node_id = Args->compute_node_id;
    auto req = &request->content.flush_xlog_info;

    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
    auto send_pointer = (DSMEngine::RDMA_Reply*)send_mr.addr;

    xlog_info = req->xlog_info;

    send_pointer->received = true;
    rdma_mg->post_send<DSMEngine::RDMA_Reply>(&send_mr, target_node_id);
    ibv_wc wc[3] = {};
    rdma_mg->poll_completion(wc, 1, client_ip, true, target_node_id);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    delete Args;
}

void MemPoolManager::fetch_xlog_info_handler(void* args){
    auto Args = (request_handler_args*)args;
    auto request = &Args->request;
    auto client_ip = Args->client_ip;
    auto target_node_id = Args->compute_node_id;

    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
    auto send_pointer = (DSMEngine::RDMA_Reply*)send_mr.addr;
    auto res = &send_pointer->content.fetch_xlog_info;

    res->xlog_info = xlog_info;

    send_pointer->received = true;
    rdma_mg->post_send<DSMEngine::RDMA_Reply>(&send_mr, target_node_id);
    ibv_wc wc[3] = {};
    rdma_mg->poll_completion(wc, 1, client_ip, true, target_node_id);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    delete Args;
}

void MemPoolManager::flush_update_vm_info_handler(void* args){
    auto Args = (request_handler_args*)args;
    auto request = &Args->request;
    auto client_ip = Args->client_ip;
    auto target_node_id = Args->compute_node_id;
    auto req = &request->content.flush_update_vm_info;

    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
    auto send_pointer = (DSMEngine::RDMA_Reply*)send_mr.addr;

    {
        std::unique_lock<std::mutex> lk(vminfo_ring.mtx);
        auto ptr = (vminfo_ring.ptr++) % vminfo_ring.size;
        vminfo_ring.ring[ptr] = req->info;
    }

    send_pointer->received = true;
    rdma_mg->post_send<DSMEngine::RDMA_Reply>(&send_mr, target_node_id);
    ibv_wc wc[3] = {};
    rdma_mg->poll_completion(wc, 1, client_ip, true, target_node_id);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    delete Args;
}

void MemPoolManager::fetch_update_vm_info_handler(void* args){
    auto Args = (request_handler_args*)args;
    auto request = &Args->request;
    auto client_ip = Args->client_ip;
    auto target_node_id = Args->compute_node_id;
    auto req = &request->content.fetch_update_vm_info;

    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
    auto send_pointer = (DSMEngine::RDMA_Reply*)send_mr.addr;
    auto res = &send_pointer->content.fetch_update_vm_info;

    if(req->ptr < vminfo_ring.ptr)
        res->info = vminfo_ring.ring[req->ptr % vminfo_ring.size];
    else
        res->info.page_id = nullKeyType;
    if(vminfo_ring.ptr > req->ptr + vminfo_ring.size)
        res->info.page_id = nullKeyType;

    send_pointer->received = true;
    rdma_mg->post_send<DSMEngine::RDMA_Reply>(&send_mr, target_node_id);
    ibv_wc wc[3] = {};
    rdma_mg->poll_completion(wc, 1, client_ip, true, target_node_id);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    delete Args;
}

void MemPoolManager::get_first_update_vm_info_idx_handler(void* args){
    auto Args = (request_handler_args*)args;
    auto request = &Args->request;
    auto client_ip = Args->client_ip;
    auto target_node_id = Args->compute_node_id;

    ibv_mr send_mr;
    rdma_mg->Allocate_Local_RDMA_Slot(send_mr, DSMEngine::Message);
    auto send_pointer = (DSMEngine::RDMA_Reply*)send_mr.addr;
    auto res = &send_pointer->content.get_first_update_vm_info_idx;

    const size_t offset = 32;
    if(vminfo_ring.ptr >= vminfo_ring.size - offset)
        res->idx = vminfo_ring.ptr - (vminfo_ring.size - offset);
    else
        res->idx = 0;

    send_pointer->received = true;
    rdma_mg->post_send<DSMEngine::RDMA_Reply>(&send_mr, target_node_id);
    ibv_wc wc[3] = {};
    rdma_mg->poll_completion(wc, 1, client_ip, true, target_node_id);
    rdma_mg->Deallocate_Local_RDMA_Slot(send_mr.addr, DSMEngine::Message);
    delete Args;
}

} // namespace mempool
