#pragma once
#include <cstdint>
#include <vector>
#include <unordered_map>
#include "storage/GroundDB/lru.hh"
#include "storage/GroundDB/request_buffer.h"
#include "storage/DSMEngine/ThreadPool.h"
#include "storage/DSMEngine/cache.h"
#include "storage/DSMEngine/rdma_manager.h"

namespace DSMEngine{
    class RDMA_Manager;
    class config_t;
}

namespace mempool{
class MemPoolManager{
public:
    int tcp_port, ib_port;
    struct resources* res;
    struct page_array{
        ibv_mr *pa_mr, *pida_mr;
        char *pa_buf, *pida_buf;
        size_t size;
    };
    DSMEngine::ThreadPool *thrd_pool;
    DSMEngine::Cache *lru;
    std::vector<page_array> page_arrays;
    FreeList freelist;

    std::shared_ptr<DSMEngine::RDMA_Manager> rdma_mg;
    size_t pr_size;
    bool exit_all_threads_ = false;
    std::vector<std::thread> main_comm_threads;
    
    void init_rdma_manager(int pr_s, DSMEngine::config_t &config);
    void Server_to_Client_Communication();
    int server_sock_connect(const char* servername, int port);
    void server_communication_thread(std::string client_ip, int socket_fd);

    void init_resources(const int tcp_port, const char *ib_devname, const int ib_port);
    void init_thread_pool(size_t thrd_num);
    void allocate_page_array(size_t pa_size);

    void flush_page_handler(void* args);
    void access_page_handler(void* args);
    void sync_pat_handler(void* args);
    void mr_info_handler(void* args);
};


} // namespace mempool