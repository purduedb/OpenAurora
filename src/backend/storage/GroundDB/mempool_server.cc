#include <unistd.h>
#include "storage/GroundDB/mempool_server.h"
#include "storage/GroundDB/rdma_server.hh"

void MemPoolMain(int argc, char *argv[], const char *dbname, const char *username) {
    auto mempool = new mempool::MemPoolManager();
    struct DSMEngine::config_t config = {
            NULL, /* dev_name */
            122189, /* tcp_port */
            1, /* ib_port */
            1, /* gid_idx */
            0,
            1};
    // mempool->init_resources(config.tcp_port, config.dev_name, config.ib_port);
    mempool->init_rdma_manager(88, config);
    mempool->init_xlog_info();
    mempool->init_thread_pool(10);
    mempool->allocate_page_array(1 << 20);
    mempool->init_vminfo_ring(1 << 15);
    mempool->Server_to_Client_Communication();
}