#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "storage/GroundDB/rdma_server.hh"

int main(int argc, char** argv) {
    auto mempool = new mempool::MemPoolManager();
    struct DSMEngine::config_t config = {
            NULL, /* dev_name */
            19843, /* tcp_port */
            1, /* ib_port */
            1, /* gid_idx */
            0,
            1};
    mempool->init_rdma_manager(88, config);
    mempool->init_thread_pool(10);
    mempool->allocate_page_array(1 << 15);
    mempool->Server_to_Client_Communication();
    return 0;
}