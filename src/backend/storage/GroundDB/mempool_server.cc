#include <unistd.h>
#include <cerrno>
#include <climits>
#include <cstdlib>
#include <cstring>
#include "storage/GroundDB/mempool_server.h"
#include "storage/GroundDB/rdma_server.hh"

void MemPoolMain(int argc, char *argv[], const char *dbname, const char *username) {
    const uint32_t default_tcp_port = 122189;
    uint32_t tcp_port = default_tcp_port;

    // Support:
    //   ./postgre --mempool <port>
    //   ./postgre --mempool --port=<port>
    if (argc > 2) {
        const char* port_arg = nullptr;
        if (strncmp(argv[2], "--port=", 7) == 0) {
            port_arg = argv[2] + 7;
        } else {
            port_arg = argv[2];
        }

        errno = 0;
        char* endptr = nullptr;
        unsigned long parsed_port = strtoul(port_arg, &endptr, 10);
        if (errno == 0 && endptr != port_arg && *endptr == '\0' &&
            parsed_port > 0 && parsed_port <= UINT_MAX) {
            tcp_port = static_cast<uint32_t>(parsed_port);
        } else {
            fprintf(stderr,
                    "Invalid mempool port '%s', fallback to default port %u\n",
                    port_arg, default_tcp_port);
        }
    }

    auto mempool = new mempool::MemPoolManager();
    struct DSMEngine::config_t config = {
            NULL, /* dev_name */
            tcp_port, /* tcp_port */
            1, /* ib_port */
            1, /* gid_idx */
            0,
            1};
    // mempool->init_resources(config.tcp_port, config.dev_name, config.ib_port);
    mempool->init_rdma_manager(88, config);
    mempool->init_xlog_info();
    mempool->init_thread_pool(20);
    mempool->allocate_page_array_by_GB(8);
    mempool->init_pvtinfo_ring(1 << 15);
    mempool->Server_to_Client_Communication();
}