#include <unistd.h>
#include <cerrno>
#include <climits>
#include <cstdlib>
#include <cstring>
#include "storage/GroundDB/mempool_server.h"
#include "storage/GroundDB/rdma_server.hh"

void MemPoolMain(int argc, char *argv[], const char *dbname, const char *username) {
    const uint32_t default_tcp_port = 122189;
    const uint32_t default_page_array_gb = 8;
    uint32_t tcp_port = default_tcp_port;
    uint32_t page_array_gb = default_page_array_gb;

    // Support (after argv[1] == --mempool):
    //   ./postgre --mempool --port=<port>
    //   ./postgre --mempool --gb=<n>        # page array size in GB (default 8)
    //   ./postgre --mempool --port=19875 --gb=16
    for (int i = 2; i < argc; i++) {
        const char* arg = argv[i];
        if (strncmp(arg, "--port=", 7) == 0) {
            const char* port_arg = arg + 7;
            errno = 0;
            char* endptr = nullptr;
            unsigned long parsed = strtoul(port_arg, &endptr, 10);
            if (errno == 0 && endptr != port_arg && *endptr == '\0' &&
                parsed > 0 && parsed <= UINT_MAX) {
                tcp_port = static_cast<uint32_t>(parsed);
            } else {
                fprintf(stderr,
                        "Invalid mempool port '%s', fallback to default port %u\n",
                        port_arg, default_tcp_port);
            }
        } else if (strncmp(arg, "--gb=", 5) == 0) {
            const char* gb_arg = arg + 5;
            errno = 0;
            char* endptr = nullptr;
            unsigned long parsed = strtoul(gb_arg, &endptr, 10);
            if (errno == 0 && endptr != gb_arg && *endptr == '\0' &&
                parsed > 0 && parsed <= 4096UL) {
                page_array_gb = static_cast<uint32_t>(parsed);
            } else {
                fprintf(stderr,
                        "Invalid --gb value '%s', fallback to default %u GB\n",
                        gb_arg, default_page_array_gb);
            }
        } else {
            fprintf(stderr,
                    "Unknown mempool argument '%s' (use --port=<n> or --gb=<n> only)\n",
                    arg);
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
    mempool->allocate_page_array_by_GB(page_array_gb);
    mempool->init_pvtinfo_ring(1 << 15);
    mempool->Server_to_Client_Communication();
}