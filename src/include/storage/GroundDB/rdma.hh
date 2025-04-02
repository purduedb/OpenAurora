#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
#include <getopt.h>
#include <vector>

#include <sys/time.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <map>
#include <mutex>
#include <memory>
#include <unordered_map>
#include <thread>
#include <shared_mutex>

namespace mempool{

/* poll CQ timeout in millisec (2 seconds) */
#define MAX_POLL_CQ_TIMEOUT 2000
#define VERIFIER "VERIFY_CONNECTION"
#define RDMAMSGR "RDMA read operation "
#define RDMAMSGW "RDMA write operation"
#define BUFFER_SIZE 1024 * 1024 /* 1024KB*/
#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

} // namespace mempool
#include "lru.hh"
#include "pat.hh"
#include "storage/GroundDB/rdma_server.hh"