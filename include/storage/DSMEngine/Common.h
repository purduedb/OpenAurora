#ifndef __COMMON_H__
#define __COMMON_H__

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include <iostream>
#include <atomic>
#include <bitset>
#include <limits>
#include "port/likely.h"

//#include "Debug.h"
//#include "HugePageAlloc.h"
//#include "Rdma.h"

//#include "WRLock.h"

// CONFIG_ENABLE_EMBEDDING_LOCK and CONFIG_ENABLE_CRC
// **cannot** be ON at the same time

// #define CONFIG_ENABLE_EMBEDDING_LOCK
// #define CONFIG_ENABLE_CRC

#define LATENCY_WINDOWS 1000000


#define COMMA ,
#define STRUCT_OFFSET(type, field)                                             \
  (char *)&((type *)(0))->field - (char *)((type *)(0))

#define CALCULATE_CLASS_OFFSET(type, field)                                             \
  (char *)&((type *)(0))->field - (char *)((type *)(0))
#define MAX_MACHINE 8

#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))

#define MESSAGE_SIZE 96 // byte

#define POST_RECV_PER_RC_QP 128

#define RAW_RECV_CQ_COUNT 128

// { app thread
#define MAX_APP_THREAD 26
//26
#define APP_MESSAGE_NR 96

// }

// { dir thread
#define NR_DIRECTORY 1

#define DIR_MESSAGE_NR 128

#define kInternalPageSize (2*1024 + 8)

#define kLeafPageSize (2*1024 + 8)

#define KEY_PADDING 12

#define VALUE_PADDING 392
// }

void bindCore(uint16_t thread_id);
char *getIP();
char *getMac();
constexpr int mem_cst_seq = __ATOMIC_SEQ_CST;
inline int bits_in(std::uint64_t u) {
  auto bs = std::bitset<64>(u);
  return bs.count();
}


namespace define {
//use the define::GB instead of 1024*1024*1024, because bydefault, the number is int which is smaller or equal than 1 GB.
constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t GB = 1024ull * MB;
constexpr uint16_t kCacheLineSize = 64;

// for remote allocate
constexpr uint64_t kChunkSize = 32 * MB;

// for store root pointer
constexpr uint64_t kRootPointerStoreOffest = kChunkSize / 2;
static_assert(kRootPointerStoreOffest % sizeof(uint64_t) == 0, "XX");

// lock on-chip memory
constexpr uint64_t kLockStartAddr = 0;
constexpr uint64_t kLockChipMemSize = 256 * 1024;

// number of locks
// we do not use 16-bit locks, since 64-bit locks can provide enough concurrency.
// if you wan to use 16-bit locks, call *cas_dm_mask*
constexpr uint64_t kNumOfLock = kLockChipMemSize / sizeof(uint64_t);

// level of tree
constexpr uint64_t kMaxLevelOfTree = 16;

constexpr uint16_t kMaxCoro = 8;
constexpr int64_t kPerCoroRdmaBuf = 128 * 1024;

constexpr uint8_t kMaxHandOverTime = 8;

constexpr int kIndexCacheSize = 8*1024ull; // MB
} // namespace define

static inline unsigned long long asm_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}
struct Key_buff {
    char buffer[20];
};
struct Value_buff {
    char buffer[400];
};
// For Tree
//using Key = uint64_t;
//using Value = uint64_t;

//using Key = Key_buff;
//using Value = Value_buff;
template<typename Key>
constexpr Key kKeyMin = std::numeric_limits<Key>::min();
template<typename Key>
constexpr Key kKeyMax = std::numeric_limits<Key>::max();

//constexpr Value kValueNull = 0;
template<class Value>
constexpr Value kValueNull = {};
//constexpr uint32_t kInternalPageSize = 1024;
//constexpr uint32_t kLeafPageSize = 1024;

__inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

inline void mfence() { asm volatile("mfence" ::: "memory"); }

inline void compiler_barrier() { asm volatile("" ::: "memory"); }


class GlobalAddress {
public:

    union {
        struct {
            uint64_t nodeID: 16;
            uint64_t offset : 48;
        };
        uint64_t val;
    };
//    uint8_t mark;

    operator uint64_t() {
        return val;
    }
    //TODO: How to distinguish the page at node 0 offest 0, and null_ptr.
    // May be we should put compute node as even number so that the global address will never
    // hase GlobalAddress::Null()

    static GlobalAddress Null() {
        static GlobalAddress zero{0, 0};
        return zero;
    };
} __attribute__((packed));
struct LocalAddress{
    uint64_t addr;
    uint32_t lkey;
};
static_assert(sizeof(GlobalAddress) == sizeof(uint64_t), "XXX");

inline GlobalAddress GADD(const GlobalAddress &addr, int off) {
    auto ret = addr;
    ret.offset += off;
    return ret;
}
// THis function will directly modify the reference.
inline void LADD(void*& addr, int off) {
    addr = (void*)((char*)addr + off);
}

inline bool operator==(const GlobalAddress &lhs, const GlobalAddress &rhs) {
    return (lhs.nodeID == rhs.nodeID) && (lhs.offset == rhs.offset);
}

inline bool operator!=(const GlobalAddress &lhs, const GlobalAddress &rhs) {
    return !(lhs == rhs);
}

inline std::ostream &operator<<(std::ostream &os, const GlobalAddress &obj) {
    os << "[" << (int)obj.nodeID << ", " << obj.offset << "]";
    return os;
}
#endif /* __COMMON_H__ */
