#ifndef RDMA_H
#define RDMA_H


#define REMOTE_DEALLOC_BUFF_SIZE (128 + 128) * sizeof(uint64_t)
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
#include <cassert>
#include <algorithm>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <memory>
#include <sstream>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include "utils/DSMEngine/thread_local.h"
#include "storage/DSMEngine/Common.h"
#include "port/port_posix.h"
#include "utils/DSMEngine/mutexlock.h"
#include "storage/DSMEngine/ThreadPool.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <list>
#include <cstdint>
#include "storage/GroundDB/rdma_server.hh"
#define _mm_clflush(addr)\
	asm volatile("clflush %0" : "+m" (*(volatile char *)(addr)))
#if __BYTE_ORDER == __LITTLE_ENDIAN
#elif __BYTE_ORDER == __BIG_ENDIAN
    static inline uint64_t htonll(uint64_t x) { return x; }
    static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif
#define RDMA_WRITE_BLOCK    (8*1024*1024)
#define INDEX_BLOCK    (8*1024*1024)
#define FILTER_BLOCK    (2*1024*1024)
#define NUM_QP_ACCROSS_COMPUTE 1

namespace mempool{
class MemPoolManager;
}

namespace DSMEngine {
class Cache;
enum Chunk_type {Internal_and_Leaf, LockTable, Message, PageArray, PageIDArray, Version_edit, IndexChunk, FilterChunk, FlushBuffer, DataChunk};
static const char * EnumStrings[] = { "Internal_and_Leaf", "LockTable", "Message", "PageArray", "PageIDArray", "Version_edit", "IndexChunk", "FilterChunk", "FlushBuffer", "DataChunk"};

static char config_file_name[100] = "../connection.conf";

struct config_t {
    const char* dev_name;        /* IB device name */
    // const char* server_name; /* server host name */
    u_int32_t tcp_port;            /* server TCP port */
    int ib_port; /* local IB port to work with, or physically port number */
    int gid_idx; /* gid index to use */
    int init_local_buffer_size; /*initial local SST buffer size*/
    uint16_t node_id;
};


/* structure to exchange data which is needed to connect the QPs */
struct Registered_qp_config {
    uint32_t qp_num; /* QP number */
    uint16_t lid;        /* LID of the IB port */
    uint8_t gid[16]; /* gid */
    uint16_t node_id;
} __attribute__((packed));


struct Registered_qp_config_xcompute {
    uint32_t qp_num[NUM_QP_ACCROSS_COMPUTE]; /* QP numbers */
    uint16_t lid;        /* LID of the IB port */
    uint8_t gid[16]; /* gid */
    uint32_t node_id_pairs; // this node (16 bytes) & target nodeid <<16 (16 bytes)
} __attribute__((packed));

using QP_Map = std::map<uint16_t, ibv_qp*>;
using QP_Info_Map = std::map<uint16_t, Registered_qp_config*>;
using CQ_Map = std::map<uint16_t, ibv_cq*>;
struct install_versionedit {
    bool trival;
    size_t buffer_size;
    size_t version_id;
    uint8_t check_byte;
    int level;
    uint64_t file_number;
    uint16_t node_id;
} __attribute__((packed));
struct sst_unpin {
    uint16_t id;
    size_t buffer_size;
} __attribute__((packed));
struct sst_compaction {
    size_t buffer_size;

} __attribute__((packed));

struct New_Root {
        GlobalAddress new_ptr;
        int level;

} __attribute__((packed));
enum RDMA_Command_Type {
    invalid_command_ = 0,
    sync_flush_page_,
    async_flush_page_,
    access_page_,
    async_remove_page_,
    sync_pat_,
    mr_info_,
    disconnect_,
    flush_xlog_info_,
    fetch_xlog_info_,
    flush_update_vm_info_,
    fetch_update_vm_info_,
    get_first_update_vm_info_idx_,
/*******/
    create_qp_,
    create_mr_,
    near_data_compaction,
    install_version_edit,
    version_unpin_,
    sync_option,
    qp_reset_,
    broadcast_root,
    page_invalidation,
    put_qp_info,
    get_qp_info,
    release_write_lock,
    release_read_lock,
    heart_beat
};
enum file_type { log_type, others };
struct fs_sync_command {
    int data_size;
    file_type type;
};
struct sst_gc {
    size_t buffer_size;
};
struct RUnlock_message{
    GlobalAddress page_addr;
};
struct WUnlock_message{
    GlobalAddress page_addr;
};
//TODO (ruihong): add the reply message address to avoid request&response conflict for the same queue pair.
// In other word, the threads will not need to figure out whether this message is a reply or response,
// when receive a message from the main queue pair.
union RDMA_Request_Content {
    mempool::flush_page_request flush_page;
    mempool::access_page_request access_page;
    mempool::remove_page_request remove_page;
    mempool::sync_pat_request sync_pat;
    mempool::mr_info_request mr_info;
    mempool::flush_xlog_info_request flush_xlog_info;
    mempool::flush_update_vm_info_request flush_update_vm_info;
    mempool::fetch_update_vm_info_request fetch_update_vm_info;
/******/
    Registered_qp_config qp_config;
};
union RDMA_Reply_Content {
    mempool::sync_pat_response sync_pat;
    mempool::mr_info_response mr_info;
    mempool::fetch_xlog_info_response fetch_xlog_info;
    mempool::fetch_update_vm_info_response fetch_update_vm_info;
    mempool::get_first_update_vm_info_idx_response get_first_update_vm_info_idx;
/********/
    Registered_qp_config qp_config;
};
struct RDMA_Request {
    RDMA_Command_Type command;
    RDMA_Request_Content content;
    void* buffer;
    uint32_t rkey;
    void* buffer_large;
    uint32_t rkey_large;
    uint32_t imm_num; // 0 for Compaction threads signal, 1 for Flushing threads signal.
//    Options opt;
} __attribute__((packed));

struct RDMA_Reply {
//    RDMA_Command_Type command;
    RDMA_Reply_Content content;
    void* buffer;
    uint32_t rkey;
    void* buffer_large;
    uint32_t rkey_large;
    volatile bool received;
} __attribute__((packed));
// Structure for the file handle in RDMA file system. it could be a link list
// for large files
struct SST_Metadata {
    std::shared_mutex file_lock;
    std::string fname;
    ibv_mr* mr;
    ibv_mr* map_pointer;
    SST_Metadata* last_ptr = nullptr;
    SST_Metadata* next_ptr = nullptr;
    unsigned int file_size = 0;
};

template <typename T>
struct atomwrapper {
    std::atomic<T> _a;

    atomwrapper() : _a() {}

    atomwrapper(const std::atomic<T>& a) : _a(a.load()) {}

    atomwrapper(const atomwrapper& other) : _a(other._a.load()) {}

    atomwrapper& operator=(const atomwrapper& other) {
        _a.store(other._a.load());
    }
};
#define ALLOCATOR_SHARD_NUM 16
class In_Use_Array {
public:
    In_Use_Array(size_t size, size_t chunk_size, ibv_mr* mr_ori)
            : element_size_(size), chunk_size_(chunk_size), mr_ori_(mr_ori) {
        for (size_t i = 0; i < element_size_; ++i) {
            free_list.push_back(i);
        }
    }
    In_Use_Array(size_t size, size_t chunk_size, ibv_mr* mr_ori,
                             std::atomic<bool>* in_use)
        : element_size_(size),
            chunk_size_(chunk_size),
            mr_ori_(mr_ori) {}
    int allocate_memory_slot() {
        // Below is a shortcut for memory allocation.
        if (Array_used_up.load()){
                return -1;
        }
        //maybe the conflict comes from here
        std::unique_lock<SpinMutex> lck(mtx);
        if (free_list.empty()){
            Array_used_up.store(true);
            return -1;    // Not find the empty memory chunk.
        }
        else{
            int result = free_list.back();
            free_list.pop_back();
            return result;
        }
    }
    bool deallocate_memory_slot(int index) {
        std::unique_lock<SpinMutex> lck(mtx);
        free_list.push_back(index);
        if (index < element_size_){
            if (Array_used_up == true)
                Array_used_up.store(false);
            return true;
        }
        else{
            assert(false);
            return false;
        }
    }
    size_t get_chunk_size() { return chunk_size_; }
    ibv_mr* get_mr_ori() { return mr_ori_; }
    size_t get_element_size() { return element_size_; }

private:
    size_t element_size_;
    size_t chunk_size_;
    std::list<int> free_list;
    SpinMutex mtx;
    ibv_mr* mr_ori_;
    std::atomic<bool> Array_used_up = false;
};

/* structure of system resources */
struct resources {
    union ibv_gid my_gid;
    struct ibv_device_attr device_attr;
    /* Device attributes */
    struct ibv_sge* sge = nullptr;
    struct ibv_recv_wr* rr = nullptr;
    struct ibv_port_attr port_attr; /* IB port attributes */
    //    std::vector<registered_qp_config> remote_mem_regions; /* memory buffers for RDMA */
    struct ibv_context* ib_ctx = nullptr;    /* device handle */
    struct ibv_pd* pd = nullptr;                     /* PD handle */
 // TODO: we can have mulitple cq_map and qp_maps to broaden the RPC bandwidth.
    std::map<uint16_t, std::pair<ibv_cq*, ibv_cq*>> cq_map; /* CQ Map */
    std::map<uint16_t, ibv_qp*> qp_map; /* QP Map */
    std::map<uint16_t, Registered_qp_config*> qp_main_connection_info;
    struct ibv_mr* mr_receive = nullptr;     /* MR handle for receive_buf */
    struct ibv_mr* mr_send = nullptr;            /* MR handle for send_buf */
    //    struct ibv_mr* mr_SST = nullptr;                                                /* MR handle for SST_buf */ struct ibv_mr* mr_remote;                                         /* remote MR handle for computing node */
    char* SST_buf = nullptr;         /* SSTable buffer pools pointer, it could contain
                                                                    multiple SSTbuffers */
    char* send_buf = nullptr;        /* SEND buffer pools pointer, it could contain
                                                                    multiple SEND buffers */
    char* receive_buf = nullptr; /* receive buffer pool pointer,    it could contain
                                                                    multiple acturall receive buffers */

    //TODO: change it
    std::map<uint16_t, int> sock_map; /* TCP socket file descriptor */
    std::map<std::string, ibv_mr*> mr_receive_map;
    std::map<std::string, ibv_mr*> mr_send_map;
};
struct IBV_Deleter {
    // Called by unique_ptr to destroy/free the Resource
    void operator()(ibv_mr* r) {
        if (r) {
            void* pointer = r->addr;
            ibv_dereg_mr(r);
            free(pointer);
        }
    }
};

class RDMA_Manager {
    friend class mempool::MemPoolManager;
    friend class DBImpl;
public:
    RDMA_Manager(config_t config);
    ~RDMA_Manager();
    static RDMA_Manager *Get_Instance(config_t* config);
    static void Delete_Instance(bool holdLock = false);
    void ClearOneConnection(uint16_t target_node_id);
    size_t GetMemoryNodeNum();
    size_t GetComputeNodeNum();
    /**
     * RDMA set up create all the resources, and create one query pair for RDMA send & Receive.
     */
    bool Client_Set_Up_One_Connection(uint16_t target_node_id);
    bool Client_Set_Up_Resources();
    void Initialize_threadlocal_map();
    // Set up the socket connection to remote shared memory.
    bool Get_Remote_qp_Info_Then_Connect(uint16_t target_node_id);

    void compute_message_handling_thread(std::string q_id, uint16_t shard_target_node_id);
    void ConnectQPThroughSocket(std::string qp_type, int socket_fd,
                                                            uint16_t& target_node_id);
    // Local memory register will register RDMA memory in local machine,
    // Both Computing node and share memory will call this function.
    // it also push the new block bit map to the Remote_Leaf_Node_Bitmap

    // Set the type of the memory pool. the mempool can be access by the pool name
    bool Mempool_initialize(Chunk_type pool_name, size_t size,
                                                    size_t allocated_size);
    //TODO: seperate the local memory registration by different shards. However,
    // now we can only seperate the registration by different compute node.
    //Allocate memory as "size", then slice the whole region into small chunks according to the pool name
    bool Local_Memory_Register(
            char** p2buffpointer, ibv_mr** p2mrpointer, size_t size,
            Chunk_type pool_name);    // register the memory on the local side
    // bulk deallocation preparation.

    //TODO: Make it register not per 1GB, allocate and register the memory all at once.
    ibv_mr * Preregister_Memory(size_t gb_number); //Pre register the memroy do not allocate bit map
    // new query pair creation and connection to remote Memory by RDMA send and receive
    bool Remote_Query_Pair_Connection(std::string& qp_type, uint16_t target_node_id);    // Only called by client.
    int RDMA_Read(GlobalAddress remote_ptr, ibv_mr *local_mr, size_t msg_size, size_t send_flag, int poll_num,
        Chunk_type pool_name, std::string qp_type = "default");
    int RDMA_Read(ibv_mr *remote_mr, ibv_mr *local_mr, uint64_t remote_offset, size_t msg_size, size_t send_flag, int poll_num,
        uint16_t target_node_id, std::string qp_type = "default");
        // TODO: implement this kind of RDMA operation for every primitive.
    int RDMA_Write(GlobalAddress remote_ptr, ibv_mr *local_mr, size_t msg_size, size_t send_flag, int poll_num,
        Chunk_type pool_name, std::string qp_type = "default");
    int RDMA_Write(ibv_mr *remote_mr, ibv_mr *local_mr, size_t msg_size, size_t send_flag, int poll_num,
        uint16_t target_node_id, std::string qp_type = "default");
    int RDMA_Write(void* addr, uint32_t rkey, ibv_mr* local_mr, size_t msg_size,
        std::string qp_type, size_t send_flag, int poll_num,
        uint16_t target_node_id);

    // the coder need to figure out whether the queue pair has two seperated queue,
    // if not, only send_cq==true is a valid option.
    // For a thread-local queue pair, the send_cq does not matter.
    int poll_completion(ibv_wc* wc_p, int num_entries, std::string qp_type,
                                            bool send_cq, uint16_t target_node_id);
    int poll_completion_xcompute(ibv_wc *wc_p, int num_entries, std::string qp_type, bool send_cq, uint16_t target_node_id,
                                                         int num_of_cp);
    void BatchGarbageCollection(uint64_t* ptr, size_t size);
    bool Deallocate_Local_RDMA_Slot(ibv_mr* mr, ibv_mr* map_pointer,
                                                                    Chunk_type buffer_type);
    bool Deallocate_Local_RDMA_Slot(void* p, Chunk_type buff_type);
    void Allocate_Local_RDMA_Slot(ibv_mr& mr_input, Chunk_type pool_name);
    size_t Calculate_size_of_pool(Chunk_type pool_name);
    // this function will determine whether the pointer is with in the registered memory
    bool CheckInsideLocalBuff(
            void* p,
            std::_Rb_tree_iterator<std::pair<void* const, In_Use_Array>>& mr_iter,
            std::map<void*, In_Use_Array>* Bitmap);
    bool CheckInsideRemoteBuff(void* p, uint16_t target_node_id);
    void mr_serialization(char*& temp, size_t& size, ibv_mr* mr);
    void mr_deserialization(char*& temp, size_t& size, ibv_mr*& mr);
    int try_poll_completions(ibv_wc* wc_p, int num_entries,
                            std::string& qp_type, bool send_cq,
                            uint16_t target_node_id);
    int try_poll_completions_xcompute(ibv_wc *wc_p, int num_entries, bool send_cq, uint16_t target_node_id,
                                    int num_of_cp);
    // Deserialization for linked file is problematic because different file may link to the same SSTdata
    void fs_deserilization(
            char*& buff, size_t& size, std::string& db_name,
            std::unordered_map<std::string, SST_Metadata*>& file_to_sst_meta,
            std::map<void*, In_Use_Array*>& remote_mem_bitmap, ibv_mr* local_mr);
    //    void mem_pool_serialization
    bool poll_reply_buffer(RDMA_Reply* rdma_reply);
    // TODO: Make all the variable more smart pointers.
//#ifndef NDEBUG
        static thread_local int thread_id;
        static thread_local int qp_inc_ticket;
//#endif
    resources* res = nullptr;
    std::map<uint16_t,std::vector<ibv_mr*>*> remote_mem_pool; /* a vector for all the remote memory regions*/
    // TODO: seperate the pool for different shards
    std::vector<ibv_mr*> local_mem_regions; /* a vector for all the local memory regions.*/
    ibv_mr* preregistered_region;
    std::list<ibv_mr*> pre_allocated_pool;
    // std::map<void*, In_Use_Array*>* Remote_Leaf_Node_Bitmap;
    //TODO: seperate the remote registered memory as different chunk types. similar to name_to_mem_pool
    std::map<uint16_t, std::map<void*, In_Use_Array*>*> Remote_Leaf_Node_Bitmap;
    std::map<uint16_t, std::map<void*, In_Use_Array*>*> Remote_Inner_Node_Bitmap;
    std::map<uint16_t, ibv_mr*> mr_map_data;
    std::map<uint16_t, uint32_t> rkey_map_data;
    std::map<uint16_t, uint64_t> base_addr_map_data;

    std::map<uint16_t, ibv_mr*> mr_map_lock;
    std::map<uint16_t, uint32_t> rkey_map_lock;
    std::map<uint16_t, uint64_t> base_addr_map_lock;
    // std::map<uint16_t, uint32_t> rkey_map_lock_area_size;
    size_t total_registered_size;

    std::shared_mutex remote_mem_mutex;

    std::shared_mutex rw_mutex;
    // std::shared_mutex main_qp_mutex;
    std::shared_mutex qp_cq_map_mutex;
    // ThreadLocalPtr* t_local_1;
    //TODO: clean up the thread local queue pair,the btree may not need so much thread local queue pair
    std::map<uint16_t, ThreadLocalPtr*> qp_local_write_flush;
    std::map<uint16_t, ThreadLocalPtr*> cq_local_write_flush;
    std::map<uint16_t, ThreadLocalPtr*> local_write_flush_qp_info;
    std::map<uint16_t, ThreadLocalPtr*> qp_local_write_compact;
    std::map<uint16_t, ThreadLocalPtr*> cq_local_write_compact;
    std::map<uint16_t, ThreadLocalPtr*> local_write_compact_qp_info;
    std::map<uint16_t, std::array<ibv_qp*, NUM_QP_ACCROSS_COMPUTE>*> qp_xcompute;
    std::map<uint16_t, std::array<ibv_cq*, NUM_QP_ACCROSS_COMPUTE*2>*> cq_xcompute;
//        std::map<uint16_t, Registered_qp_config_xcompute*> qp_xcompute_info;
    std::map<uint16_t, ThreadLocalPtr*> qp_data_default;
    std::map<uint16_t, ThreadLocalPtr*> cq_data_default;
    std::map<uint16_t, ThreadLocalPtr*> local_read_qp_info;
    std::map<uint16_t, ThreadLocalPtr*> async_counter;
    ThreadLocalPtr* read_buffer;
    ThreadLocalPtr* send_message_buffer;
    ThreadLocalPtr* receive_message_buffer;
    ThreadLocalPtr* CAS_buffer;
//    ThreadPool Invalidation_bg_threads;
        std::vector<std::thread> Invalidation_bg_threads;
        std::mutex invalidate_channel_mtx;
        std::atomic<int> sync_invalidation_qp_info_put = 0;
//        std::atomic<int> invalidation_threads_start_sync = 0;
    // TODO: replace the std::map<void*, In_Use_Array*> as a thread local vector of In_Use_Array*, so that
    // the conflict can be minimized.
    std::unordered_map<Chunk_type, std::map<void*, In_Use_Array*>> name_to_mem_pool;
    std::unordered_map<Chunk_type, size_t> name_to_chunksize;
    std::unordered_map<Chunk_type, size_t> name_to_allocated_size;
    std::shared_mutex local_mem_mutex;
    //Compute node is even, memory node is odd.
    static uint16_t node_id;
    std::unordered_map<uint16_t, ibv_mr*> comm_thread_recv_mrs;
    std::unordered_map<uint16_t , int> comm_thread_buffer;
//    std::map<uint16_t, uint64_t*> deallocation_buffers;
//    std::map<uint16_t, std::mutex*> dealloc_mtx;
//    std::map<uint16_t, std::condition_variable*> dealloc_cv;

    std::atomic<uint64_t> main_comm_thread_ready_num = 0;
//    uint64_t deallocation_buffers[REMOTE_DEALLOC_BUFF_SIZE / sizeof(uint64_t)];
//    std::map<uint16_t, ibv_mr*>    dealloc_mr;
    std::map<uint16_t, size_t>    top;

    // The variables for immutable notification RPC.
    std::map<uint16_t, std::mutex*> mtx_imme_map;
    std::map<uint16_t, std::atomic<uint32_t>*> imm_gen_map;
    std::map<uint16_t, uint32_t*> imme_data_map;
    std::map<uint16_t, uint32_t*> byte_len_map;
    std::map<uint16_t, std::condition_variable* > cv_imme_map;

    std::map<uint16_t, std::string> compute_nodes{};
    std::map<uint16_t, std::string> memory_nodes{};
    std::atomic<uint64_t> memory_connection_counter = 0;// Reuse by both compute nodes and memory nodes
    std::atomic<uint64_t> compute_connection_counter = 0;
    // This global index table is in the node 0;
    std::mutex global_resources_mtx;
    ibv_mr* global_index_table = nullptr;
    ibv_mr* global_lock_table = nullptr;

    std::map<uint16_t, int> memory_node_status;

#ifdef PROCESSANALYSIS
    static std::atomic<uint64_t> RDMAReadTimeElapseSum;
    static std::atomic<uint64_t> ReadCount;

#endif
#ifdef GETANALYSIS
    static std::atomic<uint64_t> RDMAFindmrElapseSum;
    static std::atomic<uint64_t> RDMAMemoryAllocElapseSum;
    static std::atomic<uint64_t> ReadCount1;
#endif
    //    std::unordered_map<std::string, ibv_mr*> fs_image;
    //    std::unordered_map<std::string, ibv_mr*> log_image;
    //    std::unique_ptr<ibv_mr, IBV_Deleter> log_image_mr;
    //    std::shared_mutex log_image_mutex;
    //    std::shared_mutex fs_image_mutex;
    // use thread local qp and cq instead of map, this could be lock free.
    //    static __thread std::string thread_id;
    template <typename T>
    int post_send(ibv_mr* mr, uint16_t target_node_id, std::string qp_type = "main") {
        struct ibv_send_wr sr;
        struct ibv_sge sge;
        struct ibv_send_wr* bad_wr = NULL;
        int rc;
        memset(&sge, 0, sizeof(sge));
        sge.addr = (uintptr_t)mr->addr;
        sge.length = sizeof(T);
        sge.lkey = mr->lkey;

        /* prepare the send work request */
        memset(&sr, 0, sizeof(sr));
        sr.next = NULL;
        sr.wr_id = 0;
        sr.sg_list = &sge;
        sr.num_sge = 1;
        sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
        sr.send_flags = IBV_SEND_SIGNALED;

        ibv_qp* qp;
        if (qp_type == "default"){
            qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,target_node_id);
                qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
            }
            rc = ibv_post_send(qp, &sr, &bad_wr);
        }else if (qp_type == "write_local_flush"){
            qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,target_node_id);
                qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
            }
            rc = ibv_post_send(qp, &sr, &bad_wr);

        }else if (qp_type == "write_local_compact"){
            qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,target_node_id);
                qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
            }
            rc = ibv_post_send(qp, &sr, &bad_wr);
        } else {
            std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
            rc = ibv_post_send(res->qp_map.at(target_node_id), &sr, &bad_wr);
            l.unlock();
        }
        return rc;
    }


 private:
    config_t rdma_config;
    int client_sock_connect(const char* servername, int port);

    int sock_sync_data(int sock, int xfer_size, char* local_data,
                                         char* remote_data);

    int post_send(ibv_mr* mr, std::string qp_type, size_t size, uint16_t target_node_id);
    //    int post_receives(int len);

    int post_receive(ibv_mr* mr, std::string qp_type, size_t size,
                                     uint16_t target_node_id);

    int resources_create();
    int modify_qp_to_reset(ibv_qp* qp);
    //todo: make the qp configuration configurable, different qp (x COMPUTE, to memory) should have differnt config
    int modify_qp_to_init(struct ibv_qp* qp);
    int modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn, uint16_t dlid,
                        uint8_t *dgid);
    int modify_qp_to_rts(struct ibv_qp* qp);
    ibv_qp *create_qp(uint16_t target_node_id, bool seperated_cq, std::string &qp_type, uint32_t send_outstanding_num,
                    uint32_t recv_outstanding_num);
    void create_qp_xcompute(uint16_t target_node_id, std::array<ibv_cq *, NUM_QP_ACCROSS_COMPUTE * 2> *cq_arr,
                            std::array<ibv_qp *, NUM_QP_ACCROSS_COMPUTE> *qp_arr);
    //q_id is for the remote qp informantion fetching
    int connect_qp(ibv_qp* qp, std::string& qp_type, uint16_t target_node_id);
    int connect_qp(ibv_qp* qp, Registered_qp_config* remote_con_data);
    int connect_qp_xcompute(std::array<ibv_qp *, NUM_QP_ACCROSS_COMPUTE> *qp_arr, Registered_qp_config_xcompute* remote_con_data);
    int resources_destroy();
    void print_config(void);
    void usage(const char* argv0);

    int post_receive(ibv_mr** mr_list, size_t sge_size, std::string qp_type,
                    uint16_t target_node_id);
    int post_receive_xcompute(ibv_mr *mr, uint16_t target_node_id, int num_of_qp);
    int post_send_xcompute(ibv_mr *mr, uint16_t target_node_id, int num_of_qp);

    int post_send(ibv_mr** mr_list, size_t sge_size, std::string qp_type,
                uint16_t target_node_id);
public:
    template <typename T>
    int post_receive(ibv_mr* mr, uint16_t target_node_id, std::string qp_type = "main") {
        struct ibv_recv_wr rr;
        struct ibv_sge sge;
        struct ibv_recv_wr* bad_wr;
        int rc;

        memset(&sge, 0, sizeof(sge));
        sge.addr = (uintptr_t)mr->addr;
        sge.length = sizeof(T);
        sge.lkey = mr->lkey;

        /* prepare the receive work request */
        memset(&rr, 0, sizeof(rr));
        rr.next = NULL;
        rr.wr_id = 0;
        rr.sg_list = &sge;
        rr.num_sge = 1;
        /* post the Receive Request to the RQ */
        ibv_qp* qp;
        if (qp_type == "read_local"){
            qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,target_node_id);
                qp = static_cast<ibv_qp*>(qp_data_default.at(target_node_id)->Get());
            }
            rc = ibv_post_recv(qp, &rr, &bad_wr);
        }else if (qp_type == "Xcompute"){
            qp = static_cast<ibv_qp*>((*qp_xcompute.at(target_node_id))[0]);
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,target_node_id);
                qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
            }
            rc = ibv_post_recv(qp, &rr, &bad_wr);

        }else if (qp_type == "write_local_compact"){
            qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
            if (qp == NULL) {
                Remote_Query_Pair_Connection(qp_type,target_node_id);
                qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
            }
            rc = ibv_post_recv(qp, &rr, &bad_wr);
        } else {
            std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
            rc = ibv_post_recv(res->qp_map.at(target_node_id), &rr, &bad_wr);
            l.unlock();
        }
        return rc;
    }    // For a non-thread-local queue pair, send_cq==true poll the cq of send queue, send_cq==false poll the cq of receive queue
};

}
#endif