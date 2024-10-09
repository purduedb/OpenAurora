#ifndef RDMA_H
#define RDMA_H

#include <infiniband/verbs.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdatomic.h>

extern atomic_int outstanding_wrs;
extern atomic_int total_posted_wrs;
extern atomic_int total_completed_wrs;

#define RDMA_BUFFER_SIZE (1024 * 1024)  // 1MB
#define MSG_SIZE 4096
#define RING_BUFFER_SIZE (128 * 1024 * 1024)
#define RING_BUFFER_OFFSET 16
#define MAX_OUTSTANDING_WRS 1000

struct cm_con_data_t {
    uint64_t addr;   // Buffer address
    uint32_t rkey;   // Remote key
    uint32_t qp_num; // QP number
    uint16_t lid;    // LID of the IB port
    uint8_t gid[16]; // GID
    uint32_t size;   // Buffer size
} __attribute__((packed));

struct config_t {
    const char *dev_name;
    u_int32_t tcp_port;
    int ib_port;
    int gid_idx;
};
struct resources {
    struct ibv_device_attr device_attr;
    struct ibv_port_attr port_attr;
    struct cm_con_data_t remote_props;
    struct ibv_context *ib_ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    struct ibv_mr *mr;
    char *buf;
    int sock;
    uint32_t buf_size; 
};

struct sliding_window {
    int head;
    int tail;
    int count;
    struct ibv_send_wr wrs[MAX_OUTSTANDING_WRS];
};

// Functions
void dump_mr_details(struct ibv_mr *mr);
int rdma_write(struct resources *res, struct sliding_window *window, size_t offset, size_t length);
int reset_qp(struct resources *res);
void resources_init(struct resources *res);
int resources_create(struct resources *res);
int rdma_read(struct resources *res, size_t offset, size_t length, void *dest);
int resources_destroy(struct resources *res);
int connect_qp(struct resources *res);
int post_send(struct resources *res, int opcode);
int process_completions(struct resources *res, struct sliding_window *window);
void usage(const char *argv0);
void print_config(void);
int sock_connect(const char *servername, int port);
uint64_t htonll(uint64_t x);
uint64_t ntohll(uint64_t x);

extern struct config_t config;

#endif