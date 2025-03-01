#include "..\include\tcop\rdma.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
#include <getopt.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/resource.h>
#include <rdma/rdma_cma.h>
#include "rdma_debug.h"
#include <errno.h>
#include <infiniband/verbs.h>



atomic_int outstanding_wrs = 0;
atomic_int total_posted_wrs = 0;
atomic_int total_completed_wrs = 0;


#ifndef ibv_link_layer_str
#define ibv_link_layer_str(link_layer) ((link_layer == IBV_LINK_LAYER_INFINIBAND) ? "InfiniBand" : \
                                        (link_layer == IBV_LINK_LAYER_ETHERNET) ? "Ethernet" : "Unknown")
#endif

#define MAX_POLL_CQ_TIMEOUT 10000


void init_sliding_window(struct sliding_window *window) {
    window->head = 0;
    window->tail = 0;
    window->count = 0;
}

void dump_mr_details(struct ibv_mr *mr) {
    fprintf(stdout, "DEBUG: Memory Region Details:\n");
    fprintf(stdout, "  Address: %p\n", mr->addr);
    fprintf(stdout, "  Length: %zu\n", mr->length);
    fprintf(stdout, "  LKey: %u\n", mr->lkey);
    fprintf(stdout, "  RKey: %u\n", mr->rkey);
}



int add_wr_to_window(struct sliding_window *window, struct ibv_send_wr *wr) {
    if (window->count >= MAX_OUTSTANDING_WRS) {
        fprintf(stderr, "DEBUG: Sliding window full. Current count: %d\n", window->count);
        return -1;  // Window is full
    }
    memcpy(&window->wrs[window->tail], wr, sizeof(struct ibv_send_wr));
    window->tail = (window->tail + 1) % MAX_OUTSTANDING_WRS;
    window->count++;
    atomic_fetch_add(&outstanding_wrs, 1);
    atomic_fetch_add(&total_posted_wrs, 1);
    fprintf(stdout, "DEBUG: Added WR to window. Outstanding: %d, Total Posted: %d\n", 
            atomic_load(&outstanding_wrs), atomic_load(&total_posted_wrs));
    return 0;
}

struct ibv_send_wr *get_next_wr(struct sliding_window *window) {
    if (window->count == 0) {
        return NULL;
    }
    struct ibv_send_wr *wr = &window->wrs[window->head];
    window->head = (window->head + 1) % MAX_OUTSTANDING_WRS;
    window->count--;
    return wr;
}

int process_completions(struct resources *res, struct sliding_window *window) {
    DEBUG_TRACE("Entering process_completions");
    
    struct ibv_wc wc;
    int processed = 0;
    int ret;

    struct timeval start, now;
    gettimeofday(&start, NULL);

    while (1) {
        ret = ibv_poll_cq(res->cq, 1, &wc);
        if (ret < 0) {
            DEBUG_ERROR("Error polling CQ: %s", strerror(errno));
            return -1;
        } else if (ret == 0) {
            gettimeofday(&now, NULL);
            if ((now.tv_sec - start.tv_sec) * 1000 + (now.tv_usec - start.tv_usec) > 1000) {
            
                DEBUG_WARN("Timeout waiting for completion");
                break;
            }
            continue;
        }

        if (wc.status != IBV_WC_SUCCESS) {
            DEBUG_ERROR("Completion with error: %s (status %d, vendor_err %d)", 
                        ibv_wc_status_str(wc.status), wc.status, wc.vendor_err);
            DEBUG_ERROR("WR ID: %lu, QP Num: %u, Opcode: %d", 
                        wc.wr_id, wc.qp_num, wc.opcode);
            return -1;
        }
        processed++;
        atomic_fetch_sub(&outstanding_wrs, 1);
        atomic_fetch_add(&total_completed_wrs, 1);
        DEBUG_DEBUG("Processed completion for WR ID: %lu", wc.wr_id);
        break;  // Process one completion at a time
    }

    DEBUG_INFO("Processed %d completions. Outstanding: %d, Total Completed: %d", 
               processed, atomic_load(&outstanding_wrs), atomic_load(&total_completed_wrs));
    return processed;
}




uint64_t htonll(uint64_t x) {
    return ((1==htonl(1)) ? (x) : ((uint64_t)htonl((x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32));
}

uint64_t ntohll(uint64_t x) {
    return ((1==ntohl(1)) ? (x) : ((uint64_t)ntohl((x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32));
}

struct config_t config = {
    NULL,  /* dev_name */
    19875, /* tcp_port */
    1,     /* ib_port */
    -1     /* gid_idx */
};

struct rdma_cm_id *cm_id;
struct rdma_event_channel *ec;


void print_port_info(struct ibv_context *context, int port_num) {
    struct ibv_port_attr port_attr;
    if (ibv_query_port(context, port_num, &port_attr) == 0) {
        printf("Port Info:\n");
        printf("  State: %d (%s)\n", port_attr.state, ibv_port_state_str(port_attr.state));
        printf("  Max MTU: %d\n", port_attr.max_mtu);
        printf("  Active MTU: %d\n", port_attr.active_mtu);
        printf("  LID: %d\n", port_attr.lid);
        printf("  Link Layer: %s\n", ibv_link_layer_str(port_attr.link_layer));
    } else {
        fprintf(stderr, "Failed to query port attributes\n");
    }
}

int sock_connect(const char *servername, int port)
{
    fprintf(stdout, "Entering function: %s\n", __func__);
	struct addrinfo *resolved_addr = NULL;
	struct addrinfo *iterator;
	char service[6];
	int sockfd = -1;
	int listenfd = 0;
	int tmp;
	struct addrinfo hints =
		{
			.ai_flags = AI_PASSIVE,
			.ai_family = AF_INET,
			.ai_socktype = SOCK_STREAM};
	if (sprintf(service, "%d", port) < 0)
		goto sock_connect_exit;
	
	sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
	if (sockfd < 0)
	{
		fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
		goto sock_connect_exit;
	}

	for (iterator = resolved_addr; iterator; iterator = iterator->ai_next)
	{
		sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
		if (sockfd >= 0)
		{
			if (servername){

				if ((tmp = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen)))
				{
					fprintf(stdout, "failed connect \n");
					close(sockfd);
					sockfd = -1;
				}
            }
			else
			{
					
					listenfd = sockfd;
					sockfd = -1;
					if (bind(listenfd, iterator->ai_addr, iterator->ai_addrlen))
						goto sock_connect_exit;
					listen(listenfd, 1);
					sockfd = accept(listenfd, NULL, 0);
			}
		}
	}
sock_connect_exit:
	if (listenfd)
		close(listenfd);
	if (resolved_addr)
		freeaddrinfo(resolved_addr);
	if (sockfd < 0)
	{
		if (servername)
			fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
		else
		{
			perror("server accept");
			fprintf(stderr, "accept() failed\n");
		}
	}
	return sockfd;
}

int rdma_write(struct resources *res, struct sliding_window *window, size_t offset, size_t length) {
    DEBUG_TRACE("Entering rdma_write. Offset: %zu, Length: %zu", offset, length);
    dump_mr_details(res->mr);
    
    fprintf(stdout, "DEBUG: Remote properties:\n");
    fprintf(stdout, "  Remote address: %p\n", (void*)res->remote_props.addr);
    fprintf(stdout, "  Remote key: %u\n", res->remote_props.rkey);
    fprintf(stdout, "  Remote QP number: %u\n", res->remote_props.qp_num);
    
  

    if (offset + length > res->mr->length) {
        // Handle wrap-around
        size_t first_part = res->mr->length - offset;
        size_t second_part = length - first_part;

        // Write first part
        struct ibv_send_wr wr1, *bad_wr = NULL;
        struct ibv_sge sge1;

        memset(&wr1, 0, sizeof(wr1));
        wr1.wr_id = 0;
        wr1.opcode = IBV_WR_RDMA_WRITE;
        wr1.sg_list = &sge1;
        wr1.num_sge = 1;
        wr1.send_flags = IBV_SEND_SIGNALED;
        wr1.wr.rdma.remote_addr = res->remote_props.addr + offset;
        wr1.wr.rdma.rkey = res->remote_props.rkey;

        sge1.addr = (uintptr_t)res->buf + offset;
        sge1.length = first_part;
        sge1.lkey = res->mr->lkey;

        if (ibv_post_send(res->qp, &wr1, &bad_wr) != 0) {
            fprintf(stderr, "Failed to post first part of wrapped write\n");
            return -1;
        }

        // Write second part
        struct ibv_send_wr wr2;
        struct ibv_sge sge2;

        memset(&wr2, 0, sizeof(wr2));
        wr2.wr_id = 1;
        wr2.opcode = IBV_WR_RDMA_WRITE;
        wr2.sg_list = &sge2;
        wr2.num_sge = 1;
        wr2.send_flags = IBV_SEND_SIGNALED;
        wr2.wr.rdma.remote_addr = res->remote_props.addr;
        wr2.wr.rdma.rkey = res->remote_props.rkey;

        sge2.addr = (uintptr_t)res->buf;
        sge2.length = second_part;
        sge2.lkey = res->mr->lkey;

        if (ibv_post_send(res->qp, &wr2, &bad_wr) != 0) {
            fprintf(stderr, "Failed to post second part of wrapped write\n");
            return -1;
        }

        return 0;
    }

    // Original write logic for non-wrapped writes
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id = atomic_fetch_add(&total_posted_wrs, 1);  
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = res->remote_props.addr + offset;
    wr.wr.rdma.rkey = res->remote_props.rkey;

    sge.addr = (uintptr_t)res->buf + offset;
    sge.length = length;
    sge.lkey = res->mr->lkey;

    fprintf(stdout, "DEBUG: RDMA Write details:\n");
    fprintf(stdout, "  Local address: %p\n", (void*)sge.addr);
    fprintf(stdout, "  Local key: %u\n", sge.lkey);
    fprintf(stdout, "  Remote address: %p\n", (void*)wr.wr.rdma.remote_addr);
    fprintf(stdout, "  Remote key: %u\n", wr.wr.rdma.rkey);
    fprintf(stdout, "  Length: %u\n", sge.length);

    DEBUG_DEBUG("RDMA Write details: Local address: %p, Local key: %u, Remote address: %p, Remote key: %u, Length: %u",
                (void*)sge.addr, sge.lkey, (void*)wr.wr.rdma.remote_addr, wr.wr.rdma.rkey, sge.length);

    int ret = ibv_post_send(res->qp, &wr, &bad_wr);
    if (ret) {
        DEBUG_ERROR("ibv_post_send failed: %s", strerror(ret));
    } else {
        DEBUG_INFO("Successfully posted send. WR ID: %lu", wr.wr_id);
    }

    return ret;
}

int rdma_read(struct resources *res, size_t offset, size_t length, void *dest) {
    

    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    struct ibv_wc wc;
    int poll_result;
    int max_retries = 5;
    int retry_count = 0;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id = 0;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = res->remote_props.addr + offset;
    wr.wr.rdma.rkey = res->remote_props.rkey;

    sge.addr = (uintptr_t)dest;
    sge.length = length;
    sge.lkey = res->mr->lkey;

    while (retry_count < max_retries) {
        int post_result = ibv_post_send(res->qp, &wr, &bad_wr);
        if (post_result) {
            fprintf(stderr, "Failed to post RDMA Read request (attempt %d/%d): %s\n", 
                    retry_count + 1, max_retries, strerror(post_result));
            retry_count++;
            usleep(100 * (1 << retry_count));  // Exponential backoff
            continue;
        }

        do {
            poll_result = ibv_poll_cq(res->cq, 1, &wc);
        } while (poll_result == 0);

        if (poll_result < 0) {
            fprintf(stderr, "Failed to poll completion queue\n");
            return -1;
        }

        if (wc.status == IBV_WC_SUCCESS) {
            return 0;
        }

        fprintf(stderr, "RDMA Read failed with status: %s (attempt %d/%d)\n", 
                ibv_wc_status_str(wc.status), retry_count + 1, max_retries);

        retry_count++;
        usleep(100000 * (1 << retry_count));  // Exponential backoff
    }

    fprintf(stderr, "RDMA Read failed after %d attempts\n", max_retries);
    return -1;
}

int sock_sync_data(int sock, int xfer_size, char *local_data, char *remote_data) {
    int rc;
    int read_bytes = 0;
    int total_read_bytes = 0;

    rc = write(sock, local_data, xfer_size);
    fprintf(stderr, "DEBUG: sock_sync_data - Sent %d bytes\n", rc);
    fprintf(stderr, "DEBUG: sock_sync_data - Sent data (hex): ");
    for (int i = 0; i < xfer_size; i++) {
        fprintf(stderr, "%02x ", (unsigned char)local_data[i]);
    }
    fprintf(stderr, "\n");

    while (total_read_bytes < xfer_size) {
        read_bytes = read(sock, remote_data + total_read_bytes, xfer_size - total_read_bytes);
        if (read_bytes > 0) {
            total_read_bytes += read_bytes;
        } else if (read_bytes == 0) {
            fprintf(stderr, "DEBUG: sock_sync_data - Connection closed\n");
            return -1;
        } else {
            fprintf(stderr, "DEBUG: sock_sync_data - Error reading data: %s\n", strerror(errno));
            return -1;
        }
    }

    fprintf(stderr, "DEBUG: sock_sync_data - Received %d bytes\n", total_read_bytes);
    fprintf(stderr, "DEBUG: sock_sync_data - Received data (hex): ");
    for (int i = 0; i < xfer_size; i++) {
        fprintf(stderr, "%02x ", (unsigned char)remote_data[i]);
    }
    fprintf(stderr, "\n");

    return 0;
}




int post_send(struct resources *res, int opcode)
{
    fprintf(stdout, "Entering function: %s\n", __func__);
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int rc;

    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)res->buf;
    sge.length = MSG_SIZE;
    sge.lkey = res->mr->lkey;

    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = opcode;
    sr.send_flags = IBV_SEND_SIGNALED;

    if (opcode != IBV_WR_SEND) {
        sr.wr.rdma.remote_addr = res->remote_props.addr;
        sr.wr.rdma.rkey = res->remote_props.rkey;
    }

    rc = ibv_post_send(res->qp, &sr, &bad_wr);
    //-----------Debug-----------------

    if (rc) {
        fprintf(stderr, "failed to post SR, error: %d\n", rc);
    } else {
        fprintf(stdout, "SR was posted successfully\n");
        fprintf(stdout, "RDMA operation details:\n");
        fprintf(stdout, "  opcode: %d\n", opcode);
        fprintf(stdout, "  send_flags: 0x%x\n", sr.send_flags);
        fprintf(stdout, "  addr: 0x%lx\n", (unsigned long)sge.addr);
        fprintf(stdout, "  length: %u\n", sge.length);
        fprintf(stdout, "  lkey: 0x%x\n", sge.lkey);
        if (opcode != IBV_WR_SEND) {
            fprintf(stdout, "  remote_addr: 0x%lx\n", (unsigned long)sr.wr.rdma.remote_addr);
            fprintf(stdout, "  rkey: 0x%x\n", sr.wr.rdma.rkey);
        }
    }

    //-----------Debug-----------------
    if (rc) {
        fprintf(stderr, "ibv_post_send failed with error: %d\n", rc);
    }
    else {
        switch (opcode) {
        case IBV_WR_SEND:
            fprintf(stdout, "Send Request was posted\n");
            break;
        case IBV_WR_RDMA_READ:
            fprintf(stdout, "RDMA Read Request was posted\n");
            break;
        case IBV_WR_RDMA_WRITE:
            fprintf(stdout, "RDMA Write Request was posted\n");
            break;
        default:
            fprintf(stdout, "Unknown Request was posted\n");
            break;
        }
    }
    return rc;
}




void resources_init(struct resources *res)
{
    fprintf(stdout, "Entering function: %s\n", __func__);
    memset(res, 0, sizeof *res);
    res->sock = -1;
}



int resources_create(struct resources *res)
{
    fprintf(stdout, "Entering function: %s\n", __func__);
    struct rlimit rlim;
    if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
        fprintf(stdout, "Current file descriptor limit: %lu\n", (unsigned long)rlim.rlim_cur);
    } else {
        fprintf(stderr, "Failed to get resource limits: %s\n", strerror(errno));
    }
    struct ibv_device **dev_list = NULL;
    struct ibv_qp_init_attr qp_init_attr;
    struct ibv_device *ib_dev = NULL;
    size_t size;
    int i;
    int mr_flags = 0;
    int cq_size = 0;
    int num_devices;
    int rc = 0;

    

    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list) {
        fprintf(stderr, "failed to get IB devices list\n");
        rc = 1;
        goto resources_create_exit;
    }

    if (!num_devices) {
        fprintf(stderr, "found %d device(s)\n", num_devices);
        rc = 1;
        goto resources_create_exit;
    }

    for (i = 0; i < num_devices; i++) {
        if (!config.dev_name) {
            config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
            fprintf(stdout, "device not specified, using first one found: %s\n", config.dev_name);
        }
        if (!strcmp(ibv_get_device_name(dev_list[i]), config.dev_name)) {
            ib_dev = dev_list[i];
            break;
        }
    }

    if (!ib_dev) {
        fprintf(stderr, "IB device %s wasn't found\n", config.dev_name);
        rc = 1;
        goto resources_create_exit;
    }

    size = 4096; 
    res->buf = (char *)malloc(size);
    if (!res->buf) {
        fprintf(stderr, "failed to malloc %zu bytes to memory buffer\n", size);
        rc = 1;
        goto resources_create_exit;
    }

    res->buf_size = size; 

    res->ib_ctx = ibv_open_device(ib_dev);
    if (!res->ib_ctx) {
        fprintf(stderr, "failed to open device %s\n", config.dev_name);
        rc = 1;
        goto resources_create_exit;
    }

    if (ibv_query_port(res->ib_ctx, config.ib_port, &res->port_attr)) {
        fprintf(stderr, "ibv_query_port on port %u failed\n", config.ib_port);
        rc = 1;
        goto resources_create_exit;
    }

    print_port_info(res->ib_ctx, config.ib_port);

    res->pd = ibv_alloc_pd(res->ib_ctx);
    if (!res->pd) {
        fprintf(stderr, "ibv_alloc_pd failed\n");
        rc = 1;
        goto resources_create_exit;
    }

    cq_size = 1000;
    res->cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
    if (!res->cq) {
        fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
        rc = 1;
        goto resources_create_exit;
    }

    size = RING_BUFFER_SIZE + RING_BUFFER_OFFSET;
    res->buf = (char *)malloc(size);
    if (!res->buf) {
        fprintf(stderr, "failed to malloc %zu bytes to memory buffer\n", size);
        rc = 1;
        goto resources_create_exit;
    }

    memset(res->buf, 0, size);


    res->buf_size = (uint32_t)size;

    fprintf(stdout, "Buffer initialized to zero, size: %zu bytes\n", size);

    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;


    fprintf(stdout, "Registering MR with size: %zu bytes\n", size);

    
    res->mr = ibv_reg_mr(res->pd, res->buf, size, mr_flags);

    if (res->port_attr.state != IBV_PORT_ACTIVE) {
        fprintf(stderr, "Port is not in active state (state: %d - %s)\n", 
            res->port_attr.state, 
            ibv_port_state_str(res->port_attr.state));
        fprintf(stderr, "This may be normal for RoCE environments. Continuing...\n");
    }

    if (!res->mr) {
        fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x\n", mr_flags);
        rc = 1;
        goto resources_create_exit;
    }
    dump_mr_details(res->mr);

    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    qp_init_attr.send_cq = res->cq;
    qp_init_attr.recv_cq = res->cq;
    qp_init_attr.cap.max_send_wr = 1000;
    qp_init_attr.cap.max_recv_wr = 1000;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    fprintf(stdout, "Creating QP with max_send_wr: %d, max_recv_wr: %d\n", 
        qp_init_attr.cap.max_send_wr, qp_init_attr.cap.max_recv_wr);


    res->qp = ibv_create_qp(res->pd, &qp_init_attr);
    if (!res->qp) {
        fprintf(stderr, "failed to create QP\n");
        rc = 1;
        goto resources_create_exit;
    }

resources_create_exit:
    if (rc) {
        if (res->qp) {
            ibv_destroy_qp(res->qp);
            res->qp = NULL;
        }
        if (res->mr) {
            ibv_dereg_mr(res->mr);
            res->mr = NULL;
        }
        if (res->buf) {
            free(res->buf);
            res->buf = NULL;
        }
        if (res->cq) {
            ibv_destroy_cq(res->cq);
            res->cq = NULL;
        }
        if (res->pd) {
            ibv_dealloc_pd(res->pd);
            res->pd = NULL;
        }
        if (res->ib_ctx) {
            ibv_close_device(res->ib_ctx);
            res->ib_ctx = NULL;
        }
        if (dev_list) {
            ibv_free_device_list(dev_list);
            dev_list = NULL;
        }
    }
    return rc;
}

int resources_destroy(struct resources *res)
{
    fprintf(stdout, "Entering function: %s\n", __func__);
    int rc = 0;

    if (res->qp)
        if (ibv_destroy_qp(res->qp)) {
            fprintf(stderr, "failed to destroy QP\n");
            rc = 1;
        }

    if (res->mr)
        if (ibv_dereg_mr(res->mr)) {
            fprintf(stderr, "failed to deregister MR\n");
            rc = 1;
        }

    if (res->buf)
        free(res->buf);

    if (res->cq)
        if (ibv_destroy_cq(res->cq)) {
            fprintf(stderr, "failed to destroy CQ\n");
            rc = 1;
        }

    if (res->pd)
        if (ibv_dealloc_pd(res->pd)) {
            fprintf(stderr, "failed to deallocate PD\n");
            rc = 1;
        }

    if (res->ib_ctx)
        if (ibv_close_device(res->ib_ctx)) {
            fprintf(stderr, "failed to close device context\n");
            rc = 1;
        }

    if (res->sock >= 0)
        if (close(res->sock)) {
            fprintf(stderr, "failed to close socket\n");
            rc = 1;
        }

    return rc;
}

static int modify_qp_to_init(struct ibv_qp *qp)
{
    fprintf(stdout, "Entering function: %s\n", __func__);
    struct ibv_qp_attr attr;
    int flags;
    int rc;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = config.ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc)
        fprintf(stderr, "failed to modify QP state to INIT\n");
    return rc;
}

static int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid)
{
    fprintf(stdout, "Entering function: %s\n", __func__);
    struct ibv_qp_attr attr;
    int flags;
    int rc;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_256;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 0x12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = dlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = config.ib_port;

    if (config.gid_idx >= 0) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = config.ib_port;
        memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = config.gid_idx;
        attr.ah_attr.grh.traffic_class = 0;
    }

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
        fprintf(stderr, "failed to modify QP state to RTR, error: %d\n", rc);
        return rc;
    }
    fprintf(stdout, "QP modified to RTR state successfully\n");

    fprintf(stdout, "Modifying QP to RTR with remote QP: %u, remote LID: %u\n", remote_qpn, dlid);
    if (config.gid_idx >= 0) {
        fprintf(stdout, "Using GID index: %d\n", config.gid_idx);
        fprintf(stdout, "Remote GID: ");
        for (int i = 0; i < 16; i++) {
            fprintf(stdout, "%02x", dgid[i]);
        }
        fprintf(stdout, "\n");
    }
    return rc;
}

static int modify_qp_to_rts(struct ibv_qp *qp)
{
    fprintf(stdout, "Entering function: %s\n", __func__);
    struct ibv_qp_attr attr;
    int flags;
    int rc;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 0x12;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;

    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc) {
        fprintf(stderr, "failed to modify QP state to RTS, error: %d\n", rc);
        return rc;
    }
    fprintf(stdout, "QP modified to RTS state successfully\n");
    return rc;
}

int connect_qp(struct resources *res)
{
    fprintf(stdout, "Entering function: %s\n", __func__);
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    struct cm_con_data_t tmp_con_data;
    int rc = 0;
    char temp_char;
    union ibv_gid my_gid;

    if (config.gid_idx >= 0) {
        rc = ibv_query_gid(res->ib_ctx, config.ib_port, config.gid_idx, &my_gid);
        if (rc) {
            fprintf(stderr, "could not get gid for port %d, index %d\n", config.ib_port, config.gid_idx);
            return rc;
        }
    } else
        memset(&my_gid, 0, sizeof my_gid);

    local_con_data.addr = htobe64((uintptr_t)res->buf);
    local_con_data.rkey = htonl(res->mr->rkey);
    local_con_data.qp_num = htonl(res->qp->qp_num);
    local_con_data.lid = htons(res->port_attr.lid);
    
    memcpy(local_con_data.gid, &my_gid, 16);
    local_con_data.size = htonl(res->buf_size);

    fprintf(stdout, "Local QP information:\n");
    fprintf(stdout, "  QP number: %u\n", res->qp->qp_num);
    fprintf(stdout, "  LID: %u\n", res->port_attr.lid);
    fprintf(stdout, "Local GID: ");
    
    for (int i = 0; i < 16; i++) {
        fprintf(stdout, "%02x", my_gid.raw[i]);
    }
    fprintf(stdout, "\n");

    if (sock_sync_data(res->sock, sizeof(struct cm_con_data_t), (char *)&local_con_data, (char *)&tmp_con_data) < 0) {
        fprintf(stderr, "[DEBUG] LogStore: Failed to exchange connection data\n");
        rc = 1;
        goto connect_qp_exit;
    }
    printf("[DEBUG] LogStore: Connection data exchanged\n");

    remote_con_data.addr = be64toh(tmp_con_data.addr);
    remote_con_data.rkey = ntohl(tmp_con_data.rkey);
    remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
    remote_con_data.lid = ntohs(tmp_con_data.lid);
    memcpy(remote_con_data.gid, tmp_con_data.gid, 16);
    remote_con_data.size = ntohl(tmp_con_data.size);

    res->remote_props = remote_con_data;

    printf("[DEBUG] QP connected - local QPN: %u, remote QPN: %u\n",
           res->qp->qp_num, res->remote_props.qp_num);
    printf("[DEBUG] Remote addr: %lu, Remote rkey: %u\n",
           res->remote_props.addr, res->remote_props.rkey);


    fprintf(stdout, "Remote QP information:\n");
    fprintf(stdout, "  QP number: %u\n", remote_con_data.qp_num);
    fprintf(stdout, "  LID: %u\n", remote_con_data.lid);
    fprintf(stdout, "  GID: ");
    for (int i = 0; i < 16; i++) {
        fprintf(stdout, "%02x", remote_con_data.gid[i]);
    }
    fprintf(stdout, "\n");

    if (modify_qp_to_init(res->qp)) {
        fprintf(stderr, "change QP state to INIT failed\n");
        goto connect_qp_exit;
    }

    if (res->port_attr.link_layer == IBV_LINK_LAYER_ETHERNET) {
        fprintf(stdout, "Detected Ethernet link layer (RoCE). Using GID-based addressing.\n");
        config.gid_idx = 0;  
    }

    fprintf(stdout, "Modifying QP to RTR with remote QP: %u, remote LID: %u\n", remote_con_data.qp_num, remote_con_data.lid);
    if (config.gid_idx >= 0) {
        fprintf(stdout, "Using GID index: %d\n", config.gid_idx);
        fprintf(stdout, "Remote GID: ");
        for (int i = 0; i < 16; i++) {
            fprintf(stdout, "%02x", remote_con_data.gid[i]);
        }
        fprintf(stdout, "\n");
    }

    if (modify_qp_to_rtr(res->qp, remote_con_data.qp_num, remote_con_data.lid, remote_con_data.gid)) {
        fprintf(stderr, "failed to modify QP state to RTR\n");
        goto connect_qp_exit;
    }

    if (modify_qp_to_rts(res->qp)) {
        fprintf(stderr, "failed to modify QP state to RTS\n");
        goto connect_qp_exit;
    }

    if (remote_con_data.lid == 65535) {
        fprintf(stderr, "Invalid remote LID. This might indicate a RoCE v2 setup.\n");
        fprintf(stderr, "Try setting GID index explicitly in the configuration.\n");
    }

    fprintf(stdout, "QP state was changed to RTS\n");

    fprintf(stdout, "QP ready, waiting for peer...\n");
    if (sock_sync_data(res->sock, 1, "R", &temp_char) < 0) {
        fprintf(stderr, "[DEBUG] LogStore: Final sync failed\n");
        rc = 1;
        goto connect_qp_exit;
    }

    printf("[DEBUG] LogStore: Final sync completed\n");
    fprintf(stdout, "Peer ready, beginning operations\n");

connect_qp_exit:
    return rc;
}

int reset_qp(struct resources *res) {
    struct ibv_qp_attr attr;
    int flags;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;
    if (ibv_modify_qp(res->qp, &attr, IBV_QP_STATE)) {
        fprintf(stderr, "Failed to modify QP to RESET\n");
        return -1;
    }

    if (modify_qp_to_init(res->qp) ||
        modify_qp_to_rtr(res->qp, res->remote_props.qp_num, res->remote_props.lid, res->remote_props.gid) ||
        modify_qp_to_rts(res->qp)) {
        fprintf(stderr, "Failed to transition QP back to RTS\n");
        return -1;
    }

    fprintf(stdout, "Successfully reset QP to RTS state\n");
    return 0;
}

void usage(const char *argv0)
{
    fprintf(stdout, "Usage:\n");
    fprintf(stdout, "  %s start a server and wait for connection\n", argv0);
    fprintf(stdout, "  %s <host> connect to server at <host>\n", argv0);
    fprintf(stdout, "\n");
    fprintf(stdout, "Options:\n");
    fprintf(stdout, "  -p, --port <port> listen on/connect to port <port> (default 18515)\n");
    fprintf(stdout, "  -d, --ib-dev <dev> use IB device <dev> (default first device found)\n");
    fprintf(stdout, "  -i, --ib-port <port> use port <port> of IB device (default 1)\n");
    fprintf(stdout, "  -g, --gid_idx <git index> gid index to be used in GRH (default not used)\n");
}

void print_config(void)
{
    fprintf(stdout, " ------------------------------------------------\n");
    fprintf(stdout, " Device name : \"%s\"\n", config.dev_name);
    fprintf(stdout, " IB port : %u\n", config.ib_port);
    fprintf(stdout, " TCP port : %u\n", config.tcp_port);
    if (config.gid_idx >= 0)
        fprintf(stdout, " GID index : %u\n", config.gid_idx);
    fprintf(stdout, " ------------------------------------------------\n\n");
}

