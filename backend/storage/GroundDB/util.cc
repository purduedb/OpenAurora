#include "storage/GroundDB/rdma.hh"
#include "storage/GroundDB/util.h"

namespace mempool{
/******************************************************************************
Socket operations
For simplicity, the example program uses TCP sockets to exchange control
information. If a TCP/IP stack/connection is not available, connection manager
(CM) may be used to pass this information. Use of CM is beyond the scope of
this example
******************************************************************************/
/******************************************************************************
 * Function: sock_connect
 *
 * Input
 * serverName URL of server to connect to (NULL for server mode)
 * port port of service
 *
 * Output
 * none
 *
 * Returns
 * socket (fd) on success, negative error code on failure
 *
 * Description
 * Connect a socket. If serverName is specified a client connection will be
 * initiated to the indicated server and port. Otherwise listen on the
 * indicated port for an incoming connection.
 *
 ******************************************************************************/

int sock_connect(const char *serverName, int port)
{
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
    /* Resolve DNS address, use sockfd as temp storage */
    sockfd = getaddrinfo(serverName, service, &hints, &resolved_addr);
    if (sockfd < 0)
    {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), serverName, port);
        goto sock_connect_exit;
    }
    /* Search through results and find the one we want */
    for (iterator = resolved_addr; iterator; iterator = iterator->ai_next)
    {
        sockfd = socket(iterator->ai_family, iterator->ai_socktype, iterator->ai_protocol);
        if (sockfd >= 0)
        {
            if (serverName)
            {
                /* Client mode. Initiate connection to remote */
                if ((tmp = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen)))
                {
                    fprintf(stdout, "failed connect \n");
                    close(sockfd);
                    sockfd = -1;
                }
            }
            else
            {
                /* Server mode. Set up listening socket an accept a connection */
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
        if (serverName)
            fprintf(stderr, "Couldn't connect to %s:%d\n", serverName, port);
        else
        {
            perror("server accept");
            fprintf(stderr, "accept() failed\n");
        }
    }
    return sockfd;
}
/******************************************************************************
 * Function: sock_sync_data
 *
 * Input
 * sock socket to transfer data on
 * xfer_size size of data to transfer
 * local_data pointer to data to be sent to remote
 *
 * Output
 * remote_data pointer to buffer to receive remote data
 *
 * Returns
 * 0 on success, negative error code on failure
 *
 * Description
 * Sync data across a socket. The indicated local data will be sent to the
 * remote. It will then wait for the remote to send its data back. It is
 * assumed that the two sides are in sync and call this function in the proper
 * order. Chaos will ensue if they are not. :)
 *
 * Also note this is a blocking function and will wait for the full data to be
 * received from the remote.
 *
 ******************************************************************************/
static int sock_sync_data(int sock, int xfer_size, char *local_data, char *remote_data)
{
    int rc;
    int read_bytes = 0;
    int total_read_bytes = 0;
    rc = write(sock, local_data, xfer_size);
    if (rc < xfer_size)
        fprintf(stderr, "Failed writing data during sock_sync_data\n");
    else
        rc = 0;
    while (!rc && total_read_bytes < xfer_size)
    {
        read_bytes = read(sock, remote_data, xfer_size);
        if (read_bytes > 0)
            total_read_bytes += read_bytes;
        else
            rc = read_bytes;
    }
    return rc;
}
int sock_sync_data(const struct connection* conn){
    char temp_char;
    return sock_sync_data(conn->sock, 1, "Q", &temp_char);
}
/******************************************************************************
End of socket operations
******************************************************************************/

uint64_t obtain_wr_id(struct memory_region* memreg){
	std::lock_guard<std::mutex> lk(*memreg->wr_id_mtx);
	return memreg->wr_id_cnt++;
}

/* poll_completion */
/******************************************************************************
 * Function: poll_completion
 *
 * Input
 * res pointer to resources structure
 * timeout: the number of millisec to timeout, 0 if no timeout
 *
 * Output
 * none
 *
 * Returns
 * 0 on success, 1 on failure
 *
 * Description
 * Poll the completion queue for a single event. This function will continue to
 * poll the queue until MAX_POLL_CQ_TIMEOUT milliseconds have passed.
 *
 ******************************************************************************/
int poll_completion(const struct resources *res, struct memory_region *memreg, uint64_t wr_id, size_t timeout)
{
    struct ibv_wc wc;
    unsigned long start_time_msec;
    unsigned long cur_time_msec;
    struct timeval cur_time;
    int poll_result;
    int rc = 0;
    /* poll the completion for a while before giving up of doing it .. */
    gettimeofday(&cur_time, NULL);
    start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    // todo: use shared threads to poll multiple completion queues so that CPU cost is reduced
    do
    {
        std::lock_guard<std::mutex> lk(*memreg->poll_cq_mtx);
        poll_result = 0;
        if(memreg->polled_wc.count(wr_id)){
            memreg->polled_wc.erase(wr_id);
            poll_result = 1;
        }
        else{
            poll_result = ibv_poll_cq(memreg->cq, 1, &wc);
            if(poll_result == 1 && wc.wr_id != wr_id){
                memreg->polled_wc[wc.wr_id] = wc;
                poll_result = 0;
            }
        }
        gettimeofday(&cur_time, NULL);
        cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    } while ((poll_result == 0) && (timeout == 0ul || ((cur_time_msec - start_time_msec) < timeout)));
    if (poll_result < 0)
    {
        /* poll CQ failed */
        fprintf(stderr, "poll CQ failed\n");
        rc = 1;
    }
    else if (poll_result == 0)
    { /* the CQ is empty */
        fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
        rc = 1;
    }
    else
    {
        /* CQE found */
        fprintf(stdout, "completion was found in CQ with status 0x%x\n", wc.status);
        /* check the completion status (here we don't care about the completion opcode */
        if (wc.status != IBV_WC_SUCCESS)
        {
            fprintf(stderr, "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n", wc.status,
                    wc.vendor_err);
            rc = 1;
        }
    }
    return rc;
}
// poll completion and return whenever there is a work_completion regardless of wr_id
int poll_any_completion(const struct resources *res, struct memory_region *memreg, uint64_t& wr_id, size_t timeout)
{
    struct ibv_wc wc;
    unsigned long start_time_msec;
    unsigned long cur_time_msec;
    struct timeval cur_time;
    int poll_result;
    int rc = 0;
    /* poll the completion for a while before giving up of doing it .. */
    gettimeofday(&cur_time, NULL);
    start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    do
    {
        std::lock_guard<std::mutex> lk(*memreg->poll_cq_mtx);
        poll_result = 0;
        if(!memreg->polled_wc.empty()){
            wc = memreg->polled_wc.begin()->second;
            memreg->polled_wc.erase(memreg->polled_wc.begin());
            poll_result = 1;
        }
        else
            poll_result = ibv_poll_cq(memreg->cq, 1, &wc);
        gettimeofday(&cur_time, NULL);
        cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
    } while ((poll_result == 0) && (timeout == 0ul || ((cur_time_msec - start_time_msec) < timeout)));
    if (poll_result < 0)
    {
        /* poll CQ failed */
        fprintf(stderr, "poll CQ failed\n");
        rc = 1;
    }
    else if (poll_result == 0){
        /* the CQ is empty */
        fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
        rc = 1;
    }
    else{
        /* CQE found */
        fprintf(stdout, "completion was found in CQ with status 0x%x\n", wc.status);
        /* check the completion status (here we don't care about the completion opcode */
        if (wc.status != IBV_WC_SUCCESS)
        {
            fprintf(stderr, "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n", wc.status,
                    wc.vendor_err);
            rc = 1;
        }
    }
    wr_id = wc.wr_id;
    return rc;
}
/******************************************************************************
 * Function: post_send
 *
 * Input
 * res pointer to resources structure
 * opcode IBV_WR_SEND, IBV_WR_RDMA_READ or IBV_WR_RDMA_WRITE
 *
 * Output
 * none
 *
 * Returns
 * 0 on success, error code on failure
 *
 * Description
 * This function will create and post a send work request
 ******************************************************************************/
int post_send(const struct resources *res, const struct memory_region *memreg, const struct connection *conn, const enum ibv_wr_opcode opcode, uint64_t wr_id, size_t lofs, size_t size, size_t rofs)
{
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr *bad_wr = NULL;
    int rc;
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)memreg->buf + lofs;
    sge.length = size == -1 ? memreg->mr->length : size;
    sge.lkey = memreg->mr->lkey;
    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = wr_id;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = opcode;
    sr.send_flags = IBV_SEND_SIGNALED;
    if (opcode != IBV_WR_SEND)
    {
        sr.wr.rdma.remote_addr = res->remote_props.addr + rofs;
        sr.wr.rdma.rkey = res->remote_props.rkey;
    }
    /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
    rc = ibv_post_send(conn->qp, &sr, &bad_wr);
    if (rc)
        fprintf(stderr, "failed to post SR\n");
    else
    {
        switch (opcode)
        {
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
/******************************************************************************
 * Function: post_receive
 *
 * Input
 * res pointer to resources structure
 *
 * Output
 * none
 *
 * Returns
 * 0 on success, error code on failure
 *
 * Description
 *
 ******************************************************************************/
int post_receive(const struct resources *res, const struct memory_region *memreg, const struct connection *conn, uint64_t wr_id, size_t lofs, size_t size)
{
    struct ibv_recv_wr rr;
    struct ibv_sge sge;
    struct ibv_recv_wr *bad_wr;
    int rc;
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)memreg->buf + lofs;
    sge.length = size == -1 ? memreg->mr->length : size;
    sge.lkey = memreg->mr->lkey;
    /* prepare the receive work request */
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = wr_id;
    rr.sg_list = &sge;
    rr.num_sge = 1;
    /* post the Receive Request to the RQ */
    rc = ibv_post_recv(conn->qp, &rr, &bad_wr);
    if (rc)
        fprintf(stderr, "failed to post RR\n");
    else
        fprintf(stdout, "Receive Request was posted\n");
    return rc;
}
/******************************************************************************
 * Function: resources_create
 *
 * Input
 * res pointer to resources structure to be filled in
 *
 * Output
 * res filled in with resources
 *
 * Returns
 * 0 on success, 1 on failure
 *
 * Description
 *
 * This function creates and allocates all necessary system resources. These
 * are stored in res.
 *****************************************************************************/
int resources_create(struct resources *res, const char *serverName, const char *devName, const int ibPort)
{
    struct ibv_device **dev_list = NULL;
    struct ibv_device *ib_dev = NULL;
    int i;
    int num_devices;
    int rc = 0;
    fprintf(stdout, "searching for IB devices in host\n");
    /* get device names in the system */
    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list)
    {
        fprintf(stderr, "failed to get IB devices list\n");
        rc = 1;
        goto resources_create_exit;
    }
    /* if there isn't any IB device in host */
    if (!num_devices)
    {
        fprintf(stderr, "found %d device(s)\n", num_devices);
        rc = 1;
        goto resources_create_exit;
    }
    fprintf(stdout, "found %d device(s)\n", num_devices);
    /* search for the specific device we want to work with */
    for (i = 0; i < num_devices; i++)
    {
        if (!devName)
        {
            devName = strdup(ibv_get_device_name(dev_list[i]));
            fprintf(stdout, "device not specified, using first one found: %s\n", devName);
        }
        if (!strcmp(ibv_get_device_name(dev_list[i]), devName))
        {
            ib_dev = dev_list[i];
            break;
        }
    }
    /* if the device wasn't found in host */
    if (!ib_dev)
    {
        fprintf(stderr, "IB device %s wasn't found\n", devName);
        rc = 1;
        goto resources_create_exit;
    }
    /* get device handle */
    res->ib_ctx = ibv_open_device(ib_dev);
    if (!res->ib_ctx)
    {
        fprintf(stderr, "failed to open device %s\n", devName);
        rc = 1;
        goto resources_create_exit;
    }
    /* We are now done with device list, free it */
    ibv_free_device_list(dev_list);
    dev_list = NULL;
    ib_dev = NULL;
    /* query port properties */
    if (ibv_query_port(res->ib_ctx, ibPort, &res->port_attr))
    {
        fprintf(stderr, "ibv_query_port on port %u failed\n", ibPort);
        rc = 1;
        goto resources_create_exit;
    }
    /* allocate Protection Domain */
    res->pd = ibv_alloc_pd(res->ib_ctx);
    if (!res->pd)
    {
        fprintf(stderr, "ibv_alloc_pd failed\n");
        rc = 1;
        goto resources_create_exit;
    }
resources_create_exit:
    if (rc)
    {
        /* Error encountered, cleanup */
        if (res->pd)
        {
            ibv_dealloc_pd(res->pd);
            res->pd = NULL;
        }
        if (res->ib_ctx)
        {
            ibv_close_device(res->ib_ctx);
            res->ib_ctx = NULL;
        }
        if (dev_list)
        {
            ibv_free_device_list(dev_list);
            dev_list = NULL;
        }
    }
    return rc;
}
/******************************************************************************
 * Function: register_mr
 *
 * Input
 * res pointer to resources structure
 * buf pointer to the memory segment
 * size size of the memory segment
 *
 * Output
 * none
 *
 * Returns
 * 0 on success, error code on failure
 *
 * Description
 * Register a new memory region.
 ******************************************************************************/
int register_mr(struct memory_region *&memreg, struct resources *res, const char* buf, size_t size){
    int cq_size = 0;
    int mr_flags = 0;
    int rc = 0;
    res->memregs.emplace_back();
    memreg = &res->memregs.back();

    memreg->wr_id_mtx = std::make_unique<std::mutex>();
    memreg->poll_cq_mtx = std::make_unique<std::mutex>();

    /* allocate the memory buffer that will hold the data */
    if (buf == nullptr){
        memreg->buf = new char[size]();
        if (!memreg->buf){
            fprintf(stderr, "failed to malloc %Zu bytes to memory buffer\n", size);
            rc = 1;
            goto register_mr_exit;
        }
        memreg->isBufDeletableFlag = true;
    }
    else {
        memreg->buf = (char*)buf;
        memreg->isBufDeletableFlag = false;
    }
    /* register the memory buffer */
    mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    memreg->mr = ibv_reg_mr(res->pd, memreg->buf, size, mr_flags);
    if (!memreg->mr)
    {
        fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x\n", mr_flags);
        rc = 1;
        goto register_mr_exit;
    }
    fprintf(stdout, "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
            memreg->buf, memreg->mr->lkey, memreg->mr->rkey, mr_flags);
    cq_size = 1024;
    memreg->cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
    if (!memreg->cq)
    {
        fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
        rc = 1;
        goto register_mr_exit;
    }
register_mr_exit:
    if (rc)
    {
        /* Error encountered, cleanup */
        if (memreg->mr)
        {
            ibv_dereg_mr(memreg->mr);
            memreg->mr = NULL;
        }
        if (memreg->buf && memreg->isBufDeletableFlag)
        {
            free(memreg->buf);
            memreg->buf = NULL;
        }
        if (memreg->cq)
        {
            ibv_destroy_cq(memreg->cq);
            memreg->cq = NULL;
        }
        res->memregs.pop_back();
    }
    return rc;
}
/******************************************************************************
 * Function: modify_qp_to_init
 *
 * Input
 * qp QP to transition
 *
 * Output
 * none
 *
 * Returns
 * 0 on success, ibv_modify_qp failure code on failure
 *
 * Description
 * Transition a QP from the RESET to INIT state
 ******************************************************************************/
static int modify_qp_to_init(struct ibv_qp *qp, const int ibPort)
{
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = ibPort;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc)
        fprintf(stderr, "failed to modify QP state to INIT\n");
    return rc;
}
/******************************************************************************
 * Function: modify_qp_to_rtr
 *
 * Input
 * qp QP to transition
 * remote_qpn remote QP number
 * dlid destination LID
 * dgid destination GID (mandatory for RoCEE)
 *
 * Output
 * none
 *
 * Returns
 * 0 on success, ibv_modify_qp failure code on failure
 *
 * Description
 * Transition a QP from the INIT to RTR state, using the specified QP number
 ******************************************************************************/
static int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid, uint8_t *dgid, const int ibPort, const int gid_idx)
{
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
    attr.ah_attr.port_num = ibPort;
    if (gid_idx >= 0)
    {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = gid_idx;
        attr.ah_attr.grh.traffic_class = 0;
    }
    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc)
        fprintf(stderr, "failed to modify QP state to RTR\n");
    return rc;
}
/******************************************************************************
 * Function: modify_qp_to_rts
 *
 * Input
 * qp QP to transition
 *
 * Output
 * none
 *
 * Returns
 * 0 on success, ibv_modify_qp failure code on failure
 *
 * Description
 * Transition a QP from the RTR to RTS state
 ******************************************************************************/
static int modify_qp_to_rts(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 0x12;
    attr.retry_cnt = 6;
    attr.rnr_retry = 0;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;
    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    rc = ibv_modify_qp(qp, &attr, flags);
    if (rc)
        fprintf(stderr, "failed to modify QP state to RTS\n");
    return rc;
}
/******************************************************************************
 * Function: connect_qp
 *
 * Input
 * res pointer to resources structure
 *
 * Output
 * none
 *
 * Returns
 * 0 on success, error code on failure
 *
 * Description
 * Connect the QP. Transition the server side to RTR, sender side to RTS
 ******************************************************************************/
int connect_qp(struct connection *&conn, struct resources *res, struct memory_region *memreg, const char *serverName, const int tcpPort, const int gid_idx, const int ibPort)
{
    // Initialize a struct connection
    int rc = 0;
    struct ibv_qp_init_attr qp_init_attr;
    memreg->conns.emplace_back();
    conn = &memreg->conns.back();
    /* if client side */
    if (serverName)
    {
        conn->sock = sock_connect(serverName, tcpPort);
        if (conn->sock < 0)
        {
            fprintf(stderr, "failed to establish TCP connection to server %s, port %d\n",
                    serverName, tcpPort);
            rc = -1;
            goto connect_qp_exit;
        }
    }
    else
    {
        fprintf(stdout, "waiting on port %d for TCP connection\n", tcpPort);
        conn->sock = sock_connect(NULL, tcpPort);
        if (conn->sock < 0)
        {
            fprintf(stderr, "failed to establish TCP connection with client on port %d\n",
                    tcpPort);
            rc = -1;
            goto connect_qp_exit;
        }
    }
    fprintf(stdout, "TCP connection was established\n");
    /* create the Queue Pair */
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    qp_init_attr.send_cq = memreg->cq;
    qp_init_attr.recv_cq = memreg->cq;
    qp_init_attr.cap.max_send_wr = 2500;
    qp_init_attr.cap.max_recv_wr = 2500;
    qp_init_attr.cap.max_send_sge = 30;
    qp_init_attr.cap.max_recv_sge = 30;
    conn->qp = ibv_create_qp(res->pd, &qp_init_attr);
    if (!conn->qp)
    {
        fprintf(stderr, "failed to create QP\n");
        rc = 1;
        goto connect_qp_exit;
    }
    fprintf(stdout, "QP was created, QP number=0x%x\n", conn->qp->qp_num);

    // Connect queue pair
    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    struct cm_con_data_t tmp_con_data;
    char temp_char;
    union ibv_gid my_gid;
    if (gid_idx >= 0)
    {
        rc = ibv_query_gid(res->ib_ctx, ibPort, gid_idx, &my_gid);
        if (rc)
        {
            fprintf(stderr, "could not get gid for port %d, index %d\n", ibPort, gid_idx);
            return rc;
        }
    }
    else
        memset(&my_gid, 0, sizeof my_gid);
    /* exchange using TCP sockets info required to connect QPs */
    local_con_data.addr = htonll((uintptr_t)memreg->buf);
    local_con_data.rkey = htonl(memreg->mr->rkey);
    local_con_data.qp_num = htonl(conn->qp->qp_num);
    local_con_data.lid = htons(res->port_attr.lid);
    memcpy(local_con_data.gid, &my_gid, 16);
    fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
    if (sock_sync_data(conn->sock, sizeof(struct cm_con_data_t), (char *)&local_con_data, (char *)&tmp_con_data) < 0)
    {
        fprintf(stderr, "failed to exchange connection data between sides\n");
        rc = 1;
        goto connect_qp_exit;
    }
    remote_con_data.addr = ntohll(tmp_con_data.addr);
    remote_con_data.rkey = ntohl(tmp_con_data.rkey);
    remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
    remote_con_data.lid = ntohs(tmp_con_data.lid);
    memcpy(remote_con_data.gid, tmp_con_data.gid, 16);
    /* save the remote side attributes, we will need it for the post SR */
    res->remote_props = remote_con_data;
    fprintf(stdout, "Remote address = 0x%" PRIx64 "\n", remote_con_data.addr);
    fprintf(stdout, "Remote rkey = 0x%x\n", remote_con_data.rkey);
    fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data.qp_num);
    fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data.lid);
    if (gid_idx >= 0)
    {
        uint8_t *p = remote_con_data.gid;
        fprintf(stdout, "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ", p[0],
                p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]);
    }
    /* modify the QP to init */
    rc = modify_qp_to_init(conn->qp, ibPort);
    if (rc)
    {
        fprintf(stderr, "change QP state to INIT failed\n");
        goto connect_qp_exit;
    }
    /* modify the QP to RTR */
    rc = modify_qp_to_rtr(conn->qp, remote_con_data.qp_num, remote_con_data.lid, remote_con_data.gid, ibPort, gid_idx);
    if (rc)
    {
        fprintf(stderr, "failed to modify QP state to RTR\n");
        goto connect_qp_exit;
    }
    rc = modify_qp_to_rts(conn->qp);
    if (rc)
    {
        fprintf(stderr, "failed to modify QP state to RTR\n");
        goto connect_qp_exit;
    }
    fprintf(stdout, "QP state was change to RTS\n");
    /* sync to make sure that both sides are in states that they can connect to prevent packet loose */
    if (sock_sync_data(conn->sock, 1, "Q", &temp_char)) /* just send a dummy char back and forth */
    {
        fprintf(stderr, "sync error after QPs are were moved to RTS\n");
        rc = 1;
    }
connect_qp_exit:
    if (rc)
    {
        /* Error encountered, cleanup */
        if (conn->qp)
        {
            ibv_destroy_qp(conn->qp);
            conn->qp = NULL;
        }
        if (conn->sock >= 0)
        {
            if (close(conn->sock))
                fprintf(stderr, "failed to close socket\n");
            conn->sock = -1;
        }
        memreg->conns.pop_back();
    }
    return rc;
}
/******************************************************************************
 * Function: resources_destroy
 *
 * Input
 * res pointer to resources structure
 *
 * Output
 * none
 *
 * Returns
 * 0 on success, 1 on failure
 *
 * Description
 * Cleanup and deallocate all resources used
 ******************************************************************************/
int resources_destroy(struct resources *res)
{
    int rc = 0;
    for(auto& memreg: res->memregs){
        for(auto& conn: memreg.conns){
            if (conn.qp)
                if (ibv_destroy_qp(conn.qp))
                {
                    fprintf(stderr, "failed to destroy QP\n");
                    rc = 1;
                }
            if (conn.sock >= 0)
                if (close(conn.sock))
                {
                    fprintf(stderr, "failed to close socket\n");
                    rc = 1;
                }
        }
        if (memreg.cq)
            if (ibv_destroy_cq(memreg.cq))
            {
                fprintf(stderr, "failed to destroy CQ\n");
                rc = 1;
            }
        if (memreg.mr)
            if (ibv_dereg_mr(memreg.mr))
            {
                fprintf(stderr, "failed to deregister MR\n");
                rc = 1;
            }
        if (memreg.buf && memreg.isBufDeletableFlag)
            free(memreg.buf);
    }
    if (res->pd)
        if (ibv_dealloc_pd(res->pd))
        {
            fprintf(stderr, "failed to deallocate PD\n");
            rc = 1;
        }
    if (res->ib_ctx)
        if (ibv_close_device(res->ib_ctx))
        {
            fprintf(stderr, "failed to close device context\n");
            rc = 1;
        }
    return rc;
}

} // namespace mempool