#pragma once

#include <functional>
#include <mutex>
#include "storage/GroundDB/rdma.hh"
#include "storage/GroundDB/ThreadPool.h"

namespace DSMEngine{
struct RDMA_Request;
} // namespace DSMEngine

namespace mempool{

struct XLogInfo{
    bool valid;
    uint64_t RpcXLogFlushedLsn;
    XLogRecPtr ProcLastRecPtr, XactLastRecEnd, XactLastCommitEnd;
    XLogRecPtr LogwrtResult_Write, LogwrtResult_Flush;
};
struct UpdatePVTInfo{
    KeyType page_id;
    XLogRecPtr lsn;
};
struct flush_page_request{
	uint8_t page_data[BLCKSZ];
	KeyType page_id;
};

struct access_page_request{
	KeyType page_id;
};

struct remove_page_request{
	KeyType page_id;
};

struct sync_rat_request{
	size_t pa_idx;
	size_t pa_ofs;
};
// Now only support fixed size
#define SYNC_RAT_SIZE 256
struct sync_rat_response{
	KeyType page_id_array[SYNC_RAT_SIZE];
};

struct mr_info_request{
	size_t pa_idx;
};
struct mr_info_response{
	ibv_mr pa_mr;
	ibv_mr pida_mr;
};

struct flush_xlog_info_request{
    XLogInfo xlog_info;
};
struct fetch_xlog_info_response{
    XLogInfo xlog_info;
};

struct flush_update_pvt_info_request{
    UpdatePVTInfo info;
};
struct fetch_update_pvt_info_request{
    size_t ptr;
};
struct fetch_update_pvt_info_response{
    UpdatePVTInfo info;
};
struct get_first_update_pvt_info_idx_response{
    size_t idx;
};

struct register_page_request{
    KeyType page_id;
};
struct register_page_response{
    bool exists;
    size_t pa_idx, pa_ofs;
};
struct unregister_page_request{
    KeyType page_id;
};

} // namespace mempool