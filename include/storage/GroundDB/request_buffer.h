#pragma once

#include <functional>
#include <mutex>
#include "storage/GroundDB/rdma.hh"
#include "storage/DSMEngine/ThreadPool.h"

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
struct UpdateVersionMapInfo{
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

struct sync_pat_request{
	size_t pa_idx;
	size_t pa_ofs;
};
// Now only support fixed size
#define SYNC_PAT_SIZE 256
struct sync_pat_response{
	KeyType page_id_array[SYNC_PAT_SIZE];
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

struct flush_update_vm_info_request{
    UpdateVersionMapInfo info;
};
struct fetch_update_vm_info_request{
    size_t ptr;
};
struct fetch_update_vm_info_response{
    UpdateVersionMapInfo info;
};
struct get_first_update_vm_info_idx_response{
    size_t idx;
};

} // namespace mempool