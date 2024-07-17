#pragma once
#include <functional>
#include <mutex>
#include "storage/GroundDB/rdma.hh"
#include "storage/DSMEngine/ThreadPool.h"

namespace DSMEngine{
	struct RDMA_Request;
}

namespace mempool{

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

} // namespace mempool