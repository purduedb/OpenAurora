#pragma once
#include "c.h"
#include "access/logindex_hashmap.h"
#include "storage/bufpage.h"
#include "storage/GroundDB/mempool_client.h"

// todo (te): OpenAurora/include/port.h define following macros which causes link error. We undef them for now.
#undef printf
#undef sprintf
#undef fprintf

namespace mempool{

class KeyTypeEqualFunction{
public:
    bool operator() (const KeyType &key1, const KeyType &key2) const;
};
#define nullKeyType ((KeyType){(uint64_t)-1, (uint64_t)-1, (uint64_t)-1, (uint32_t)-1, -1})

class PageAddressTable{
public:
	size_t page_array_count();
	size_t page_array_size(size_t pa_idx);
	void get_memnode_id(size_t pa_idx, size_t& memnode_id, size_t& memnode_pa_idx);
	void init(size_t memnode_cnt);
	void append_page_array(size_t memnode_id, size_t pa_idx, size_t pa_size, const ibv_mr& pa_mr, const ibv_mr& pida_mr);
	void at(KeyType pid, RDMAReadPageInfo& info);
	void update(size_t pa_idx, size_t pa_ofs, KeyType pid);
};

} // namespace mempool