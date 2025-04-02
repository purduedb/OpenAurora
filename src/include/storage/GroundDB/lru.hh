#pragma once
#include <list>
#include <memory>
#include <mutex>

namespace mempool {

struct PageMeta{
	void* page_addr;
	void* page_id_addr;
};
class FreeList{
public:
	std::list<PageMeta*> list_;
	std::unique_ptr<std::mutex> mtx_;

	void init(){
		mtx_ = std::make_unique<std::mutex>();
	}
	void push_back(PageMeta* ele){
		std::lock_guard lk(*mtx_);
		list_.push_back(ele);
	}
	PageMeta* pop_front(){
		std::lock_guard lk(*mtx_);
		if(list_.empty())
			return nullptr;
		auto ele = list_.front();
		list_.pop_front();
		return ele;
	}
};

} // namespace mempool