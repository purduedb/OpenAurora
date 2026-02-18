#include "storage/GroundDB/ThreadPool.h"

namespace DSMEngine {

void ThreadPool::BGThread(uint32_t thread_id) {
	BGQueue& q = shared_queue_;
	while (true) {
		BGItem item;
		{
			std::unique_lock<std::mutex> lock(q.mtx);
			q.cv.wait(lock, [this, &q] {
				return exit_all_threads_.load() || !q.items.empty();
			});

			if (exit_all_threads_.load()) {
				if (!wait_for_jobs_to_complete_.load() || q.items.empty())
					break;
			}
			if (q.items.empty())
				continue;

			item = std::move(q.items.front());
			q.items.pop_front();
		}

		void* args = item.args;
		item.function(args);
	}
}

void ThreadPool::StartBGThreads() {
	for (int i = 0; i < total_threads_limit_; ++i) {
		port::Thread p_t(&ThreadPool::BGThread, this, i);
		bgthreads_.push_back(std::move(p_t));
	}
}

void ThreadPool::Schedule(std::function<void(void *args)> &&func, void *args, uint32_t thread_id) {
	if (exit_all_threads_.load())
		return;
	BGItem item;
	item.function = std::move(func);
	item.args = args;
	{
		std::lock_guard<std::mutex> lock(shared_queue_.mtx);
		shared_queue_.items.push_back(std::move(item));
		shared_queue_.cv.notify_one();
	}
}

void ThreadPool::JoinThreads(bool wait_for_jobs_to_complete) {
	assert(!exit_all_threads_);

	wait_for_jobs_to_complete_.store(wait_for_jobs_to_complete);
	exit_all_threads_.store(true);
	total_threads_limit_ = 0;

	shared_queue_.cv.notify_all();
	for (auto& th : bgthreads_) {
		th.join();
	}
	bgthreads_.clear();
	exit_all_threads_.store(false);
	wait_for_jobs_to_complete_.store(false);
}

void ThreadPool::SetBackgroundThreads(int num){
	total_threads_limit_ = num;
	StartBGThreads();
}

}  // namespace DSMEngine
