#include "storage/DSMEngine/ThreadPool.h"

namespace DSMEngine {

void ThreadPool::BGThread(uint32_t thread_id) {
	uint32_t  miss_poll_counter = 0;
	auto task_queue =  queue_pool[thread_id];
	while (true) {
		// Wait until there is an item that is ready to run
		// Stop waiting if the thread needs to do work or needs to terminate.

		while (!exit_all_threads_.load() && task_queue->empty() ) {
			if(++miss_poll_counter < 10240){
				continue;
			}
			if(++miss_poll_counter < 20480){
				usleep(1);
				continue;
			}
			if(++miss_poll_counter < 40960){
				usleep(16);
				continue;
			}else{
				usleep(512);
				continue;
			}
		}
		miss_poll_counter = 0;

		if (exit_all_threads_.load()) {  // mechanism to let BG threads exit safely
			if (!wait_for_jobs_to_complete_.load() ||
				task_queue->empty()) {
				break;
			}
		}

		auto func = std::move(task_queue->front().function);
		void* args = task_queue->front().args;
		func(args);
		task_queue->pop();
	}
}

void ThreadPool::StartBGThreads() {
	// Start background thread if necessary

	for (int i = 0; i < total_threads_limit_; ++i) {
		auto temp = new boost::lockfree::spsc_queue<BGItem>(RECEIVE_OUTSTANDING_SIZE);
		queue_pool.emplace_back(temp);
	}
	for (int i = 0; i < total_threads_limit_; ++i) {
		port::Thread p_t(&ThreadPool::BGThread, this, i);
		bgthreads_.push_back(std::move(p_t));
	}

}

void ThreadPool::Schedule(std::function<void(void *args)> &&func, void *args, uint32_t thread_id) {
	if (exit_all_threads_.load()) {
	return;
	}
	BGItem item = BGItem();
	item.function = std::move(func);
	item.args = args;
	// Add to priority queue
	queue_pool[thread_id == -1 ? rand() % total_threads_limit_ : thread_id]->push(item);
}

void ThreadPool::JoinThreads(bool wait_for_jobs_to_complete) {
	assert(!exit_all_threads_);

	wait_for_jobs_to_complete_.store(wait_for_jobs_to_complete);
	exit_all_threads_.store(true);
	// prevent threads from being recreated right after they're joined, in case
	// the user is concurrently submitting jobs.
	total_threads_limit_ = 0;

	for (auto& th : bgthreads_) {
		th.join();
	}

	bgthreads_.clear();
	for(auto iter : queue_pool){
		delete iter;
	}
	exit_all_threads_.store(false);
	wait_for_jobs_to_complete_.store(false);
}

void ThreadPool::SetBackgroundThreads(int num){
	total_threads_limit_ = num;
	StartBGThreads();
}

}  // namespace DSMEngine
