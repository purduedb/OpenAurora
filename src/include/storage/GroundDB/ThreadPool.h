//
// Created by ruihong on 7/29/21.
//

#ifndef DSMEngine_THREADPOOL_H
#define DSMEngine_THREADPOOL_H
#include <condition_variable>
#include <deque>
#include <mutex>
#include <functional>
#include <vector>
#include <atomic>
#include <port/port_posix.h>
#include <assert.h>
namespace DSMEngine {
struct BGItem {
  std::function<void(void* args)> function;
  void* args;
};
struct BGQueue {
  std::mutex mtx;
  std::condition_variable cv;
  std::deque<BGItem> items;
};
//TODO: need the thread pool to be lightweight so that the invalidation message overhead will be minimum.
class ThreadPool{
 public:

  std::vector<port::Thread> bgthreads_;
  BGQueue shared_queue_;
  int total_threads_limit_;
  std::atomic<bool> exit_all_threads_ = false;
  std::atomic<bool> wait_for_jobs_to_complete_;
  void BGThread(uint32_t thread_id);
  void StartBGThreads();
  void Schedule(std::function<void(void *args)> &&func, void *args, uint32_t thread_id = -1);
  void JoinThreads(bool wait_for_jobs_to_complete);
  void SetBackgroundThreads(int num);
};


}


#endif  // DSMEngine_THREADPOOL_H
