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
#include <boost/lockfree/spsc_queue.hpp>
namespace DSMEngine {
class DBImpl;
enum ThreadPoolType{FlushThreadPool, CompactionThreadPool, SubcompactionThreadPool};
struct BGItem {
  std::function<void(void* args)> function;
  void* args;
};
struct BGThreadMetadata {
  void* rdma_mg;
  void* func_args;
};
//TODO: need the thread pool to be lightweight so that the invalidation message overhead will be minimum.
class ThreadPool{
 public:

  std::vector<port::Thread> bgthreads_;
  std::vector<boost::lockfree::spsc_queue<BGItem>*> queue_pool;
  ThreadPoolType Type_;
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
