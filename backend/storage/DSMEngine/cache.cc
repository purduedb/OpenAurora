// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "storage/DSMEngine/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <infiniband/verbs.h>

#include "port/port.h"
#include "port/thread_annotations.h"
#include "storage/DSMEngine/HugePageAlloc.h"
#include "utils/DSMEngine/hash.h"
// #include "rdma.h"
// #include "storage/page.h"

// DO not enable the two at the same time otherwise there will be a bug.
#define BUFFER_HANDOVER
//#define EARLY_LOCK_RELEASE
uint64_t cache_miss[MAX_APP_THREAD][8];
uint64_t cache_hit[MAX_APP_THREAD][8];
uint64_t invalid_counter[MAX_APP_THREAD][8];
uint64_t lock_fail[MAX_APP_THREAD][8];
uint64_t pattern[MAX_APP_THREAD][8];
uint64_t hierarchy_lock[MAX_APP_THREAD][8];
uint64_t handover_count[MAX_APP_THREAD][8];
uint64_t hot_filter_count[MAX_APP_THREAD][8];
uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
extern bool Show_Me_The_Print;
int TimePrintCounter[MAX_APP_THREAD];
namespace DSMEngine {

bool KeyTypeCmp (const KeyType& a, const KeyType& b) {
    return a.SpcID == b.SpcID && a.DbID == b.DbID && a.RelID == b.RelID
        && a.ForkNum == b.ForkNum && a.BlkNum == b.BlkNum;
}


//std::atomic<uint64_t> LRUCache::counter = 0;
Cache::~Cache() {}








LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
      Unref(e, nullptr);
    e = next;
  }
}
//Can we use the lock within the handle to reduce the conflict here so that the critical seciton
// of the cache shard lock will be minimized.
    void LRUCache::Ref(LRUHandle* e) {
        if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
            LRU_Remove(e);
            LRU_Append(&in_use_, e);
        }
        e->refs++;
    }

void LRUCache::Unref(LRUHandle *e, SpinLock *spin_l) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate.
      //Finish erase will only goes here, or directly return. it will never goes to next if clause
//        mutex_.unlock();
#ifdef EARLY_LOCK_RELEASE
      if (spin_l!= nullptr){
          //Early releasing the lock to avoid the RDMA lock releasing in the critical section.
          assert(spin_l->check_own() == true);
          spin_l->Unlock();
      }
#endif
    LRU_Remove(e);
    assert(!e->in_cache);
    if(e->deleter != nullptr)
      (*e->deleter)(e);
    delete e;
  } else if (e->in_cache && e->refs == 1) {
    // No longer in use; move to lru_ list.
    LRU_Remove(e);// remove from in_use list move to LRU list.
    LRU_Append(&lru_, e);
  }
}


void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const KeyType& key, uint32_t hash) {
//  MutexLock l(&mutex_);
    SpinLock l(&mutex_);
    //TOTHINK(ruihong): shoul we update the lru list after look up a key?
    //  Answer: Ref will refer this key and later, the outer function has to call
    // Unref or release which will update the lRU list.
    LRUHandle* e = table_.Lookup(key, hash);
    if (e != nullptr) {
        assert(e->refs >= 1);
        Ref(e);
    }
    return reinterpret_cast<Cache::Handle*>(e);
}
Cache::Handle *DSMEngine::LRUCache::LookupInsert(const KeyType &key, uint32_t hash, void *value, size_t charge,
                                                 void (*deleter)(Cache::Handle* handle)) {
    SpinLock l(&mutex_);
    //TOTHINK(ruihong): shoul we update the lru list after look up a key?
    //  Answer: Ref will refer this key and later, the outer function has to call
    // Unref or release which will update the lRU list.
    LRUHandle* e = table_.Lookup(key, hash);
    if (e != nullptr) {
        assert(e->refs >= 1);
        Ref(e);
        return reinterpret_cast<Cache::Handle*>(e);
    }else{
        // This LRU handle is not initialized.
        // TODO: get the LRU handle from the free list.
        e = new LRUHandle();

        e->value = value;
        e->remote_lock_status = 0;
        e->remote_lock_urge = false;
        e->strategy = 1;
        e->page_id = key;

        e->deleter = deleter;
        e->charge = charge;
        e->hash = hash;
        e->in_cache = false;
        e->refs = 1;  // for the returned handle.
        if (capacity_ > 0) {
            e->refs++;  // for the table_cache's reference. refer here and unrefer outside
            e->in_cache = true;
            LRU_Append(&in_use_, e);// Finally it will be pushed into LRU list
            usage_ += charge;
            FinishErase(table_.Insert(e), &l);//table_.Insert(e) will return LRUhandle with duplicate key as e, and then delete it by FinishErase
        } else {  // don't do caching. (capacity_==0 is supported and turns off caching.)
            // next is read by key() in an assert, so it must be initialized
            e->next = nullptr;
        }
#ifdef EARLY_LOCK_RELEASE
        if (!l.check_own()){
            l.Lock();
        }
#endif
        assert(usage_ <= capacity_ + kLeafPageSize + kInternalPageSize);
        // This will remove some entry from LRU if the table_cache over size.
#ifdef BUFFER_HANDOVER
        bool already_reuse_fl_entry = false;
#endif
//        if (counter.fetch_add(1) == 100000){
//            printf("capacity is %zu, usage is %zu\n", capacity_, usage_);
//            counter = 0;
//        }
        while (usage_ > capacity_ && lru_.next != &lru_) {
            LRUHandle* old = lru_.next;
            assert(old->refs == 1);
            if(e->value == nullptr) e->value = old->value;
            // Directly reuse the mr if the evicted cache entry is the same size as the new inserted on.
#ifdef BUFFER_HANDOVER
            if (value == nullptr && !already_reuse_fl_entry){
                e->value = old->value;
                already_reuse_fl_entry = true;
            }
#endif
            bool erased = FinishErase(table_.Remove(old->key(), old->hash), &l);
#ifdef EARLY_LOCK_RELEASE
            if (!l.check_own()){
                l.Lock();
            }
#endif
            if (!erased) {  // to avoid unused variable when compiled NDEBUG
                assert(erased);
            }

        }
        assert(usage_ <= capacity_);

        if(value == nullptr && !already_reuse_fl_entry)
          e->value = freelist_->pop_front();

        return reinterpret_cast<Cache::Handle*>(e);
    }
}
Cache::Handle* LRUCache::LookupErase(const KeyType& key, uint32_t hash) {
    SpinLock l(&mutex_);
    LRUHandle* e = table_.Remove(key, hash);
    if (e != nullptr) {
        assert(e->refs >= 1);
        Ref(e);
        FinishErase(e, &l);
        freelist_->push_back((mempool::PageMeta*)(e->value));
    }
    return reinterpret_cast<Cache::Handle*>(e);
}
void LRUCache::Release(Cache::Handle* handle) {
  SpinLock l(&mutex_);
    Unref(reinterpret_cast<LRUHandle *>(handle), &l);
}
//If the inserted key has already existed, then the old LRU handle will be removed from
// the cache, but it may not garbage-collected right away.
Cache::Handle* LRUCache::Insert(const KeyType& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(Cache::Handle* handle)) {
//  MutexLock l(&mutex_);

  //TODO: set the LRUHandle within the page, so that we can check the reference, during the direct access, or we reserver
  // a place hodler for the address pointer to the LRU handle of the page.
  LRUHandle* e = new LRUHandle();
//      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));

    e->remote_lock_status = 0;
    e->remote_lock_urge = false;
    e->strategy = 1;
    e->page_id = key;

  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
//  std::memcpy(e->key_data, key.data(), key.size());
//  WriteLock l(&mutex_);
  SpinLock l(&mutex_);
  if (capacity_ > 0) {
    e->refs++;  // for the table_cache's reference. refer here and unrefer outside
    e->in_cache = true;
    LRU_Append(&in_use_, e);// Finally it will be pushed into LRU list
    usage_ += charge;
      FinishErase(table_.Insert(e), &l);//table_.Insert(e) will return LRUhandle with duplicate key as e, and then delete it by FinishErase
  } else {  // don't do caching. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }
#ifdef EARLY_LOCK_RELEASE

    if (!l.check_own()){
        l.Lock();
    }
#endif
        assert(usage_ <= capacity_ + kLeafPageSize + kInternalPageSize);
  // This will remove some entry from LRU if the table_cache over size.
#ifdef BUFFER_HANDOVER
            bool already_reuse_fl_entry = false;
#endif

    while (usage_ > capacity_ && lru_.next != &lru_) {

    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
#ifdef BUFFER_HANDOVER
      if (value == nullptr && !already_reuse_fl_entry){
          e->value = old->value;
          already_reuse_fl_entry = true;
      }
#endif
    assert(l.check_own());
    bool erased = FinishErase(table_.Remove(old->key(), old->hash), &l);
    //some times the finsih Erase will release the spinlock to let other threads working during the RDMA lock releasing.
    //We need to regain the lock here in case that there is another cache entry eviction.
#ifdef EARLY_LOCK_RELEASE
      if (!l.check_own()){
          l.Lock();
      }
#endif
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  if(value == nullptr && !already_reuse_fl_entry)
    e->value = freelist_->pop_front();

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the table_cache;
// it must have already been removed from the hash table.  Return whether e != nullptr.
// Remove the handle from LRU and change the usage.
bool LRUCache::FinishErase(LRUHandle *e, SpinLock *spin_l) {

  if (e != nullptr) {
//#ifndef NDEBUG
//      if (e->gptr.offset < 9480863232){
//          printf("page of %lu is removed from the cache\n", e->gptr.offset);
//      }
//#endif
    assert(e->in_cache);
    e->in_cache = false;
    usage_ -= e->charge;
  // decrease the reference of cache, making it not pinned by cache, but it
  // can still be pinned outside the cache.
//      assert(e->refs == 1);
      Unref(e, spin_l);

  }
  return e != nullptr;
}

void LRUCache::Erase(const KeyType& key, uint32_t hash) {
//  MutexLock l(&mutex_);
//  WriteLock l(&mutex_);
  SpinLock l(&mutex_);
  LRUHandle* e = table_.Remove(key, hash);
  auto page_meta = (mempool::PageMeta*)(e->value);
  FinishErase(e, &l);
  freelist_->push_back(page_meta);
}

void LRUCache::Prune() {
//  MutexLock l(&mutex_);
//  WriteLock l(&mutex_);
    SpinLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash), nullptr);
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}




static const int kNumShardBits = 7;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;
  size_t capacity_;
  mempool::FreeList* freelist_;

  static inline uint32_t HashKeyType(const KeyType& s) {
    return Hash(&s, 0);
  }

  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity, mempool::FreeList* fl) : last_id_(0), freelist_(fl) {
    capacity_ = capacity;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity((capacity + s) / kNumShards);
      shard_[s].freelist_ = fl;
    }
  }
  ~ShardedLRUCache() override {}
  size_t GetCapacity() override{
      return capacity_;
  }
    // if there has already been a cache entry with the same key, the old one will be
    // removed from the cache, but it may not be garbage collected right away
  Handle* Insert(const KeyType& key, void* value, size_t charge,
                 void (*deleter)(Cache::Handle* handle)) override {
#ifndef NDEBUG
        assert(capacity_ >= 1000);
        if (TotalCharge() > 0.9 * capacity_ ){
            for (int i = 0; i < kNumShards - 1; ++i) {
                if (shard_[i+1].TotalCharge() >0 && shard_[i].TotalCharge()/shard_[i+1].TotalCharge() >= 2){
//                    printf("Uneven cache distribution\n");
                    assert(false);
                    break;
                }
            }
        }

#endif
    const uint32_t hash = HashKeyType(key);

//        auto handle = shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
//        printf("Insert: refer to handle %p\n", handle);

        return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  //TODO: Change the search key to GlobalAddress.
  Handle* Lookup(const KeyType& key) override {
      assert(capacity_ >= 1000);
    const uint32_t hash = HashKeyType(key);

//    auto handle = shard_[Shard(hash)].Lookup(key, hash);
//      printf("Look up: refer to handle %p\n", handle);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
    Handle* LookupInsert(const KeyType& key,  void* value,
                         size_t charge,
                         void (*deleter)(Cache::Handle* handle)) override{

                assert(capacity_ >= 1000);
                const uint32_t hash = HashKeyType(key);
                return shard_[Shard(hash)].LookupInsert(key, hash, value, charge, deleter);
  };
  Handle* LookupErase(const KeyType& key) override {
    const uint32_t hash = HashKeyType(key);
    return shard_[Shard(hash)].LookupErase(key, hash);
  }
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
//      printf("release handle %p\n", handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  void Erase(const KeyType& key) override {
    const uint32_t hash = HashKeyType(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};


Cache* NewLRUCache(size_t capacity, mempool::FreeList* fl) { return new ShardedLRUCache(capacity, fl); }


LocalBuffer::LocalBuffer(const CacheConfig &cache_config) {
        size = cache_config.cacheSize;
        data = (uint64_t)hugePageAlloc(size * define::GB);
    if (data == 0){
        data = (int64_t) malloc(size * define::GB);
    }
}

}  // namespace DSMEngine
