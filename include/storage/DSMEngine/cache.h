// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the table_cache
// capacity.  For example, a table_cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin table_cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable table_cache sizing, etc.)

#ifndef STORAGE_DSMEngine_INCLUDE_CACHE_H_
#define STORAGE_DSMEngine_INCLUDE_CACHE_H_

#include <cstdint>
#include <map>

#include "c.h"
#include "access/logindex_hashmap.h"
#include "storage/DSMEngine/export.h"
#include <shared_mutex>

#include "storage/DSMEngine/Config.h"
#include "utils/DSMEngine/mutexlock.h"
#include "storage/GroundDB/lru.hh"

namespace DSMEngine {
//class RDMA_Manager;
//class ibv_mr;

bool KeyTypeCmp (const KeyType& a, const KeyType& b);

    // LRU table_cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the table_cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the table_cache.
//
// The table_cache keeps two linked lists of items in the table_cache.  All items in the
// table_cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the table_cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the table_cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.

class DSMEngine_EXPORT Cache;

// Create a new table_cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
DSMEngine_EXPORT Cache* NewLRUCache(size_t capacity, mempool::FreeList* fl);

class DSMEngine_EXPORT Cache {
 public:
  Cache() = default;
    struct Handle {
    public:
        void* value = nullptr;
        std::atomic<uint32_t> refs;     // References, including table_cache reference, if present.
        //TODO: the internal node may not need the rw_mtx below, maybe we can delete them.
        std::atomic<bool> remote_lock_urge;
        std::atomic<int> remote_lock_status = 0; // 0 unlocked, 1 read locked, 2 write lock
        KeyType page_id;
        std::atomic<int> strategy = 1; // strategy 1 normal read write locking without releasing, strategy 2. Write lock with release, optimistic latch free read.
        bool keep_the_mr = false;
        std::shared_mutex rw_mtx;
        // RDMA_Manager* rdma_mg = nullptr;
        void (*deleter)(Cache::Handle* handle);
        ~Handle(){}
    };
  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache();

  // Opaque handle to an entry stored in the table_cache.

  virtual size_t GetCapacity() = 0;

  //TODO: change the KeyType key to GlobalAddress& key.

  // Insert a mapping from key->value into the table_cache and assign it
  // the specified charge against the total table_cache capacity.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  virtual Handle* Insert(const KeyType& key, void* value, size_t charge,
                         void (*deleter)(Cache::Handle* handle)) = 0;

  // If the table_cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle* Lookup(const KeyType& key) = 0;
  //Atomic cache look up and Insert a new handle atomically for the key if not found.
  virtual Handle* LookupInsert(const KeyType& key, void* value,
                                size_t charge,
                                void (*deleter)(Cache::Handle* handle)) = 0;
  virtual Handle* LookupErase(const KeyType& key) = 0;
  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void* Value(Handle* handle) = 0;

  // If the table_cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const KeyType& key) = 0;
//TODO: a new function which pop out a least recent used entry. which will be reused later.

  // Return a new numeric id.  May be used by multiple clients who are
  // sharing the same table_cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its table_cache keys.
  virtual uint64_t NewId() = 0;

  // Remove all table_cache entries that are not actively in use.  Memory-constrained
  // applications may wish to call this method to reduce memory usage.
  // Internal_and_Leaf implementation of Prune() does nothing.  Subclasses are strongly
  // encouraged to override the default implementation.  A future release of
  // DSMEngine may change Prune() to a pure abstract method.
  virtual void Prune() {}

  // Return an estimate of the combined charges of all elements stored in the
  // table_cache.
  virtual size_t TotalCharge() const = 0;

 private:
  void LRU_Remove(Handle* e);
  void LRU_Append(Handle* e);
  void Unref(Handle* e);

  struct Rep;
  Rep* rep_;
};
    struct LRUHandle : Cache::Handle {


        LRUHandle* next_hash;// Next LRUhandle in the hash
        LRUHandle* next;
        LRUHandle* prev;
        size_t charge;  // TODO(opt): Only allow uint32_t?
        std::atomic<bool> in_cache;     // Whether entry is in the table_cache.
        uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
//        char key_data[1];  // Beginning of key

        KeyType key() const {
            // next_ is only equal to this if the LRU handle is the list head of an
            // empty list. List heads never have meaningful keys.
            assert(next != this);
            return this->page_id;
        }
    };
class LocalBuffer {

    public:
    LocalBuffer(const CacheConfig &cache_config);

        uint64_t data;
        uint64_t size;

    private:
    };

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
public:
    HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
    ~HandleTable() { delete[] list_; }

    LRUHandle* Lookup(const KeyType& key, uint32_t hash) {
        return *FindPointer(key, hash);
    }
    //
    LRUHandle* Insert(LRUHandle* h) {
        LRUHandle** ptr = FindPointer(h->key(), h->hash);
        LRUHandle* old = *ptr;
        // if we find a LRUhandle whose key is same as h, we replace that LRUhandle
        // with h, if not find, we just put the h at the end of the list.
        h->next_hash = (old == nullptr ? nullptr : old->next_hash);
        *ptr = h;
        if (old == nullptr) {
            ++elems_;
            if (elems_ > length_) { // length_ is the size limit for current cache.
                // Since each table_cache entry is fairly large, we aim for a small
                // average linked list length (<= 1).
                Resize();
            }
        }
        return old;
    }

    LRUHandle* Remove(KeyType key, uint32_t hash) {
        LRUHandle** ptr = FindPointer(key, hash);
        LRUHandle* result = *ptr;
        //TODO: only erase those lru handles which has been accessed. THis can prevent
        // a index block being invalidated multiple times.
        if (result != nullptr) {
            //*ptr belongs to the Handle previous to the result.
            *ptr = result->next_hash;// ptr is the "next_hash" in the handle previous to the result
            --elems_;
        }
        return result;
    }

private:
    // The table consists of an array of buckets where each bucket is
    // a linked list of table_cache entries that hash into the bucket.
    uint32_t length_;
    uint32_t elems_;
    LRUHandle** list_;
    // Return a pointer to slot that points to a table_cache entry that
    // matches key/hash.  If there is no such table_cache entry, return a
    // pointer to the trailing slot in the corresponding linked list.
    LRUHandle** FindPointer(KeyType key, uint32_t hash) {
        LRUHandle** ptr = &list_[hash & (length_ - 1)];
        while (*ptr != nullptr && ((*ptr)->hash != hash || !KeyTypeCmp(key, (*ptr)->key()))) {
            ptr = &(*ptr)->next_hash;
        }
        // This iterator will stop at the LRUHandle whose next_hash is nullptr or its nexthash's
        // key and hash value is the target.
        return ptr;
    }

    void Resize() {
        uint32_t new_length = 4;// it originally is 4
        while (new_length < elems_) {
            new_length *= 2;
        }
        LRUHandle** new_list = new LRUHandle*[new_length];
        memset(new_list, 0, sizeof(new_list[0]) * new_length);
        uint32_t count = 0;
        //TOTHINK: will each element in list_ be supposed to have only one element?
        // Probably yes.
        for (uint32_t i = 0; i < length_; i++) {
            LRUHandle* h = list_[i];
            while (h != nullptr) {
                LRUHandle* next = h->next_hash;
                uint32_t hash = h->hash;
                LRUHandle** ptr = &new_list[hash & (new_length - 1)];
                h->next_hash = *ptr;
                *ptr = h;
                h = next;
                count++;
            }
        }
        assert(elems_ == count);
        delete[] list_;
        list_ = new_list;
        length_ = new_length;
    }
};
// A single shard of sharded table_cache.
class LRUCache {
public:
    LRUCache();
    ~LRUCache();

    // Separate from constructor so caller can easily make an array of LRUCache
    void SetCapacity(size_t capacity) { capacity_ = capacity; }

    // Like Cache methods, but with an extra "hash" parameter.
    Cache::Handle* Insert(const KeyType& key, uint32_t hash, void* value,
                          size_t charge,
                          void (*deleter)(Cache::Handle* handle));
    Cache::Handle* Lookup(const KeyType& key, uint32_t hash);
    Cache::Handle* LookupInsert(const KeyType& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(Cache::Handle* handle));
    Cache::Handle* LookupErase(const KeyType& key, uint32_t hash);
    //TODO: make the release not acquire the cache lock.
    void Release(Cache::Handle* handle);
    void Erase(const KeyType& key, uint32_t hash);
    void Prune();
    size_t TotalCharge() const {
//    MutexLock l(&mutex_);
//    ReadLock l(&mutex_);
        SpinLock l(&mutex_);
        return usage_;
    }

    mempool::FreeList* freelist_;
private:
    void LRU_Remove(LRUHandle* e);
    void LRU_Append(LRUHandle* list, LRUHandle* e);
    void Ref(LRUHandle* e);
//    void Ref_in_LookUp(LRUHandle* e);
    void Unref(LRUHandle *e, SpinLock *spin_l);
//    void Unref_WithoutLock(LRUHandle* e);
    bool FinishErase(LRUHandle *e, SpinLock *spin_l) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    // Initialized before use.
    size_t capacity_;

    // mutex_ protects the following state.
//  mutable port::RWMutex mutex_;
    mutable SpinMutex mutex_;
    size_t usage_ GUARDED_BY(mutex_);

    // Dummy head of LRU list.
    // lru.prev is newest entry, lru.next is oldest entry.
    // Entries have refs==1 and in_cache==true.
    LRUHandle lru_ GUARDED_BY(mutex_);

    // Dummy head of in-use list.
    // Entries are in use by clients, and have refs >= 2 and in_cache==true.
    LRUHandle in_use_ GUARDED_BY(mutex_);

    HandleTable table_ GUARDED_BY(mutex_);
//    static std::atomic<uint64_t> counter;
};

}  // namespace DSMEngine

#endif  // STORAGE_DSMEngine_INCLUDE_CACHE_H_
