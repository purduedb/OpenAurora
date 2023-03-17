//
// Created by rainman on 2023/3/8.
//

#ifndef DB2_PG_REL_CACHE_H
#define DB2_PG_REL_CACHE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#define NUM_REL_SIZE_LWLOCKS 128

struct RelKeyType {
    uint64_t SpcId;
    uint64_t DbId;
    uint64_t RelId;
    uint32_t forkNum;
};

typedef struct RelKeyType RelKey;

// Insert relKey to cache,
// If relKey doesn't exist in cache, create a new one
//      Else, update the existed value
extern void InsertRelSizeCache(RelKey relKey, uint32_t blockNum);

// Get relation size from cache
// If find this key in cache, return true and return blockNum via $result
// Otherwise, return false
extern bool GetRelSizeCache(RelKey relKey, uint32_t *result);

// Call this function at the beginning of Postmaster process
// 1. Allocate a shared memory and name it.
// 2. Construct a named hashmap inside the shared memory
extern void CreateRelSizeCache();

// Call this function at the beginning of Aux process
// 1. Find and attach the shared memory created by Postmaster
// 2. Find and attach the named hashmap object.
extern void LoadRelSizeCache();

// Calculate the shared memory size needed by Relation size hashmap LwLocks
// We only store LWLocks using PostgreSQL shared memory
// The real hashmap are currently implemented by boost interprocess library
extern Size RelSizeShmemSize();

extern void RelSizeShmemInit();

extern void RelSizeExclusiveLock(RelKey relKey);

extern void RelSizeSharedLock(RelKey relKey);

extern void RelSizeReleaseLock(RelKey relKey);

extern void RelSizePthreadLockInit();

extern void RelSizePthreadLocksDestroy();

extern void RelSizePthreadWriteLock(RelKey relKey);

extern void RelSizePthreadReadLock(RelKey relKey);

extern void RelSizePthreadUnlock(RelKey relKey);
#ifdef __cplusplus
}
#endif

#endif //DB2_PG_REL_CACHE_H
