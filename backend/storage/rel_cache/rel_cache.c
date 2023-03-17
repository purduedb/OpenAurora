#include "postgres.h"
#include "storage/rel_cache.h"
#include "storage/boost_shmht.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/builtin_shmht.h"

#include <pthread.h>

#define MAX_PATH_LEN (256)

LWLockPadded *relSizeLwLocks;

pthread_rwlock_t *relSizePthreadLocks;

// spc, db, rel, fork
#define REL_SIZE_CACHE_KEY  ("rel_cache_%lu_%lu_%lu_%u\0")

static void ParseRelKey2String(RelKey relKey, char* tempKey) {
    snprintf(tempKey, MAX_PATH_LEN, REL_SIZE_CACHE_KEY,
             relKey.SpcId, relKey.DbId, relKey.RelId, relKey.forkNum) ;
//    printf("relkey = %s\n", tempKey);
//    fflush(stdout);
}

static uint32_t HashRelKey(RelKey key) {
//    return tag_hash((void*) &key, sizeof(KeyType));
    uint32_t res = 0;
    res += (key.SpcId&0xFF) * 13 + 7;
    res += (key.DbId&0xFF) * 17 + 5;
    res += (key.RelId&0xFF) * 11 + 7;
    res += (key.forkNum&0xFF) * 3 + 29;

    return res;
}


void RelSizeExclusiveLock(RelKey relKey) {
    uint32_t bucket = HashRelKey(relKey) % NUM_REL_SIZE_LWLOCKS;
//    printf("%s %d, exclusive lock %u\n", __func__ , __LINE__, bucket);
//    fflush(stdout);

    LWLockAcquire(&relSizeLwLocks[bucket].lock, LW_EXCLUSIVE);
}


void RelSizeSharedLock(RelKey relKey) {
    uint32_t bucket = HashRelKey(relKey) % NUM_REL_SIZE_LWLOCKS;
//    printf("%s %d, shared lock %u\n", __func__ , __LINE__, bucket);
//    fflush(stdout);
//    printf("%s %d, shared lock 0's address is %p, pid = %d\n", __func__ , __LINE__, &relSizeLwLocks[0], getpid());
//    fflush(stdout);

    LWLockAcquire(&relSizeLwLocks[bucket].lock, LW_SHARED);
}

void RelSizeReleaseLock(RelKey relKey) {
    uint32_t bucket = HashRelKey(relKey) % NUM_REL_SIZE_LWLOCKS;

//    printf("%s %d, release lock %u\n", __func__ , __LINE__, bucket);
//    fflush(stdout);
    LWLockRelease(&relSizeLwLocks[bucket].lock);
}

void ParseRelKey2RelTag(RelKey relKey, RelTag *relTag) {
    relTag->reln.spcNode = relKey.SpcId;
    relTag->reln.dbNode = relKey.DbId;
    relTag->reln.relNode = relKey.RelId;
    relTag->forkNumber = relKey.forkNum;
}

void InsertRelSizeCache(RelKey relKey, uint32_t blockNum) {
//    char tempKey[MAX_PATH_LEN];
//    snprintf(tempKey, sizeof(tempKey), REL_SIZE_CACHE_KEY,
//             relKey.SpcId, relKey.DbId, relKey.RelId, relKey.forkNum) ;
//    ParseRelKey2String(relKey, tempKey);

    RelTag tag;
    ParseRelKey2RelTag(relKey, &tag);
    uint32 hashCode = RelSizeTableHashCode(&tag);

//    UnorderedHashMapInsert(tempKey, blockNum);
//    printf("%s %d, %lu_%lu_%lu_%d = %u\n", __func__ , __LINE__, tag.reln.spcNode,
//           tag.reln.dbNode, tag.reln.relNode, tag.forkNumber, blockNum);
//    fflush(stdout);
    RelSizeTableInsert(&tag, hashCode, (int)blockNum);
//    printf("%s %d\n", __func__ , __LINE__);
//    fflush(stdout);
    // If this value already exists, delete it and insert the latest value
//    if(result != -1) {
//        RelSizeTableDelete(&tag, hashCode);
//        RelSizeTableInsert(&tag, hashCode, (int)blockNum);
//    }
}

bool GetRelSizeCache(RelKey relKey, uint32_t *result) {
//    char tempKey[MAX_PATH_LEN];
//    snprintf(tempKey, sizeof(tempKey), REL_SIZE_CACHE_KEY,
//             relKey.SpcId, relKey.DbId, relKey.RelId, relKey.forkNum) ;
//    ParseRelKey2String(relKey, tempKey);

//    uint32_t bucketNum = HashRelKey(relKey) % NUM_REL_SIZE_LWLOCKS;
//
//    bool found = UnorderedHashMapGet(tempKey, result);

    RelTag tag;
    ParseRelKey2RelTag(relKey, &tag);
    uint32 hashCode = RelSizeTableHashCode(&tag);


    int re = RelSizeTableLookup(&tag, hashCode);
    if(re == -1)
        return false;

//    printf("%s %d,  %lu_%lu_%lu_%d, hashcode=%u, result = %d\n", __func__ , __LINE__, tag.reln.spcNode, tag.reln.dbNode,
//           tag.reln.relNode, tag.forkNumber, hashCode, re);
//    fflush(stdout);
    *result = re;
    return true;
}

void CreateRelSizeCache() {
//    UnorderedHashMapCreate();
//    printf("%s %d\n", __func__ , __LINE__);
//    fflush(stdout);
}

void LoadRelSizeCache() {
//    RelSizeShmemInit();

//    UnorderedHashMapAttach();
}

Size RelSizeShmemSize() {
    Size		size;
    size = 0;
    size = (NUM_REL_SIZE_LWLOCKS+1) * sizeof(LWLockPadded);

//    printf("%s %d, result = %lu\n", __func__ , __LINE__ , size);
//    fflush(stdout);
    return size;
}

void RelSizeShmemInit() {
//    printf("%s %d\n", __func__ , __LINE__);
//    fflush(stdout);

    bool foundRelSize;

    char *ptr = (char *)
            ShmemInitStruct("RelSizeCache", RelSizeShmemSize(), &foundRelSize);

    if(foundRelSize) {
//        printf("%s %d\n", __func__ , __LINE__);
//        fflush(stdout);
        return;
    }

//    printf("%s %d\n", __func__ , __LINE__);
//    fflush(stdout);
    // Initialize shared memory structs

    // Align LwLocks, pay attention to memory address add operation!
    ptr += sizeof(LWLockPadded) - ((uintptr_t) ptr) % sizeof(LWLockPadded);
    relSizeLwLocks = (LWLockPadded*) ptr;

    for (int i = 0; i < NUM_REL_SIZE_LWLOCKS; i++)
    {
        LWLockInitialize(&relSizeLwLocks[i].lock, LWTRANCHE_REL_SIZE_HASHMAP);
    }

    return;
}

void RelSizePthreadLockInit() {
    relSizePthreadLocks = (pthread_rwlock_t*) malloc(sizeof(pthread_rwlock_t) * NUM_REL_SIZE_LWLOCKS);
    for(int i = 0; i < NUM_REL_SIZE_LWLOCKS; i++) {
        pthread_rwlock_init(&(relSizePthreadLocks[i]), NULL);
    }
}

void RelSizePthreadLocksDestroy() {
    free(relSizePthreadLocks);
}

void RelSizePthreadWriteLock(RelKey relKey) {
    uint32_t bucket = HashRelKey(relKey) % NUM_REL_SIZE_LWLOCKS;
    pthread_rwlock_wrlock(&(relSizePthreadLocks[bucket]));
}

void RelSizePthreadReadLock(RelKey relKey) {
    uint32_t bucket = HashRelKey(relKey) % NUM_REL_SIZE_LWLOCKS;
    pthread_rwlock_rdlock(&(relSizePthreadLocks[bucket]));
}

void RelSizePthreadUnlock(RelKey relKey) {
    uint32_t bucket = HashRelKey(relKey) % NUM_REL_SIZE_LWLOCKS;
    pthread_rwlock_unlock(&(relSizePthreadLocks[bucket]));
}
