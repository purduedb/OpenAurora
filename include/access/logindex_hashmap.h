//
// Created by xi on 10/15/22.
//

#ifndef TEST_MIX_C_C___FUNC_H
#define TEST_MIX_C_C___FUNC_H


#ifdef __cplusplus
extern "C" {
#endif

#define HASH_HEAD_NUM (15)
#define HASH_ELEM_NUM (30)

struct HashBucket;
struct HashNodeHead;
struct HashNodeEle;

typedef struct HashBucket HashBucket;
typedef struct HashNodeHead HashNodeHead;
typedef struct HashNodeEle HashNodeEle;


struct KeyTypeStruct {
    uint64_t SpcID;
    uint64_t DbID;
    uint64_t RelID;
    uint32_t ForkNum;
    // If blkNum is -1, then this is a relation key
    // else, it's a block key
    int64_t BlkNum;
};

typedef struct KeyTypeStruct KeyType;

struct LsnEntry {
    uint64_t lsn;
    int pageNum;
};
typedef struct LsnEntry LsnEntry;

struct HashBucket {
//    Lock bucketLock;
    pthread_rwlock_t bucketLock;
    pthread_mutex_t replayLock;
    struct timeval lastReplayTime;
    HashNodeHead* nodeList;
};

struct HashNodeHead {
    KeyType key;
    uint32_t hashValue;

    HashBucket* bucket;
//    Lock headLock;
    pthread_rwlock_t headLock;

    LsnEntry lsnEntry[HASH_HEAD_NUM];
    uint64_t maxLsn; // Max lsn stored in this element node
    int entryNum; // How many values stored in the element node

    uint64_t replayedLsn; // The largest lsn need be replayed

    // Next head node linked by this headNode
    // Same hashSlot with different relation key
    HashNodeHead *nextHead;
    HashNodeHead *prevHead;

    HashNodeEle  *nextEle;
    HashNodeEle  *tailEle;

    // Finish Vacuum Time
    struct timeval finishVacuumTime;
};

struct HashNodeEle {
    uint64_t maxLsn; // Max lsn stored in this element node
    int entryNum; // How many values stored in the element node

    LsnEntry lsnEntry[HASH_ELEM_NUM];

    // When entryNum reaches HASH_ELEM_NUM, malloc a new
    // element node linked with this node
    HashNodeEle *nextEle;
    HashNodeEle *prevEle;
};

struct HashMapStruct {
    HashBucket* bucketList;
    int bucketNum;
} ;

typedef struct HashMapStruct* HashMap;



//extern int GetInteger(int i);
//extern void *ReadLock(void*);
//extern void *WriteLock(void*);

extern void HashMapInit(HashMap* hashMap, int bucketNum);
extern void HashMapDestroy(HashMap hashMap);
extern bool HashMapInsertKey(HashMap hashMap, KeyType key, uint64_t lsn, int pageNum, bool noEmptyFirstSlot);
extern bool HashMapFindLowerBoundEntry(HashMap hashMap, KeyType key, uint64_t targetLsn, uint64_t* foundLsn, int* foundPageNum);

extern bool HashMapGetBlockReplayList(HashMap hashMap, KeyType key, uint64_t targetLsn, uint64_t *replayedLsn, uint64_t **toReplayList, int *listLen);
extern bool HashMapUpdateFirstEmptySlot(HashMap hashMap, KeyType key, uint64_t lsn);
extern bool HashMapUpdateReplayedLsn(HashMap hashMap, KeyType key, uint64_t lsn, bool holdHeadLock);

#ifdef __cplusplus
}
#endif

#endif //TEST_MIX_C_C___FUNC_H
