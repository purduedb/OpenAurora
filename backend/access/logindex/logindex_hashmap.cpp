#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <iostream>
#include "c.h"
#include "access/logindex_hashmap.h"
#include "access/xlogdefs.h"
#include "storage/buf_internals.h"
#include "storage/kv_interface.h"
#include <atomic>

//#define DEBUG_TIMING
#ifdef DEBUG_TIMING

#include <sys/time.h>
#include <pthread.h>
#include <cstdlib>

int initialized4 = 0;
struct timeval output_timing4;

pthread_mutex_t timing_mutex4 = PTHREAD_MUTEX_INITIALIZER;
long findLowerTime[16];
long findLowerCount[16];

void PrintTimingResult4() {
    struct timeval now;

    if(!initialized4){
        gettimeofday(&output_timing4, NULL);
        initialized4 = 1;

        memset(findLowerTime, 0, 16*sizeof(findLowerTime[0]));
        memset(findLowerCount, 0, 16*sizeof(findLowerCount[0]));
    }


    gettimeofday(&now, NULL);

    if(now.tv_sec-output_timing4.tv_sec >= 5) {
        for(int i = 0 ; i < 9; i++) {
            if(findLowerCount[i] == 0)
                continue;
            printf("findLowerTime_%d = %ld\n",i,  findLowerTime[i]/findLowerCount[i]);
            printf("total_findLowerTime_%d = %ld, count = %ld\n",i,  findLowerTime[i], findLowerCount[i]);
            fflush(stdout);
        }

        output_timing4 = now;
    }
}

#define START_TIMING(start_p)  \
do {                         \
    gettimeofday(start_p, NULL); \
} while(0);

#define RECORD_TIMING(start_p, end_p, global_timing, global_count) \
do { \
    gettimeofday(end_p, NULL); \
    pthread_mutex_lock(&timing_mutex4); \
    (*global_timing) += ((*end_p.tv_sec*1000000+*end_p.tv_usec) - (*start_p.tv_sec*1000000+*start_p.tv_usec)); \
    (*global_count)++;                                                               \
    pthread_mutex_unlock(&timing_mutex4); \
    PrintTimingResult4(); \
    gettimeofday(start_p, NULL); \
} while (0);

#endif

//#define ENABLE_DEBUG_INFO3
//#define ENABLE_DEBUG_INFO2
//#define ENABLE_DEBUG_INFO

//typedef boost::shared_mutex Lock;
//typedef boost::unique_lock< Lock >  WriterLock;
//typedef boost::shared_lock< Lock >  ReaderLock;

//Lock myLock;

// The lsnList is sorted from small to large
int LsnListFindLowerBound(uint64_t targetLsn, LsnEntry* entryList, int listSize) {
    if(listSize == 0) {
        return -1;
    }

    int largeSide = listSize-1;
    int smallSide = 0;

    while(largeSide != smallSide) {
        int mid = (largeSide+smallSide+1) /2;
        if(targetLsn < entryList[mid].lsn) {
            largeSide = mid-1;
        } else {
            smallSide = mid;
        }

        if(entryList[mid].lsn == targetLsn) {
            largeSide = mid;
            smallSide = mid;
        }
    }

    if(entryList[largeSide].lsn > targetLsn) {
        return -1;
    }

    return largeSide;
}


HashMap pageVersionHashMap;

void HashMapInit(HashMap *hashMap_p, int bucketNum) {
    *hashMap_p = (HashMap) malloc(sizeof(struct HashMapStruct) );
#ifdef ENABLE_DEBUG_INFO
    printf("%s malloc %lu for HashMapStruct\n", __func__ , sizeof(struct HashMapStruct));
#endif
    (*hashMap_p)->bucketNum = bucketNum;
    (*hashMap_p)->bucketList = (HashBucket*) malloc(bucketNum * sizeof(HashBucket));

    for(int i = 0; i < bucketNum; i++) {
        (*hashMap_p)->bucketList[i].nodeList = NULL;
        pthread_rwlock_init(&(*hashMap_p)->bucketList[i].bucketLock, NULL);
        pthread_mutex_init(&(*hashMap_p)->bucketList[i].replayLock, NULL);
        (*hashMap_p)->bucketList[i].lastReplayTime.tv_sec = 0;
        (*hashMap_p)->bucketList[i].lastReplayTime.tv_usec = 0;
    }

    (*hashMap_p)->computeNodeNum = 0;
    (*hashMap_p)->computeNodeList = NULL;
    (*hashMap_p)->minComputeLsn = InvalidXLogRecPtr;
    pthread_rwlock_init(&(*hashMap_p)->computeNodeLock, NULL);
#ifdef ENABLE_DEBUG_INFO
    printf("Hashmap Address = %p\n", (*hashMap_p));
    printf("%s finished \n", __func__ );
    fflush(stdout);
#endif
}

uint32_t HashKey(KeyType key) {
//    return tag_hash((void*) &key, sizeof(KeyType));
    uint32_t res = 0;
    res += (key.SpcID&0xFF) * 13 + 7;
    res += (key.DbID&0xFF) * 17 + 5;
    res += (key.RelID&0xFF) * 11 + 7;
    res += (key.ForkNum&0xFF) * 3 + 29;

    if(key.BlkNum != -1) {
        res += (key.BlkNum&0xFF) * 7 + 11;
    }
//    res |= (key.SpcID&0xFF) * 13;
//    res <<= 7;
//    res |= (key.DbID&0xFF) * 17;
//    res <<= 5;
//    res |= (key.RelID&0xFF) * 11;
//    res <<= 7;
//    res |= (key.ForkNum&0xFF) * 3;
#ifdef ENABLE_DEBUG_INFO
    printf("%s=%u\n", __func__ , res);
#endif

    return res;
}

bool KeyMatch(KeyType key1, KeyType key2) {
    if(key1.SpcID == key2.SpcID
    && key1.DbID == key2.DbID
    && key1.RelID == key2.RelID
    && key1.ForkNum == key2.ForkNum
    && key1.BlkNum == key2.BlkNum)
        return true;
    else
        return false;
}

// It can be called by two different logics
// 1. Set the base page as the replayed page (lsn=1), and we should acquire holdHeadLock in this function
// 2. After GetReplayLsnList (it holds the header lock and doesn't release), and ApplyLsnList, we use this function
//      to update the final step (ReplayedLsn), and its parameter is holdHeadLock=True, we should only release lock.
bool HashMapUpdateReplayedLsn(HashMap hashMap, KeyType key, uint64_t lsn, bool holdHeadLock) {
    uint32_t hashValue = HashKey(key);
    uint32_t bucketPos = hashValue % hashMap->bucketNum;

#ifdef ENABLE_DEBUG_INFO
    printf("%s start, hashValue = %u, bucketPos = %u \n", __func__ , hashValue, bucketPos);
    fflush(stdout);
#endif
    pthread_rwlock_rdlock(&hashMap->bucketList[bucketPos].bucketLock);

#ifdef ENABLE_DEBUG_INFO2
    printf("%s %d, got the bucketLock, bucketPos = %u, tid = %d\n", __func__ , __LINE__, bucketPos, gettid());
    fflush(stdout);
#endif
    HashNodeHead* iter = hashMap->bucketList[bucketPos].nodeList;
    bool foundHead = false;
    while(iter != NULL) {
        // If found matched key, break the loop
        if(iter->hashValue == hashValue
           && KeyMatch(iter->key, key)) {
            foundHead = true;
            break;
        }

        iter = iter->nextHead;
    }

    if (!foundHead) {
        pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO2
        printf("%s %d, release the bucketLock, bucketPos = %u, tid = %d\n", __func__ , __LINE__, bucketPos, gettid());
        fflush(stdout);
#endif
        return false;
    }


    pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO2
    printf("%s %d, release the bucketLock, bucketPos = %u, tid = %d\n", __func__ , __LINE__, bucketPos, gettid());
    fflush(stdout);
#endif
    if(!holdHeadLock) {

#ifdef ENABLE_DEBUG_INFO
        printf("%s try to get header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
        fflush(stdout);
#endif
        pthread_rwlock_wrlock(&iter->headLock);
    }
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    if (iter->replayedLsn < lsn) {
        iter->replayedLsn = lsn;
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
        fflush(stdout);
#endif
        pthread_rwlock_unlock(&iter->headLock);
        return true;
    } else {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
        fflush(stdout);
#endif
        pthread_rwlock_unlock(&iter->headLock);
        return false;
    }
}

bool HashMapUpdateMaterializedStatus(HashMap hashMap, KeyType key, uint64_t lsn, bool holdHeadLock, bool status) {
    uint32_t hashValue = HashKey(key);
    uint32_t bucketPos = hashValue % hashMap->bucketNum;

    pthread_rwlock_rdlock(&hashMap->bucketList[bucketPos].bucketLock);

    HashNodeHead* iter = hashMap->bucketList[bucketPos].nodeList;
    bool foundHead = false;
    while(iter != NULL) {
        if(iter->hashValue == hashValue
           && KeyMatch(iter->key, key)) {
            foundHead = true;
            break;
        }
        iter = iter->nextHead;
    }

    if (!foundHead) {
        pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
        return false;
    }

    pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
    if(!holdHeadLock) {
        pthread_rwlock_wrlock(&iter->headLock);
    }
    for(int i = 0; i < iter->entryNum; i++){
        if(iter->lsnEntry[i].lsn == lsn){
            iter->lsnEntry[i].materialized = status;
            pthread_rwlock_unlock(&iter->headLock);
            return true;
        }
    }
    auto eleIter = iter->nextEle;
    while(eleIter != NULL){
        for(int i = 0; i < eleIter->entryNum; i++){
            if(eleIter->lsnEntry[i].lsn == lsn){
                eleIter->lsnEntry[i].materialized = status;
                pthread_rwlock_unlock(&iter->headLock);
                return true;
            }
        }
        eleIter = eleIter->nextEle;
    }
    pthread_rwlock_unlock(&iter->headLock);
    return false;
}

bool HashMapInsertKey(HashMap hashMap, KeyType key, uint64_t lsn, int pageNum, bool noEmptyFirstSlot) {
#ifdef ENABLE_DEBUG_INFO3
    printf("%s start, spc = %lu, db = %lu, rel = %lu, fork = %d, blk = %ld, lsn = %lu\n", __func__ ,
           key.SpcID, key.DbID, key.RelID, key.ForkNum, key.BlkNum, lsn);
    fflush(stdout);
#endif

    uint32_t hashValue = HashKey(key);
    uint32_t bucketPos = hashValue % hashMap->bucketNum;

#ifdef ENABLE_DEBUG_INFO
    printf("%s bucketPos = %u, hashValue = %u\n", __func__ , bucketPos, hashValue);
    fflush(stdout);
#endif
    // Lock this slot
    // Maybe we won't add a new head, and only need a ReadLock
    // TODO: use readLock first, if we need create head, writeLock it later
//    WriterLock w_lock(hashMap->bucketList[bucketPos].bucketLock);
    pthread_rwlock_wrlock(&hashMap->bucketList[bucketPos].bucketLock);

#ifdef ENABLE_DEBUG_INFO2
    printf("%s %d, got the bucketLock, bucketPos = %u, tid = %d\n", __func__ , __LINE__, bucketPos, gettid());
    fflush(stdout);
#endif

    HashNodeHead* iter = hashMap->bucketList[bucketPos].nodeList;
    bool foundHead = false;
    while(iter != NULL) {

#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif
        // If found matched key, break the loop
        if(iter->hashValue == hashValue
        && KeyMatch(iter->key, key)) {
            foundHead = true;
            break;
        }

        iter = iter->nextHead;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    // If no head matches in this slot, crate a new head node
    if(!foundHead) {
#ifdef ENABLE_DEBUG_INFO
        printf("Create head node\n");
#endif
        HashNodeHead* head = (HashNodeHead*) malloc(sizeof(HashNodeHead));

        head->key = key;
        head->hashValue = hashValue;
        head->bucket = &hashMap->bucketList[bucketPos];

        pthread_rwlock_init(&head->headLock, NULL);

        if(key.BlkNum != -1 && !noEmptyFirstSlot) { // this is for page version lsn list
//            printf("%s %d, insertLsn = %lu\n", __func__ , __LINE__, lsn);
//            fflush(stdout);

            head->lsnEntry[0].lsn = 0;
            head->lsnEntry[1].lsn = lsn;
            head->lsnEntry[0].materialized = false;
            head->lsnEntry[1].materialized = false;
            head->entryNum = 2;
        } else{ // this is for rel nblocks
//            printf("%s %d, insertLsn = %lu\n", __func__ , __LINE__, lsn);
//            fflush(stdout);

            head->lsnEntry[0].lsn = lsn;
            head->lsnEntry[0].pageNum = pageNum;
            head->lsnEntry[0].materialized = false;
            head->entryNum = 1;
        };
        head->maxLsn = lsn;
        // If toReplayLsn == 0, then no page was replayed
        head->replayedLsn = 0;

        head->prevHead = NULL;
        head->nextHead = NULL;
        head->nextEle = NULL;
        head->tailEle = NULL;
        head->finishVacuumTime.tv_sec = 0;
        head->finishVacuumTime.tv_usec = 0;

        // Add this new head to the first position of bucket list

        // if bucket nodelist is not null, then set its current first head's prev_head pointer to this new head node
        if(head->bucket->nodeList != NULL) {
            head->bucket->nodeList->prevHead = head;
        }
        head->nextHead = head->bucket->nodeList;
        head->bucket->nodeList = head;

        pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);

#ifdef ENABLE_DEBUG_INFO2
        printf("%s %d, release the bucketLock, bucketPos = %u, tid = %d\n", __func__ , __LINE__, bucketPos, gettid());
        fflush(stdout);
#endif
        return true;
    }


    // Move this recently inserted node to the first position in the bucket head list
    if(iter->prevHead != NULL) { // if this head node is not the first head element in the list
        iter->prevHead->nextHead = iter->nextHead;
        if(iter->nextHead != NULL) { // if this head node is not the rear node in the list
            iter->nextHead->prevHead = iter->prevHead;
        }
        iter->prevHead = NULL;
        iter->nextHead = iter->bucket->nodeList;
        iter->nextHead->prevHead = iter;
        iter->bucket->nodeList = iter;
    }

    pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO2
    printf("%s %d, release the bucketLock, bucketPos = %u, tid = %d\n", __func__ , __LINE__, bucketPos, gettid());
    fflush(stdout);
#endif
    // Now, we found the matched head node in this slot

    // First, lock this header
#ifdef ENABLE_DEBUG_INFO
    printf("%s try to get header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
    fflush(stdout);
#endif
//    WriterLock w_header_lock(iter->headLock);

    pthread_rwlock_rdlock(&iter->headLock);

#ifdef ENABLE_DEBUG_INFO
    printf("%s Get header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
    fflush(stdout);
#endif

    // If this lsn is smaller than the maximum lsn, do nothing
    if(iter->maxLsn > lsn) {
#ifdef ENABLE_DEBUG_INFO
        printf("%s try to insert failed, logindex_maxLsn = %lu, parameter lsn = %lu\n", __func__ , iter->maxLsn, lsn);
        printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
        fflush(stdout);
#endif

//        pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
        pthread_rwlock_unlock(&iter->headLock);
        return false;
    }

    // In the same transaction, RpcCreate, RpcExtend will update page number
    // If this happened, just update the page number in place
    if(iter->maxLsn == lsn) {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif
        // the maxLsn should always be the tail element in this head's list

        // Check whether it's in the head
        if(iter->entryNum <= HASH_HEAD_NUM && iter->nextEle == NULL) {
#ifdef ENABLE_DEBUG_INFO
            printf("%s %d, found in head entryPos = %d\n", __func__ , __LINE__, iter->entryNum-1);
            fflush(stdout);
#endif

            iter->lsnEntry[iter->entryNum-1].pageNum = pageNum;
        } else { // Check the tail node in this head list
#ifdef ENABLE_DEBUG_INFO
            printf("%s %d, found in node entryPos = %d\n", __func__ , __LINE__, iter->tailEle->entryNum-1);
            fflush(stdout);
#endif

            iter->tailEle->lsnEntry[ iter->tailEle->entryNum -1 ].pageNum = pageNum;
        }

//        pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
        fflush(stdout);
#endif
        pthread_rwlock_unlock(&iter->headLock);
        return true;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    // If head still has available space, add it to head
    if(iter->entryNum < HASH_HEAD_NUM) {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif
        iter->lsnEntry[iter->entryNum].pageNum = pageNum;
        iter->lsnEntry[iter->entryNum].lsn = lsn;
        iter->lsnEntry[iter->entryNum].materialized = false;
        iter->entryNum++;

        iter->maxLsn = lsn;

//        pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
        fflush(stdout);
#endif
        pthread_rwlock_unlock(&iter->headLock);
        return true;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    // If the tail node has available space, add this entry to tail node
    if(iter->tailEle
        && iter->tailEle->entryNum < HASH_ELEM_NUM) {

#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
//        printf("add into tail node\n");
#endif
        HashNodeEle *nodeEle = iter->tailEle;
        nodeEle->lsnEntry[nodeEle->entryNum].pageNum = pageNum;
        nodeEle->lsnEntry[nodeEle->entryNum].lsn = lsn;
        nodeEle->lsnEntry[nodeEle->entryNum].materialized = false;

        nodeEle->entryNum++;

        // update this node's maxLsn and header's lsn
        nodeEle->maxLsn = lsn;
        iter->maxLsn = lsn;

//        pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
        fflush(stdout);
#endif
        pthread_rwlock_unlock(&iter->headLock);
        return true;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    // If the tail node doesn't have enough space or there is no element node
    // Add a new element node to the head list tail.

//    printf("Create tail node\n");
    HashNodeEle* eleNode = (HashNodeEle*) malloc(sizeof(HashNodeEle));
    eleNode->maxLsn = lsn;
    eleNode->lsnEntry[0].pageNum = pageNum;
    eleNode->lsnEntry[0].lsn = lsn;
    eleNode->lsnEntry[0].materialized = false;
    eleNode->entryNum = 1;
    eleNode->nextEle = NULL;
    eleNode->prevEle = NULL;

    iter->maxLsn = lsn;

    // If header has one or more element nodes
    if(iter->tailEle != NULL) {
        eleNode->prevEle = iter->tailEle;
        iter->tailEle->nextEle = eleNode;
        iter->tailEle = eleNode;
    } else { // If no element node linked by this header
        iter->nextEle = eleNode;
        iter->tailEle = eleNode;
    }

//    pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
    fflush(stdout);
#endif
    pthread_rwlock_unlock(&iter->headLock);
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    return true;
}

#define AddToToReplayList(lsn) do{ \
    (*toReplayList)[toReplayCount] = (lsn); \
    toReplayCount++; \
    if(toReplayCount == mallocSize) { \
        mallocSize *= 2; \
        uint64_t* newList = (uint64_t*) realloc(*toReplayList, sizeof(uint64_t)*mallocSize); \
        *toReplayList = newList; \
    } \
}while(0)

// This function will return lsn list that need be replayed
// Parameters:
//      $targetLsn is caller's request lsn, we need to replay until current lsn >= targetLsn
//      $replayedLsn is in this block lsn list, which largest lsn has been replayed, if no page is replayed, set it as 0
//      $toReplayList and $listLen is caller's value, toReplayList should start from first lsn need replay
//      return value: if list not exist, return false. if list exists and no version need to be replayed, return true and an empty list (listLen = 0),
//                      in this case, the replayedLsn is the version that largest lsn that <= targetLsn
// If *listLen > 0, caller should release head's lock and free() *toReplayList
bool HashMapGetBlockReplayList(HashMap hashMap, KeyType key, uint64_t targetLsn, uint64_t *replayedLsn, uint64_t **toReplayList, int *listLen) {
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d 2\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    uint32_t hashValue = HashKey(key);
    uint32_t bucketPos = hashValue % hashMap->bucketNum;

#ifdef ENABLE_DEBUG_INFO
    printf("%s start, hashValue = %u, bucketPos = %u, pid = %d \n", __func__ , hashValue, bucketPos, getpid());
    fflush(stdout);
#endif
    // Lock this slot
    //    ReaderLock r_lock(hashMap->bucketList[bucketPos].bucketLock);
    pthread_rwlock_wrlock(&hashMap->bucketList[bucketPos].bucketLock);

#ifdef ENABLE_DEBUG_INFO2
    printf("%s %d, got the bucketLock, bucketPos = %u, tid = %d\n", __func__ , __LINE__, bucketPos, gettid());
    fflush(stdout);
#endif

    // Find the match head
    HashNodeHead* iter = hashMap->bucketList[bucketPos].nodeList;
    bool foundHead = false;
    while(iter != NULL) {

#ifdef ENABLE_DEBUG_INFO
        printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
        fflush(stdout);
#endif
        // If found matched key, break the loop
        if(iter->hashValue == hashValue
           && KeyMatch(iter->key, key)) {
            foundHead = true;
            break;
        }

        iter = iter->nextHead;
    }

    // TODO, Now we can release bucketList lock
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
    fflush(stdout);
#endif

    // If this relation doesn't exist in hash map, return false
    if(!foundHead) {
        pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO2
        printf("%s %d, release the bucketLock, bucketPos = %u, tid = %d\n", __func__ , __LINE__, bucketPos, gettid());
        fflush(stdout);
#endif
        return false;
    }

    // Move this recently inserted node to the first position in the bucket head list
    if(iter->prevHead != NULL) { // if this head node is not the first head element in the list
        iter->prevHead->nextHead = iter->nextHead;
        if(iter->nextHead != NULL) { // if this head node is not the rear node in the list
            iter->nextHead->prevHead = iter->prevHead;
        }
        iter->prevHead = NULL;
        iter->nextHead = iter->bucket->nodeList;
        iter->nextHead->prevHead = iter;
        iter->bucket->nodeList = iter;
    }
    pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO2
    printf("%s %d, release the bucketLock, bucketPos = %u, tid = %d\n", __func__ , __LINE__, bucketPos, gettid());
        fflush(stdout);
#endif


#ifdef ENABLE_DEBUG_INFO
    printf("%s try to get header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
    fflush(stdout);
#endif
    // Unlock it until caller func has finished replaying task
    pthread_rwlock_wrlock(&iter->headLock);
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
    fflush(stdout);
#endif

    // Check whether we need redo or not
    if (iter->replayedLsn == targetLsn) {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
        fflush(stdout);
#endif
        *listLen = 0;
        *replayedLsn = iter->replayedLsn;
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
        fflush(stdout);
#endif
        pthread_rwlock_unlock(&iter->headLock);
        return true;
    }

    if (iter->replayedLsn > targetLsn) { // no need replay
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
        fflush(stdout);
#endif
        // Now, in head, find the largest lsn that <= targetLsn
        if(iter->lsnEntry[iter->entryNum-1].lsn >= targetLsn) { // Should find in head
            int resultIndex = LsnListFindLowerBound(targetLsn, iter->lsnEntry, iter->entryNum);

            // If all list elements are larger than targetLsn, return false
            // This case should not happen
            if(resultIndex == -1) {
#ifdef ENABLE_DEBUG_INFO
                printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
                fflush(stdout);
#endif
                pthread_rwlock_unlock(&iter->headLock);
                return false;
            } else {
#ifdef ENABLE_DEBUG_INFO
                printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
                fflush(stdout);
#endif

                if(iter->lsnEntry[resultIndex].materialized){
                    *listLen = 0;
                    *replayedLsn = iter->lsnEntry[resultIndex].lsn;
                    pthread_rwlock_unlock(&iter->headLock);
                }
                else{
                    int mallocSize = 16;
                    int toReplayCount = 0;
                    *toReplayList = (uint64_t*) malloc(sizeof(uint64_t)* mallocSize);
                    for(int i = resultIndex; i >= 0; i--)
                        if(iter->lsnEntry[i].materialized){
                            *replayedLsn = iter->lsnEntry[i].lsn;
                            break;
                        }
                        else
                            AddToToReplayList(iter->lsnEntry[i].lsn);
                    for(int i = 0, j = toReplayCount - 1; i < j; i++, j--){
                        uint64_t tmp = (*toReplayList)[i];
                        (*toReplayList)[i] = (*toReplayList)[j];
                        (*toReplayList)[j] = tmp;
                    }
                    *listLen = toReplayCount;
                    iter->lsnEntry[resultIndex].materialized = true; // todo (te): Here we assume the caller will always materialize this version, but it needs to be changed.
                }
                return true;
            }

        }
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
        fflush(stdout);
#endif

        // Try to find it in following nodes
        // Initialize the currentLsn as the rear lsn of list
        int resultIndex = iter->entryNum - 1;
        LsnEntry* currentEntry = &(iter->lsnEntry[iter->entryNum-1]);

        HashNodeEle* eleIter = iter->nextEle;
        while(eleIter != NULL) {
            // fast skip
            if(eleIter->maxLsn < targetLsn) {
                currentEntry = &(eleIter->lsnEntry[eleIter->entryNum-1]);

                eleIter = eleIter->nextEle;
                continue;
            }

            resultIndex = LsnListFindLowerBound(targetLsn, eleIter->lsnEntry, eleIter->entryNum);

            if(resultIndex >= 0)
                currentEntry = &(eleIter->lsnEntry[resultIndex]);

            // Found the desired lsn
            break;
        }
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
        fflush(stdout);
#endif
        if(currentEntry->materialized){
            *listLen = 0;
            *replayedLsn = currentEntry->lsn;
            pthread_rwlock_unlock(&iter->headLock);
        }
        else{
            int mallocSize = 16;
            int toReplayCount = 0;
            *toReplayList = (uint64_t*) malloc(sizeof(uint64_t)* mallocSize);

            bool foundReplayedEntry = false;
            for(auto eIter = eleIter; eIter != NULL; eIter = eIter->prevEle, resultIndex = eIter != NULL ? eIter->entryNum - 1 : iter->entryNum - 1)
                for(int i = resultIndex; i >= 0; i--)
                    if(eIter->lsnEntry[i].materialized){
                        foundReplayedEntry = true;
                        *replayedLsn = eIter->lsnEntry[i].lsn;
                        break;
                    }
                    else
                        AddToToReplayList(eIter->lsnEntry[i].lsn);
            if(!foundReplayedEntry){
                for(int i = resultIndex; i >= 0; i--)
                    if(iter->lsnEntry[i].materialized){
                        foundReplayedEntry = true;
                        *replayedLsn = iter->lsnEntry[i].lsn;
                        break;
                    }
                    else
                        AddToToReplayList(iter->lsnEntry[i].lsn);
            }
            for(int i = 0, j = toReplayCount - 1; i < j; i++, j--){
                uint64_t tmp = (*toReplayList)[i];
                (*toReplayList)[i] = (*toReplayList)[j];
                (*toReplayList)[j] = tmp;
            }
            *listLen = toReplayCount;
            currentEntry->materialized = true; // todo (te): Here we assume the caller will always materialize this version, but it needs to be changed.
        }
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
        fflush(stdout);
#endif
        return true;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
    fflush(stdout);
#endif
    // If we have replayed all the page version in this list, and the targetLsn is larger than iter->maxLsn
    if(iter->replayedLsn == iter->maxLsn) {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
        fflush(stdout);
#endif
        *replayedLsn = iter->maxLsn;
        *listLen = 0;
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
        fflush(stdout);
#endif
        pthread_rwlock_unlock(&iter->headLock);
        return true;
    }

    // If this list was created by StartupProcess and hasn't ever been replayed
    // And the first element in the list is larger than targetLSN.
    if(iter->replayedLsn == 0 && iter->entryNum > 1 && iter->lsnEntry[0].lsn == 0 && iter->lsnEntry[1].lsn > targetLsn) {
        *listLen = 0;
        *replayedLsn = 0;
        pthread_rwlock_unlock(&iter->headLock);
        return true;
    }

    // Now, we should have some versions to be replayed
    *replayedLsn = iter->replayedLsn;
    // Find the where is the last replayed element

    int mallocSize = 16;
    int toReplayCount = 0;
    *toReplayList = (uint64_t*) malloc(sizeof(uint64_t)* mallocSize);
    LsnEntry* toReplayedLsnEntry;

    // Circumstance: ... $replayedLsn ..(what we need).. $targetLsn
    int foundReplayLsnPosition = 0;
    // last replayed lsn in head
    if(iter->replayedLsn <= iter->lsnEntry[iter->entryNum-1].lsn) {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
        fflush(stdout);
#endif
        foundReplayLsnPosition = 1;
        //Firstly, find the replayedLsn position, this is our start
        int replayedLsnIndex = LsnListFindLowerBound(iter->replayedLsn, iter->lsnEntry, iter->entryNum);
//        printf("%s %d, get replayedLsnIndex = %d\n", __func__ , __LINE__, replayedLsnIndex);
//        fflush(stdout);

        for(int i = replayedLsnIndex+1; i < iter->entryNum; i++) {
#ifdef ENABLE_DEBUG_INFO3
            printf("%s %d, iter->lsn = %lu, targetLsn = %lu\n", __func__ , __LINE__, iter->lsnEntry[i].lsn, targetLsn);
            fflush(stdout);
#endif
            if(iter->lsnEntry[i].lsn <= targetLsn) {
                AddToToReplayList(iter->lsnEntry[i].lsn);
                toReplayedLsnEntry = &(iter->lsnEntry[i]);
            } else { // found all toReplay list
                break;
            }
        }
    }
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
    fflush(stdout);
#endif

    HashNodeEle* eleIter = iter->nextEle;
    while(eleIter != NULL) {
        if(eleIter->lsnEntry[0].lsn > targetLsn) {
            break;
        }

        // fast skip
        if(eleIter->maxLsn < iter->replayedLsn) {
            eleIter = eleIter->nextEle;
            continue;
        }

        int startIndex = 0;
        if(!foundReplayLsnPosition) {
            startIndex = LsnListFindLowerBound(iter->replayedLsn, eleIter->lsnEntry, eleIter->entryNum);
            startIndex +=1;
            foundReplayLsnPosition = 1;
        }


        // targetEntry should be found in the list
        for(int i = startIndex; i < eleIter->entryNum; i++) {
            if(eleIter->lsnEntry[i].lsn <= targetLsn) {
                AddToToReplayList(eleIter->lsnEntry[i].lsn);
                toReplayedLsnEntry = &(eleIter->lsnEntry[i]);
            } else {  // found all toReplay list
                break;
            }
        }

        eleIter = eleIter->nextEle;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
    fflush(stdout);
#endif
    *listLen = toReplayCount;
    if(toReplayCount == 0) {
        pthread_rwlock_unlock(&iter->headLock);
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d , pid = %d, toReplayCount = 0, spc = %lu, db = %lu, rel = %lu, fork = %u, blk = %ld, replayed lsn = %lu, targetLsn = %lu\n", __func__ , __LINE__, getpid(),
               iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum, iter->replayedLsn, targetLsn);
        fflush(stdout);
#endif
        free(*toReplayList);
    }
    else{
        toReplayedLsnEntry->materialized = true; // todo (te): Here we assume the caller will always materialize this version, but it needs to be changed.
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
    fflush(stdout);
#endif

//    pthread_rwlock_unlock(&iter->headLock);
    // don't release head's lock
    return true;
}

bool HashMapGarbageCollectKey(HashMap hashMap, KeyType key){
    uint32_t hashValue = HashKey(key);
    uint32_t bucketPos = hashValue % hashMap->bucketNum;

    pthread_rwlock_rdlock(&hashMap->bucketList[bucketPos].bucketLock);

    HashNodeHead* iter = hashMap->bucketList[bucketPos].nodeList;
    bool foundHead = false;
    while(iter != NULL) {
        if(iter->hashValue == hashValue
           && KeyMatch(iter->key, key)) {
            foundHead = true;
            break;
        }
        iter = iter->nextHead;
    }

    if (!foundHead) {
        pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
        return false;
    }
    pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
    pthread_rwlock_wrlock(&iter->headLock);
    HashMapGarbageCollectNode(hashMap, iter);
    pthread_rwlock_unlock(&iter->headLock);
    return true;
}

// Delete corresponding RocksDb pages and hashNodeEle
// Should remember ele->prev/next in advance, ele will be erased in this func
void VacuumHashNode(HashNodeHead* head, HashNodeEle* ele, BufferTag bufferTag) {
//    for(int i = 0; i < ele->entryNum; i++) {
//        DeletePageFromRocksdb(bufferTag, ele->lsnEntry[i].lsn);
//    }
    if(ele == head->nextEle) { // if it is the first node
        head->nextEle = ele->nextEle;
        if(ele->nextEle != NULL) {
            ele->nextEle->prevEle = NULL;
        }
    } else {
        ele->prevEle->nextEle = ele->nextEle;
        if(ele->nextEle != NULL) {
            ele->nextEle->prevEle = ele->prevEle;
        }
    }
    free(ele);
}

void HashMapGarbageCollectNode(HashMap hashMap, HashNodeHead *head){
    auto minComputeLsn = HashMapGetMinComputeLsn(hashMap);
    if(minComputeLsn == InvalidXLogRecPtr || head->replayedLsn < minComputeLsn)
        minComputeLsn = head->replayedLsn;
    if(minComputeLsn <= 0ull)
        return;

    uint64_t toKeepLsn = InvalidXLogRecPtr;
    BufferTag bufferTag;
    RelFileNode rnode;

    rnode.spcNode = head->key.SpcID;
    rnode.dbNode = head->key.DbID;
    rnode.relNode = head->key.RelID;

    INIT_BUFFERTAG(bufferTag, rnode, (ForkNumber)head->key.ForkNum, head->key.BlkNum);

    for(int i = 0; i < head->entryNum; i++)
        if(head->lsnEntry[i].lsn <= minComputeLsn){
            if(head->lsnEntry[i].materialized)
                toKeepLsn = head->lsnEntry[i].lsn;
        }
        else
            break;
    HashNodeEle *ele = head->nextEle;
    while(ele != NULL) {
        bool to_break = false;
        for(int i = 0; i < ele->entryNum; i++)
            if(ele->lsnEntry[i].lsn <= minComputeLsn){
                if(ele->lsnEntry[i].materialized)
                    toKeepLsn = ele->lsnEntry[i].lsn;
            }
            else{
                to_break = true;
                break;
            }
        if(to_break)
            break;
        ele = ele->nextEle;
    }

    for(int i = 0; i < head->entryNum; i++)
        if(head->lsnEntry[i].lsn < toKeepLsn){
            if(head->lsnEntry[i].materialized){
                DeletePageFromRocksdb(bufferTag, head->lsnEntry[i].lsn);
                head->lsnEntry[i].materialized = false;
            }
        }
        else
            break;
    ele = head->nextEle;
    while(ele != NULL) {
        bool to_break = false;
        for(int i = 0; i < ele->entryNum; i++)
            if(ele->lsnEntry[i].lsn < toKeepLsn){
                if(ele->lsnEntry[i].materialized){
                    DeletePageFromRocksdb(bufferTag, ele->lsnEntry[i].lsn);
                    ele->lsnEntry[i].materialized = false;
                }
            }
            else{
                to_break = true;
                break;
            }
        if(to_break)
            break;
        auto nextEle = ele->nextEle;
        VacuumHashNode(head, ele, bufferTag);
        ele = nextEle;
    }
}

void HashMapDestroy(HashMap hashMap){
    for(int i = 0; i < hashMap->bucketNum; i++) {
        while (hashMap->bucketList[i].nodeList) {
            HashNodeHead* head = hashMap->bucketList[i].nodeList;

            while(head->nextEle) {
                HashNodeEle* elemNode = head->nextEle;
                head->nextEle = head->nextEle->nextEle;
                free(elemNode);
            }

            hashMap->bucketList[i].nodeList = head->nextHead;
            free(head);
        }
    }
    free(hashMap->bucketList);
    if(hashMap->computeNodeList != NULL)
        free(hashMap->computeNodeList);
    free(hashMap);
}

//void *ReadLock(void*)
//{
//    sleep(1);
//    ReaderLock r_lock(myLock);
//    printf("read_start\n");
//    //Do reader stuff
//    sleep(3);
//    printf("read_end\n");
//    return NULL;
//}
//
//void *WriteLock(void*)
//{
//    WriterLock w_lock(myLock);
//    printf("write_start\n");
//    sleep(5);
//    printf("write_end\n");
//    return NULL;
//}
//
//int GetInteger(int i) {
//    std::cout << "i2nput is " << i << std::endl;
//    return i+1;
//}

int32_t HashMapRegisterSecondaryNode(HashMap hashMap, bool primary, uint64_t lsn){
    pthread_rwlock_wrlock(&hashMap->computeNodeLock);
    hashMap->computeNodeNum++;
    hashMap->computeNodeList = (ComputeNodeInfo*) realloc(hashMap->computeNodeList, sizeof(ComputeNodeInfo) * hashMap->computeNodeNum);
    uint32_t id = hashMap->computeNodeNum <= 1 ? 0 : hashMap->computeNodeList[hashMap->computeNodeNum - 2].id;
    hashMap->computeNodeList[hashMap->computeNodeNum - 1].id = id;
    hashMap->computeNodeList[hashMap->computeNodeNum - 1].primary = primary;
    hashMap->computeNodeList[hashMap->computeNodeNum - 1].active = true;
    hashMap->computeNodeList[hashMap->computeNodeNum - 1].lsn = lsn;
    gettimeofday(&hashMap->computeNodeList[hashMap->computeNodeNum - 1].activeTime, NULL);
    if(hashMap->minComputeLsn == InvalidXLogRecPtr || lsn < hashMap->minComputeLsn)
        hashMap->minComputeLsn = lsn;
    pthread_rwlock_unlock(&hashMap->computeNodeLock);
    return id;
}

void HashMapSecondaryNodeUpdatesLsn(HashMap hashMap, int32_t node_id, int64_t lsn){
    pthread_rwlock_wrlock(&hashMap->computeNodeLock);
    hashMap->minComputeLsn = InvalidXLogRecPtr;
    for(int i = 0; i < hashMap->computeNodeNum; i++){
        if(hashMap->computeNodeList[i].id == node_id){
            hashMap->computeNodeList[i].lsn = lsn;
            gettimeofday(&hashMap->computeNodeList[i].activeTime, NULL);
        }
        if(hashMap->computeNodeList[i].active && (hashMap->minComputeLsn == InvalidXLogRecPtr || hashMap->computeNodeList[i].lsn < hashMap->minComputeLsn))
            hashMap->minComputeLsn = hashMap->computeNodeList[i].lsn;
    }
    pthread_rwlock_unlock(&hashMap->computeNodeLock);
}

uint64_t HashMapGetMinComputeLsn(HashMap hashMap){
    pthread_rwlock_rdlock(&hashMap->computeNodeLock);
    uint64_t ret = hashMap->minComputeLsn;
    pthread_rwlock_unlock(&hashMap->computeNodeLock);
    return ret;
}

void HashMapClearInactiveComputeNode(HashMap hashMap){
    pthread_rwlock_wrlock(&hashMap->computeNodeLock);
    if(hashMap->computeNodeNum <= 0){
        pthread_rwlock_unlock(&hashMap->computeNodeLock);
        return;
    }

    int inactive_cnt = 0;
    timeval now;
    gettimeofday(&now, NULL);
    unsigned long now_usec = (now.tv_sec * 1000000ul) + now.tv_usec;

    hashMap->minComputeLsn = InvalidXLogRecPtr;
    for(int i = 0; i < hashMap->computeNodeNum; i++){
        unsigned long active_usec = (hashMap->computeNodeList[i].activeTime.tv_sec * 1000000ul) + hashMap->computeNodeList[i].activeTime.tv_usec;
        if(now_usec - active_usec > HashMapComputeNodeInactiveTimeout_us){
            inactive_cnt++;
            continue;
        }
        if(hashMap->minComputeLsn == InvalidXLogRecPtr || hashMap->computeNodeList[i].lsn < hashMap->minComputeLsn)
            hashMap->minComputeLsn = hashMap->computeNodeList[i].lsn;
        if(inactive_cnt > 0)
            memcpy(&hashMap->computeNodeList[i - inactive_cnt], &hashMap->computeNodeList[i], sizeof(ComputeNodeInfo));
    }
    hashMap->computeNodeNum -= inactive_cnt;
    if(hashMap->computeNodeNum <= 0){
        free(hashMap->computeNodeList);
        hashMap->computeNodeList = NULL;
    }
    else
        hashMap->computeNodeList = (ComputeNodeInfo*) realloc(hashMap->computeNodeList, hashMap->computeNodeNum * sizeof(ComputeNodeInfo));
    pthread_rwlock_unlock(&hashMap->computeNodeLock);
}
