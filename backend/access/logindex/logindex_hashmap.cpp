#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <iostream>
#include "access/logindex_hashmap.h"
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

// Update header's first element's lsn
// Also update header's replayedLsn
// This function only called by ReadBufferCommon, and before this function, header lock
// has already been acquired.
bool HashMapUpdateFirstEmptySlot(HashMap hashMap, KeyType key, uint64_t lsn) {
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

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

        return false;
    }


    pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO2
    printf("%s %d, release the bucketLock, bucketPos = %u, tid = %d\n", __func__ , __LINE__, bucketPos, gettid());
    fflush(stdout);
#endif

//    pthread_rwlock_wrlock(&iter->headLock);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    if (iter->lsnEntry[0].lsn == 0) {
        iter->lsnEntry[0].lsn = lsn;

        if(iter->replayedLsn < lsn) {
            iter->replayedLsn = lsn;
        }
//        pthread_rwlock_unlock(&iter->headLock);

        return true;
    } else {
//        pthread_rwlock_unlock(&iter->headLock);

        return false;
    }
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

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
            head->entryNum = 2;
        } else{ // this is for rel nblocks
//            printf("%s %d, insertLsn = %lu\n", __func__ , __LINE__, lsn);
//            fflush(stdout);

            head->lsnEntry[0].lsn = lsn;
            head->lsnEntry[0].pageNum = pageNum;
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

                printf("Error, %s can't find any match lsn\n", __func__ );
                return false;
            } else {
#ifdef ENABLE_DEBUG_INFO
                printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
                fflush(stdout);
#endif
                pthread_rwlock_unlock(&iter->headLock);

                *listLen = 0;
                *replayedLsn = iter->lsnEntry[resultIndex].lsn;
                return true;
            }

        }
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
        fflush(stdout);
#endif

        // Try to find it in following nodes
        // Initialize the currentLsn as the rear lsn of list
        uint64_t  currentLsn = iter->lsnEntry[iter->entryNum-1].lsn;

        HashNodeEle* eleIter = iter->nextEle;
        while(eleIter != NULL) {
            // fast skip
            if(eleIter->maxLsn < targetLsn) {
                currentLsn = eleIter->lsnEntry[eleIter->entryNum-1].lsn;

                eleIter = eleIter->nextEle;
                continue;
            }

            int resultIndex = LsnListFindLowerBound(targetLsn, eleIter->lsnEntry, eleIter->entryNum);

            if(resultIndex >= 0)
                currentLsn = eleIter->lsnEntry[resultIndex].lsn;

            // Found the desired lsn
            break;
        }
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
        fflush(stdout);
#endif
        *replayedLsn = currentLsn;
        *listLen = 0;
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
        fflush(stdout);
#endif
        pthread_rwlock_unlock(&iter->headLock);
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
                (*toReplayList)[toReplayCount] = iter->lsnEntry[i].lsn;
//                printf("%s %d, set %d slot as lsn = %lu\n", __func__ , __LINE__, toReplayCount, iter->lsnEntry[i].lsn);
//                fflush(stdout);
                toReplayCount++;
                if(toReplayCount == mallocSize) {
                    mallocSize *= 2;
                    uint64_t* newList = (uint64_t*) realloc(*toReplayList, sizeof(uint64_t)*mallocSize);
                    *toReplayList = newList;
                }
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
                (*toReplayList)[toReplayCount] = eleIter->lsnEntry[i].lsn;
                toReplayCount++;
                if(toReplayCount == mallocSize) {
                    mallocSize *= 2;
                    uint64_t* newList = (uint64_t*) realloc(*toReplayList, sizeof(uint64_t)*mallocSize);
                    *toReplayList = newList;
                }
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
        //TODO check here if ERROR occurred
        pthread_rwlock_unlock(&iter->headLock);
        printf("ERROR: %s toReplayCount = 0, spc = %lu, db = %lu, rel = %lu, fork = %u, blk = %ld, replayed lsn = %lu, targetLsn = %lu\n", __func__ ,
               iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum, iter->replayedLsn, targetLsn);
        fflush(stdout);
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d , pid = %d\n", __func__ , __LINE__, getpid());
    fflush(stdout);
#endif

//    pthread_rwlock_unlock(&iter->headLock);
    // don't release head's lock
    return true;
}

// [lsn(0), lsn(1), ... lsn(i), lsn(i+1), ..., lsn(n)]
// If lsn(i) <= targetLsn < lsn(i+1), return ( true, lsn(i) )
// If targetLsn >= lsn(n), return true, return ( true, lsn(n) )
// If lsn list is empty, return false
bool HashMapFindLowerBoundEntry(HashMap hashMap, KeyType key, uint64_t targetLsn, uint64_t* foundLsn, int* foundPageNum) {
#ifdef DEBUG_TIMING
    struct timeval start, end;
    START_TIMING(&start);
#endif

#ifdef ENABLE_DEBUG_INFO
    printf("%s start, hashMap Address = %p \n", __func__ , hashMap);
    fflush(stdout);
    printf("%s %d bucketNum = %d \n", __func__ , __LINE__, hashMap->bucketNum);
    fflush(stdout);
#endif
    uint32_t hashValue = HashKey(key);
    uint32_t bucketPos = hashValue % hashMap->bucketNum;

#ifdef ENABLE_DEBUG_INFO
    printf("%s start, targetLsn = %lu, hashValue = %u, bucketPos = %u \n", __func__ , targetLsn, hashValue, bucketPos);
    fflush(stdout);
#endif

    // Lock this slot
//    ReaderLock r_lock(hashMap->bucketList[bucketPos].bucketLock);
    pthread_rwlock_rdlock(&hashMap->bucketList[bucketPos].bucketLock);

#ifdef ENABLE_DEBUG_INFO2
    printf("%s %d, got the bucketLock, bucketPos = %u, tid = %d\n", __func__ , __LINE__, bucketPos, gettid());
    fflush(stdout);
#endif
#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(findLowerTime[0]), &(findLowerCount[0]))
#endif

#ifdef DEBUG_ITER_TIME
    int iter_header_time = 0;
    int iter_ele_time = 0;
#endif

    // Find the match head
    HashNodeHead* iter = hashMap->bucketList[bucketPos].nodeList;
    bool foundHead = false;
    while(iter != NULL) {

#ifdef DEBUG_ITER_TIME
        iter_header_time++;
#endif

#ifdef ENABLE_DEBUG_INFO
        printf("%s %d \n", __func__ , __LINE__);
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
#ifdef DEBUG_ITER_TIME
    printf("%s hashmap_iter_head = %d\n", __func__ , iter_header_time);
    fflush(stdout);
#endif

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d \n", __func__ , __LINE__);
    fflush(stdout);
#endif
    // If this relation doesn't exist in hash map, return false
    if(!foundHead) {
        pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO2
        printf("%s %d, release the bucketLock, bucketPos = %u, tid = %d\n", __func__ , __LINE__, bucketPos, gettid());
        fflush(stdout);
#endif
#ifdef DEBUG_TIMING
        RECORD_TIMING(&start, &end, &(findLowerTime[1]), &(findLowerCount[1]))
#endif
        return false;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d \n", __func__ , __LINE__);
    fflush(stdout);
#endif
    // Unlock bucketList, now it's useless
    pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO2
    printf("%s %d, release the bucketLock, bucketPos = %u, tid = %d\n", __func__ , __LINE__, bucketPos, gettid());
    fflush(stdout);
#endif
#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(findLowerTime[2]), &(findLowerCount[2]))
#endif
    // Lock this head
//    ReaderLock r_head_lock(iter->headLock);
#ifdef ENABLE_DEBUG_INFO
    printf("%s try to get header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
    fflush(stdout);
#endif
    pthread_rwlock_rdlock(&iter->headLock);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(findLowerTime[3]), &(findLowerCount[3]))
#endif
    uint64_t currentLsn = -1;
    int      currentPageNum;
    // If lsn is in this head
    // Iterate all the elements in the head
    if(iter->lsnEntry[iter->entryNum-1].lsn >= targetLsn) {
        int resultIndex = LsnListFindLowerBound(targetLsn, iter->lsnEntry, iter->entryNum);

#ifdef DEBUG_ITER_TIME
        printf("%s hashmap_iter_ele = %d\n", __func__ , iter->entryNum);
        fflush(stdout);
#endif

#ifdef DEBUG_TIMING
        RECORD_TIMING(&start, &end, &(findLowerTime[4]), &(findLowerCount[4]))
#endif
        // If all list elements are larger than targetLsn, return false
        if(resultIndex == -1) {
//            pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO
            printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
            fflush(stdout);
#endif
            pthread_rwlock_unlock(&iter->headLock);

#ifdef ENABLE_DEBUG_INFO
            printf("%s %d \n", __func__ , __LINE__);
            fflush(stdout);
#endif
            return false;
        } else {
//            pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO
            printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
            fflush(stdout);
#endif
            pthread_rwlock_unlock(&iter->headLock);

            *foundLsn = iter->lsnEntry[resultIndex].lsn;
            *foundPageNum = iter->lsnEntry[resultIndex].pageNum;

#ifdef ENABLE_DEBUG_INFO
            printf("%s %d \n", __func__ , __LINE__);
            fflush(stdout);
#endif
            return true;
        }
    }
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d \n", __func__ , __LINE__);
    fflush(stdout);
#endif

#ifdef DEBUG_ITER_TIME
    iter_ele_time += HASH_HEAD_NUM;
#endif
    // Iterate all following element nodes

    // Initialize the currentLsn as the rear lsn of list
    currentLsn = iter->lsnEntry[iter->entryNum-1].lsn;
    currentPageNum = iter->lsnEntry[iter->entryNum-1].pageNum;

//    HashNodeEle* eleIter = iter->nextEle;
    HashNodeEle* eleIter = iter->tailEle;
#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(findLowerTime[5]), &(findLowerCount[5]))
#endif

    while(eleIter != NULL) {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d \n", __func__ , __LINE__);
        fflush(stdout);
#endif
        // fast skip
        if(eleIter->lsnEntry[0].lsn > targetLsn) {
            eleIter = eleIter->prevEle;
            continue;
        }
//        if(eleIter->maxLsn < targetLsn) {
//            currentLsn = eleIter->lsnEntry[eleIter->entryNum-1].lsn;
//            currentPageNum = eleIter->lsnEntry[eleIter->entryNum-1].pageNum;
//
//            eleIter = eleIter->nextEle;
//#ifdef DEBUG_ITER_TIME
//            iter_ele_time += HASH_ELEM_NUM;
//#endif
//            continue;
//        }

#ifdef DEBUG_ITER_TIME
        iter_ele_time += eleIter->entryNum;
#endif
        // targetEntry should be found in the list
        int resultIndex = LsnListFindLowerBound(targetLsn, eleIter->lsnEntry, eleIter->entryNum);
        if(resultIndex != -1) {
            currentLsn = eleIter->lsnEntry[resultIndex].lsn;
            currentPageNum = eleIter->lsnEntry[resultIndex].pageNum;
        }

        // Found the desired lsn
        break;
    }
#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(findLowerTime[6]), &(findLowerCount[6]))
#endif

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d \n", __func__ , __LINE__);
    fflush(stdout);
#endif

#ifdef DEBUG_ITER_TIME
    printf("%s hashmap_iter_ele = %d\n", __func__ , iter_ele_time);
    fflush(stdout);
#endif

    *foundLsn = currentLsn;
    *foundPageNum = currentPageNum;

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d release header lock, %lu, %lu, %lu, fork = %u, blk = %lu\n", __func__, __LINE__, iter->key.SpcID, iter->key.DbID, iter->key.RelID, iter->key.ForkNum, iter->key.BlkNum );
    fflush(stdout);
#endif
    pthread_rwlock_unlock(&iter->headLock);
#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(findLowerTime[7]), &(findLowerCount[7]))
#endif
//    pthread_rwlock_unlock(&hashMap->bucketList[bucketPos].bucketLock);
#ifdef ENABLE_DEBUG_INFO
    printf("%s end\n", __func__ );
    fflush(stdout);
#endif
    return true;
}

//void HashMapUpdateKey(KeyType key, int value) {
//    uint32_t hashValue = HashKey(key);
//    uint32_t bucketPos = hashValue % hashMap->bucketNum;
//    HashBucket* targetBucket = &hashMap->bucketList[bucketPos];
//
//    WriterLock w_lock(targetBucket->bucketLock);
//
//    bool find = false;
//    HashNodeHead* node = targetBucket->nodeList;
//    while(node != NULL) {
//        if(KeyEqual(node->key, key)) {
//            find = true;
//            break;
//        }
//        node = node->next;
//    }
//
//    if(find) {
//        node->value = value;
//        return;
//    } else {
//        HashNodeHead* node = (HashNodeHead*) malloc(sizeof(HashNodeHead));
//        node->key = key;
//        node->bucket = &hashMap->bucketList[bucketPos];
//        node->value = value;
//        node->hashValue = hashValue;
//        node->next = node->bucket->nodeList;
//        node->bucket->nodeList = node;
//        return;
//    }
//}
//
//
//int HashMapFindKey(KeyType key) {
//    uint32_t hashValue = HashKey(key);
//    uint32_t bucketPos = hashValue % hashMap->bucketNum;
//    HashBucket* targetBucket = &hashMap->bucketList[bucketPos];
////    printf("hashValue = %u, Slot = %u\n", hashValue, bucketPos);
//
//    ReaderLock r_lock(targetBucket->bucketLock);
//
//    bool find = false;
//    HashNodeHead* node = targetBucket->nodeList;
//    while(node != NULL) {
//        if(KeyEqual(node->key, key)) {
//            find = true;
//            break;
//        }
//        node = node->next;
//    }
//
//    if(find)
//        return node->value;
//    else
//        return -1; // no find
//}

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

