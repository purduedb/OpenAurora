#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <iostream>
#include "access/logindex_hashmap.h"
#include <atomic>

#define HASH_HEAD_NUM (15)
#define HASH_ELEM_NUM (30)

//typedef boost::shared_mutex Lock;
//typedef boost::unique_lock< Lock >  WriterLock;
//typedef boost::shared_lock< Lock >  ReaderLock;

//Lock myLock;

struct HashBucket;
struct HashNodeHead;
struct HashNodeEle;
HashBucket* HashMap;

struct LsnEntry {
    uint64_t lsn;
    int pageNum;
};

struct HashBucket {
//    Lock bucketLock;
    pthread_rwlock_t bucketLock;
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

    // Next head node linked by this headNode
    // Same hashSlot with different relation key
    HashNodeHead *nextHead;
    HashNodeEle  *nextEle;
    HashNodeEle  *tailEle;
};

struct HashNodeEle {
    uint64_t maxLsn; // Max lsn stored in this element node
    int entryNum; // How many values stored in the element node

    LsnEntry lsnEntry[HASH_ELEM_NUM];

    // When entryNum reaches HASH_ELEM_NUM, malloc a new
    // element node linked with this node
    HashNodeEle *nextEle;
};

struct HashMap {
    HashBucket* bucketList;
    int bucketNum;
} hashMap;

void HashMapInit(int bucketNum) {
    hashMap.bucketNum = bucketNum;
    hashMap.bucketList = (HashBucket*) malloc(bucketNum * sizeof(HashBucket));

    for(int i = 0; i < bucketNum; i++) {
        hashMap.bucketList[i].nodeList = NULL;
        pthread_rwlock_init(&hashMap.bucketList[i].bucketLock, NULL);
    }
}

uint32_t HashKey(struct KeyType key) {
//    return tag_hash((void*) &key, sizeof(KeyType));
    uint32_t res = 0;
    res |= key.SpcID&0xFF;
    res <<= 8;
    res |= key.DbID&0xFF;
    res <<= 8;
    res |= key.RelID&0xFF;
    res <<= 8;
    res |= key.ForkNum&0xFF;
    return res;
}

bool KeyMatch(struct KeyType key1, struct KeyType key2) {
    if(key1.SpcID == key2.SpcID
    && key1.DbID == key2.DbID
    && key1.RelID == key2.RelID
    && key1.ForkNum == key2.ForkNum)
        return true;
    else
        return false;
}

bool HashMapInsertKey(KeyType key, uint64_t lsn, int pageNum) {
    uint32_t hashValue = HashKey(key);
    uint32_t bucketPos = hashValue % hashMap.bucketNum;

    printf("%s bucketPos = %u, hashValue = %u\n", __func__ , bucketPos, hashValue);
    // Lock this slot
    // Maybe we won't add a new head, and only need a ReadLock
//    WriterLock w_lock(hashMap.bucketList[bucketPos].bucketLock);
    pthread_rwlock_wrlock(&hashMap.bucketList[bucketPos].bucketLock);

    printf("%s get bucket lock\n", __func__ );

    HashNodeHead* iter = hashMap.bucketList[bucketPos].nodeList;
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

    // If no head matches in this slot, crate a new head node
    if(!foundHead) {
        printf("Create head node\n");
        HashNodeHead* head = (HashNodeHead*) malloc(sizeof(HashNodeHead));

        head->key = key;
        head->hashValue = hashValue;
        head->bucket = &hashMap.bucketList[bucketPos];

        pthread_rwlock_init(&head->headLock, NULL);

        head->lsnEntry[0].lsn = lsn;
        head->lsnEntry[0].pageNum = pageNum;
        head->entryNum = 1;
        head->maxLsn = lsn;

        head->nextHead = NULL;
        head->nextEle = NULL;
        head->tailEle = NULL;

        // Add this new head to the first position of bucket list
        head->nextHead = head->bucket->nodeList;
        head->bucket->nodeList = head;

        pthread_rwlock_unlock(&hashMap.bucketList[bucketPos].bucketLock);
        return true;
    }

    // Now, we found the matched head node in this slot

    // First, lock this header
    printf("%s try to get  header lock\n", __func__ );
//    WriterLock w_header_lock(iter->headLock);
    pthread_rwlock_rdlock(&iter->headLock);
    printf("%s Get header lock\n", __func__ );

    // If this lsn is smaller than the maximum lsn, do nothing
    if(iter->maxLsn >= lsn) {
        printf("%s try to insert failed, logindex_maxLsn = %lu, parameter lsn = %lu\n", __func__ , iter->maxLsn, lsn);

        pthread_rwlock_unlock(&hashMap.bucketList[bucketPos].bucketLock);
        pthread_rwlock_unlock(&iter->headLock);
        return false;
    }

    // If head still has available space, add it to head
    if(iter->entryNum < HASH_HEAD_NUM) {
//        printf("Add to head\n");
        iter->lsnEntry[iter->entryNum].pageNum = pageNum;
        iter->lsnEntry[iter->entryNum].lsn = lsn;
        iter->entryNum++;

        iter->maxLsn = lsn;

        pthread_rwlock_unlock(&hashMap.bucketList[bucketPos].bucketLock);
        pthread_rwlock_unlock(&iter->headLock);
        return true;
    }

    // If the tail node has available space, add this entry to tail node
    if(iter->tailEle
        && iter->tailEle->entryNum < HASH_ELEM_NUM) {

//        printf("add into tail node\n");
        HashNodeEle *nodeEle = iter->tailEle;
        nodeEle->lsnEntry[nodeEle->entryNum].pageNum = pageNum;
        nodeEle->lsnEntry[nodeEle->entryNum].lsn = lsn;

        nodeEle->entryNum++;

        // update this node's maxLsn and header's lsn
        nodeEle->maxLsn = lsn;
        iter->maxLsn = lsn;

        pthread_rwlock_unlock(&hashMap.bucketList[bucketPos].bucketLock);
        pthread_rwlock_unlock(&iter->headLock);
        return true;
    }

    // If the tail node doesn't have enough space or there is no element node
    // Add a new element node to the head list tail.

//    printf("Create tail node\n");
    HashNodeEle* eleNode = (HashNodeEle*) malloc(sizeof(HashNodeEle));
    eleNode->maxLsn = lsn;
    eleNode->lsnEntry[0].pageNum = pageNum;
    eleNode->lsnEntry[0].lsn = lsn;
    eleNode->entryNum = 1;
    eleNode->nextEle = NULL;

    iter->maxLsn = lsn;

    // If header has one or more element nodes
    if(iter->tailEle != NULL) {
        iter->tailEle->nextEle = eleNode;
        iter->tailEle = eleNode;
    } else { // If no element node linked by this header
        iter->nextEle = eleNode;
        iter->tailEle = eleNode;
    }

    pthread_rwlock_unlock(&hashMap.bucketList[bucketPos].bucketLock);
    pthread_rwlock_unlock(&iter->headLock);
    return true;
}

// [lsn(0), lsn(1), ... lsn(i), lsn(i+1), ..., lsn(n)]
// If lsn(i) <= targetLsn < lsn(i+1), return ( true, lsn(i) )
// If targetLsn >= lsn(n), return true, return ( true, lsn(n) )
// If lsn list is empty, return false
bool HashMapFindLowerBoundEntry(KeyType key, uint64_t targetLsn, uint64_t* foundLsn, int* foundPageNum) {
    uint32_t hashValue = HashKey(key);
    uint32_t bucketPos = hashValue % hashMap.bucketNum;

    // Lock this slot
//    ReaderLock r_lock(hashMap.bucketList[bucketPos].bucketLock);
    pthread_rwlock_rdlock(&hashMap.bucketList[bucketPos].bucketLock);

    // Find the match head
    HashNodeHead* iter = hashMap.bucketList[bucketPos].nodeList;
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

    // If this relation doesn't exist in hash map, return false
    if(!foundHead) {
        pthread_rwlock_unlock(&hashMap.bucketList[bucketPos].bucketLock);
        return false;
    }

    // Lock this head
//    ReaderLock r_head_lock(iter->headLock);
    pthread_rwlock_rdlock(&iter->headLock);

    uint64_t currentLsn = -1;
    int      currentPageNum;
    // If lsn is in this head
    // Iterate all the elements in the head
    if(iter->lsnEntry[iter->entryNum-1].lsn >= targetLsn) {
        for(int i = 0; i < iter->entryNum; i++) {
            if(iter->lsnEntry[i].lsn > targetLsn) {
                break;
            } else {
                currentLsn = iter->lsnEntry[i].lsn;
                currentPageNum = iter->lsnEntry[i].pageNum;
            }
        }
        // If all list elements are larger than targetLsn, return false
        if(currentLsn == -1) {
            pthread_rwlock_unlock(&hashMap.bucketList[bucketPos].bucketLock);
            pthread_rwlock_unlock(&iter->headLock);

            return false;
        } else {
            pthread_rwlock_unlock(&hashMap.bucketList[bucketPos].bucketLock);
            pthread_rwlock_unlock(&iter->headLock);

            *foundLsn = currentLsn;
            *foundPageNum = currentPageNum;
            return true;
        }
    }

    // Iterate all following element nodes

    // Initialize the currentLsn as the rear lsn of list
    currentLsn = iter->lsnEntry[iter->entryNum-1].lsn;
    currentPageNum = iter->lsnEntry[iter->entryNum-1].pageNum;

    HashNodeEle* eleIter = iter->nextEle;
    while(eleIter != NULL) {
        // fast skip
        if(eleIter->maxLsn < targetLsn) {
            currentLsn = eleIter->lsnEntry[eleIter->entryNum-1].lsn;
            currentPageNum = eleIter->lsnEntry[eleIter->entryNum-1].pageNum;

            eleIter = eleIter->nextEle;
            continue;
        }

        // targetEntry should be found in the list
        for(int i = 0; i < eleIter->entryNum; i++) {
//            printf("Get in, comparedLsn = %lu\n", eleIter->lsnEntry[i].lsn);
            if(eleIter->lsnEntry[i].lsn > targetLsn) {
                break;
            } else {
                currentLsn = eleIter->lsnEntry[i].lsn;
                currentPageNum = eleIter->lsnEntry[i].pageNum;
            }
        }

        // Found the desired lsn
        break;
    }

    *foundLsn = currentLsn;
    *foundPageNum = currentPageNum;

    pthread_rwlock_unlock(&iter->headLock);
    pthread_rwlock_unlock(&hashMap.bucketList[bucketPos].bucketLock);
    return true;
}

//void HashMapUpdateKey(KeyType key, int value) {
//    uint32_t hashValue = HashKey(key);
//    uint32_t bucketPos = hashValue % hashMap.bucketNum;
//    HashBucket* targetBucket = &hashMap.bucketList[bucketPos];
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
//        node->bucket = &hashMap.bucketList[bucketPos];
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
//    uint32_t bucketPos = hashValue % hashMap.bucketNum;
//    HashBucket* targetBucket = &hashMap.bucketList[bucketPos];
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

void HashMapDestroy(){
    for(int i = 0; i < hashMap.bucketNum; i++) {
        while (hashMap.bucketList[i].nodeList) {
            HashNodeHead* head = hashMap.bucketList[i].nodeList;

            while(head->nextEle) {
                HashNodeEle* elemNode = head->nextEle;
                head->nextEle = head->nextEle->nextEle;
                free(elemNode);
            }

            hashMap.bucketList[i].nodeList = head->nextHead;
            free(head);
        }
    }
    free(hashMap.bucketList);
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
