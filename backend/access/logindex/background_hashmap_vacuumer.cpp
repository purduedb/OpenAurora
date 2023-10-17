//
// Created by pang65 on 1/14/23.
//

#include "c.h"
#include "postgres.h"
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <iostream>
#include "access/logindex_hashmap.h"
#include <atomic>
#include "storage/kv_interface.h"
#include "tcop/storage_server.h"
#include <sys/time.h>
#include <pthread.h>
#include <cstdlib>
#include <access/background_hashmap_vacuumer.h>
#include "tcop/storage_server.h"

#define ITER_BATCH_SIZE 10
#define MAX_REPLAY_VERSION_SIZE 20

#define ITER_BUCKET_INTERVAL 5
#define ITER_HEAD_INTERVAL 1000

void VacuumHashNode(HashNodeHead* head, HashNodeEle* ele, BufferTag bufferTag);
void BackgroundReplayHeadNode(HashNodeHead * head);
void BackgroundVacuumUnusedVersions(HashNodeHead* head);

//#define ENABLE_DEBUG_INFO2

//#define ENABLE_DEBUG_INFO
// Use try_rdlock to get a bucketLock, if failed, iterate to next bucket immediately
// Then iterate head list, when try_wrlock head lock successfully, vacuum that head node list
bool BackgroundHashMapCleanRocksdb(HashMap hashMap) {

    int currentBucketID = gettid() % hashMap->bucketNum;
    HashNodeHead *headNodes[ITER_BATCH_SIZE];
    int recordNumber = 0;
    struct timeval now;

    while(1) { // Iterate all buckets
        usleep(3000);
        // Iterate every bucket one by one
        currentBucketID = (currentBucketID+1) % hashMap->bucketNum;
//        printf("%s %s %d, currentBucketID %d\n", __FILE__, __func__ , __LINE__, currentBucketID);
//        fflush(stdout);
        // How many heads we have processed
        int currentFinishHeadNum = 0;
        // flag: finish iterate all the nodes in this bucket
        int finishIterThisBucket = 0;

        int statisticFinishVacuum = 0;

//        printf("%s %s %d\n", __FILE__, __func__ , __LINE__);
//        fflush(stdout);
        // Set the initial iter to bucket's first
        HashNodeHead *iter = hashMap->bucketList[currentBucketID].nodeList;
        if (pthread_mutex_trylock(&(hashMap->bucketList[currentBucketID].replayLock)) != 0) { // Other replay process is processing this bucket
            continue;
        }

//        printf("%s %s %d\n", __FILE__, __func__ , __LINE__);
//        fflush(stdout);

        // Now get the replay lock, check whether it has enough interval before last vacuum
        gettimeofday(&now, NULL);
        if (now.tv_sec > hashMap->bucketList[currentBucketID].lastReplayTime.tv_sec && now.tv_sec - hashMap->bucketList[currentBucketID].lastReplayTime.tv_sec < ITER_BUCKET_INTERVAL) {
            pthread_mutex_unlock(&(hashMap->bucketList[currentBucketID].replayLock));
            continue;
        }

//        printf("%s %s %d\n", __FILE__, __func__ , __LINE__);
//        fflush(stdout);

        while(1) { // Iterate all the head nodes in this bucket
#ifdef ENABLE_DEBUG_INFO2
            printf("%s %d, background_vacuumer %d, start vacuum bucket %d\n", __func__ , __LINE__, gettid(), currentBucketID);
            fflush(stdout);
#endif
//            printf("%s %s %d\n", __FILE__, __func__ , __LINE__);
//            fflush(stdout);
            recordNumber = 0;

            // Add a lock to this bucket
            pthread_rwlock_rdlock(&hashMap->bucketList[currentBucketID].bucketLock);


            for(int i = 0; i < currentFinishHeadNum; i++) {
                if(iter!=NULL) {
                    iter = iter->nextHead;
                };
            }
            // Collect ITER_BENCH_SIZE heads
            for(int i = 0; i < ITER_BATCH_SIZE; i++) {
                if(iter!=NULL) {
                    if(iter->replayedLsn < iter->maxLsn)
                        headNodes[recordNumber++] = iter;
                    iter = iter->nextHead;
                } else {
                    finishIterThisBucket = 1;
                    break;
                }
            }

            // Release this lock
            pthread_rwlock_unlock(&hashMap->bucketList[currentBucketID].bucketLock);

#ifdef ENABLE_DEBUG_INFO2
            printf("%s %d, background_vacuumer %d, got %d heads\n", __func__ , __LINE__, gettid(), recordNumber);
            fflush(stdout);
#endif

            // ITER these ITER_BATCH_SIZE heads
            for(int i = 0; i < recordNumber; i++) {
                if( pthread_rwlock_trywrlock(&(headNodes[i]->headLock)) != 0) { // failed to grab lock, skip this node
                    continue;
                }

#ifdef ENABLE_DEBUG_INFO2
                printf("%s %d, background_vacuumer %d, vacuuming %d head\n", __func__ , __LINE__, gettid(), i);
                fflush(stdout);
#endif
                // TODO: check finish_vacuum_time, and replay this head

                struct timeval now;
                gettimeofday(&now, NULL);
                if(now.tv_sec - headNodes[i]->finishVacuumTime.tv_sec >= ITER_HEAD_INTERVAL) {
//                    printf("%s %s %d\n", __FILE__, __func__ , __LINE__);
//                    fflush(stdout);
                    BackgroundVacuumUnusedVersions(headNodes[i]);
                    headNodes[i]->finishVacuumTime = now;
                }

#ifdef ENABLE_DEBUG_INFO2
                printf("%s %d, background_vacuumer %d, finish vacuum %d head\n", __func__ , __LINE__, gettid(), i);
                fflush(stdout);
#endif

                pthread_rwlock_unlock(&(headNodes[i]->headLock));
            }

            // Skip these replayed heads in the next turn
            currentFinishHeadNum += ITER_BATCH_SIZE;

            statisticFinishVacuum += recordNumber;
            // If we finish iterate all the heads in this bucket, iterate next bucket.
            if(finishIterThisBucket) {
                break;
            }
        }


        // If we finish replayed all the heads in this bucket, wait an interval for the next scanning
        if(1 || statisticFinishVacuum == 0) {
            // update the last replay time for this bucket
            gettimeofday(&now, NULL);
            hashMap->bucketList[currentBucketID].lastReplayTime.tv_sec = now.tv_sec;
            hashMap->bucketList[currentBucketID].lastReplayTime.tv_usec = now.tv_usec;
        }
        // now other replay process can hold the lock again.
        pthread_mutex_unlock(&(hashMap->bucketList[currentBucketID].replayLock));
        if(statisticFinishVacuum != 0) {
            printf("%s %d, successfully cleaned %d bucket %d heads\n", __func__ , __LINE__, currentBucketID, statisticFinishVacuum);
            fflush(stdout);
        }

    }


}

#define VIRTUAL_NETWORK_LATENCY 8192
extern uint64_t RpcXLogFlushedLsn;

void BackgroundVacuumUnusedVersions(HashNodeHead* head) {
    uint64_t replayedLsn = head->replayedLsn;

    BufferTag bufferTag;
    RelFileNode rnode;

    rnode.spcNode = head->key.SpcID;
    rnode.dbNode = head->key.DbID;
    rnode.relNode = head->key.RelID;

    INIT_BUFFERTAG(bufferTag, rnode, (ForkNumber)head->key.ForkNum, head->key.BlkNum);

    if (replayedLsn == 0) {
        // This head node doesn't have any replayed version, just skip it
        return;
    }

    // Vacuum the head node
    // If the last version is 0, then this head has already been vacuumed to empty
    if(head->lsnEntry[head->entryNum-1].lsn != 0) {
        for (int i = 0; i < head->entryNum; i++) {
//            if(head->lsnEntry[i].lsn != 0 && head->lsnEntry[i].lsn < replayedLsn-VIRTUAL_NETWORK_LATENCY) {
            // Should use lsn+VIRTUAL_NETWORK_LATENCY < replayed. Not lsn < replayedLsn-VIRTUAL_NETWORK_LATENCY
            // Because replayedLsn is an unsigned int, if replayedLsn < VIRTUAL_NETWORK_LATENCY, then replayedLsn-VIRTUAL_NETWORK_LATENCY will be a very large number
            if(head->lsnEntry[i].lsn != 0 && head->lsnEntry[i].lsn+VIRTUAL_NETWORK_LATENCY < replayedLsn) {
                    // This version is no longer needed, we can vacuum it
                    DeletePageFromRocksdb(bufferTag, head->lsnEntry[i].lsn);
#ifdef ENABLE_DEBUG_INFO
                    printf("%s %s %d, vacuumed spc = %u, db = %u, rel = %u, fork = %d, blk = %ld, lsn = %lu, head replayed lsn = %lu\n", __FILE__, __func__, __LINE__, rnode.spcNode, rnode.dbNode, rnode.relNode, head->key.ForkNum, head->key.BlkNum, head->lsnEntry[i].lsn, replayedLsn);
                    fflush(stdout);
#endif
                    head->lsnEntry[i].lsn = 0;
            } else if (head->lsnEntry[i].lsn+VIRTUAL_NETWORK_LATENCY >= replayedLsn) {
                // This version is still needed, we can't vacuum it
                break;
            }
        }
    }

    HashNodeEle* ele = head->nextEle;
    while(ele != NULL) {
        for(int i = 0; i < ele->entryNum; i++) {
            if(ele->lsnEntry[i].lsn != 0 && ele->lsnEntry[i].lsn+VIRTUAL_NETWORK_LATENCY < replayedLsn) {
#ifdef ENABLE_DEBUG_INFO
                printf("%s %s %d, vacuumed spc = %u, db = %u, rel = %u, fork = %d, blk = %ld, lsn = %lu, head replayed lsn = %lu\n", __FILE__, __func__, __LINE__, rnode.spcNode, rnode.dbNode, rnode.relNode, head->key.ForkNum, head->key.BlkNum, ele->lsnEntry[i].lsn, replayedLsn);
                fflush(stdout);
#endif
                // This version is no longer needed, we can vacuum it
                DeletePageFromRocksdb(bufferTag, ele->lsnEntry[i].lsn);
                ele->lsnEntry[i].lsn = 0;
            } else if (ele->lsnEntry[i].lsn+VIRTUAL_NETWORK_LATENCY >= replayedLsn) {
                // This version is still needed, we can't vacuum it
                break;
            }
        }
        // This node is empty, we can march to the next node, and delete this one
        if(ele->lsnEntry[ele->entryNum-1].lsn == 0){
            HashNodeEle* nextEle = ele->nextEle;
            VacuumHashNode(head, ele, bufferTag);
            ele = nextEle;
        } else {
            // This node still has many useful versions, we can't vacuum it
            // By now, the vacuuming of this head is finished
            break;
        }
    }
}



// Before
void BackgroundReplayHeadNode(HashNodeHead * head) {
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    uint64_t replayedLsn = head->replayedLsn;
    uint64_t lsnList[MAX_REPLAY_VERSION_SIZE];
    int listSize = 0;
    char* basePage = NULL;
    BufferTag bufferTag;
    RelFileNode rnode;

    rnode.spcNode = head->key.SpcID;
    rnode.dbNode = head->key.DbID;
    rnode.relNode = head->key.RelID;

    INIT_BUFFERTAG(bufferTag, rnode, (ForkNumber)head->key.ForkNum, head->key.BlkNum);
    int foundBasePage = 1;
    bool gotBasePageFromRocksDb = false;

    if(replayedLsn == 0) { // Don't have basePage in RocksDB
        if(GetPageFromRocksdb(bufferTag, 1, &basePage) == 0) {
            if(SyncGetRelSize(bufferTag.rnode, bufferTag.forkNum, 0) == -1) {
                foundBasePage = 0;
            } else {
                // If not created by RpcMdExtend, get page from StandAlone process
                basePage = (char*) malloc(BLCKSZ);
                GetBasePage(rnode, (ForkNumber)head->key.ForkNum, (BlockNumber)head->key.BlkNum, basePage);
            }
        } else {
            gotBasePageFromRocksDb = true;
        }
    } else {
        GetPageFromRocksdb(bufferTag, replayedLsn, &basePage);
        gotBasePageFromRocksDb = true;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    // If all LSNs in head have been replayed, skip it
    if(replayedLsn < head->lsnEntry[head->entryNum-1].lsn) {
        for(int i = 0; i < head->entryNum; i++) {
            if(replayedLsn < head->lsnEntry[i].lsn) {
#ifdef ENABLE_DEBUG_INFO
                printf("%s %d, %lu, %lu\n", __func__ , __LINE__, replayedLsn, head->lsnEntry[i].lsn);
                fflush(stdout);
#endif
                lsnList[listSize++] = head->lsnEntry[i].lsn;
            }
            if(listSize >= MAX_REPLAY_VERSION_SIZE) {
                break;
            }
        }
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    HashNodeEle *ele = head->nextEle;
    while(ele != NULL) {
        HashNodeEle *nextEle = ele->nextEle;
        if(replayedLsn < ele->lsnEntry[ele->entryNum-1].lsn) {
           for(int i = 0; i < ele->entryNum; i++) {
               if(replayedLsn < ele->lsnEntry[i].lsn) {
                   lsnList[listSize++] = ele->lsnEntry[i].lsn;
               }
               if(listSize >= MAX_REPLAY_VERSION_SIZE) {
                   break;
               }
           }

           // Check whether we have collected enough version LSNs
           if(listSize == MAX_REPLAY_VERSION_SIZE) {

               if(lsnList[listSize-1] == ele->lsnEntry[ele->entryNum-1].lsn
                    && ele->nextEle != NULL) {
                   VacuumHashNode(head, ele, bufferTag);
               }
               break;
           }
        }

        if(ele->nextEle != NULL) { // If there have following nodes, free the current ele node
            VacuumHashNode(head, ele, bufferTag);
        }

        ele = nextEle;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    // For now, we have collect 0~MAX_REPLAY_VERSION_SIZE versions from this element node

    if(listSize > 0) {
        char * replayedPage = NULL;
        if(!foundBasePage) {
            basePage = (char*) malloc(BLCKSZ);
            ApplyOneLsnWithoutBasePage(bufferTag.rnode, bufferTag.forkNum, bufferTag.blockNum, lsnList[0], basePage);
        }

        // For now, we should have base pages
        if (!foundBasePage) {
            if(listSize-1 <= 0) { // only has one version and already been replayed
                PutPage2Rocksdb(bufferTag, lsnList[0], basePage);
                free(basePage);
                head->replayedLsn = lsnList[0];
#ifdef ENABLE_DEBUG_INFO
                printf("%s %d\n", __func__ , __LINE__);
                fflush(stdout);
#endif
                return;
            }
        }

#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif
        // we have other following version to be replayed
        replayedPage = (char*) malloc(BLCKSZ);
        if (!foundBasePage) {
#ifdef ENABLE_DEBUG_INFO
            printf("%s %d\n", __func__ , __LINE__);
            fflush(stdout);
#endif
            ApplyLsnList(bufferTag.rnode, bufferTag.forkNum, bufferTag.blockNum, lsnList + 1, listSize - 1, basePage, replayedPage);
        }else {
#ifdef ENABLE_DEBUG_INFO
            printf("%s %d, basePageLsn = %d, listSize=%d\n", __func__ , __LINE__, PageGetLSN(basePage), listSize);
            for(int i = 0; i< listSize; i++)
                printf("%lu, ", lsnList[i]);
            printf("\n");
            fflush(stdout);
#endif
            ApplyLsnList(bufferTag.rnode, bufferTag.forkNum, bufferTag.blockNum, lsnList, listSize, basePage, replayedPage);
        }
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif
        PutPage2Rocksdb(bufferTag, lsnList[listSize-1], replayedPage);
        DeletePageFromRocksdb(bufferTag, replayedLsn);
        head->replayedLsn = lsnList[listSize-1];
        if(!gotBasePageFromRocksDb)
            free(basePage);
        if(gotBasePageFromRocksDb)
            DeletePageFromRocksdb(bufferTag, replayedLsn==0?1:replayedLsn);
        free(replayedPage);
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif
        return;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    if(replayedLsn == 0 && !gotBasePageFromRocksDb) {
        PutPage2Rocksdb(bufferTag, 1, basePage);
        head->replayedLsn = 1;
    }

    if(!gotBasePageFromRocksDb)
        free(basePage);
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    return;
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























