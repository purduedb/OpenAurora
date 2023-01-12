#include "postgres.h"
#include "common/hashfn.h"
#include "common/partition_lock.h"

#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>

void InitializePartitionMutex(PartitionMutex* partitionMutexPtr, int slotNum, size_t keySize) {
//    printf("%s %s %d, keySize = %ld\n", __func__ , __FILE__ , __LINE__, keySize);
//    fflush(stdout);
    *partitionMutexPtr = (PartitionMutex) malloc(sizeof(struct PartitionMutexData)) ;


    (*partitionMutexPtr)->mutex_list = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t)*slotNum);
    (*partitionMutexPtr)->slot_num = slotNum;
    (*partitionMutexPtr)->key_size = keySize;

    for(int i = 0; i < slotNum; i++) {
        pthread_mutex_init(&(*partitionMutexPtr)->mutex_list[i], NULL);
    }
}

void DestroyPartitionMutex(PartitionMutex partitionMutex) {
//    printf("%s %s %d tid=%d\n", __func__ , __FILE__ , __LINE__, gettid());
//    fflush(stdout);
    for(int i = 0; i < partitionMutex->slot_num; i++) {
        pthread_mutex_destroy(&partitionMutex->mutex_list[i]);
    }
    free(partitionMutex->mutex_list);
    free(partitionMutex);
//    printf("%s %s %d tid=%d\n", __func__ , __FILE__ , __LINE__, gettid());
//    fflush(stdout);
}

void PartitionLock(PartitionMutex partitionMutex, void* tag) {
//    printf("Try to lock tid = %d caller = %s\n", gettid(), funcName);
//    fflush(stdout);

    uint32 lockPos = tag_hash(tag, partitionMutex->key_size) % partitionMutex->slot_num;

//    printf("%s try lock %d, tid=%d\n", __func__ , lockPos, gettid());
//    fflush(stdout);
//    pthread_mutex_t targetMutex = partitionMutex->mutex_list[lockPos];
    pthread_mutex_lock(&(partitionMutex->mutex_list[lockPos]));

//    printf("%s tid = %d caller = %s succeed, pos = %d\n", __func__ , gettid(), funcName, lockPos);
//    fflush(stdout);

}

void PartitionUnlock(PartitionMutex partitionMutex, void* tag) {
//    printf("%s tid = %d\n", __func__ , gettid());
//    fflush(stdout);

    uint32 lockPos = tag_hash(tag, partitionMutex->key_size) % partitionMutex->slot_num;

//    printf("%s try unlock %d, tid=%d\n", __func__ , lockPos, gettid());
//    fflush(stdout);
//    pthread_mutex_t targetMutex = partitionMutex->mutex_list[lockPos];
    pthread_mutex_unlock(&(partitionMutex->mutex_list[lockPos]));
}