#ifndef PARTITION_LOCK_H
#define PARTITION_LOCK_H
#include <pthread.h>

struct PartitionMutexData {
    pthread_mutex_t *mutex_list;
    int slot_num;
    size_t key_size;
};

typedef struct PartitionMutexData* PartitionMutex;

extern void InitializePartitionMutex(PartitionMutex *partitionMutex, int slotNum, size_t keySize);
extern void DestroyPartitionMutex(PartitionMutex partitionMutex);
extern void PartitionLock(PartitionMutex partitionMutex, void* tag);
extern void PartitionUnlock(PartitionMutex partitionMutex, void* tag);

#endif //PARTITION_LOCK_H