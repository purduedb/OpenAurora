//
// Created by xi on 10/15/22.
//

#ifndef TEST_MIX_C_C___FUNC_H
#define TEST_MIX_C_C___FUNC_H


#ifdef __cplusplus
extern "C" {
#endif

struct KeyType {
    uint64_t SpcID;
    uint64_t DbID;
    uint64_t RelID;
    uint32_t ForkNum;
};


//extern int GetInteger(int i);
//extern void *ReadLock(void*);
//extern void *WriteLock(void*);

extern void HashMapInit(int bucketNum);
extern void HashMapDestroy();
extern bool HashMapInsertKey(struct KeyType key, uint64_t lsn, int pageNum);
extern bool HashMapFindLowerBoundEntry(struct KeyType key, uint64_t targetLsn, uint64_t* foundLsn, int* foundPageNum);

#ifdef __cplusplus
}
#endif

#endif //TEST_MIX_C_C___FUNC_H
