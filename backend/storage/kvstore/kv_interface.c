#include "postgres.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "storage/kv_interface.h"
#include "pg_config_manual.h"
#include "stdlib.h"
#include "string.h"
#include <unistd.h>  // sysconf() - get CPU count
#include <zconf.h>
#include <string.h>
#include "c.h"
#include "miscadmin.h"
#include "postgres.h"
#include "storage/buf_internals.h"
#include "access/xlogreader.h"
#ifdef USE_LIGHT_KV
#include "storage/light_weighted_kvstore_api.h"
#endif

#define USE_ROCKSDB 1
//#define USE_LIGHT_KV 1

#ifdef USE_ROCKSDB

#include "rocksdb/c.h"
rocksdb_t *db = NULL;

#endif

// $SpcID_$DbID_$RelID_$ForkNum_$BlkNum
#define ROCKSDB_LSN_LIST_KEY  ("rocks_list_%lu_%lu_%lu_%d_%u\0")

// $SpcID_$DbID_$RelID_$ForkNum_$BlkNum_$LSN
#define ROCKSDB_PAGE_VERSION_KEY  ("rocks_page_%lu_%lu_%lu_%d_%u_%lu\0")

// $LSN
#define ROCKSDB_XLOG_KEY ("rocks_xlog_%lu\0")

#define MAX_PATH_LEN (256)

char KvStorePath[MAXPGPATH];

//! Lsn List Format: $ListLen, $CurrPos, [v0, v1, ... , v($ListLen-1)]
//! Total length = uint64 * ($ListLen+2)
//! $CurrPos points to [0, ..., (n-1)]

#ifdef USE_LIGHT_KV
void InitKvStore() {
    InitLightWeightedKVStore();
}
#endif

#ifdef USE_ROCKSDB
void InitKvStore() {
    if (db != NULL) {
        return;
    }
    printf("%s start\n", __func__ );
    fflush(stdout);

    snprintf(KvStorePath, sizeof(KvStorePath), "%s/rocksdb_test_db", DataDir);

    rocksdb_options_t *options = rocksdb_options_create();
//    int max_file_num = rocksdb_options_get_max_open_files(options);
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("MAX FILE NUM = %d\n", max_file_num)));
//
//    sleep(15);
    rocksdb_options_set_max_open_files(options, 1024);
#if defined(OS_WIN)
    SYSTEM_INFO system_info;
    GetSystemInfo(&system_info);
    long cpus = system_info.dwNumberOfProcessors;
#else
    long cpus = sysconf(_SC_NPROCESSORS_ONLN);
#endif
//    rocksdb_options_increase_parallelism(options, (int)(cpus));
//    rocksdb_options_optimize_level_style_compaction(options, 0);
//    // create the DB if it's not already present
//    rocksdb_options_set_create_if_missing(options, 1);
    rocksdb_options_increase_parallelism(options, (int)(cpus));
    rocksdb_options_optimize_level_style_compaction(options, 0);

    rocksdb_options_set_max_background_compactions(options, (int)(cpus));
    rocksdb_options_set_max_background_flushes(options, (int)(cpus));

    rocksdb_options_set_compression(options, (int)(cpus));
    rocksdb_options_set_max_successive_merges(options, 1000);
//    rocksdb_options_set_manual_wal_flush(options, 1);
    rocksdb_options_set_write_buffer_size(options, (size_t)10*1024*1024*1024);

    rocksdb_env_t *options_env = rocksdb_create_default_env();
    rocksdb_env_set_high_priority_background_threads(options_env, (int)(cpus));
    rocksdb_env_set_low_priority_background_threads(options_env, (int)(cpus));
    rocksdb_options_set_env(options,options_env);
    // create the DB if it's not already present
    rocksdb_options_set_create_if_missing(options, 1);


    // open DB
    char *err = NULL;
    db = rocksdb_open(options, KvStorePath, &err);
    rocksdb_options_destroy(options);
    printf("%s ends \n", __func__ );
    fflush(stdout);
    return;
}
#endif


#ifdef USE_ROCKSDB
int KvPut(char *key, char *value, int valueLen) {
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("[KvPut] key = %s, valueLen = %d\n", key,  valueLen)));
    InitKvStore();

    char * err = NULL;
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_put(db, writeoptions, key, strlen(key), value, valueLen,
                &err);
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("[KvPut] Put completed\n")));
    rocksdb_writeoptions_destroy(writeoptions);
    if (err != NULL) {
        printf("%s failed, error = %s\n", __func__ , err);
//        ereport(ERROR,
//                (errcode(ERRCODE_KV_COMMAND_ERROR),
//                        errmsg("[KvPut] KvPut failed, error = %s\n", err)));
        return 1;
    }
    return 0;
}
#endif

#ifdef USE_LIGHT_KV
int KvPut(char *key, char *value, int valueLen) {
    KvStoreInsertKVPair(key, strlen(key), value);
    return 0;
}

#endif


#ifdef USE_ROCKSDB
// returned_value should be freed by caller function.
int KvGet(char *key, char **value, size_t *len) {
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("[KvGet] key = %s \n", key)));
    InitKvStore();
    char *err = NULL;
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    (*value) =
            rocksdb_get(db, readoptions, key, strlen(key), len, &err);
    rocksdb_readoptions_destroy(readoptions);
    if (err != NULL) {
        return 1;
    }
    return 0;
}
#endif

#ifdef USE_LIGHT_KV
// returned_value should be freed by caller function.
int KvGet(char *key, char **value, size_t *len) {
    *value = (char*)malloc(8192);
    int err = KvStoreGetValue(key, strlen(key), *value);
    if (!err) {
        *len = 8192;
        return 0;
    } else {
        free(*value);
        *value = NULL;
        return 1;
    }
}

#endif

#ifdef DISABLED_FUNCTION
// if the key doesn't exist, return 1
// else return 0
int KvGetInt(char *key, int *result) {
    char *value;
    size_t valueLen;
    KvGet(key, &value, &valueLen);
    if (value == NULL) {
        return 1;
    }
    *result = *(int *)value;
    free(value);
    return 0;
}

int KvPutInt(char *key, int value) {
    int *p = malloc(sizeof(int));
    p[0] = value;
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("[KvPutInt] Started\n\n\n")));
    int err = KvPut(key, (char*)p, sizeof(int));
    free(p);
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("[KvPutInt] End\n\n\n")));
    return err;
}
#endif

#ifdef USE_ROCKSDB
void KvClose() {
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("[KvClose] Start Close\n\n\n")));
    if (db != NULL) {
        rocksdb_close(db);
        db = NULL;
//        ereport(NOTICE,
//                (errcode(ERRCODE_INTERNAL_ERROR),
//                        errmsg("[KvClose] Start Close, success\n\n\n")));
    }

    return;
}
#endif

#ifdef USE_LIGHT_KV
void KvClose() {
    DestroyLightWeightedKVStore();
    return;
}
#endif


#ifdef USE_ROCKSDB
int KvDelete(char *key) {
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("[KvDelete]Started\n")));
    InitKvStore();
    char * err;
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_delete(db, writeoptions, key, strlen(key), &err);
    rocksdb_writeoptions_destroy(writeoptions);
    if (err != NULL) {
        return 1;
    }
    return 0;
}
#endif

#ifdef USE_LIGHT_KV
int KvDelete(char *key) {
    KvStoreDeleteValue(key, strlen(key));
    return 0;
}
#endif


int PutXlogWithLsn(XLogRecPtr lsn, XLogRecord* record) {

    char tempKey[MAX_PATH_LEN];
    snprintf(tempKey, sizeof(tempKey), ROCKSDB_XLOG_KEY, lsn);

    return KvPut(tempKey, (char*)record, record->xl_tot_len);
}

int GetXlogWithLsn(XLogRecPtr lsn, XLogRecord** record, size_t* record_size) {
    char tempKey[MAX_PATH_LEN];
    snprintf(tempKey, sizeof(tempKey), ROCKSDB_XLOG_KEY, lsn);

    KvGet(tempKey, (char**)record, record_size);
    return *record_size > 0;
}

#ifdef DISABLED_FUNCTION
// Put list:
//      input: list, listNum
//      Set listNum to rocksdb
//      This function treats list as a pure list,
//          doesn't care about LsnList format
int PutListWithKey(char* key, uint64_t* listPointer, int listSize) {
    return KvPut(key, (char*)listPointer, listSize*sizeof(uint64_t));
}


// Get list:
//      malloc (maxSize)
//      Get from rocksdb and convert to uint64List
// Out of function, free pointer
int GetListWithKey(char* key, uint64_t** listPointer, int* listSize) {
    size_t size;
    KvGet(key, (char**)listPointer, &size);
    *listSize = size / sizeof(uint64_t);
#ifdef ENABLE_DEBUG_INFO
    printf("%s get values size is %lu, listSize = %d\n", __func__ , size, *listSize);
#endif
    // If found, return 1. Else, return 0
    return (size>0);
}


// Returned values should be freed
// return value: 0->not found, 1->found
int GetListFromRocksdb(BufferTag bufferTag, uint64_t** listPointer, int* listSize) {
    char tempKey[MAX_PATH_LEN];
    snprintf(tempKey, sizeof(tempKey), ROCKSDB_LSN_LIST_KEY, bufferTag.rnode.spcNode,
             bufferTag.rnode.dbNode, bufferTag.rnode.relNode, bufferTag.forkNum, bufferTag.blockNum);

    size_t size;
    int err = KvGet(tempKey, (char**)listPointer, &size);

    if(err) {
        printf("%s, KvGet failed\n", __func__ );
        return 0;
    }
    if(size == 0)
        return 0;

    *listSize = size/sizeof(uint64_t);

    return 1;
}

void PutList2Rocksdb(BufferTag bufferTag, uint64_t* listPointer, int listSize) {
    char tempKey[MAX_PATH_LEN];
    snprintf(tempKey, sizeof(tempKey), ROCKSDB_LSN_LIST_KEY, bufferTag.rnode.spcNode,
             bufferTag.rnode.dbNode, bufferTag.rnode.relNode, bufferTag.forkNum, bufferTag.blockNum);

    KvPut(tempKey, (char*)listPointer, listSize*sizeof(uint64_t));

    return;
}

// Find lower bound of uint64 list
// If didn't find any matched LSN, return 0
//      Else (found), return 1
//
// The uintList has two element($lsn, $pos) ahead of real content
int FindListLowerBound(uint64_t* uintList, uint64_t targetLsn, uint64_t *foundLsn, uint64_t *foundPos) {
    uint64_t listLen = uintList[0];
    uint64_t currPos = uintList[1];

    if(listLen == 0)
        return 0;

    if(targetLsn < uintList[0+2])
        return 0;

    for(int i = 2; i < listLen+2; i++) {
        if(uintList[i] <= targetLsn) {
            *foundLsn = uintList[i];
            *foundPos = i;
        } else {
            break;
        }
    }

    return 1;
}
#endif

void DeletePageFromRocksdb(BufferTag bufferTag, uint64_t lsn) {
    char tempKey[MAX_PATH_LEN];
    snprintf(tempKey, sizeof(tempKey), ROCKSDB_PAGE_VERSION_KEY, bufferTag.rnode.spcNode,
             bufferTag.rnode.dbNode, bufferTag.rnode.relNode, bufferTag.forkNum, bufferTag.blockNum, lsn);

    KvDelete(tempKey);
}

// pageContent should be freed by caller functions
// return value: found->1, not found->0
int GetPageFromRocksdb(BufferTag bufferTag, uint64_t lsn, char** pageContent) {
    char tempKey[MAX_PATH_LEN];
    snprintf(tempKey, sizeof(tempKey), ROCKSDB_PAGE_VERSION_KEY, bufferTag.rnode.spcNode,
             bufferTag.rnode.dbNode, bufferTag.rnode.relNode, bufferTag.forkNum, bufferTag.blockNum, lsn);

    size_t valueSize;

    // error occurred
    if(KvGet(tempKey, pageContent, &valueSize)){
        printf("%s failed, because of KvGet function failed\n", __func__ );
        return 0;
    }
    if(valueSize == 0)
        return 0;

    return 1;
}

void PutPage2Rocksdb(BufferTag bufferTag, uint64_t lsn, char* pageContent) {
    char tempKey[MAX_PATH_LEN];
    snprintf(tempKey, sizeof(tempKey), ROCKSDB_PAGE_VERSION_KEY, bufferTag.rnode.spcNode,
             bufferTag.rnode.dbNode, bufferTag.rnode.relNode, bufferTag.forkNum, bufferTag.blockNum, lsn);
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d, key = %s\n", __func__ , __LINE__, tempKey);
    fflush(stdout);
#endif

    KvPut(tempKey, pageContent, BLCKSZ);

    return;
}


#ifdef DISABLED_FUNCTION

void InsertLsn2RocksdbList(BufferTag bufferTag, uint64_t lsn) {
#ifdef ENABLE_DEBUG_INFO
    printf("%s 2Starts\n", __func__ );
    fflush(stdout);
#endif

    char tempKey[MAX_PATH_LEN];
    snprintf(tempKey, sizeof(tempKey), ROCKSDB_LSN_LIST_KEY, bufferTag.rnode.spcNode,
            bufferTag.rnode.dbNode, bufferTag.rnode.relNode, bufferTag.forkNum, bufferTag.blockNum);

#ifdef ENABLE_DEBUG_INFO
    printf("tempKey = %s\n", tempKey);
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    int listSize;
    uint64_t *int64List;
    int found = GetListWithKey(tempKey, &int64List, &listSize);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d, old_list_size = %d\n", __func__ , __LINE__, listSize);
    fflush(stdout);
#endif
    // Create a new version list, if no list exist
    if(!found) {
#ifdef ENABLE_DEBUG_INFO
        printf("%s %d\n", __func__ , __LINE__);
        fflush(stdout);
#endif
        uint64_t newList[8];
        // Insert the first lsn to list
        newList[2] = lsn;
        // Set list len
        newList[0] = 1;
        // Set current position
        newList[1] = 0;
        // Put new list to Rocksdb
        PutListWithKey(tempKey, newList, 3);
        return;
    }
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    // Insert new element to rocksdb

    // If the last element is larger than parameter $lsn, ignore it
    if(int64List[listSize-1] >= lsn) {
        free(int64List);
        return;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    // Assign a new lsn list
    uint64_t *extendList = (uint64_t*) malloc( (listSize+1) * sizeof(uint64_t) );
    memcpy(extendList, int64List, listSize*sizeof(uint64_t));

    // Update list's first element (list len)
    extendList[0] = extendList[0]+1;

    // Add new element to extendList
    extendList[listSize] = lsn;

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    PutListWithKey(tempKey, extendList, listSize+1);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
    free(int64List);
    free(extendList);

#ifdef ENABLE_DEBUG_INFO
    printf("%s Ends\n", __func__ );
    fflush(stdout);
#endif
    return;
}

void MarshalIntList(int* numList, int size, char **p) {
    *p = malloc(sizeof(int) * (size+1));
    int* pInt = (int*)*p;
    pInt[0] = size;
    for(int i = 0; i < size; i++) {
        pInt[i+1] = numList[i];
    }
    return;
}

int UnmarshalListGetSize(char *p) {
    int *pInt = (int*)p;
    return pInt[0];
}

int* UnmarshalListGetList(char *p) {
    int *pInt = (int*)p;
    return &pInt[1];
}

#endif