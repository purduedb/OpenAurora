#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "storage/kvstore.h"
#include "pg_config_manual.h"
#include "stdlib.h"
#include "string.h"
#include <unistd.h>  // sysconf() - get CPU count
#include <zconf.h>
#include <string.h>
#include "c.h"
#include "miscadmin.h"
#include "postgres.h"

#define DEBUG_INFO
#ifdef USE_ROCKS_KV
#include "rocksdb/c.h"
rocksdb_t *db = NULL;


char KvStorePath[MAXPGPATH];

void InitKvStore() {
    if (db != NULL) {
        return;
    }

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
    rocksdb_options_increase_parallelism(options, (int)(cpus));
    rocksdb_options_optimize_level_style_compaction(options, 0);
    // create the DB if it's not already present
    rocksdb_options_set_create_if_missing(options, 1);

    // open DB
    char *err = NULL;
    db = rocksdb_open(options, KvStorePath, &err);
    rocksdb_options_destroy(options);
    return;
}


int KvPut(char *key, char *value, int valueLen) {
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[KvPut] key = %s, valueLen = %d\n", key,  valueLen)));
    InitKvStore();

    char * err = NULL;
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_put(db, writeoptions, key, strlen(key), value, valueLen + 1,
                &err);
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[KvPut] Put completed\n")));
    rocksdb_writeoptions_destroy(writeoptions);
    if (err != NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_KV_COMMAND_ERROR),
                        errmsg("[KvPut] KvPut failed, error = %s\n", err)));
        return 1;
    }
    return 0;
}


// returned_value should be freed by caller function.
int KvGet(char *key, char **value) {
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[KvGet] key = %s \n", key)));
    InitKvStore();
    char *err = NULL;
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    size_t len;
    (*value) =
            rocksdb_get(db, readoptions, key, strlen(key), &len, &err);
    rocksdb_readoptions_destroy(readoptions);
    if (err != NULL) {
        return 1;
    }
    return 0;
}

// if the key doesn't exist, return 1
// else return 0
int KvGetInt(char *key, int *result) {
    char *value;
    KvGet(key, &value);
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
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[KvPutInt] Started\n\n\n")));
    int err = KvPut(key, (char*)p, sizeof(int));
    free(p);
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[KvPutInt] End\n\n\n")));
    return err;
}

void KvClose() {
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[KvClose] Start Close\n\n\n")));
    if (db != NULL) {
        rocksdb_close(db);
        db = NULL;
        ereport(NOTICE,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("[KvClose] Start Close, success\n\n\n")));
    }

    return;
}

int KvDelete(char *key) {
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[KvDelete]Started\n")));
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

#else

#include "hiredis/hiredis.h"
redisContext *c;

void initKvStore() {
    const char *hostname = "127.0.0.1";
    int port = 6379;

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds

    c = redisConnectWithTimeout(hostname, port, timeout);
    if (c == NULL || c->err) {
        if (c) {
            printf("Connection error: %s\n", c->errstr);
            redisFree(c);
        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }
}


void KvPrefixCopyDir(char* srcPath, char* dstPath, const char* prefixKey) {
    int prefixLen = (int) strlen(prefixKey);
    char parseKey[1024];
    snprintf(parseKey, sizeof(parseKey),"%s%s/*", prefixKey, srcPath);
    //printf("Parse key = %s\n", parseKey);
    redisReply *reply;
    reply = redisCommand(c, "KEYS %s", parseKey);
    if (c->err) {
        printf("Error \n");
        return;
    }


    int srcPathLen = strlen(srcPath);
    for (int i=0; i<reply->elements; i++) {
        redisCommand(c, "COPY %s %s%s%s", reply->element[i]->str, prefixKey, dstPath, (reply->element[i]->str)+prefixLen+srcPathLen);
        if (c->err) {
            printf("COPY ERROR \n");
            return;
        }
    }
}

int KvPut(char *key, char *value, int valueLen) {
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[KvPut] key = %s\n", key)));
#endif

    if (c == NULL) {
        initKvStore();
    }

    redisReply *reply;

    //value[valueLen] = 0;
    reply = redisCommand(c,"SET %s %b", key, value, valueLen);
    if (c->err != 0) {
        printf("[KvPut] error = %d, err_msg = %s\n", c->err, c->errstr);
        return c->err;
    }

    freeReplyObject(reply);
    return 0;
}


// returned_value should be freed by caller function.
int KvGet(char *key, char **value) {
    if (c == NULL) {
        initKvStore();
    }
    redisReply *reply;
    reply = redisCommand(c,"GET %s", key);
    if (reply->str == NULL) {
#ifdef DEBUG_INFO
        ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[KvGet] key = %s, doesn't exist\n", key)));
#endif

        *value = NULL;
        return 0;
    }
    if (c->err != 0) {
        printf("[KvGet] error = %d, err_msg = %s\n", c->err, c->errstr);
        return c->err;
    }
#ifdef DEBUG_INFO
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[KvGet] key = %s, returned value\n", key)));
#endif
    //*value = (char *) malloc(sizeof(char) * reply->len);
    //printf("error = %d %s %d\n", c->err, c->errstr, reply->str==NULL);
    for (int i = 0; i < reply->len; i++) {
        (*value)[i] = reply->str[i];
    }
    freeReplyObject(reply);

    return 0;
}

void KvClose() {
    if (c != NULL) {
        redisFree(c);
        c = NULL;
    }
    return;
}


void MarshalIntList(int* numList, int size, char **p) {
    *p = malloc(sizeof(int) * (size+1) +1);
    int* pInt = (int*)*p;
    pInt[0] = size;
    for(int i = 0; i < size; i++) {
        pInt[i+1] = numList[i];
    }
    p[sizeof(int) * (size+1)] = 0;
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


int UnmarshalUnsignedLongListGetSize(char *p) {
    unsigned long*pLong = (unsigned long*)p;
    return (int)pLong[0];
}

unsigned long * UnmarshalUnsignedLongListGetList (char *p) {
    unsigned long*pLong = (unsigned long*)p;
    return &pLong[1];
}


void MarshalUnsignedLongList(const unsigned long *numList, int size, char **p) {
    *p = malloc(sizeof(unsigned long) * (size+1) +1);
    unsigned long* pInt = (unsigned long*)*p;
    pInt[0] = size;
    for(int i = 0; i < size; i++) {
        pInt[i+1] = numList[i];
    }
    p[sizeof(unsigned long) * (size+1)] = 0;
}


// Here the numList should be a Dynamic space
// Pay attention, here is kvList, the first elem is ListLen
int AddLargestElem2OrderedKVList(unsigned long **kvList, unsigned long newEle) {
    unsigned long listSize = (*kvList)[0];
    if((*kvList)[listSize] >= newEle) {
        // insert failed, since it's not bigger than last elem.
        return 1;
    }
    // expand the space, add one unsigned long elem space
    *kvList = realloc(*kvList, sizeof(unsigned long) * (listSize+2));
    (*kvList)[0] = listSize+1;
    (*kvList)[listSize+1] = newEle;
    return 0;
}

// if all list elements are bigger than target, return -1
int FindLowerBound_UnsignedLong(const unsigned long *list, int listSize, unsigned long target) {
    if (listSize == 0) {
        return  -1;
    }

    int theEnd = listSize-1;
    int theStart = 0;

    int theMid;

    while (theStart < theEnd) {
        theMid = (theEnd+theStart+1) /2;
        if (list[theMid] == target)
            return theMid;
        else if (list[theMid] < target)
            theStart = theMid;
        else
            theEnd = theMid-1;
    }

    if(list[theStart] > target) {
        return -1;
    }

    return theStart;
}


// if the key doesn't exist, return 1
// else return 0
int KvGetInt(char *key, int *result) {
    char *value = (char *) malloc(sizeof(int));
    KvGet(key, &value);
    if (value == NULL) {
        return -1;
    }
    *result = *(int *)value;
    free(value);
    return 0;
}

int KvPutInt(char *key, int value) {
    int *p = malloc(sizeof(int));
    p[0] = value;
    KvPut(key, (char*)p, sizeof(int));
    free(p);
    return 0;
}

int KvDelete(char *key) {
    if(c == NULL) {
        initKvStore();
    }

    redisReply *reply;

    reply = redisCommand(c,"DEL %s", key);
    freeReplyObject(reply);

    return 0;
}



#endif
