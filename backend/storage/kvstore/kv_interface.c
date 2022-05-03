#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "storage/kvstore.h"
#include "pg_config_manual.h"
#include "stdlib.h"
#include <unistd.h>  // sysconf() - get CPU count
#include <zconf.h>
#include "c.h"
#include "miscadmin.h"
#include "postgres.h"
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/prctl.h>
#include <semaphore.h>

#define USE_ROCKS_KV

#ifdef USE_ROCKS_KV
#include "rocksdb/c.h"


/**************** DEFINITION of KvMessage ****************/
#define SHARED_BLOCK_SIZE 12288

struct KvResponse {
    int status;
    int msgLen;
};
typedef struct KvResponse KvResponse;

struct KvRequest {
    int RequestType; // 0:Get, 1:Put, 2:Delete, 3:Copy
    int Key1Len;
    int Key2Len;
    int Key3Len;
};
typedef struct KvRequest KvRequest;

KvResponse  ResponseMsg;
KvRequest   RequestMsg;

#define DIFF_PROC_NUM 5
sem_t* mySem[4*DIFF_PROC_NUM];
char*   shemPtr[2*DIFF_PROC_NUM];
bool InitializedShem = false;
int ProcNoForRocksdb = -1;
/*******************************************************/




rocksdb_t *db = NULL;


char KvStorePath[MAXPGPATH];

void initShem() {
//    printf("initShem Start, pid = %d\n", getpid());
    if(InitializedShem == true)
        return;
    InitializedShem = true;



    int fd1, fd3;
    int flag;

    // Here create the semaphore shared memory
    fd1 = open("./sem1", O_CREAT|O_RDWR|O_TRUNC, 0666);
    ftruncate(fd1, 8192*4*DIFF_PROC_NUM);
//    char pdd[256];
//    getcwd(pdd, 256);
//    printf("StartRocksDbWriteProcess cwd=%s\n", pdd);
//    return;
    for(int i = 0; i < 4*DIFF_PROC_NUM; i++) {
        mySem[i] = (sem_t*) mmap(NULL, sizeof(sem_t), PROT_READ|PROT_WRITE, MAP_SHARED, fd1, i*8192);
        if (i == 0) {
            flag = sem_init(mySem[i], 1, 1);
        } else {
            flag = sem_init(mySem[i], 1, 0);
        }
        if (flag != 0) {
            printf("Initialize semaphore 1 failed\n");
        }
    }
    // Here initialize the shared memory for exchanging the data
    fd3 = open("./share_memory", O_CREAT|O_RDWR|O_TRUNC, 0666);
    ftruncate(fd3, SHARED_BLOCK_SIZE*2*DIFF_PROC_NUM);
    for(int i = 0; i < 2*DIFF_PROC_NUM; i++) {
        shemPtr[i] = (void*) mmap(NULL, SHARED_BLOCK_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd3, i*SHARED_BLOCK_SIZE);
    }

}

void closeShem() {
//    printf("Get in closeShem, pid = %d\n", getpid());
    if(InitializedShem == false) {
        return;
    }
    InitializedShem = false;
    for(int i = 0; i < 4*DIFF_PROC_NUM; i++) {
        sem_destroy(mySem[i]);
        munmap(mySem[i], sizeof(sem_t));
    }
    for(int i = 0; i < 2*DIFF_PROC_NUM; i++) {
        munmap(shemPtr[i], SHARED_BLOCK_SIZE);
    }
//    printf("Out closeShem\n");
}


void InitKvStore() {
    if (db != NULL) {
        return;
    }

//    printf("Start InitKvStore, pid = %d\n", getpid());
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[InitKvStore] Start\n\n\n")));

//    initShem();
//    atexit(KvClose);
    snprintf(KvStorePath, sizeof(KvStorePath), "%s/rocksdb_test_db", DataDir);

    rocksdb_options_t *options = rocksdb_options_create();
//    int max_file_num = rocksdb_options_get_max_open_files(options);
//    ereport(NOTICE,
//            (errcode(ERRCODE_INTERNAL_ERROR),
//                    errmsg("MAX FILE NUM = %d\n", max_file_num)));
//
//    sleep(15);
    rocksdb_options_set_max_open_files(options, 512);
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
//    sleep(3);
    return;
}


void KvPut_kernel(int proc_num) {
//    sem_wait(mySem[1]);

    KvRequest *kvReq = (KvRequest*)shemPtr[2*proc_num];
    if(db == NULL) {
        InitKvStore();
    }
    char * err = NULL;
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_put(db, writeoptions, shemPtr[2*proc_num]+sizeof(KvRequest), kvReq->Key1Len, shemPtr[2*proc_num]+sizeof(KvRequest)+kvReq->Key1Len, kvReq->Key2Len,
                &err);
    rocksdb_writeoptions_destroy(writeoptions);


    // til here, data saved to the disk.
    // Now respond to interface

    KvResponse kvResp;
    if (err == 0) {
        kvResp.status = 0;
    } else {
        kvResp.status = 1;
        printf("[KvPut_kernel] put operation failed, error = %s\n", err);
    }
    memcpy(shemPtr[1+2*proc_num], (char *)&kvResp, sizeof(KvResponse));
    sem_post(mySem[2+4*proc_num]);

}

int KvPut(char *key, char *value, int valueLen) {
//    printf("[KvPut] Start\n");
    initShem();

    KvRequest kvReq;
    kvReq.RequestType = 1;
    kvReq.Key1Len = (int)strlen(key);
    kvReq.Key2Len = valueLen;

    sem_wait(mySem[4*ProcNoForRocksdb]);
    memcpy(shemPtr[2*ProcNoForRocksdb], (char *)&kvReq, sizeof(KvRequest));
    memcpy(shemPtr[2*ProcNoForRocksdb]+sizeof(KvRequest), key, kvReq.Key1Len);
    memcpy(shemPtr[2*ProcNoForRocksdb]+sizeof(KvRequest)+kvReq.Key1Len, value, valueLen);
    sem_post(mySem[1+4*ProcNoForRocksdb]);

    // the request has been sent
    // now, start receive response
    sem_wait(mySem[2+4*ProcNoForRocksdb]);
    KvResponse *kvResp = (KvResponse*)shemPtr[1+2*ProcNoForRocksdb];
    int status = kvResp->status;
    sem_post(mySem[0+4*ProcNoForRocksdb]);

    return status;
}

int KvGet(char *key, char **value) {

//    printf("[KvGet] Start pid=%ld  ppid=%ld\n", (long)getpid(), (long)getppid());
    initShem();

    KvRequest kvReq;
    kvReq.RequestType = 0;
    kvReq.Key1Len = (int)strlen(key);
    kvReq.Key2Len = 0;

    sem_wait(mySem[4*ProcNoForRocksdb]);
    memcpy(shemPtr[2*ProcNoForRocksdb], (char *)&kvReq, sizeof(KvRequest));
    memcpy(shemPtr[2*ProcNoForRocksdb]+sizeof(KvRequest), key, kvReq.Key1Len);
    sem_post(mySem[1+4*ProcNoForRocksdb]);

    // the request has been sent
    // now, start receive response
    sem_wait(mySem[2+4*ProcNoForRocksdb]);
    KvResponse *kvResp = (KvResponse*)shemPtr[1+2*ProcNoForRocksdb];
    int status = kvResp->status;
    if (kvResp->msgLen == 0) {
        *value = NULL;
    } else {
        memcpy(*value, shemPtr[1+2*ProcNoForRocksdb]+sizeof(KvResponse), kvResp->msgLen);
        (*value)[kvResp->msgLen] = '\0';
    }
    sem_post(mySem[0+4*ProcNoForRocksdb]);
//    printf("[KvGet] End\n");
    return status;
}

void KvGet_kernel(int proc_num){
//    sem_wait(mySem[1]);
    if(db == NULL) {
        InitKvStore();
    }

    KvRequest *kvReq = (KvRequest *)shemPtr[2*proc_num];

    char *err = NULL;
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    size_t len;
//    printf("key = %.*s; len = %d;\n", kvReq->Key1Len, shemPtr[2*proc_num]+sizeof(KvRequest), kvReq->Key1Len);

    //! todo, why add sleep can fix??????
//    sleep(1);
    char *value =
            rocksdb_get(db, readoptions, shemPtr[2*proc_num]+sizeof(KvRequest), kvReq->Key1Len, &len, &err);
    rocksdb_readoptions_destroy(readoptions);

    // til here, data saved to the disk.
    // Now respond to interface


    KvResponse kvResp;
    if (err == NULL) {
        kvResp.status = 0;
    } else {
        printf("[KvGet_kernel] failed, error = %s\n", err);
        free(err);
        kvResp.status = 1;
    }
    kvResp.msgLen = (int)len;

    memcpy(shemPtr[1+2*proc_num], (char *)&kvResp, sizeof(KvResponse));
    memcpy(shemPtr[1+2*proc_num]+sizeof(KvResponse), value, len);
    free(value);
    sem_post(mySem[2+4*proc_num]);
}


void KvClose() {
//    printf("Start KvClose, pid = %d\n", getpid());
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[KvClose] Start Close\n\n\n")));
    if (db != NULL) {
//        printf("KvClose really close, pid = %d\n", getpid());
        rocksdb_close(db);
        db = NULL;
        ereport(NOTICE,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("[KvClose] Start Close, success\n\n\n")));
    }

//    closeShem();
    return;
}
int KvDelete(char *key) {

//    printf("[KvDelete] Start\n");
    initShem();

    KvRequest kvReq;
    kvReq.RequestType = 2;
    kvReq.Key1Len = (int)strlen(key);
    kvReq.Key2Len = 0;

// mySem = 2*ProcNoForRocksdb
// shemPtr = 4*ProcNoForRocksdb
    sem_wait(mySem[4*ProcNoForRocksdb]);
    memcpy(shemPtr[2*ProcNoForRocksdb], (char *)&kvReq, sizeof(KvRequest));
    memcpy(shemPtr[2*ProcNoForRocksdb]+sizeof(KvRequest), key, kvReq.Key1Len);
    sem_post(mySem[4*ProcNoForRocksdb+1]);

    // the request has been sent
    // now, start receive response
    sem_wait(mySem[4*ProcNoForRocksdb+2]);
    KvResponse *kvResp = (KvResponse*)shemPtr[2*ProcNoForRocksdb+1];
    int status = kvResp->status;
    sem_post(mySem[4*ProcNoForRocksdb]);

    return status;
}

void KvDelete_kernel(int proc_num) {
//    sem_wait(mySem[1]);

    if(db == NULL) {
        InitKvStore();
    }

    KvRequest *kvReq = (KvRequest*)shemPtr[2*proc_num];

    char * err = NULL;
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_delete(db, writeoptions, shemPtr[2*proc_num]+sizeof(KvRequest), kvReq->Key1Len, &err);
    rocksdb_writeoptions_destroy(writeoptions);

    // til here, data saved to the disk.
    // Now respond to interface

    KvResponse kvResp;
    if (err == NULL) {
        kvResp.status = 0;
    } else {
//        printf("[KvGet_kernel] failed, error = %s\n", err);
//        free(err);
        kvResp.status = 1;
    }
    kvResp.msgLen = 0;
    memcpy(shemPtr[1+2*proc_num], (char *)&kvResp, sizeof(KvResponse));
    sem_post(mySem[2+4*proc_num]);
}

void KvPrefixCopyDir(char* srcPath, char* dstPath, const char* prefixKey) {
//    printf("[KvCopy] Start\n");

    initShem();

    KvRequest kvReq;
    kvReq.RequestType = 3;
    kvReq.Key1Len = (int) strlen(srcPath);
    kvReq.Key2Len = (int) strlen(dstPath);
    kvReq.Key3Len = (int) strlen(prefixKey);

    sem_wait(mySem[4*ProcNoForRocksdb]);
    memcpy(shemPtr[2*ProcNoForRocksdb], (char *)&kvReq, sizeof(KvRequest));
    memcpy(shemPtr[2*ProcNoForRocksdb]+sizeof(KvRequest),
           srcPath, kvReq.Key1Len);
    memcpy(shemPtr[2*ProcNoForRocksdb]+sizeof(KvRequest)+kvReq.Key1Len,
           dstPath, kvReq.Key2Len);
    memcpy(shemPtr[2*ProcNoForRocksdb]+sizeof(KvRequest)+kvReq.Key1Len+kvReq.Key2Len,
           prefixKey ,kvReq.Key3Len);
    sem_post(mySem[1+4*ProcNoForRocksdb]);

    // the request has been sent
    // now, start receive response
    sem_wait(mySem[2+4*ProcNoForRocksdb]);
    KvResponse *kvResp = (KvResponse*)shemPtr[1+2*ProcNoForRocksdb];
    int status = kvResp->status;
    sem_post(mySem[4*ProcNoForRocksdb]);

}

void KvPrefixCopyDir_kernel(int proc_num) {
    if(db == NULL) {
        InitKvStore();
    }

//    printf("Get in\n");
    KvRequest *kvReq = (KvRequest*)shemPtr[2*proc_num];
    char *srcPath = shemPtr[2*proc_num]+sizeof(KvRequest);
    char *dstPath = shemPtr[2*proc_num]+sizeof(KvRequest)+kvReq->Key1Len;
    char *prefixKey = shemPtr[2*proc_num]+sizeof(KvRequest)+kvReq->Key1Len+kvReq->Key2Len;

    int srcPathLen = kvReq->Key1Len;
    int dstPathLen = kvReq->Key2Len;
    int prefixLen = kvReq->Key3Len;
//    printf("%.*s   ,  %.*s   , %.*s\n",srcPathLen, srcPath, dstPathLen, dstPath, prefixLen, prefixKey);

    char parseKey[1024];
    snprintf(parseKey, sizeof(parseKey),"%.*s%.*s/", prefixLen, prefixKey, srcPathLen, srcPath);
//    int parseKeyLen = (int)strlen(parseKey);
    int parseKeyLen = prefixLen+srcPathLen+1;
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    rocksdb_iterator_t* iter = rocksdb_create_iterator(db, readoptions);
    rocksdb_iter_seek_to_first(iter);
    if (!rocksdb_iter_valid(iter)) {
        printf("iterator is invalid\n");
        rocksdb_readoptions_destroy(readoptions);
        rocksdb_iter_destroy(iter);
        KvClose();
        return;
    }

    char newKey[1024];
    char * err = NULL;
    rocksdb_writeoptions_t * wrtOpt = rocksdb_writeoptions_create();

    while(rocksdb_iter_valid(iter)) {
        size_t kLen=0, vLen=0;
        const char *key;
        const char *value;
        key = rocksdb_iter_key(iter, &kLen);
        value = rocksdb_iter_value(iter, &vLen);
        if (strncmp(parseKey, key, parseKeyLen) != 0) {
            rocksdb_iter_next(iter);
            continue;
        }
        size_t newKeyLen = kLen+dstPathLen-srcPathLen;
        snprintf(newKey, sizeof(newKey), "%.*s%.*s%s",prefixLen, prefixKey, dstPathLen, dstPath, key+prefixLen+srcPathLen);
        rocksdb_put(db, wrtOpt, newKey, newKeyLen, value, vLen, &err);
        if(err != NULL){
            printf("Rocksdb Put Operation Failed, err = %s\n", err);
        }
//        printf("original key = %.*s, new key = %.*s, originalKeyLen = %d, newKeyLen = %d\n",
//               kLen, key , newKeyLen, newKey, kLen, strlen(newKey));
//        printf("valueLen = %d, value = %.*s\n", vLen, vLen, value);

        rocksdb_iter_next(iter);
    }

    rocksdb_writeoptions_destroy(wrtOpt);
    rocksdb_iter_destroy(iter);
    rocksdb_readoptions_destroy(readoptions);

    /********************* KvCopy Completed, Start Reply ************************/

    KvResponse kvResp;
    if (err == NULL) {
        kvResp.status = 0;
    } else {
        kvResp.status = 1;
    }
    kvResp.msgLen = 0;
    memcpy(shemPtr[1+2*proc_num], (char *)&kvResp, sizeof(KvResponse));
    sem_post(mySem[2+4*proc_num]);
}

void sigTermHandler(int sig) {
//    printf("Term signal is caught, pid = %d\n", getpid());
    KvClose();
    exit(0);
}

void StartRocksDbWriteProcess() {

//    printf("[StartRocksDbWriteProcess] Start, pid = %d\n", getpid());
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("StartRocksDbWriteProcess!!!!!!!!\n\n\n")));
    // There is another rocksdbProcess
    if(ProcNoForRocksdb != -1) {
        return;
    }
//    printf("[StartRocksDbWriteProcess] really Start, pid = %d\n", getpid());
    ProcNoForRocksdb = 0;

    initShem();

    pid_t pid_before_fork = getpid();
    pid_t pid = fork();
    if (pid == 0) { // child
//        printf("Rocksdb process pid = %d\n", getpid());
        int r = prctl(PR_SET_PDEATHSIG, SIGTERM);
        if (r == -1) {
            printf("Prctl failed\n");
            exit(1);
        }
        if (getppid() != pid_before_fork) {
            printf("pid not match\n");
            exit(1);
        }

        // Catch the TERM signal
        struct sigaction catchTermSig;
        struct sigaction oldSigAction;
        catchTermSig.sa_handler = sigTermHandler;
        int setTermSigSucc = sigaction(SIGTERM, &catchTermSig, &oldSigAction);
        if(setTermSigSucc == -1) {
            printf("Set signal action failed \n");
        }


        while (1) {
            for (int i = 0; i < DIFF_PROC_NUM; i++) {
//                printf("server process try %d\n", i);
                if (sem_trywait(mySem[1 + 4 * i]) != 0) {
                    continue;
                }

                KvRequest *kvReq = (KvRequest *) shemPtr[2 * i];
                if (kvReq->RequestType == 0) {
                    KvGet_kernel(i);
                } else if (kvReq->RequestType == 1) {
                    KvPut_kernel(i);
                } else if (kvReq->RequestType == 2) {
                    KvDelete_kernel(i);
                } else if (kvReq->RequestType == 3) {
                    KvPrefixCopyDir_kernel(i);
                }
            }
        }
    }
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
        *value = NULL;
        return 0;
    }

    if (c->err != 0) {
        printf("[KvGet] error = %d, err_msg = %s\n", c->err, c->errstr);
        return c->err;
    }

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

// if the key doesn't exist, return 1
// else return 0
int KvGetInt(char *key, int *result) {
    char *value = (char *) malloc(sizeof(int));
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
    KvPut(key, (char*)p, sizeof(int));
    free(p);
    return 0;
}


