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
#include <sys/sem.h>
#include <sys/shm.h>
#include <errno.h>

#define USE_ROCKS_KV

#define DEBUG_INFO
#ifdef USE_ROCKS_KV
#include "rocksdb/c.h"

/***************** DEFINITION of Semaphore ***************/
#define OBJ_FLAGES (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP)
union semun {
    int              val;    /* Value for SETVAL */
    struct semid_ds *buf;    /* Buffer for IPC_STAT, IPC_SET */
    unsigned short  *array;  /* Array for GETALL, SETALL */
    struct seminfo  *__buf;  /* Buffer for IPC_INFO
                                           (Linux-specific) */
};

/* return valid parameter operation number
 * For example,
 *          value1 = 0, value2 = 1, value3 = -1
 *          then, valid parameter is 2 (value2 && value3)
 */
int prepare3SemunParam(struct sembuf *sembufList, int value1, int noWait1, int value2, int noWait2, int value3, int noWait3) {
    if (sembufList == NULL) {
        return 0;
    }

    int validParamNum = 0;

    if(value1 != 0) {
        sembufList[validParamNum].sem_num = 0;
        sembufList[validParamNum].sem_op = value1;
        sembufList[validParamNum].sem_flg = (noWait1 == 1) ? IPC_NOWAIT: (short)0;
        validParamNum++;
    }

    if(value2 != 0) {
        sembufList[validParamNum].sem_num = 1;
        sembufList[validParamNum].sem_op = value2;
        sembufList[validParamNum].sem_flg = (noWait2 == 1) ? IPC_NOWAIT: (short)0;
        validParamNum++;
    }

    if(value3 != 0) {
        sembufList[validParamNum].sem_num = 2;
        sembufList[validParamNum].sem_op = value3;
        sembufList[validParamNum].sem_flg = (noWait3 == 1) ? IPC_NOWAIT: (short)0;
        validParamNum++;
    }

    return validParamNum;
}

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
char*   shemPtr[2*DIFF_PROC_NUM];
int semIdList[DIFF_PROC_NUM];

bool InitializedShem = false;
int ProcNoForRocksdb = -1;
/*******************************************************/




rocksdb_t *db = NULL;


char KvStorePath[MAXPGPATH];

int shmId;
void *shmp;
void initShem() {
//    printf("initShem Start, pid = %d\n", getpid());
    if(InitializedShem == true)
        return;
    InitializedShem = true;

// Initialize semaphore
    for(int i = 0; i < DIFF_PROC_NUM; i++) {
        semIdList[i] = semget(IPC_PRIVATE, 3, IPC_CREAT|OBJ_FLAGES);
        if (semIdList[i] != -1) { // successful create semaphore
            printf("create semaphore successfully\n");
            union semun arg;
            arg.array = calloc(3, sizeof(arg.array[0]));
            if(arg.array == NULL) {
                printf("calloc failed !\n");
            }
            arg.array[0] = 1;

            if(errno == EEXIST){}
            if (semctl(semIdList[i], 0, SETALL, arg) == -1) {
                printf("set initial value of semaphore failed\n");
                free(arg.array);
                exit(-1);
            } else {
                printf("Initialize semaphore successfully\n");
                free(arg.array);
            }
        } else {
            printf("create semaphore failed\n");
        }
    }


    shmId = shmget(IPC_PRIVATE, SHARED_BLOCK_SIZE*2*DIFF_PROC_NUM, IPC_CREAT|S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);
    shmp = shmat(shmId, NULL, 0);

    // Here initialize the shared memory for exchanging the data
    for(int i = 0; i < 2*DIFF_PROC_NUM; i++) {
//        shemPtr[i] = (void*) mmap(NULL, SHARED_BLOCK_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd3, i*SHARED_BLOCK_SIZE);
        shemPtr[i] = ((char*)shmp) + i*SHARED_BLOCK_SIZE;
    }

}

void closeShem() {
//    printf("Get in closeShem, pid = %d\n", getpid());
    if(InitializedShem == false) {
        return;
    }
    InitializedShem = false;

    // detach semaphore
    for (int i = 0; i < DIFF_PROC_NUM; i++) {
        if (semctl(semIdList[i], 3, IPC_RMID) == -1) {
            printf("Delete semaphore failed\n");
        } else {
            printf("Delete semaphore succeed\n");
        }
    }

    // detach shared memory
    shmdt(shmp);
    shmctl(shmId, IPC_RMID, 0);
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
    // todo
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
    sleep(3);
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
    struct sembuf sembufList[3];
    int validParamNum = prepare3SemunParam(sembufList, 0, 0, 0, 0, 1, 0);
    if(semop(semIdList[proc_num], sembufList, validParamNum) == -1) {
        printf("[KvPut_kernel] sem_post failed\n");
        return;
    }

}
int KvPut(char *key, char *value, int valueLen) {
    initShem();

    KvRequest kvReq;
    kvReq.RequestType = 1;
    kvReq.Key1Len = (int) strlen(key);
    kvReq.Key2Len = valueLen;

    int validParamList;
    struct sembuf sembufList[3];

    validParamList = prepare3SemunParam(sembufList, -1, 0, 0, 0, 0, 0);
    if (semop(semIdList[ProcNoForRocksdb], sembufList, validParamList) == -1) {
        printf("[KvPut] semaphore wait value1 failed\n");
    }

//    sem_wait(mySem[4*ProcNoForRocksdb]);
    memcpy(shemPtr[2 * ProcNoForRocksdb], (char *) &kvReq, sizeof(KvRequest));
    memcpy(shemPtr[2 * ProcNoForRocksdb] + sizeof(KvRequest), key, kvReq.Key1Len);
    memcpy(shemPtr[2 * ProcNoForRocksdb] + sizeof(KvRequest) + kvReq.Key1Len, value, valueLen);
//    sem_post(mySem[1+4*ProcNoForRocksdb]);
    validParamList = prepare3SemunParam(sembufList, 0, 0, 1, 0, 0, 0);
    if (semop(semIdList[ProcNoForRocksdb], sembufList, validParamList) == -1) {
        printf("[KvPut] semaphore post value2 failed\n");
    }

    // the request has been sent
    // now, start receive response
//    sem_wait(mySem[2+4*ProcNoForRocksdb]);
    validParamList = prepare3SemunParam(sembufList, 0, 0, 0, 0, -1, 0);
    if (semop(semIdList[ProcNoForRocksdb], sembufList, validParamList) == -1) {
        printf("[KvPut] semaphore wait value3 failed\n");
    }
    KvResponse *kvResp = (KvResponse *) shemPtr[1 + 2 * ProcNoForRocksdb];
    int status = kvResp->status;


    validParamList = prepare3SemunParam(sembufList, 1, 0, 0, 0, 0, 0);
    if (semop(semIdList[ProcNoForRocksdb], sembufList, validParamList) == -1) {
        printf("[KvPut] semaphore post value1 failed\n");
    }

    return status;
}

int KvGet(char *key, char **value) {

//    printf("[KvGet] Start pid=%ld  ppid=%ld\n", (long)getpid(), (long)getppid());
    initShem();

    KvRequest kvReq;
    kvReq.RequestType = 0;
    kvReq.Key1Len = (int)strlen(key);
    kvReq.Key2Len = 0;

    struct sembuf sembufList[3];
    int validParamNum;

    validParamNum = prepare3SemunParam(sembufList, -1, 0, 0, 0, 0, 0);
    if(semop(semIdList[ProcNoForRocksdb], sembufList, validParamNum) == -1) {
        printf("[KvGet] semaphore wait value1 failed\n");
    }

    memcpy(shemPtr[2*ProcNoForRocksdb], (char *)&kvReq, sizeof(KvRequest));
    memcpy(shemPtr[2*ProcNoForRocksdb]+sizeof(KvRequest), key, kvReq.Key1Len);

    validParamNum = prepare3SemunParam(sembufList, 0, 0, 1, 0, 0, 0);
    if(semop(semIdList[ProcNoForRocksdb], sembufList, validParamNum) == -1) {
        printf("[KvGet] semaphore post value2 failed\n");
    }

    // the request has been sent
    // now, start receive response
    validParamNum = prepare3SemunParam(sembufList, 0, 0, 0, 0, -1, 0);
    if(semop(semIdList[ProcNoForRocksdb], sembufList, validParamNum) == -1) {
        printf("[KvGet] semaphore wait value3 failed\n");
    }
    KvResponse *kvResp = (KvResponse*)shemPtr[1+2*ProcNoForRocksdb];
    int status = kvResp->status;
    if (kvResp->msgLen == 0) {
        *value = NULL;
    } else {
        memcpy(*value, shemPtr[1+2*ProcNoForRocksdb]+sizeof(KvResponse), kvResp->msgLen);
        (*value)[kvResp->msgLen] = '\0';
    }
    validParamNum = prepare3SemunParam(sembufList, 1, 0, 0, 0, 0, 0);
    if(semop(semIdList[ProcNoForRocksdb], sembufList, validParamNum) == -1) {
        printf("[KvGet] semaphore post value1 failed\n");
    }
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

    struct sembuf sembufList[3];
    int validParamNum = prepare3SemunParam(sembufList, 0, 0, 0, 0, 1, 0);
    if(semop(semIdList[proc_num], sembufList, validParamNum) == -1) {
        printf("[KvGet_kernel] sem_post failed\n");
        return;
    }
}


void KvClose() {
    printf("Start KvClose, pid = %d\n", getpid());
    ereport(NOTICE,
            (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("[KvClose] Try, Start Close\n\n\n")));
    if (db != NULL) {
        printf("KvClose really close, pid = %d\n", getpid());
        rocksdb_close(db);
        db = NULL;
        ereport(NOTICE,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("[KvClose] Start Close, success\n\n\n")));
        closeShem();
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
    struct sembuf sembufList[3];
    int validParamNum;

    validParamNum = prepare3SemunParam(sembufList, -1, 0, 0, 0, 0, 0);
    if(semop(semIdList[ProcNoForRocksdb], sembufList, validParamNum) == -1) {
        printf("[KvDelete] semaphore wait value1 failed\n");
    }
    memcpy(shemPtr[2*ProcNoForRocksdb], (char *)&kvReq, sizeof(KvRequest));
    memcpy(shemPtr[2*ProcNoForRocksdb]+sizeof(KvRequest), key, kvReq.Key1Len);
    validParamNum = prepare3SemunParam(sembufList, 0, 0, 1, 0, 0, 0);
    if(semop(semIdList[ProcNoForRocksdb], sembufList, validParamNum) == -1) {
        printf("[KvDelete] semaphore post value2 failed\n");
    }

    // the request has been sent
    // now, start receive response
    validParamNum = prepare3SemunParam(sembufList, 0, 0, 0, 0, -1, 0);
    if(semop(semIdList[ProcNoForRocksdb], sembufList, validParamNum) == -1) {
        printf("[KvDelete] semaphore wait value3 failed\n");
    }
    KvResponse *kvResp = (KvResponse*)shemPtr[2*ProcNoForRocksdb+1];
    int status = kvResp->status;
    validParamNum = prepare3SemunParam(sembufList, 1, 0, 0, 0, 0, 0);
    if(semop(semIdList[ProcNoForRocksdb], sembufList, validParamNum) == -1) {
        printf("[KvDelete] semaphore post value1 failed\n");
    }

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

    struct sembuf sembufList[3];
    int validParamNum = prepare3SemunParam(sembufList, 0, 0, 0, 0, 1, 0);
    if(semop(semIdList[proc_num], sembufList, validParamNum) == -1) {
        printf("[KvDelete_kernel] sem_post failed\n");
        return;
    }
}

void KvPrefixCopyDir(char* srcPath, char* dstPath, const char* prefixKey) {
//    printf("[KvCopy] Start\n");

    initShem();

    KvRequest kvReq;
    kvReq.RequestType = 3;
    kvReq.Key1Len = (int) strlen(srcPath);
    kvReq.Key2Len = (int) strlen(dstPath);
    kvReq.Key3Len = (int) strlen(prefixKey);

    struct sembuf sembufList[3];
    int validParamNum;

    validParamNum = prepare3SemunParam(sembufList, -1, 0, 0, 0, 0, 0);
    if(semop(semIdList[ProcNoForRocksdb], sembufList, validParamNum) == -1) {
        printf("[KvPrefixCopyDir] semaphore wait value1 failed\n");
    }

    memcpy(shemPtr[2*ProcNoForRocksdb], (char *)&kvReq, sizeof(KvRequest));
    memcpy(shemPtr[2*ProcNoForRocksdb]+sizeof(KvRequest),
           srcPath, kvReq.Key1Len);
    memcpy(shemPtr[2*ProcNoForRocksdb]+sizeof(KvRequest)+kvReq.Key1Len,
           dstPath, kvReq.Key2Len);
    memcpy(shemPtr[2*ProcNoForRocksdb]+sizeof(KvRequest)+kvReq.Key1Len+kvReq.Key2Len,
           prefixKey ,kvReq.Key3Len);

    validParamNum = prepare3SemunParam(sembufList, 0, 0, 1, 0, 0, 0);
    if(semop(semIdList[ProcNoForRocksdb], sembufList, validParamNum) == -1) {
        printf("[KvPrefixCopyDir] semaphore post value2 failed\n");
    }

    // the request has been sent
    // now, start receive response
    validParamNum = prepare3SemunParam(sembufList, 0, 0, 0, 0, -1, 0);
    if(semop(semIdList[ProcNoForRocksdb], sembufList, validParamNum) == -1) {
        printf("[KvPrefixCopyDir] semaphore wait value3 failed\n");
    }

    KvResponse *kvResp = (KvResponse*)shemPtr[1+2*ProcNoForRocksdb];
    int status = kvResp->status;

    validParamNum = prepare3SemunParam(sembufList, 1, 0, 0, 0, 0, 0);
    if(semop(semIdList[ProcNoForRocksdb], sembufList, validParamNum) == -1) {
        printf("[KvPrefixCopyDir] semaphore post value1 failed\n");
    }
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
    struct sembuf sembufList[3];
    int validParamNum = prepare3SemunParam(sembufList, 0, 0, 0, 0, 1, 0);
    if(semop(semIdList[proc_num], sembufList, validParamNum) == -1) {
        printf("[KvCopyDir_kernel] sem_post failed\n");
        return;
    }
}

void sigTermHandlerSelf(int sig) {
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
        printf("Rocksdb process pid = %d\n", getpid());
        int r = prctl(PR_SET_PDEATHSIG, SIGTERM);
        if (r == -1) {
            printf("Prctl failed\n");
            exit(1);
        }
        if (getppid() != pid_before_fork) {
            printf("pid not match\n");
            exit(1);
        }

        atexit(KvClose);
        // Catch the TERM signal
//        struct sigaction catchTermSig;
//        struct sigaction oldSigAction;
//        catchTermSig.sa_handler = sigTermHandlerSelf;
//        int setTermSigSucc = sigaction(SIGTERM, &catchTermSig, &oldSigAction);
//        if(setTermSigSucc == -1) {
//            printf("Set signal action failed \n");
//        }


        while (1) {
            struct sembuf semops[3];
            int validParamNum = prepare3SemunParam(semops, 0, 0, -1, 1, 0, 0);

            int count = 0;
            for (int i = 0; i < DIFF_PROC_NUM; i++) {
//                printf("server process try %d\n", i);
                if(semop(semIdList[i], semops, validParamNum) == -1) {
//                        printf("[no_wait] can not get immediately\n");
                    if (count++ %100 == 0 ){
                        count = 0;
                        if(getppid() != pid_before_fork) {
                            KvClose();
                            exit(0);
                        }
                    }
                    continue;
                }
                count = 0;

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
    printf("[KvGet] start rediscommand\n");
    reply = redisCommand(c,"GET %s", key);
    printf("[KvGet] end rediscommand\n");
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
    printf("reply->len=%d\n", reply->len);
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


