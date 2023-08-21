//
// Created by pang65 on 6/21/22.
//

#include <signal.h>
#include <unistd.h>

#include "c.h"
#include "postgres.h"
#include "storage/pmsignal.h"
#include "storage/rpcserver.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "storage/rel_cache.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "signal.h"
#include "pgstat.h"
#include <pthread.h>
#include <sys/types.h>
#include "libpq/pqsignal.h"
#include "postmaster/fork_process.h"
#include "postmaster/startup.h"
#include "bootstrap/bootstrap.h"
#include "storage/sync.h"
#include "tcop/wal_redo.h"
#include "replication/walreceiver.h"
#include "storage/md.h"
#include "access/logindex_hashmap.h"
#include "storage/kv_interface.h"
#include "access/background_hashmap_vacuumer.h"
#include "access/wakeup_latch.h"

extern HashMap pageVersionHashMap;

extern XLogRecPtr *RpcXlblocks;
extern char* RpcXLogPages;
extern pthread_rwlock_t *RpcXLogPagesLocks;

void sigIntHandler(int sig) {
    printf("Start to clean up process\n");
    KvClose();
    proc_exit(0);
}

uint64_t RpcXLogFlushedLsn = 0;
int IsRpcServer = 0;
pid_t WalRcvPid = 0;
pid_t StartupPid = 0;

//#define ENABLE_DEBUG_INFO
//#define DEBUG_TIMING
#ifdef DEBUG_TIMING
struct timeval output_timing3;
int initialized3 = 0;

long SyncGetRelSizeTime = 0;
long SyncGetRelSizeCount = 0;
long ApplyOneLsnTime = 0;
long ApplyOneLsnCount = 0;
long GetBasePageTime = 0;
long GetBasePageCount = 0;
long GetPageByLsnTime = 0;
long GetPageByLsnCount = 0;
#endif

#define REPLAY_PROCESS_NUM 5
pthread_mutex_t replayProcessMutex[REPLAY_PROCESS_NUM];

static void
getInstallationPaths(const char *argv0)
{
    DIR		   *pdir;

    /* Locate the postgres executable itself */
    if (find_my_exec(argv0, my_exec_path) < 0)
        elog(FATAL, "%s: could not locate my own executable path", argv0);

#ifdef EXEC_BACKEND
    /* Locate executable backend before we change working directory */
	if (find_other_exec(argv0, "postgres", PG_BACKEND_VERSIONSTR,
						postgres_exec_path) < 0)
		ereport(FATAL,
				(errmsg("%s: could not locate matching postgres executable",
						argv0)));
#endif

    /*
     * Locate the pkglib directory --- this has to be set early in case we try
     * to load any modules from it in response to postgresql.conf entries.
     */
    get_pkglib_path(my_exec_path, pkglib_path);

    /*
     * Verify that there's a readable directory there; otherwise the Postgres
     * installation is incomplete or corrupt.  (A typical cause of this
     * failure is that the postgres executable has been moved or hardlinked to
     * some directory that's not a sibling of the installation lib/
     * directory.)
     */
    pdir = AllocateDir(pkglib_path);
    if (pdir == NULL)
        ereport(ERROR,
                (errcode_for_file_access(),
                        errmsg("could not open directory \"%s\": %m",
                               pkglib_path),
                        errhint("This may indicate an incomplete PostgreSQL installation, or that the file \"%s\" has been moved away from its proper location.",
                                my_exec_path)));
    FreeDir(pdir);

    /*
     * XXX is it worth similarly checking the share/ directory?  If the lib/
     * directory is there, then share/ probably is too.
     */
}

static pid_t
StartChildProcess(AuxProcType type)
{
    pid_t		pid;
    char	   *av[10];
    int			ac = 0;
    char		typebuf[32];

    /*
     * Set up command-line arguments for subprocess
     */
    av[ac++] = "postgres";

#ifdef EXEC_BACKEND
    av[ac++] = "--forkboot";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
#endif

    snprintf(typebuf, sizeof(typebuf), "-x%d", type);
    av[ac++] = typebuf;

    av[ac] = NULL;
    Assert(ac < lengthof(av));

#ifdef EXEC_BACKEND
    pid = postmaster_forkexec(ac, av);
#else							/* !EXEC_BACKEND */
    pid = fork_process();

    if (pid == 0)				/* child */
    {
        fflush(stdout);
        InitPostmasterChild();

        /* Close the postmaster's sockets */
        ClosePostmasterPorts(false);

        /* Release postmaster's working memory context */
        MemoryContextSwitchTo(TopMemoryContext);
        MemoryContextDelete(PostmasterContext);
        PostmasterContext = NULL;

        AuxiliaryProcessMain(ac, av);
        exit(0);
    }
#endif							/* EXEC_BACKEND */

    if (pid < 0)
    {
        /* in parent, fork failed */
        int			save_errno = errno;

        errno = save_errno;
        switch (type)
        {
            case StartupProcess:
                ereport(LOG,
                        (errmsg("could not fork startup process: %m")));
                break;
            case BgWriterProcess:
                ereport(LOG,
                        (errmsg("could not fork background writer process: %m")));
                break;
            case CheckpointerProcess:
                ereport(LOG,
                        (errmsg("could not fork checkpointer process: %m")));
                break;
            case WalWriterProcess:
                ereport(LOG,
                        (errmsg("could not fork WAL writer process: %m")));
                break;
            case WalReceiverProcess:
                ereport(LOG,
                        (errmsg("could not fork WAL receiver process: %m")));
                break;
            default:
                ereport(LOG,
                        (errmsg("could not fork process: %m")));
                break;
        }

        /*
         * fork failure is fatal during startup, but there's no need to choke
         * immediately if starting other child types fails.
         */
        if (type == StartupProcess)
            exit(1);
        return 0;
    }

    /*
     * in parent, successful fork
     */
    return pid;
}

static void
sigusr1_handler(SIGNAL_ARGS) {
#ifdef ENABLE_DEBUG_INFO
    printf("%s %s  received a signal \n", __FILE__, __func__ );
    fflush(stdout);
#endif
    if (CheckPostmasterSignal(PMSIGNAL_START_WALRECEIVER))
    {
        if (WalRcvPid == 0) {
            printf("%s , pid = %d ,postmaster call wal receiver process \n", __func__ , getpid());
            fflush(stdout);

            WalRcvPid = StartChildProcess(WalReceiverProcess);
        }

    }
//    SetLatch(MyLatch);
//    WakeupRecovery();
    WakeupStartupRecovery();
    latch_sigusr1_handler();
}

int **serverPipe;
int **computePipe;

static void
proc_die(SIGNAL_ARGS) {
    HashMapDestroy(pageVersionHashMap);
    RelSizePthreadLocksDestroy();
    WakeupStartupResourceDestroy();

    free(RpcXlblocks);
    free(RpcXLogPages);
    free(RpcXLogPagesLocks);

    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        free(serverPipe[i]);
        free(computePipe[i]);
    }
    free(serverPipe);
    free(computePipe);
    exit(0);
}

// WalRedo Process will use this variable
// it will determine which pipe should use
int ReplayProcessNum = -1;

static void
StartWalRedoProcess(int argc, char *argv[],
                    const char *dbname,
                    const char *username) {

    // Init ReplayProcess thread locks
    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        pthread_mutex_init(&(replayProcessMutex[i]), NULL);
    }
    serverPipe = (int**) malloc(REPLAY_PROCESS_NUM*sizeof(int*));
    computePipe = (int**) malloc(REPLAY_PROCESS_NUM*sizeof(int*));
    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        serverPipe[i] = (int*) malloc(sizeof(int)*2);
        computePipe[i] = (int*) malloc(sizeof(int)*2);
    }

    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        if(pipe(serverPipe[i]) == -1)
            printf("Error on pipe\n");

        if(pipe(computePipe[i]) == -1)
            printf("Error on pipe\n");
    }

    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        __pid_t pid = fork();
        if(pid > 0) {
            close(computePipe[i][1]); // Close comp's write pipe
            close(serverPipe[i][0]); //Close server's read pipe

            // Set the comp's read pipe to non-block
            int flags = 0|O_NONBLOCK;
            fcntl(computePipe[i][0], F_SETFL, flags);

            //todo: should close at the end
        } else if (pid == 0) { // Child Process
            ReplayProcessNum = i;

            close(computePipe[i][0]); //Close comp's read pipe
            close(serverPipe[i][1]); //Close server's write pipe

            // Set the server's read pipe to non-block
            int flags = 0|O_NONBLOCK;
            fcntl(serverPipe[i][0], F_SETFL, flags);

            // Close pipes that master connects to other processes.
            for(int j = 0; j < ReplayProcessNum; j++) {
                if(j != i) {
                    close(computePipe[j][0]);
                    close(computePipe[j][1]);
                    close(serverPipe[j][0]);
                    close(serverPipe[j][1]);
                }
            }


            InitPostmasterChild();

            /* Close the postmaster's sockets */
            ClosePostmasterPorts(false);

            WalRedoMain(argc, argv, dbname, username);

            close(computePipe[i][1]);
            close(serverPipe[i][0]);
            exit(0);
        }

    }

}


int SyncGetRelSize(RelFileNode relFileNode, ForkNumber forkNumber, XLogRecPtr lsn) {
#ifdef ENABLE_DEBUG_INFO
    printf("%s start \n", __func__ );
    fflush(stdout);
#endif

#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    int tid = gettid();
    int replayPid = tid%REPLAY_PROCESS_NUM;
    int gotLock = 0;
    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        if(pthread_mutex_trylock( &(replayProcessMutex[ (replayPid+i)%REPLAY_PROCESS_NUM ]) ) == 0) { // Lock successfully
            gotLock = 1;
            replayPid = (replayPid+i) % REPLAY_PROCESS_NUM;
            break;
        }
    }

    // if we didn't get lock with trylock(), then use block wait lock
    if(gotLock == 0) {
        pthread_mutex_lock(&(replayProcessMutex[replayPid]));
    }

    // ------- Send "ApplyRecordUntil" request to replay process ------
    char requestBuffer[1024];
    int32 msgLen = 0;

    //TODO should not request 'ApplyRecordUntil'
//    requestBuffer[0] = 'U'; // Request function "ApplyRecordUntil"
//
//    msgLen = 4; // $msgLen itself
//    msgLen += sizeof(lsn);  // Add parameter $lsn length
//    msgLen = pg_hton32(msgLen);
//
//
//    if(WalRcv != NULL) {
//        lsn = WalRcv->flushedUpto;
//        printf("read lsn from writtenUpto, lsn = %ld\n", lsn);
//    }
//    lsn = pg_hton64(lsn);
//
//    memcpy(&requestBuffer[1], &msgLen, sizeof(msgLen));
//    memcpy(&requestBuffer[1+sizeof(msgLen)], &lsn, sizeof(lsn));
//
//    int targetMsgLen = 1+4+sizeof(lsn);
//
//    int sendLen = 0;
//    while(sendLen < targetMsgLen) {
//        int writeLen = write(serverPipe[1], &requestBuffer[sendLen], targetMsgLen-sendLen);
//
//        sendLen+=writeLen;
//    }
//    printf("%s sent U request to standalone PG process\n", __func__ );

    // -------- Send "MdNblocks" request to replay process -------
    int targetMsgLen = 0;
    int sendLen = 0;

    requestBuffer[0] = 'M';

    msgLen = 4; // $msgLen itself
    msgLen += sizeof(unsigned char); //forknum
    msgLen += 4; // $spc
    msgLen += 4; // $db
    msgLen += 4; // $rel
    msgLen = pg_hton32(msgLen);

    // prepare parameters to network encoding
    relFileNode.spcNode = pg_hton32(relFileNode.spcNode);
    relFileNode.dbNode = pg_hton32(relFileNode.dbNode);
    relFileNode.relNode = pg_hton32(relFileNode.relNode);

    memcpy(&requestBuffer[1], &msgLen, sizeof(msgLen));
    memcpy(&requestBuffer[1+4], &forkNumber, sizeof(unsigned char));
    memcpy(&requestBuffer[1+4+1], &relFileNode.spcNode, 4);
    memcpy(&requestBuffer[1+4+1+4], &relFileNode.dbNode, 4);
    memcpy(&requestBuffer[1+4+1+4+4], &relFileNode.relNode, 4);

    targetMsgLen = 1+4+1+4+4+4;
    sendLen = 0;
#ifdef ENABLE_DEBUG_INFO
    printf("%s send M request to standalone PG process\n", __func__ );
    fflush(stdout);
#endif
    while(sendLen < targetMsgLen) {
        int writeLen = write(serverPipe[replayPid][1], &requestBuffer[sendLen], targetMsgLen-sendLen);
        sendLen+=writeLen;
    }

    // ------- Read target page from replay process ------
    int recvLen = 0;
    int nblocks = 0;
    while (recvLen < sizeof(int)) {
        char* tempP = &nblocks;
        int readLen = read(computePipe[replayPid][0], &(tempP[recvLen]), sizeof(int) - recvLen);
        if (readLen < 0) {
            if (errno == EAGAIN) {
                continue;
            }
            else {
                printf("Error on read\n");
                exit(1);
            }
        }
        Assert(readLen >= 0);
        recvLen += readLen;
    }

    Assert(recvLen == sizeof(int));
#ifdef ENABLE_DEBUG_INFO
    printf("%s, get page number = %d\n", __func__ , nblocks);
    fflush(stdout);
#endif
    pthread_mutex_unlock(&(replayProcessMutex[replayPid]));

#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    SyncGetRelSizeTime += ((end.tv_sec*1000000+end.tv_usec) - (start.tv_sec*1000000+start.tv_usec));
    SyncGetRelSizeCount++;

    struct timeval now;

    if(!initialized3){
        gettimeofday(&output_timing3, NULL);
        initialized3 = 1;
    }
    gettimeofday(&now, NULL);
    if(now.tv_sec-output_timing3.tv_sec >= 5) {

        printf("walredo_SyncGetRelSize = %ld, count = %d\n",SyncGetRelSizeTime, SyncGetRelSizeCount);
        fflush(stdout);
        output_timing3 = now;
    }
#endif
    return nblocks;

}

void ApplyOneLsnWithoutBasePage(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, XLogRecPtr lsn, char* targetPage) {
    int tid = gettid();
    int replayPid = tid%REPLAY_PROCESS_NUM;
    int gotLock = 0;
    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        if(pthread_mutex_trylock( &(replayProcessMutex[ (replayPid+i)%REPLAY_PROCESS_NUM ]) ) == 0) { // Lock successfully
            gotLock = 1;
            replayPid = (replayPid+i) % REPLAY_PROCESS_NUM;
            break;
        }
    }

    // if we didn't get lock with trylock(), then use block wait lock
    if(gotLock == 0) {
        pthread_mutex_lock(&(replayProcessMutex[replayPid]));
    }


#ifdef ENABLE_DEBUG_INFO
    printf("%s %s %d , spcID = %u, dbID = %u, tabID = %u, fornum = %d, blkNum = %u, lsn = %lu\n", __func__ , __FILE__, __LINE__,
           relFileNode.spcNode, relFileNode.dbNode, relFileNode.relNode, forkNumber, blockNumber, lsn);
    fflush(stdout);
#endif

#ifdef XLOG_IN_ROCKSDB
    // Read XlogRecord from RocksDB
    XLogRecord *record;
    size_t record_size;
    GetXlogWithLsn(lsn, &record, &record_size);
#endif

    // ------ Send "ApplyOneLsn" request to replay process ------
#ifdef XLOG_IN_ROCKSDB
    char *requestBuffer = (char*) malloc(8192+1024+record->xl_tot_len);
#else
    char *requestBuffer = (char*) malloc(8192+1024);
#endif
    int32 msgLen = 0;

    requestBuffer[0] = 'E';

    msgLen = 4; // $msgLen itself
    msgLen += sizeof(unsigned char); //forknum
    msgLen += 4; // $spc
    msgLen += 4; // $db
    msgLen += 4; // $rel
    msgLen += 4; // $blknum
    msgLen += 8; // $lsn
//    msgLen += BLCKSZ; // $pageContent
#ifdef XLOG_IN_ROCKSDB
    msgLen += record->xl_tot_len; // record, wal_redo will get its length by its header
#endif

    msgLen = pg_hton32(msgLen);

    // prepare parameters to network encoding
    relFileNode.spcNode = pg_hton32(relFileNode.spcNode);
    relFileNode.dbNode = pg_hton32(relFileNode.dbNode);
    relFileNode.relNode = pg_hton32(relFileNode.relNode);
    blockNumber = pg_hton32(blockNumber);
    lsn = pg_hton64(lsn);

    memcpy(&requestBuffer[1], &msgLen, sizeof(msgLen));
    memcpy(&requestBuffer[1+4], &forkNumber, sizeof(unsigned char));
    memcpy(&requestBuffer[1+4+1], &relFileNode.spcNode, 4);
    memcpy(&requestBuffer[1+4+1+4], &relFileNode.dbNode, 4);
    memcpy(&requestBuffer[1+4+1+4+4], &relFileNode.relNode, 4);
    memcpy(&requestBuffer[1+4+1+4+4+4], &blockNumber, 4);
    memcpy(&requestBuffer[1+4+1+4+4+4+4], &lsn, 8);
//    memcpy(&requestBuffer[1+4+1+4+4+4+4+8], origPage, BLCKSZ);
#ifdef XLOG_IN_ROCKSDB
    memcpy(&requestBuffer[1+4+1+4+4+4+4+8], record, record->xl_tot_len);
#endif


#ifdef XLOG_IN_ROCKSDB
    int targetMsgLen = 1+4+1+4+4+4+4+8+record->xl_tot_len;
#else
    int targetMsgLen = 1+4+1+4+4+4+4+8;
#endif
    int sendLen = 0;
    while(sendLen < targetMsgLen) {
        int writeLen = write(serverPipe[replayPid][1], &requestBuffer[sendLen], targetMsgLen-sendLen);
        sendLen+=writeLen;
    }

#ifdef XLOG_IN_ROCKSDB
    free(record);
#endif
    free(requestBuffer);

    // ------- Read target page from replay process ------
    int recvLen = 0;
    while (recvLen < BLCKSZ) {
        int readLen = read(computePipe[replayPid][0], &targetPage[recvLen], BLCKSZ - recvLen);
        if (readLen < 0) {
            if (errno == EAGAIN) {
                continue;
            }
            else {
                printf("Error on read\n");
                exit(1);
            }
        }
        Assert(readLen >= 0);
        recvLen += readLen;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s read %d from standalone \n", __func__ , recvLen);
    fflush(stdout);
#endif

    Assert(recvLen == BLCKSZ);
    pthread_mutex_unlock(&(replayProcessMutex[replayPid]));

}

// targetPage should be allocated by caller function
// This function can be optimized by passing []lsn to PgStandalone and get several pages from PgStandalone
void ApplyOneLsn(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, XLogRecPtr lsn, char* origPage, char* targetPage) {
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    int tid = gettid();
    int replayPid = tid%REPLAY_PROCESS_NUM;
    int gotLock = 0;
    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        if(pthread_mutex_trylock( &(replayProcessMutex[ (replayPid+i)%REPLAY_PROCESS_NUM ]) ) == 0) { // Lock successfully
            gotLock = 1;
            replayPid = (replayPid+i) % REPLAY_PROCESS_NUM;
            break;
        }
    }

    // if we didn't get lock with trylock(), then use block wait lock
    if(gotLock == 0) {
        pthread_mutex_lock(&(replayProcessMutex[replayPid]));
    }


//    pthread_mutex_lock(&replayProcessMutex);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %s %d , spcID = %u, dbID = %u, tabID = %u, fornum = %d, blkNum = %u, lsn = %lu\n", __func__ , __FILE__, __LINE__,
           relFileNode.spcNode, relFileNode.dbNode, relFileNode.relNode, forkNumber, blockNumber, lsn);
    fflush(stdout);
#endif

#ifdef XLOG_IN_ROCKSDB
    // Read XlogRecord from RocksDB
    XLogRecord *record;
    size_t record_size;
    GetXlogWithLsn(lsn, &record, &record_size);
#endif

    // ------ Send "ApplyOneLsn" request to replay process ------
#ifdef XLOG_IN_ROCKSDB
    char *requestBuffer = (char*) malloc(8192+1024+record->xl_tot_len);
#else
    char *requestBuffer = (char*) malloc(8192+1024);
#endif
    int32 msgLen = 0;

    requestBuffer[0] = 'D';

    msgLen = 4; // $msgLen itself
    msgLen += sizeof(unsigned char); //forknum
    msgLen += 4; // $spc
    msgLen += 4; // $db
    msgLen += 4; // $rel
    msgLen += 4; // $blknum
    msgLen += 8; // $lsn
    msgLen += BLCKSZ; // $pageContent
#ifdef XLOG_IN_ROCKSDB
    msgLen += record->xl_tot_len; // record, wal_redo will get its length by its header
#endif

    msgLen = pg_hton32(msgLen);

    // prepare parameters to network encoding
    relFileNode.spcNode = pg_hton32(relFileNode.spcNode);
    relFileNode.dbNode = pg_hton32(relFileNode.dbNode);
    relFileNode.relNode = pg_hton32(relFileNode.relNode);
    blockNumber = pg_hton32(blockNumber);
    lsn = pg_hton64(lsn);

    memcpy(&requestBuffer[1], &msgLen, sizeof(msgLen));
    memcpy(&requestBuffer[1+4], &forkNumber, sizeof(unsigned char));
    memcpy(&requestBuffer[1+4+1], &relFileNode.spcNode, 4);
    memcpy(&requestBuffer[1+4+1+4], &relFileNode.dbNode, 4);
    memcpy(&requestBuffer[1+4+1+4+4], &relFileNode.relNode, 4);
    memcpy(&requestBuffer[1+4+1+4+4+4], &blockNumber, 4);
    memcpy(&requestBuffer[1+4+1+4+4+4+4], &lsn, 8);
    memcpy(&requestBuffer[1+4+1+4+4+4+4+8], origPage, BLCKSZ);
#ifdef XLOG_IN_ROCKSDB
    memcpy(&requestBuffer[1+4+1+4+4+4+4+8+BLCKSZ], record, record->xl_tot_len);
#endif


#ifdef XLOG_IN_ROCKSDB
    int targetMsgLen = 1+4+1+4+4+4+4+8+BLCKSZ+record->xl_tot_len;
#else
    int targetMsgLen = 1+4+1+4+4+4+4+8+BLCKSZ;
#endif
    int sendLen = 0;
    while(sendLen < targetMsgLen) {
        int writeLen = write(serverPipe[replayPid][1], &requestBuffer[sendLen], targetMsgLen-sendLen);
        sendLen+=writeLen;
    }

#ifdef XLOG_IN_ROCKSDB
    free(record);
#endif
    free(requestBuffer);

    // ------- Read target page from replay process ------
    int recvLen = 0;
    while (recvLen < BLCKSZ) {
        int readLen = read(computePipe[replayPid][0], &targetPage[recvLen], BLCKSZ - recvLen);
        if (readLen < 0) {
            if (errno == EAGAIN) {
                continue;
            }
            else {
                printf("Error on read\n");
                exit(1);
            }
        }
        Assert(readLen >= 0);
        recvLen += readLen;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s read %d from standalone \n", __func__ , recvLen);
    fflush(stdout);
#endif

    Assert(recvLen == BLCKSZ);
    pthread_mutex_unlock(&(replayProcessMutex[replayPid]));
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    ApplyOneLsnTime += ((end.tv_sec*1000000+end.tv_usec) - (start.tv_sec*1000000+start.tv_usec));
    ApplyOneLsnCount++;

    struct timeval now;

    if(!initialized3){
        gettimeofday(&output_timing3, NULL);
        initialized3 = 1;
    }
    gettimeofday(&now, NULL);
    if(now.tv_sec-output_timing3.tv_sec >= 5) {

        //printf("walredo_wait_time = %ld\n",walRedoWaitLockTime);
        printf("walredo_ApplyOneLsn = %ld, count = %d\n",ApplyOneLsnTime, ApplyOneLsnCount);
        fflush(stdout);
        output_timing3 = now;
    }
#endif
}



// targetPage should be allocated by caller function
// This function can be optimized by passing []lsn to PgStandalone and get several pages from PgStandalone
void ApplyLsnListAndGetUpdatedPage(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, XLogRecPtr* lsnList, int listSize,  char* targetPage) {
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    int tid = gettid();
    int replayPid = tid%REPLAY_PROCESS_NUM;
    int gotLock = 0;
    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        if(pthread_mutex_trylock( &(replayProcessMutex[ (replayPid+i)%REPLAY_PROCESS_NUM ]) ) == 0) { // Lock successfully
            gotLock = 1;
            replayPid = (replayPid+i) % REPLAY_PROCESS_NUM;
            break;
        }
    }

    // if we didn't get lock with trylock(), then use block wait lock
    if(gotLock == 0) {
        pthread_mutex_lock(&(replayProcessMutex[replayPid]));
    }


//    pthread_mutex_lock(&replayProcessMutex);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %s %d , spcID = %lu, dbID = %lu, tabID = %lu, fornum = %d, blkNum = %lu, listSize = %d\n", __func__ , __FILE__, __LINE__,
           relFileNode.spcNode, relFileNode.dbNode, relFileNode.relNode, forkNumber, blockNumber, listSize);
    fflush(stdout);
#endif

#ifdef XLOG_IN_ROCKSDB
    // Read XlogRecord from RocksDB
    XLogRecord *record;
    size_t record_size;
    GetXlogWithLsn(lsn, &record, &record_size);
#endif

    // ------ Send "ApplyOneLsn" request to replay process ------
#ifdef XLOG_IN_ROCKSDB
    char *requestBuffer = (char*) malloc(8192+1024+record->xl_tot_len);
#else
    char *requestBuffer = (char*) malloc(1024+listSize*sizeof(uint64_t));
#endif
    int32 msgLen = 0;

    requestBuffer[0] = 'o';

    msgLen = 4; // $msgLen itself
    msgLen += sizeof(unsigned char); //forknum
    msgLen += 4; // $spc
    msgLen += 4; // $db
    msgLen += 4; // $rel
    msgLen += 4; // $blknum
    msgLen += 4; // $listSize
    msgLen += 8*listSize; // $lsnList
#ifdef XLOG_IN_ROCKSDB
    msgLen += record->xl_tot_len; // record, wal_redo will get its length by its header
#endif
    int origMsgLen = msgLen;
    msgLen = pg_hton32(msgLen);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    // prepare parameters to network encoding
    relFileNode.spcNode = pg_hton32(relFileNode.spcNode);
    relFileNode.dbNode = pg_hton32(relFileNode.dbNode);
    relFileNode.relNode = pg_hton32(relFileNode.relNode);
    blockNumber = pg_hton32(blockNumber);

    // Important
    int origListSize = listSize;
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d, listsize = %d\n", __func__ , __LINE__, listSize);
    for(int i = 0; i < origListSize; i++) {
        printf("lsn %d = %lu\n", i, lsnList[i]);
    }
    fflush(stdout);
#endif

    // h: 1 -> n:16777216
    listSize = pg_hton32(listSize);

//    for(int i = 0; i < origListSize; i++) {
//        lsnList[i] = pg_hton64(lsnList[i]);
//    }
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif


    int currLen = 1;
    memcpy(&requestBuffer[currLen], &msgLen, sizeof(msgLen));
    currLen+=4;
    memcpy(&requestBuffer[currLen], &forkNumber, sizeof(unsigned char));
    currLen+=1;
    memcpy(&requestBuffer[currLen], &relFileNode.spcNode, 4);
    currLen+=4;
    memcpy(&requestBuffer[currLen], &relFileNode.dbNode, 4);
    currLen+=4;
    memcpy(&requestBuffer[currLen], &relFileNode.relNode, 4);
    currLen+=4;
    memcpy(&requestBuffer[currLen], &blockNumber, 4);
    currLen+=4;
    memcpy(&requestBuffer[currLen], &listSize, 4);
    currLen+=4;
    for(int i = 0; i < origListSize; i++) {
        uint64_t netLsn = pg_hton64(lsnList[i]);
        memcpy(&requestBuffer[currLen], &(netLsn), 8);
        currLen+=8;
    }
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d origin page lsn = %lu\n", __func__ , __LINE__, PageGetLSN(origPage));
    fflush(stdout);
#endif
#ifdef XLOG_IN_ROCKSDB
    memcpy(&requestBuffer[1+4+1+4+4+4+4+8+BLCKSZ], record, record->xl_tot_len);
#endif


#ifdef ENABLE_DEBUG_INFO
    printf("%s %d ready to send %d lsns\n", __func__ , __LINE__, origListSize);
    fflush(stdout);
#endif


#ifdef XLOG_IN_ROCKSDB
    int targetMsgLen = 1+4+1+4+4+4+4+8+BLCKSZ+record->xl_tot_len;
#else
    int targetMsgLen = 1+origMsgLen;
#endif
    int sendLen = 0;
    while(sendLen < targetMsgLen) {
        int writeLen = write(serverPipe[replayPid][1], &requestBuffer[sendLen], targetMsgLen-sendLen);
        sendLen+=writeLen;
    }

#ifdef XLOG_IN_ROCKSDB
    free(record);
#endif
    free(requestBuffer);

    // ------- Read target page from replay process ------
    int recvLen = 0;
    while (recvLen < BLCKSZ) {
        int readLen = read(computePipe[replayPid][0], &targetPage[recvLen], BLCKSZ - recvLen);
        if (readLen < 0) {
            if (errno == EAGAIN) {
                continue;
            }
            else {
                printf("Error on read\n");
                exit(1);
            }
        }
        Assert(readLen >= 0);
        recvLen += readLen;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s read %d from standalone \n", __func__ , recvLen);
    fflush(stdout);
#endif

    Assert(recvLen == BLCKSZ);
    pthread_mutex_unlock(&(replayProcessMutex[replayPid]));
}


void WalRedoExtendRel(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, char * content) {
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    int tid = gettid();
    int replayPid = tid%REPLAY_PROCESS_NUM;
    int gotLock = 0;
    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        if(pthread_mutex_trylock( &(replayProcessMutex[ (replayPid+i)%REPLAY_PROCESS_NUM ]) ) == 0) { // Lock successfully
            gotLock = 1;
            replayPid = (replayPid+i) % REPLAY_PROCESS_NUM;
            break;
        }
    }

    // if we didn't get lock with trylock(), then use block wait lock
    if(gotLock == 0) {
        pthread_mutex_lock(&(replayProcessMutex[replayPid]));
    }


//    pthread_mutex_lock(&replayProcessMutex);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %s %d , spcID = %lu, dbID = %lu, tabID = %lu, fornum = %d, blkNum = %lu, listSize = %d\n", __func__ , __FILE__, __LINE__,
           relFileNode.spcNode, relFileNode.dbNode, relFileNode.relNode, forkNumber, blockNumber, listSize);
    fflush(stdout);
#endif

#ifdef XLOG_IN_ROCKSDB
    // Read XlogRecord from RocksDB
    XLogRecord *record;
    size_t record_size;
    GetXlogWithLsn(lsn, &record, &record_size);
#endif

    // ------ Send "ApplyOneLsn" request to replay process ------
#ifdef XLOG_IN_ROCKSDB
    char *requestBuffer = (char*) malloc(8192+1024+record->xl_tot_len);
#else
    char *requestBuffer = (char*) malloc(8192+1024);
#endif
    int32 msgLen = 0;

    requestBuffer[0] = 'F';

    msgLen = 4; // $msgLen itself
    msgLen += sizeof(unsigned char); //forknum
    msgLen += 4; // $spc
    msgLen += 4; // $db
    msgLen += 4; // $rel
    msgLen += 4; // $blknum
    msgLen += BLCKSZ; // $pageContent
#ifdef XLOG_IN_ROCKSDB
    msgLen += record->xl_tot_len; // record, wal_redo will get its length by its header
#endif
    int origMsgLen = msgLen;
    msgLen = pg_hton32(msgLen);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    // prepare parameters to network encoding
    relFileNode.spcNode = pg_hton32(relFileNode.spcNode);
    relFileNode.dbNode = pg_hton32(relFileNode.dbNode);
    relFileNode.relNode = pg_hton32(relFileNode.relNode);
    blockNumber = pg_hton32(blockNumber);


    int currLen = 1;
    memcpy(&requestBuffer[currLen], &msgLen, sizeof(msgLen));
    currLen+=4;
    memcpy(&requestBuffer[currLen], &forkNumber, sizeof(unsigned char));
    currLen+=1;
    memcpy(&requestBuffer[currLen], &relFileNode.spcNode, 4);
    currLen+=4;
    memcpy(&requestBuffer[currLen], &relFileNode.dbNode, 4);
    currLen+=4;
    memcpy(&requestBuffer[currLen], &relFileNode.relNode, 4);
    currLen+=4;
    memcpy(&requestBuffer[currLen], &blockNumber, 4);
    currLen+=4;
    memcpy(&requestBuffer[currLen], content, BLCKSZ);

#ifdef XLOG_IN_ROCKSDB
    int targetMsgLen = 1+4+1+4+4+4+4+8+BLCKSZ+record->xl_tot_len;
#else
    int targetMsgLen = 1+origMsgLen;
#endif
    int sendLen = 0;
    while(sendLen < targetMsgLen) {
        int writeLen = write(serverPipe[replayPid][1], &requestBuffer[sendLen], targetMsgLen-sendLen);
        sendLen+=writeLen;
    }

#ifdef XLOG_IN_ROCKSDB
    free(record);
#endif
    free(requestBuffer);

    // ------- Read target page from replay process ------
    int recvLen = 0;
    while (recvLen < sizeof(int)) {
        int readLen = read(computePipe[replayPid][0], &content[recvLen], sizeof(int) - recvLen);
        if (readLen < 0) {
            if (errno == EAGAIN) {
                continue;
            }
            else {
                printf("Error on read\n");
                exit(1);
            }
        }
        Assert(readLen >= 0);
        recvLen += readLen;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s read %d from standalone \n", __func__ , recvLen);
    fflush(stdout);
#endif

    pthread_mutex_unlock(&(replayProcessMutex[replayPid]));

}


void WalRedoCreateRel(RelFileNode relFileNode, ForkNumber forkNumber) {
#ifdef ENABLE_DEBUG_INFO
    printf("%s start \n", __func__ );
    fflush(stdout);
#endif

#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    int tid = gettid();
    int replayPid = tid%REPLAY_PROCESS_NUM;
    int gotLock = 0;
    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        if(pthread_mutex_trylock( &(replayProcessMutex[ (replayPid+i)%REPLAY_PROCESS_NUM ]) ) == 0) { // Lock successfully
            gotLock = 1;
            replayPid = (replayPid+i) % REPLAY_PROCESS_NUM;
            break;
        }
    }

    // if we didn't get lock with trylock(), then use block wait lock
    if(gotLock == 0) {
        pthread_mutex_lock(&(replayProcessMutex[replayPid]));
    }

    // ------- Send "ApplyRecordUntil" request to replay process ------
    char requestBuffer[1024];
    int32 msgLen = 0;


    // -------- Send "CreateRel" request to replay process -------
    int targetMsgLen = 0;
    int sendLen = 0;

    requestBuffer[0] = 'C';

    msgLen = 4; // $msgLen itself
    msgLen += sizeof(unsigned char); //forknum
    msgLen += 4; // $spc
    msgLen += 4; // $db
    msgLen += 4; // $rel
    msgLen = pg_hton32(msgLen);

    // prepare parameters to network encoding
    relFileNode.spcNode = pg_hton32(relFileNode.spcNode);
    relFileNode.dbNode = pg_hton32(relFileNode.dbNode);
    relFileNode.relNode = pg_hton32(relFileNode.relNode);

    memcpy(&requestBuffer[1], &msgLen, sizeof(msgLen));
    memcpy(&requestBuffer[1+4], &forkNumber, sizeof(unsigned char));
    memcpy(&requestBuffer[1+4+1], &relFileNode.spcNode, 4);
    memcpy(&requestBuffer[1+4+1+4], &relFileNode.dbNode, 4);
    memcpy(&requestBuffer[1+4+1+4+4], &relFileNode.relNode, 4);

    targetMsgLen = 1+4+1+4+4+4;
    sendLen = 0;
#ifdef ENABLE_DEBUG_INFO
    printf("%s send M request to standalone PG process\n", __func__ );
    fflush(stdout);
#endif
    while(sendLen < targetMsgLen) {
        int writeLen = write(serverPipe[replayPid][1], &requestBuffer[sendLen], targetMsgLen-sendLen);
        sendLen+=writeLen;
    }

    // ------- Read target response from replay process ------
    int recvLen = 0;
    int nblocks = 0;
    while (recvLen < sizeof(int)) {
        char* tempP = &nblocks;
        int readLen = read(computePipe[replayPid][0], &(tempP[recvLen]), sizeof(int) - recvLen);
        if (readLen < 0) {
            if (errno == EAGAIN) {
                continue;
            }
            else {
                printf("Error on read\n");
                exit(1);
            }
        }
        Assert(readLen >= 0);
        recvLen += readLen;
    }

    Assert(recvLen == sizeof(int));
#ifdef ENABLE_DEBUG_INFO
    printf("%s, get page number = %d\n", __func__ , nblocks);
    fflush(stdout);
#endif
    pthread_mutex_unlock(&(replayProcessMutex[replayPid]));

#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    SyncGetRelSizeTime += ((end.tv_sec*1000000+end.tv_usec) - (start.tv_sec*1000000+start.tv_usec));
    SyncGetRelSizeCount++;

    struct timeval now;

    if(!initialized3){
        gettimeofday(&output_timing3, NULL);
        initialized3 = 1;
    }
    gettimeofday(&now, NULL);
    if(now.tv_sec-output_timing3.tv_sec >= 5) {

        printf("walredo_SyncGetRelSize = %ld, count = %d\n",SyncGetRelSizeTime, SyncGetRelSizeCount);
        fflush(stdout);
        output_timing3 = now;
    }
#endif
    return;
}




// targetPage should be allocated by caller function
// This function can be optimized by passing []lsn to PgStandalone and get several pages from PgStandalone
void ApplyLsnList(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, XLogRecPtr* lsnList, int listSize, char* origPage, char* targetPage) {
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    int tid = gettid();
    int replayPid = tid%REPLAY_PROCESS_NUM;
    int gotLock = 0;
    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        if(pthread_mutex_trylock( &(replayProcessMutex[ (replayPid+i)%REPLAY_PROCESS_NUM ]) ) == 0) { // Lock successfully
            gotLock = 1;
            replayPid = (replayPid+i) % REPLAY_PROCESS_NUM;
            break;
        }
    }

    // if we didn't get lock with trylock(), then use block wait lock
    if(gotLock == 0) {
        pthread_mutex_lock(&(replayProcessMutex[replayPid]));
    }


//    pthread_mutex_lock(&replayProcessMutex);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %s %d , spcID = %lu, dbID = %lu, tabID = %lu, fornum = %d, blkNum = %lu, listSize = %d\n", __func__ , __FILE__, __LINE__,
           relFileNode.spcNode, relFileNode.dbNode, relFileNode.relNode, forkNumber, blockNumber, listSize);
    fflush(stdout);
#endif

#ifdef XLOG_IN_ROCKSDB
    // Read XlogRecord from RocksDB
    XLogRecord *record;
    size_t record_size;
    GetXlogWithLsn(lsn, &record, &record_size);
#endif

    // ------ Send "ApplyOneLsn" request to replay process ------
#ifdef XLOG_IN_ROCKSDB
    char *requestBuffer = (char*) malloc(8192+1024+record->xl_tot_len);
#else
    char *requestBuffer = (char*) malloc(8192+1024+listSize*sizeof(uint64_t));
#endif
    int32 msgLen = 0;

    requestBuffer[0] = 'O';

    msgLen = 4; // $msgLen itself
    msgLen += sizeof(unsigned char); //forknum
    msgLen += 4; // $spc
    msgLen += 4; // $db
    msgLen += 4; // $rel
    msgLen += 4; // $blknum
    msgLen += 4; // $listSize
    msgLen += 8*listSize; // $lsnList
    msgLen += BLCKSZ; // $pageContent
#ifdef XLOG_IN_ROCKSDB
    msgLen += record->xl_tot_len; // record, wal_redo will get its length by its header
#endif
    int origMsgLen = msgLen;
    msgLen = pg_hton32(msgLen);

#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif

    // prepare parameters to network encoding
    relFileNode.spcNode = pg_hton32(relFileNode.spcNode);
    relFileNode.dbNode = pg_hton32(relFileNode.dbNode);
    relFileNode.relNode = pg_hton32(relFileNode.relNode);
    blockNumber = pg_hton32(blockNumber);

    // Important
    int origListSize = listSize;
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d, listsize = %d\n", __func__ , __LINE__, listSize);
    for(int i = 0; i < origListSize; i++) {
        printf("lsn %d = %lu\n", i, lsnList[i]);
    }
    fflush(stdout);
#endif

    // h: 1 -> n:16777216
    listSize = pg_hton32(listSize);

//    for(int i = 0; i < origListSize; i++) {
//        lsnList[i] = pg_hton64(lsnList[i]);
//    }
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d\n", __func__ , __LINE__);
    fflush(stdout);
#endif


    int currLen = 1;
    memcpy(&requestBuffer[currLen], &msgLen, sizeof(msgLen));
    currLen+=4;
    memcpy(&requestBuffer[currLen], &forkNumber, sizeof(unsigned char));
    currLen+=1;
    memcpy(&requestBuffer[currLen], &relFileNode.spcNode, 4);
    currLen+=4;
    memcpy(&requestBuffer[currLen], &relFileNode.dbNode, 4);
    currLen+=4;
    memcpy(&requestBuffer[currLen], &relFileNode.relNode, 4);
    currLen+=4;
    memcpy(&requestBuffer[currLen], &blockNumber, 4);
    currLen+=4;
    memcpy(&requestBuffer[currLen], &listSize, 4);
    currLen+=4;
    for(int i = 0; i < origListSize; i++) {
        uint64_t netLsn = pg_hton64(lsnList[i]);
        memcpy(&requestBuffer[currLen], &(netLsn), 8);
        currLen+=8;
    }
    memcpy(&requestBuffer[currLen], origPage, BLCKSZ);
#ifdef ENABLE_DEBUG_INFO
    printf("%s %d origin page lsn = %lu\n", __func__ , __LINE__, PageGetLSN(origPage));
    fflush(stdout);
#endif
#ifdef XLOG_IN_ROCKSDB
    memcpy(&requestBuffer[1+4+1+4+4+4+4+8+BLCKSZ], record, record->xl_tot_len);
#endif


#ifdef ENABLE_DEBUG_INFO
    printf("%s %d ready to send %d lsns\n", __func__ , __LINE__, origListSize);
    fflush(stdout);
#endif


#ifdef XLOG_IN_ROCKSDB
    int targetMsgLen = 1+4+1+4+4+4+4+8+BLCKSZ+record->xl_tot_len;
#else
    int targetMsgLen = 1+origMsgLen;
#endif
    int sendLen = 0;
    while(sendLen < targetMsgLen) {
        int writeLen = write(serverPipe[replayPid][1], &requestBuffer[sendLen], targetMsgLen-sendLen);
        sendLen+=writeLen;
    }

#ifdef XLOG_IN_ROCKSDB
    free(record);
#endif
    free(requestBuffer);

    // ------- Read target page from replay process ------
    int recvLen = 0;
    while (recvLen < BLCKSZ) {
        int readLen = read(computePipe[replayPid][0], &targetPage[recvLen], BLCKSZ - recvLen);
        if (readLen < 0) {
            if (errno == EAGAIN) {
                continue;
            }
            else {
                printf("Error on read\n");
                exit(1);
            }
        }
        Assert(readLen >= 0);
        recvLen += readLen;
    }

#ifdef ENABLE_DEBUG_INFO
    printf("%s read %d from standalone \n", __func__ , recvLen);
    fflush(stdout);
#endif

    Assert(recvLen == BLCKSZ);
    pthread_mutex_unlock(&(replayProcessMutex[replayPid]));
}

void GetBasePage(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, char* buffer) {
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    int tid = gettid();
    int replayPid = tid%REPLAY_PROCESS_NUM;
    int gotLock = 0;
    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        if(pthread_mutex_trylock( &(replayProcessMutex[ (replayPid+i)%REPLAY_PROCESS_NUM ]) ) == 0) { // Lock successfully
            gotLock = 1;
            replayPid = (replayPid+i) % REPLAY_PROCESS_NUM;
            break;
        }
    }

    // if we didn't get lock with trylock(), then use block wait lock
    if(gotLock == 0) {
        pthread_mutex_lock(&(replayProcessMutex[replayPid]));
    }


//    pthread_mutex_lock(&replayProcessMutex);
    // ------- Send "GetPage" request to replay process ------
    char requestBuffer[1024];
    int32 msgLen = 0;
    requestBuffer[0] = 'G';

    msgLen = 4; // $msgLen itself
    msgLen += sizeof(unsigned char); //forknum
    msgLen += 4; // $spc
    msgLen += 4; // $db
    msgLen += 4; // $rel
    msgLen += 4; // $blknum
    msgLen = pg_hton32(msgLen);

    // prepare parameters to network encoding
    relFileNode.spcNode = pg_hton32(relFileNode.spcNode);
    relFileNode.dbNode = pg_hton32(relFileNode.dbNode);
    relFileNode.relNode = pg_hton32(relFileNode.relNode);
    blockNumber = pg_hton32(blockNumber);

    memcpy(&requestBuffer[1], &msgLen, sizeof(msgLen));
    memcpy(&requestBuffer[1+4], &forkNumber, sizeof(unsigned char));
    memcpy(&requestBuffer[1+4+1], &relFileNode.spcNode, 4);
    memcpy(&requestBuffer[1+4+1+4], &relFileNode.dbNode, 4);
    memcpy(&requestBuffer[1+4+1+4+4], &relFileNode.relNode, 4);
    memcpy(&requestBuffer[1+4+1+4+4+4], &blockNumber, 4);

    int targetMsgLen = 1+4+1+4+4+4+4;
    int sendLen = 0;
    while(sendLen < targetMsgLen) {
        int writeLen = write(serverPipe[replayPid][1], &requestBuffer[sendLen], targetMsgLen-sendLen);
        sendLen+=writeLen;
    }



    // ------- Read target page from replay process ------
    int recvLen = 0;
    while (recvLen < BLCKSZ) {
        int readLen = read(computePipe[replayPid][0], &buffer[recvLen], BLCKSZ - recvLen);
        if (readLen < 0) {
            if (errno == EAGAIN) {
                continue;
            }
            else {
                printf("Error on read\n");
                exit(1);
            }
        }
        Assert(readLen >= 0);
        recvLen += readLen;
    }

    Assert(recvLen == BLCKSZ);
    pthread_mutex_unlock(&(replayProcessMutex[replayPid]));
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    GetBasePageTime += ((end.tv_sec*1000000+end.tv_usec) - (start.tv_sec*1000000+start.tv_usec));
    GetBasePageCount++;

    struct timeval now;

    if(!initialized3){
        gettimeofday(&output_timing3, NULL);
        initialized3 = 1;
    }
    gettimeofday(&now, NULL);
    if(now.tv_sec-output_timing3.tv_sec >= 5) {

        //printf("walredo_wait_time = %ld\n",walRedoWaitLockTime);
        printf("walredo_getBasePage = %ld, count = %d\n",GetBasePageTime, GetBasePageCount);
        fflush(stdout);
        output_timing3 = now;
    }
#endif
}

void GetPageByLsn(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, XLogRecPtr lsn, char* buffer) {
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    int tid = gettid();
    int replayPid = tid%REPLAY_PROCESS_NUM;
    int gotLock = 0;
    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        if(pthread_mutex_trylock( &(replayProcessMutex[ (replayPid+i)%REPLAY_PROCESS_NUM ]) ) == 0) { // Lock successfully
            gotLock = 1;
            replayPid = (replayPid+i) % REPLAY_PROCESS_NUM;
            break;
        }
    }

    // if we didn't get lock with trylock(), then use block wait lock
    if(gotLock == 0) {
        pthread_mutex_lock(&(replayProcessMutex[replayPid]));
    }



//    pthread_mutex_lock(&replayProcessMutex);

    // ------- Send "ApplyRecordUntil" request to replay process ------
    char requestBuffer[1024];
    int32 msgLen = 0;

    requestBuffer[0] = 'U'; // Request function "ApplyRecordUntil"

    msgLen = 4; // $msgLen itself
    msgLen += sizeof(lsn);  // Add parameter $lsn length
    msgLen = pg_hton32(msgLen);


    //todo temporary
    if(WalRcv != NULL) {
        lsn = WalRcv->flushedUpto;
#ifdef ENABLE_DEBUG_INFO
        printf("read lsn from writtenUpto, lsn = %ld\n", lsn);
#endif
    }
    lsn = pg_hton64(lsn);

    memcpy(&requestBuffer[1], &msgLen, sizeof(msgLen));
    memcpy(&requestBuffer[1+sizeof(msgLen)], &lsn, sizeof(lsn));


    int targetMsgLen = 1+4+sizeof(lsn);

    int sendLen = 0;
    while(sendLen < targetMsgLen) {
        int writeLen = write(serverPipe[replayPid][1], &requestBuffer[sendLen], targetMsgLen-sendLen);

        sendLen+=writeLen;
    }


    // ------- Send "GetPage" request to replay process ------
    requestBuffer[0] = 'G';

    msgLen = 4; // $msgLen itself
    msgLen += sizeof(unsigned char); //forknum
    msgLen += 4; // $spc
    msgLen += 4; // $db
    msgLen += 4; // $rel
    msgLen += 4; // $blknum
    msgLen = pg_hton32(msgLen);

    // prepare parameters to network encoding
    relFileNode.spcNode = pg_hton32(relFileNode.spcNode);
    relFileNode.dbNode = pg_hton32(relFileNode.dbNode);
    relFileNode.relNode = pg_hton32(relFileNode.relNode);
    blockNumber = pg_hton32(blockNumber);

    memcpy(&requestBuffer[1], &msgLen, sizeof(msgLen));
    memcpy(&requestBuffer[1+4], &forkNumber, sizeof(unsigned char));
    memcpy(&requestBuffer[1+4+1], &relFileNode.spcNode, 4);
    memcpy(&requestBuffer[1+4+1+4], &relFileNode.dbNode, 4);
    memcpy(&requestBuffer[1+4+1+4+4], &relFileNode.relNode, 4);
    memcpy(&requestBuffer[1+4+1+4+4+4], &blockNumber, 4);

    targetMsgLen = 1+4+1+4+4+4+4;
    sendLen = 0;
    while(sendLen < targetMsgLen) {
        int writeLen = write(serverPipe[replayPid][1], &requestBuffer[sendLen], targetMsgLen-sendLen);
        sendLen+=writeLen;
    }



    // ------- Read target page from replay process ------
    int recvLen = 0;
    while (recvLen < BLCKSZ) {
        int readLen = read(computePipe[replayPid][0], &buffer[recvLen], BLCKSZ - recvLen);
        if (readLen < 0) {
            if (errno == EAGAIN) {
                continue;
            }
            else {
                printf("Error on read\n");
                exit(1);
            }
        }
        Assert(readLen >= 0);
        recvLen += readLen;
    }

    Assert(recvLen == BLCKSZ);
    pthread_mutex_unlock(&(replayProcessMutex[replayPid]));

#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    GetBasePageTime += ((end.tv_sec*1000000+end.tv_usec) - (start.tv_sec*1000000+start.tv_usec));
    GetBasePageCount++;

    struct timeval now;

    if(!initialized3){
        gettimeofday(&output_timing3, NULL);
        initialized3 = 1;
    }
    gettimeofday(&now, NULL);
    if(now.tv_sec-output_timing3.tv_sec >= 5) {

        //printf("walredo_wait_time = %ld\n",walRedoWaitLockTime);
        printf("walredo_getPageByLsn = %ld, count = %d\n",GetBasePageTime, GetBasePageCount);

        fflush(stdout);
        output_timing3 = now;
    }
#endif
}

void SyncReplayProcess() {
    int tid = gettid();
    int replayPid = tid%REPLAY_PROCESS_NUM;
    int gotLock = 0;
    for(int i = 0; i < REPLAY_PROCESS_NUM; i++) {
        if(pthread_mutex_trylock( &(replayProcessMutex[ (replayPid+i)%REPLAY_PROCESS_NUM ]) ) == 0) { // Lock successfully
            gotLock = 1;
            replayPid = (replayPid+i) % REPLAY_PROCESS_NUM;
            break;
        }
    }

    // if we didn't get lock with trylock(), then use block wait lock
    if(gotLock == 0) {
        pthread_mutex_lock(&(replayProcessMutex[replayPid]));
    }

    // ------- Send "ApplyRecordUntil" request to replay process ------
    XLogRecPtr lsn = 0;
    char requestBuffer[1024];
    int32 msgLen = 0;

    requestBuffer[0] = 'U'; // Request function "ApplyRecordUntil"

    msgLen = 4; // $msgLen itself
    msgLen += sizeof(lsn);  // Add parameter $lsn length
    msgLen = pg_hton32(msgLen);

    //todo temporary
    if(WalRcv != NULL) {
        lsn = WalRcv->flushedUpto;
#ifdef ENABLE_DEBUG_INFO
        printf("read lsn from writtenUpto, lsn = %ld\n", lsn);
#endif
    }
    lsn = pg_hton64(lsn);

    memcpy(&requestBuffer[1], &msgLen, sizeof(msgLen));
    memcpy(&requestBuffer[1+sizeof(msgLen)], &lsn, sizeof(lsn));

    int targetMsgLen = 1+4+sizeof(lsn);

    int sendLen = 0;
    while(sendLen < targetMsgLen) {
        int writeLen = write(serverPipe[1], &requestBuffer[sendLen], targetMsgLen-sendLen);
        sendLen+=writeLen;
    }

    // -------- Send "SyncLsnReplay" request to replay process ---------
    requestBuffer[0] = 'S'; // Request function "SyncLsnReplay"

    msgLen = 4; // $msgLen itself
    msgLen += sizeof(lsn);  // Add parameter $lsn length
    msgLen = pg_hton32(msgLen);

    lsn = pg_hton64(lsn);
    memcpy(&requestBuffer[1], &msgLen, sizeof(msgLen));
    memcpy(&requestBuffer[1+sizeof(msgLen)], &lsn, sizeof(lsn));

    targetMsgLen = 1+4+sizeof(lsn);

    sendLen = 0;
    while(sendLen < targetMsgLen) {
        int writeLen = write(serverPipe[1], &requestBuffer[sendLen], targetMsgLen-sendLen);
        sendLen+=writeLen;
    }


    // --------- Receive "ok" flag from replay process ----------
    char buffer[8];
    int recvLen = 0;
    while (recvLen < 2) {
#ifdef ENABLE_DEBUG_INFO
        printf("%s Start reading\n", __func__ );
        fflush(stdout);
#endif
        int readLen = read(computePipe[0], &buffer[recvLen], 2 - recvLen);
        if (readLen < 0) {
            if (errno == EAGAIN) {
                continue;
            }
            else {
                printf("Error on read\n");
                exit(1);
            }
        }
        Assert(readLen >= 0);
        recvLen += readLen;
    }

    if(recvLen != 2) {
        printf("%s, Error reply, expected len 2, received len %d\n", __func__ , recvLen);
        exit(1);
    }


    pthread_mutex_unlock(&(replayProcessMutex[replayPid]));
}

pthread_t XlogStartupTid2 = 0;
void BackgroundHashMapCleanPageVersion() {
    BackgroundHashMapCleanRocksdb(pageVersionHashMap);
}
#include <sys/time.h>
extern XLogRecPtr XLogParseUpto;
void* RecordReplayProgress() {
    struct timeval now;
    while(1) {
        usleep(100*1000);
        gettimeofday(&now, NULL);
        printf("RecordReplayProgress, parsed=%ld, sec=%lu, us=%lu\n", XLogParseUpto, now.tv_sec, now.tv_usec);
        fflush(stdout);
    }
}


void
RpcServerMain(int argc, char *argv[],
              const char *dbname,
              const char *username) {

#ifdef ENABLE_DEBUG_INFO
    printf("%s start, pid = %d\n", __func__ , getpid());
    fflush(stdout);
#endif
    HashMapInit(&pageVersionHashMap, 1023);
    RelSizePthreadLockInit();

    for(int i = 0; i < 5; i++) {
        printf("%s start background replayer %d\n", __func__ , i);
        pthread_t tempTid;
        pthread_create(&tempTid, NULL, (void*) BackgroundHashMapCleanPageVersion, NULL);
    }
#ifdef ENABLE_DEBUG_INFO
    printf("%s HashMapAddress = %p\n", __func__ , pageVersionHashMap);
    fflush(stdout);
#endif

    /***********Clean environment before exit********/
    struct sigaction catchTermSig;
    struct sigaction oldSigAction;
    catchTermSig.sa_handler = sigIntHandler;
    int setTermSigSucc = sigaction(SIGINT, &catchTermSig, &oldSigAction);
    if(setTermSigSucc == -1) {
        printf("Set signal action failed \n");
    }
    /***************Clean register complete*********/
    IsRpcServer = 1;

    //Init pid to accept child process signals
    InitProcessGlobals();
    PostmasterPid = MyProcPid;

    getInstallationPaths(argv[0]);

    //Init MemoryContext as PMaster role
    MemoryContextInit();
    PostmasterContext = AllocSetContextCreate(TopMemoryContext,
                                              "Postmaster",
                                              ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(PostmasterContext);

    init_ps_display("postgres RpcMain function");

    pqinitmask();
//    PG_SETMASK(&BlockSig);
    pqsignal_pm(SIGINT, proc_die); /* send SIGTERM and shut down */
    pqsignal_pm(SIGQUIT, proc_die);	/* send SIGQUIT and die */
    pqsignal_pm(SIGTERM, proc_die);	/* wait for children and shut down */

    pqsignal_pm(SIGUSR1, sigusr1_handler);	/* message from child process */

    InitializeGUCOptions();

    if (!SelectConfigFiles(NULL, progname))
        proc_exit(1);    checkDataDir();
    ChangeToDataDir();
    CreateDataDirLockFile(true);

    InitKvStore();

    LocalProcessControlFile(false);

    process_shared_preload_libraries();

    set_max_safe_fds();
    InitializeMaxBackends();

    //Create shared memory before start child process
//    CreateSharedMemoryAndSemaphores();
    InitPostmasterDeathWatchHandle();
    BaseInit();

    InitBufferPoolBackend();
    InitProcess();
    InitializeLatchSupport();

    InitPostmasterChild_Thread();
    InitAuxiliaryProcess();

//    StartupPid = StartChildProcess(StartupProcess);
    pthread_create(&XlogStartupTid2, NULL, (void*)StartupXLOG, NULL);
//    pthread_t tempTid;
//    pthread_create(&tempTid, NULL, (void*)RecordReplayProgress, NULL);
//    pthread_create(&XlogStartupTid2, NULL, (void*)StartupProcessMain, NULL);

    StartWalRedoProcess(argc, argv, dbname, username);

    CreateAuxProcessResourceOwner();

    /*************************BaseInit**********************************/
    //Here is the content of BaseInit(). We move the CreateSharedMemoryAndSemaphores to ahead
    //The RpcServer will call ReadBuffer, BaseInit() will prepare the environment
//    DebugFileOpen();
    /* Do local initialization of file, storage and buffer managers */
//    InitFileAccess();
//    InitSync();
//    smgrinit();
//    InitBufferPoolAccess();
    /************************End BaseInit**************************************/

    InitProcess();

//    pthread_t threadID[200];
//    for (int j = 0; j < 1000; ++j) {
//        printf("000 %d \n", j);
//        fflush(stdout);
//        for(int i = 0; i < 200; i++) {
//            pthread_create(&threadID[i], NULL, (void*)mdtest, NULL);
//        }
//        for(int i = 0; i < 20; i++) {
//            pthread_join(threadID[i], NULL);
//        }
//    }



    RpcServerLoop();
    proc_exit(0);
}




pthread_t XlogStartupTid = 0;
pthread_t WalRcvTid = 0;
void
RpcServerMain_(int argc, char *argv[],
              const char *dbname,
              const char *username) {

    IsRpcServer = 1;

    MemoryContextInit();
    /* Initialize startup process environment if necessary. */
    InitStandaloneProcess(argv[0]);

    SetProcessingMode(InitProcessing);

    struct sigaction catchTermSig;
    struct sigaction oldSigAction;
    catchTermSig.sa_handler = sigIntHandler;
    int setTermSigSucc = sigaction(SIGINT, &catchTermSig, &oldSigAction);
    if(setTermSigSucc == -1) {
        printf("Set signal action failed \n");
    }

    /*
     * Set default values for command-line options.
     */
    InitializeGUCOptions();
//    set_debug_options(5, PGC_POSTMASTER, PGC_S_ARGV);
    /* Acquire configuration parameters */
    if (!SelectConfigFiles(NULL, progname))
        proc_exit(1);

    /* Acquire configuration parameters */
//    SetDataDir("~/PgDir/data_dir/db2/");
    /*
	 * Validate we have been given a reasonable-looking DataDir and change into it.
	 */
    checkDataDir();
    ChangeToDataDir();

    LocalProcessControlFile(false);
    /*
     * Create lockfile for data directory.
     */
    CreateDataDirLockFile(false);

    process_shared_preload_libraries();

    /* Initialize MaxBackends (if under postmaster, was done already) */
    InitializeMaxBackends();

    BaseInit();
//    InitAuxiliaryProcess();

    InitBufferPoolBackend();
    InitProcess();
//    CreateSharedMemoryAndSemaphores();
//    DebugFileOpen();

    /* Do local initialization of file, storage and buffer managers */
//    InitFileAccess();
//    CreateSharedMemoryAndSemaphores();

//    CreateLWLocks();
    // Prepare resource owner for BufAlloc()
    CreateAuxProcessResourceOwner();


    pgstat_initialize();
    pgstat_bestart();
    SetProcessingMode(NormalProcessing);

    pthread_create(&XlogStartupTid, NULL, (void*)StartupXLOG, NULL);

    RpcServerLoop();
    proc_exit(0);
}
