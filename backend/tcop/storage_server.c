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
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "signal.h"
#include "pgstat.h"
#include <pthread.h>
#include <sys/types.h>
#include "libpq/pqsignal.h"
#include "postmaster/fork_process.h"
#include "bootstrap/bootstrap.h"
#include "storage/sync.h"
#include "tcop/wal_redo.h"
#include "replication/walreceiver.h"
#include "storage/md.h"

void sigIntHandler(int sig) {
    printf("Start to clean up process\n");
    proc_exit(0);
}

int IsRpcServer = 0;
pid_t WalRcvPid = 0;
pid_t StartupPid = 0;


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
    if (CheckPostmasterSignal(PMSIGNAL_START_WALRECEIVER))
    {
        if (WalRcvPid == 0) {
            WalRcvPid = StartChildProcess(WalReceiverProcess);
        }

    }
}

static void
proc_die(SIGNAL_ARGS) {
   exit(0);
}

int serverPipe[2];
int computePipe[2];

static void
StartWalRedoProcess(int argc, char *argv[],
                    const char *dbname,
                    const char *username) {

    if(pipe(serverPipe) == -1)
        printf("Error on pipe\n");

    if(pipe(computePipe) == -1)
        printf("Error on pipe\n");

    __pid_t pid = fork();
    if(pid > 0) {
        close(computePipe[1]); // Close comp's write pipe
        close(serverPipe[0]); //Close server's read pipe

        //todo: should close at the end
        //close(serverPipe[1]);
        //close(computePipe[0]);
    } else if (pid == 0) {
        close(computePipe[0]); //Close comp's read pipe
        close(serverPipe[1]); //Close server's write pipe

//        if(computePipe[1] != STDOUT_FILENO) {
//            if(dup2(computePipe[1], STDOUT_FILENO) == -1)
//                exit(1);
//            if(close(computePipe[1]) == -1)
//                exit(2);
//        }
//
//        if(serverPipe[0] != STDIN_FILENO) {
//            if(dup2(serverPipe[0], STDIN_FILENO) == -1)
//                exit(3);
//            if(close(serverPipe[0]) == -1)
//                exit(4);
//        }


        InitPostmasterChild();

        /* Close the postmaster's sockets */
        ClosePostmasterPorts(false);

        WalRedoMain(argc, argv, dbname, username);

        close(computePipe[1]);
        close(serverPipe[0]);
        exit(0);
    }

}

pthread_mutex_t replayProcessMutex = PTHREAD_MUTEX_INITIALIZER;

void GetPageByLsn(RelFileNode relFileNode, ForkNumber forkNumber, BlockNumber blockNumber, XLogRecPtr lsn, char* buffer) {


    pthread_mutex_lock(&replayProcessMutex);

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
        printf("read lsn from writtenUpto, lsn = %ld\n", lsn);
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
        int writeLen = write(serverPipe[1], &requestBuffer[sendLen], targetMsgLen-sendLen);
        sendLen+=writeLen;
    }



    // ------- Read target page from replay process ------
    int recvLen = 0;
    while (recvLen < BLCKSZ) {
        int readLen = read(computePipe[0], &buffer[recvLen], BLCKSZ - recvLen);
        Assert(readLen >= 0);
        recvLen += readLen;
    }

    Assert(recvLen == BLCKSZ);
    pthread_mutex_unlock(&replayProcessMutex);
}

void SyncReplayProcess() {

    pthread_mutex_lock(&replayProcessMutex);

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
        printf("read lsn from writtenUpto, lsn = %ld\n", lsn);
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
        printf("%s Start reading\n", __func__ );
        fflush(stdout);
        int readLen = read(computePipe[0], &buffer[recvLen], 2 - recvLen);
        if(readLen <= 0) {
            printf("%s read pipe line error, readLen = %d\n", __func__ , readLen);
            exit(0);
        }
        Assert(readLen >= 0);
        recvLen += readLen;
    }

    if(recvLen != 2) {
        printf("%s, Error reply, expected len 2, received len %d\n", __func__ , recvLen);
        exit(1);
    }


    pthread_mutex_unlock(&replayProcessMutex);
}

void
RpcServerMain(int argc, char *argv[],
              const char *dbname,
              const char *username) {

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
    printf("LINE %d \n", __LINE__);
    fflush(stdout);

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

    LocalProcessControlFile(false);

    process_shared_preload_libraries();

    set_max_safe_fds();
    InitializeMaxBackends();

    //Create shared memory before start child process
    CreateSharedMemoryAndSemaphores();
    InitPostmasterDeathWatchHandle();

    StartupPid = StartChildProcess(StartupProcess);

    StartWalRedoProcess(argc, argv, dbname, username);

    CreateAuxProcessResourceOwner();

    /*************************BaseInit**********************************/
    //Here is the content of BaseInit(). We move the CreateSharedMemoryAndSemaphores to ahead
    //The RpcServer will call ReadBuffer, BaseInit() will prepare the environment
    DebugFileOpen();
    /* Do local initialization of file, storage and buffer managers */
    InitFileAccess();
    InitSync();
    smgrinit();
    InitBufferPoolAccess();
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
