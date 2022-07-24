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

void sigIntHandler(int sig) {
    printf("Start to clean up process\n");
    proc_exit(0);
}

void
RpcServerMain(int argc, char *argv[],
              const char *dbname,
              const char *username) {
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

    /*
     * Create lockfile for data directory.
     */
    CreateDataDirLockFile(false);

    process_shared_preload_libraries();

    /* Initialize MaxBackends (if under postmaster, was done already) */
    InitializeMaxBackends();

    CreateSharedMemoryAndSemaphores();
    DebugFileOpen();

    /* Do local initialization of file, storage and buffer managers */
    InitFileAccess();
//    CreateSharedMemoryAndSemaphores();

//    CreateLWLocks();
    RpcServerLoop();
}
