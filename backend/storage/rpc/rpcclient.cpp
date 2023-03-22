/*-------------------------------------------------------------------------
 *
 * rpcclient.c
 *	  This code manages gRPC interfaces that read and write data pages to storage nodes.
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/rpc/rpcclient.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>

#include <iostream>
#include <fstream>
#include <string.h>

#include "postgres.h"
#include "storage/rpcclient.h"
#include "DataPageAccess.h"
#include "storage/copydir.h"
#include "storage/smgr.h"
#include "catalog/storage.h"
#include "access/xlog.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

/*** behavior for mdopen & _mdfd_getseg ***/
/* ereport if segment not present */
#define EXTENSION_FAIL				(1 << 0)
/* return NULL if segment not present */
#define EXTENSION_RETURN_NULL		(1 << 1)
/* create new segments as needed */
#define EXTENSION_CREATE			(1 << 2)
/* create new segments if needed during recovery */
#define EXTENSION_CREATE_RECOVERY	(1 << 3)
/*
 * Allow opening segments which are preceded by segments smaller than
 * RELSEG_SIZE, e.g. inactive segments (see above). Note that this breaks
 * mdnblocks() and related functionality henceforth - which currently is ok,
 * because this is only required in the checkpointer which never uses
 * mdnblocks().
 */
#define EXTENSION_DONT_CHECK_SIZE	(1 << 4)

#define PRIMARY_NODE_IP ("10.145.21.36")
#define BLCKSZ 8192

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace tutorial;

std::shared_ptr<TTransport> rpcsocket;
std::shared_ptr<TTransport> rpctransport;
std::shared_ptr<TProtocol> rpcprotocol;
DataPageAccessClient *client=NULL;

int IsRpcClient = 0;
pid_t MyPid = 0;

//#define DEBUG_TIMING
//#define DEBUG_TIMING2
#ifdef DEBUG_TIMING

#include <sys/time.h>
#include <pthread.h>
#include <cstdlib>

int initialized5 = 0;
struct timeval output_timing5;

pthread_mutex_t timing_mutex5 = PTHREAD_MUTEX_INITIALIZER;
long client_nblocks_time[16];
long client_nblocks_count[16];

long client_readbuffer_time[16];
long client_readbuffer_count[16];

long client_extend_time[16];
long client_extend_count[16];

long client_exist_time[16];
long client_exist_count[16];

long client_create_time[16];
long client_create_count[16];

long client_truncate_time[16];
long client_truncate_count[16];

long client_read_time[16];
long client_read_count[16];

void PrintTimingResult5() {
    struct timeval now;

    if(!initialized5){
        gettimeofday(&output_timing5, NULL);
        initialized5 = 1;

        memset(client_nblocks_time, 0, 16*sizeof(client_nblocks_time[0]));
        memset(client_nblocks_count, 0, 16*sizeof(client_nblocks_count[0]));

        memset(client_readbuffer_time, 0, 16*sizeof(client_readbuffer_time[0]));
        memset(client_readbuffer_count, 0, 16*sizeof(client_readbuffer_count[0]));

        memset(client_extend_time, 0, 16*sizeof(client_nblocks_time[0]));
        memset(client_extend_count, 0, 16*sizeof(client_nblocks_count[0]));

        memset(client_exist_time, 0, 16*sizeof(client_nblocks_time[0]));
        memset(client_exist_count, 0, 16*sizeof(client_nblocks_count[0]));

        memset(client_create_time, 0, 16*sizeof(client_nblocks_time[0]));
        memset(client_create_count, 0, 16*sizeof(client_nblocks_count[0]));

        memset(client_truncate_time, 0, 16*sizeof(client_nblocks_time[0]));
        memset(client_truncate_count, 0, 16*sizeof(client_nblocks_count[0]));

        memset(client_read_time, 0, 16*sizeof(client_nblocks_time[0]));
        memset(client_read_count, 0, 16*sizeof(client_nblocks_count[0]));
    }


    gettimeofday(&now, NULL);

    if(now.tv_sec-output_timing5.tv_sec >= 5) {
        output_timing5 = now;

        for(int i = 0 ; i < 9; i++) {
            if(client_nblocks_count[i] == 0)
                continue;
            printf("client_nblocks_%d = %ld\n",i,  client_nblocks_time[i]/client_nblocks_count[i]);
            printf("client_nblocks_%d = %ld, count = %ld\n",i,  client_nblocks_time[i], client_nblocks_count[i]);
            fflush(stdout);
        }

        for(int i = 0 ; i < 9; i++) {
            if(client_readbuffer_count[i] == 0)
                continue;
            printf("client_readbuffer_%d = %ld\n",i,  client_readbuffer_time[i]/client_readbuffer_count[i]);
            printf("client_readbuffer_%d = %ld, count = %ld\n",i,  client_readbuffer_time[i], client_readbuffer_count[i]);
            fflush(stdout);
        }

        for(int i = 0 ; i < 9; i++) {
            if(client_extend_count[i] == 0)
                continue;
            printf("client_extend_%d = %ld\n",i,  client_extend_time[i]/client_extend_count[i]);
            printf("client_extend_%d = %ld, count = %ld\n",i,  client_extend_time[i], client_extend_count[i]);
            fflush(stdout);
        }

        for(int i = 0 ; i < 9; i++) {
            if(client_exist_count[i] == 0)
                continue;
            printf("client_exist_%d = %ld\n",i,  client_exist_time[i]/client_exist_count[i]);
            printf("client_exist_%d = %ld, count = %ld\n",i,  client_exist_time[i], client_exist_count[i]);
            fflush(stdout);
        }

        for(int i = 0 ; i < 9; i++) {
            if(client_create_count[i] == 0)
                continue;
            printf("client_create_%d = %ld\n",i,  client_create_time[i]/client_create_count[i]);
            printf("client_create_%d = %ld, count = %ld\n",i,  client_create_time[i], client_create_count[i]);
            fflush(stdout);
        }

        for(int i = 0 ; i < 9; i++) {
            if(client_truncate_count[i] == 0)
                continue;
            printf("client_truncate_%d = %ld\n",i,  client_truncate_time[i]/client_truncate_count[i]);
            printf("client_truncate_%d = %ld, count = %ld\n",i,  client_truncate_time[i], client_truncate_count[i]);
            fflush(stdout);
        }

        for(int i = 0 ; i < 9; i++) {
            if(client_truncate_count[i] == 0)
                continue;
            printf("client_read_%d = %ld\n",i,  client_read_time[i]/client_read_count[i]);
            printf("client_read_%d = %ld, count = %ld\n",i,  client_read_time[i], client_read_count[i]);
            fflush(stdout);
        }

    }
}

#define START_TIMING(start_p)  \
do {                         \
    gettimeofday(start_p, NULL); \
} while(0);

#define RECORD_TIMING(start_p, end_p, global_timing, global_count) \
do { \
    gettimeofday(end_p, NULL); \
    pthread_mutex_lock(&timing_mutex5); \
    (*global_timing) += ((*end_p.tv_sec*1000000+*end_p.tv_usec) - (*start_p.tv_sec*1000000+*start_p.tv_usec))/1000; \
    (*global_count)++;                                                               \
    pthread_mutex_unlock(&timing_mutex5); \
    PrintTimingResult5(); \
    gettimeofday(start_p, NULL); \
} while (0);


#endif

void RpcInit()
{
#ifdef ENABLE_DEBUG_INFO
    printf("%s Start\n", __func__ );
    fflush(stdout);
#endif

    int myPid = getpid();
    if(myPid == MyPid)
        return;
    rpcsocket = std::make_shared<TSocket>(PRIMARY_NODE_IP, 9090);
    rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
    rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
    client = new DataPageAccessClient(rpcprotocol);
    rpctransport->open();

#ifdef ENABLE_DEBUG_INFO
    printf("%s transport created\n", __func__ );
    fflush(stdout);
#endif

    MyPid = myPid;
}

void RpcTransportClose() {
    int myPid = getpid();
#ifdef ENABLE_DEBUG_INFO
    printf("%s Start\n", __func__ );
    fflush(stdout);
#endif
    // Only close transport created by itself
    if(myPid == MyPid)
        rpctransport->close();
}

_Smgr_Relation MarshalSmgrRelation2RPC(SMgrRelation reln) {
    _Smgr_Relation _reln;
    _reln._rel_node = reln->smgr_rnode.node.relNode;
    _reln._spc_node = reln->smgr_rnode.node.spcNode;
    _reln._db_node = reln->smgr_rnode.node.dbNode;
    _reln._backend_id = reln->smgr_rnode.backend;
    return _reln;
}

//#define ENABLE_FUNCTION_TIMING
#ifdef ENABLE_FUNCTION_TIMING
#include <sys/time.h>
#include <pthread.h>
#include <cstdlib>

class FunctionTiming {
public:
    struct timeval start;
    char* funcname;
    FunctionTiming(char* paraFuncName) {
        funcname = paraFuncName;
        gettimeofday(&start, NULL);
    }
    inline ~FunctionTiming() {
        struct timeval end;
        gettimeofday(&end, NULL);
        printf("%s function timeing = %ld us\n", funcname,
               (end.tv_sec*1000000+end.tv_usec) - (start.tv_sec*1000000+start.tv_usec));                            \
    }
};
#endif

void RpcReadBuffer_common(char* buff, SMgrRelation reln, char relpersistence, ForkNumber forkNum,
                          BlockNumber blockNum, ReadBufferMode mode) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
    printf("%s start, pid = %d, spc = %ld, db = %ld, rel = %ld, fork = %d, blk = %ld, pid = %d\n", __func__ , getpid(),
           reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forkNum, blockNum, getpid());
    ::fflush(stdout);
#endif
#ifdef DEBUG_TIMING2
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif

#ifdef ENABLE_DEBUG_INFO
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d, blk=%u, lsn = %lu\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forkNum, blockNum, GetLogWrtResultLsn());
    fflush(stdout);
#endif

#ifdef DEBUG_TIMING
    struct timeval start, end;
    START_TIMING(&start);
#endif
    RpcInit();

    _Page _return;
    int32_t _forkNum, _blkNum, _relpersistence, _readBufferMode;

    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);
    _forkNum = forkNum;
    _blkNum = blockNum;
    _relpersistence = (int32_t)relpersistence;
    _readBufferMode = mode;

#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_readbuffer_time[0]), &(client_readbuffer_count[0]))
#endif
    client->ReadBufferCommon(_return, _reln, _relpersistence, _forkNum, _blkNum, _readBufferMode, GetLogWrtResultLsn());

    _return.copy(buff, BLCKSZ);

#ifdef DEBUG_TIMING2
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif

#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_readbuffer_time[1]), &(client_readbuffer_count[1]))
#endif
#ifdef ENABLE_DEBUG_INFO
    printf("%s End, spc=%u, db=%u, rel=%u, forkNum=%d, blk=%u, lsn = %lu\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forkNum, blockNum, GetLogWrtResultLsn());
    fflush(stdout);
#endif
}

void RpcMdRead(char* buff, SMgrRelation reln, ForkNumber forknum, BlockNumber blknum) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING2
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
#ifdef ENABLE_DEBUG_INFO
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d, blk=%u\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, blknum);
    fflush(stdout);
#endif

#ifdef DEBUG_TIMING
    struct timeval start, end;
    START_TIMING(&start);
#endif
    RpcInit();
    _Page _return;
    int32_t _forkNum, _blkNum;
    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);

//    printf("%s %s %d , spcID = %d, dbID = %d, tabID = %d, fornum = %d, blkNum = %d\n", __func__ , __FILE__, __LINE__,
//           _reln._spc_node, _reln._db_node, _reln._rel_node, forknum, blknum );
    _forkNum = forknum;
    _blkNum = blknum;

#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_read_time[0]), &(client_read_count[0]))
#endif
    client->RpcMdRead(_return, _reln, _forkNum, _blkNum, GetLogWrtResultLsn());
    _return.copy(buff, BLCKSZ);

#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_read_time[1]), &(client_read_count[1]))
#endif
#ifdef ENABLE_DEBUG_INFO
    printf("%s End\n", __func__ );
    fflush(stdout);
#endif
#ifdef DEBUG_TIMING2
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return;
}

int32_t RpcMdExists(SMgrRelation reln, int32_t forknum) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING2
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
#ifdef ENABLE_DEBUG_INFO
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d, lsn=%lu\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, GetLogWrtResultLsn());
    fflush(stdout);
#endif

#ifdef DEBUG_TIMING
    struct timeval start, end;
    START_TIMING(&start);
#endif
    RpcInit();
    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);
    int32_t _forknum = forknum;

#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_exist_time[0]), &(client_exist_count[0]))
#endif
    int32_t result = client->RpcMdExists(_reln, _forknum, GetLogWrtResultLsn());

#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_exist_time[1]), &(client_exist_count[1]))
#endif
#ifdef ENABLE_DEBUG_INFO
    printf("%s End, exist = %d spc=%u, db=%u, rel=%u, forkNum=%d, lsn=%lu\n", __func__, result, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, GetLogWrtResultLsn());
    fflush(stdout);
#endif
#ifdef DEBUG_TIMING2
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcMdNblocks(SMgrRelation reln, int32_t forknum) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING2
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
#ifdef ENABLE_DEBUG_INFO
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d, lsn=%lu\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, GetLogWrtResultLsn());
    fflush(stdout);
#endif

#ifdef DEBUG_TIMING
    struct timeval start, end;
    START_TIMING(&start);
#endif
    RpcInit();

    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);
    int32_t _forknum = forknum;

#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_nblocks_time[0]), &(client_nblocks_count[0]))
#endif
    int32_t result = client->RpcMdNblocks(_reln, _forknum, GetLogWrtResultLsn());

#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_nblocks_time[1]), &(client_nblocks_count[1]))
#endif
#ifdef ENABLE_DEBUG_INFO
    printf("%s End, result = %d,  spc=%u, db=%u, rel=%u, forkNum=%d, lsn=%lu\n", __func__,  result, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, GetLogWrtResultLsn());
    fflush(stdout);
#endif

#ifdef DEBUG_TIMING2
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

void RpcMdCreate(SMgrRelation reln, int32_t forknum, int32_t isRedo) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING2
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
#ifdef ENABLE_DEBUG_INFO
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d, lsn=%lu\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, GetLogWrtResultLsn());
    fflush(stdout);
#endif

#ifdef DEBUG_TIMING
    struct timeval start, end;
    START_TIMING(&start);
#endif
    RpcInit();

    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);
    int32_t _forknum = forknum;
    int32_t _isRedo = isRedo;

#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_create_time[0]), &(client_create_count[0]))
#endif
    client->RpcMdCreate(_reln, _forknum, _isRedo, GetLogWrtResultLsn());
#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_create_time[1]), &(client_create_count[1]))
#endif
#ifdef ENABLE_DEBUG_INFO
    printf("%s End, spc=%u, db=%u, rel=%u, forkNum=%d, lsn=%lu\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, GetLogWrtResultLsn());
    fflush(stdout);
#endif
#ifdef DEBUG_TIMING2
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
}

void RpcMdExtend(SMgrRelation reln, int32_t forknum, int32_t blknum, char* buff, int32_t skipFsync) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING2
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
#ifdef ENABLE_DEBUG_INFO
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d, blk=%u pageIsNew=%d\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, blknum, PageIsNew(buff));
    fflush(stdout);
#endif

#ifdef DEBUG_TIMING
    struct timeval start, end;
    START_TIMING(&start);
#endif
    RpcInit();
    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);
    int32_t _forknum = forknum;
    int32_t _blknum = blknum;
    int32_t _skipFsync = skipFsync;
    _Page _buff;

    _buff.assign(buff, BLCKSZ);

#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_extend_time[0]), &(client_extend_count[0]))
#endif
    client->RpcMdExtend(_reln, _forknum, _blknum, _buff, _skipFsync, GetLogWrtResultLsn());

#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_extend_time[1]), &(client_extend_count[1]))
#endif
#ifdef ENABLE_DEBUG_INFO
    printf("%s End, spc=%u, db=%u, rel=%u, forkNum=%d, blk=%u pageIsNew=%d\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, blknum, PageIsNew(buff));
    fflush(stdout);
#endif
#ifdef DEBUG_TIMING2
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
}

void RpcMdTruncate(SMgrRelation reln, int32_t forknum, int32_t blknum) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING2
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
#ifdef ENABLE_DEBUG_INFO
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d, blk=%u\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, blknum);
    fflush(stdout);
#endif

#ifdef DEBUG_TIMING
    struct timeval start, end;
    START_TIMING(&start);
#endif
    RpcInit();
    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);
    int32_t _forknum = forknum;
    int32_t _blknum = blknum;

#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_truncate_time[0]), &(client_truncate_count[0]))
#endif
    client->RpcTruncate(_reln, _forknum, _blknum, GetLogWrtResultLsn());
#ifdef DEBUG_TIMING
    RECORD_TIMING(&start, &end, &(client_truncate_time[1]), &(client_truncate_count[1]))
#endif
#ifdef ENABLE_DEBUG_INFO
    printf("%s End\n", __func__ );
    fflush(stdout);
#endif
#ifdef DEBUG_TIMING2
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
}


// Following are Rpc interfaces for fd.c

void RpcFileClose(const int fd) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );
    //rpctransport->open();
    client->RpcFileClose((_File)fd);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return;
}

void RpcTablespaceCreateDbspace(const int64_t _spcnode, const int64_t _dbnode, const bool isRedo) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );
//    printf("[%s] spcNode = %ld, dbNode = %ld, isRedo = %d\n", __func__ , _spcnode, _dbnode, isRedo);
    //rpctransport->open();
    client->RpcTablespaceCreateDbspace(_spcnode, _dbnode, isRedo);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return;
}

int RpcPathNameOpenFile(const char* path, const int32_t _flag) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );
//    printf("[%s] %s %s %d \n", __func__ , _func, _file, _line);
    _File result = 0;
    _Path _path;
    _path.assign(path);
    //rpctransport->open();
    result = client->RpcPathNameOpenFile(_path, _flag);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcFileWrite(const int _fd, const char* page, const int32_t _amount, const int64_t _seekpos, const int32_t _wait_event_info) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );
    int32_t result = 0;
    _Page _page;
    _page.assign(page, BLCKSZ);
    //rpctransport->open();
    result = client->RpcFileWrite(_fd, _page, _amount, _seekpos, _wait_event_info);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

char* RpcFilePathName(const int _fd) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );
    _Path _return;

    char * filename = (char*) malloc(sizeof(char*)* 1024);

    //rpctransport->open();
    client->RpcFilePathName(_return, _fd);
    _return.copy(filename, _return.length());
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return filename;
}

int RpcFileRead(char *buff, const int _fd, const int32_t _amount,  const int64_t _seekpos, const int32_t _wait_event_info) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();

    _Page _return;
    //rpctransport->open();
    client->RpcFileRead(_return, _fd, _amount, _seekpos, _wait_event_info);
    //rpctransport->close();
    _return.copy(buff, BLCKSZ);
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return _return.length();
}

int32_t RpcFileTruncate(const int _fd, const int64_t _offset) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );
    int32_t result;

    //rpctransport->open();
    result = client->RpcFileTruncate(_fd, _offset);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int64_t RpcFileSize(const int _fd) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start %d , fd = %d\n", __func__ , getpid(), _fd);
//    printf("[%s] %s %s %d\n", __func__, _func, _file, _line);
    int64_t result;
    //rpctransport->open();
    result = client->RpcFileSize(_fd);
    //rpctransport->close();
//    printf("[%s] function end %d , result = %d , fd = %d\n", __func__ , getpid(), result, _fd);
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcFilePrefetch(const int _fd, const int64_t _offset, const int32_t _amount, const int32_t wait_event_info) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );
    int32_t result;
    //rpctransport->open();
    result = client->RpcFilePrefetch(_fd, _offset, _amount, wait_event_info);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

void RpcFileWriteback(const int _fd, const int64_t _offset, const int64_t nbytes, const int32_t wait_event_info) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );
    //rpctransport->open();
    client->RpcFileWriteback(_fd, _offset, nbytes, wait_event_info);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return;
}

int32_t RpcUnlink(const char* filepath) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );
    _Path _path;
    int32_t result = 0;

    _path.assign(filepath);

    //rpctransport->open();
    result = client->RpcUnlink(_path);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcFtruncate(const int _fd, const int64_t _offset) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );
    int32_t result = 0;

    //rpctransport->open();
    result = client->RpcFtruncate(_fd, _offset);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

void RpcInitFile(char* _return, const char* _path) {

}

int RpcOpenTransientFile(const char* filename, const int32_t _fileflags) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );
    _File result = 0;
    _Path _filename;

    _filename.assign(filename);

    //rpctransport->open();
    result = client->RpcOpenTransientFile(_filename, _fileflags);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcCloseTransientFile(const int _fd) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );

    int32_t result;
    //rpctransport->open();
    result = client->RpcCloseTransientFile(_fd);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcFileSync(const int _fd, const int32_t _wait_event_info) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );

    int32_t result;
    //rpctransport->open();
    result = client->RpcFileSync(_fd, _wait_event_info);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcPgPRead(const int _fd, char *p, const int32_t _amount, const int32_t _offset) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();

    int32_t result;
    _Page _return;
    //rpctransport->open();
    client->RpcPgPRead(_return, _fd, _amount, _offset);
    //rpctransport->close();
    _return.copy(p, _return.length());
//    printf("[%s] return value = %d\n", __func__ , (int32_t)_return.length());
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return (int32_t)_return.length();
}

int32_t RpcPgPWrite(const int _fd, char *p, const int32_t _amount, const int32_t _offset) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );

    int32_t result;
    _Page _page;
    _page.assign(p, _amount);
    //rpctransport->open();
    result = client->RpcPgPWrite(_fd, _page, _amount, _offset);
    //rpctransport->close();
//    printf("[%s] result = %d \n", __func__ , result);
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcClose(const int _fd) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );

    int32_t result;
    //rpctransport->open();
    result = client->RpcClose(_fd);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcBasicOpenFile(char *path, int32_t _flags) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start , path = %s\n", __func__ , path);

    int32_t result;
    _Path _path;
    _path.assign(path);
    //rpctransport->open();
    result = client->RpcBasicOpenFile(_path, _flags);
    //rpctransport->close();
//    printf("[%s] result = %d\n", __func__ , result);
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcPgFdatasync(const int32_t _fd) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );

    int32_t result;
    //rpctransport->open();
    result = client->RpcPgFdatasync(_fd);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}


int32_t RpcPgFsyncNoWritethrough(const int32_t _fd) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );

    int32_t result;
    //rpctransport->open();
    result = client->RpcPgFsyncNoWritethrough(_fd);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcLseek(const int32_t _fd, const int64_t _offset, const int32_t _flag) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
    int32_t result;
    //rpctransport->open();
    result = client->RpcLseek(_fd, _offset, _flag);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif

    return result;
}

int RpcStat(const char* path, struct stat* _stat) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
    _Path _path;
    _path.assign(path);
    _Stat_Resp response;
    response._stat_mode = 0;
    response._result = 0;
    //rpctransport->open();
    client->RpcStat(response, _path);
    //rpctransport->close();
    _stat->st_mode = response._stat_mode;
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return response._result;
}

int32_t RpcDirectoryIsEmpty(const char* path) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
    _Path _path;
    _path.assign(path);
    int32_t result;
    //rpctransport->open();
    result = client->RpcDirectoryIsEmpty(_path);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcCopyDir(const char* _src, const char* _dst) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
    // create another copy of initialized-db in compute node
    char temp_src[1024];
    char temp_dst[1024];
    strcpy(temp_src, _src);
    strcpy(temp_dst, _dst);
    copydir(temp_src, temp_dst, false);

//    printf("[%s] src=%s, dst=%s \n", __func__, _src, _dst);
    _Path _path_src, _path_dst;
    _path_src.assign(_src);
    _path_dst.assign(_dst);
    int32_t result;
    //rpctransport->open();
    result = client->RpcCopyDir(_path_src, _path_dst);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcPgFsync(const int32_t _fd) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
    int32_t result;
    //rpctransport->open();
    result = client->RpcPgFsync(_fd);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcDurableUnlink(const char * filename, const int32_t _flag) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();
    _Path _fname;
    _fname.assign(filename);

    int32_t result;
    //rpctransport->open();
    result = client->RpcDurableUnlink(_fname, _flag);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;

}

int32_t RpcDurableRenameExcl(const char* oldFname, const char* newFname, const int32_t _elevel) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
#ifdef DEBUG_TIMING
    struct timeval start, end;
    gettimeofday(&start, NULL);
#endif
    RpcInit();

    _Path _oldFname, _newFname;
    _oldFname.assign(oldFname);
    _newFname.assign(newFname);

    int32_t result;
    //rpctransport->open();
    result = client->RpcDurableRenameExcl(_oldFname, _newFname, _elevel);
    //rpctransport->close();
#ifdef DEBUG_TIMING
    gettimeofday(&end, NULL);
    uint64_t usec = (end.tv_sec + end.tv_usec*1000000) - (start.tv_sec + start.tv_usec*1000000);
    printf("%s time cost = %lu\n", __func__ , usec);
    fflush(stdout);
#endif
    return result;
}

int32_t RpcXLogWriteWithPosition(const int _fd, char *p, const int32_t _amount, const int32_t _offset, int startIdx, int blkNum, uint64_t* xlblocks, int xlblocksBufferNum, uint64_t lsn) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
    RpcInit();
//    printf("[%s] function start \n", __func__ );

    int32_t result;
    _Page _page;
    _page.assign(p, _amount);
    vector<int64_t> xlblocksVec;
    for(int i = 0; i < blkNum; i++) {
        xlblocksVec.push_back( xlblocks[(startIdx+i) % xlblocksBufferNum] );
    }

    result = client->RpcXLogWrite(_fd, _page, _amount, _offset, xlblocksVec, blkNum, startIdx, lsn);
    return result;
}

int RpcXLogFileInit(XLogSegNo logsegno, bool *use_existent, bool use_lock) {
#ifdef ENABLE_FUNCTION_TIMING
    FunctionTiming functionTiming(const_cast<char *>(__func__));
#endif
    RpcInit();
    int64_t _logsegno = logsegno;
    int _use_existent = *use_existent;
    int _use_lock = use_lock;

    _XLog_Init_File_Resp resp;
    client->RpcXLogFileInit(resp, _logsegno, _use_existent, _use_lock);

    *use_existent = resp._use_existent;
    return resp._fd;
}

//void TryRpcInitFile(_Page& _return, _Path& _path)
//{
//    int trycount=0;
//    int maxcount=3;
//    do{
//        try{
//            rpctransport->open();
//            client->RpcInitFile(_return, _path);
//            rpctransport->close();
//            trycount=maxcount;
//        }catch(TException& tx){
//            std::cout << "ERROR: " << tx.what() << std::endl;
//            rpcsocket = std::make_shared<TSocket>(PRIMARY_NODE_IP, RPCPORT);
//            rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
//            rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
//            delete client;
//            client = new DataPageAccessClient(rpcprotocol);
//
//            trycount++;
//            printf("Try again %d\n", trycount);
//        }
//    }while(trycount < maxcount);
//};
