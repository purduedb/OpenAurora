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

#define DEBUG_TIMING
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

void RpcClose() {
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

void RpcReadBuffer_common(char* buff, SMgrRelation reln, char relpersistence, ForkNumber forkNum,
                          BlockNumber blockNum, ReadBufferMode mode) {
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
    return;
}

int32_t RpcMdExists(SMgrRelation reln, int32_t forknum) {
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
    return result;
}

int32_t RpcMdNblocks(SMgrRelation reln, int32_t forknum) {
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

    return result;
}

void RpcMdCreate(SMgrRelation reln, int32_t forknum, int32_t isRedo) {
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
}

void RpcMdExtend(SMgrRelation reln, int32_t forknum, int32_t blknum, char* buff, int32_t skipFsync) {
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
}

void RpcMdTruncate(SMgrRelation reln, int32_t forknum, int32_t blknum) {
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
