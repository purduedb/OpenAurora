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

void RpcInit()
{
    printf("%s Start\n", __func__ );
    fflush(stdout);

    int myPid = getpid();
    if(myPid == MyPid)
        return;
    rpcsocket = std::make_shared<TSocket>(PRIMARY_NODE_IP, 9090);
    rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
    rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
    client = new DataPageAccessClient(rpcprotocol);
    rpctransport->open();

    printf("%s transport created\n", __func__ );
    fflush(stdout);

    MyPid = myPid;
}

void RpcClose() {
    int myPid = getpid();
    printf("%s Start\n", __func__ );
    fflush(stdout);
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
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d, blk=%u\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forkNum, blockNum);
    fflush(stdout);

    RpcInit();

    _Page _return;
    int32_t _forkNum, _blkNum, _relpersistence, _readBufferMode;

    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);
    _forkNum = forkNum;
    _blkNum = blockNum;
    _relpersistence = (int32_t)relpersistence;
    _readBufferMode = mode;

    client->ReadBufferCommon(_return, _reln, _relpersistence, _forkNum, _blkNum, _readBufferMode);

    _return.copy(buff, BLCKSZ);

    printf("%s End\n", __func__ );
    fflush(stdout);
}

void RpcMdRead(char* buff, SMgrRelation reln, ForkNumber forknum, BlockNumber blknum) {
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d, blk=%u\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, blknum);
    fflush(stdout);

    RpcInit();
    _Page _return;
    int32_t _forkNum, _blkNum;
    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);

//    printf("%s %s %d , spcID = %d, dbID = %d, tabID = %d, fornum = %d, blkNum = %d\n", __func__ , __FILE__, __LINE__,
//           _reln._spc_node, _reln._db_node, _reln._rel_node, forknum, blknum );
    _forkNum = forknum;
    _blkNum = blknum;

    client->RpcMdRead(_return, _reln, _forkNum, _blkNum);
    _return.copy(buff, BLCKSZ);

    printf("%s End\n", __func__ );
    fflush(stdout);
    return;
}

int32_t RpcMdExists(SMgrRelation reln, int32_t forknum) {
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum);
    fflush(stdout);

    RpcInit();
    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);
    int32_t _forknum = forknum;

    int32_t result = client->RpcMdExists(_reln, _forknum);

    printf("%s End, exist = %d\n", __func__ , result);
    fflush(stdout);
    return result;
}

int32_t RpcMdNblocks(SMgrRelation reln, int32_t forknum) {
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum);
    fflush(stdout);

    RpcInit();

    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);
    int32_t _forknum = forknum;

    int32_t result = client->RpcMdNblocks(_reln, _forknum);

    printf("%s End, result = %d\n", __func__ , result);
    fflush(stdout);

    return result;
}

void RpcMdCreate(SMgrRelation reln, int32_t forknum, int32_t isRedo) {
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum);
    fflush(stdout);

    RpcInit();

    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);
    int32_t _forknum = forknum;
    int32_t _isRedo = isRedo;

    client->RpcMdCreate(_reln, _forknum, _isRedo);
    printf("%s End\n", __func__ );
    fflush(stdout);
}

void RpcMdExtend(SMgrRelation reln, int32_t forknum, int32_t blknum, char* buff, int32_t skipFsync) {
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d, blk=%u pageIsNew=%d\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, blknum, PageIsNew(buff));
    fflush(stdout);

    RpcInit();
    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);
    int32_t _forknum = forknum;
    int32_t _blknum = blknum;
    int32_t _skipFsync = skipFsync;
    _Page _buff;

    _buff.assign(buff, BLCKSZ);

    client->RpcMdExtend(_reln, _forknum, _blknum, _buff, _skipFsync);

    printf("%s End\n", __func__ );
    fflush(stdout);
}

void RpcMdTruncate(SMgrRelation reln, int32_t forknum, int32_t blknum) {
    printf("%s Start, spc=%u, db=%u, rel=%u, forkNum=%d, blk=%u\n", __func__, reln->smgr_rnode.node.spcNode,
           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode, forknum, blknum);
    fflush(stdout);

    RpcInit();
    _Smgr_Relation _reln = MarshalSmgrRelation2RPC(reln);
    int32_t _forknum = forknum;
    int32_t _blknum = blknum;

    client->RpcTruncate(_reln, _forknum, _blknum);
    printf("%s End\n", __func__ );
    fflush(stdout);
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
