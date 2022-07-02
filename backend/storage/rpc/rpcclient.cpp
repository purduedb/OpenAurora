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

#include <unistd.h>
#include <fcntl.h>

#include <iostream>
#include <fstream>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include "storage/rpcclient.h"
#include "DataPageAccess.h"

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

#define PRIMARY_NODE_IP ("127.0.0.1")
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


void RpcInit()
{
    if(client != NULL)
        return;
    rpcsocket = std::make_shared<TSocket>(PRIMARY_NODE_IP, 9090);
    rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
    rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
    client = new DataPageAccessClient(rpcprotocol);
}

void RpcFileClose(const int fd) {
    RpcInit();
    printf("[%s] function start \n", __func__ );
    rpctransport->open();
    client->RpcFileClose((_File)fd);
    rpctransport->close();
    return;
}

void RpcTablespaceCreateDbspace(const int64_t _spcnode, const int64_t _dbnode, const bool isRedo) {
    RpcInit();
    printf("[%s] function start \n", __func__ );
    printf("[%s] spcNode = %ld, dbNode = %ld, isRedo = %d\n", __func__ , _spcnode, _dbnode, isRedo);
    rpctransport->open();
    client->RpcTablespaceCreateDbspace(_spcnode, _dbnode, isRedo);
    rpctransport->close();
    return;
}

int RpcPathNameOpenFile(const char* path, const int32_t _flag) {
    RpcInit();
    printf("[%s] function start \n", __func__ );
    _File result = 0;
    _Path _path;
    _path.assign(path);
    rpctransport->open();
    result = client->RpcPathNameOpenFile(_path, _flag);
    rpctransport->close();
    return result;
}

int32_t RpcFileWrite(const int _fd, const char* page, const int32_t _amount, const int64_t _seekpos, const int32_t _wait_event_info) {
    RpcInit();
    printf("[%s] function start \n", __func__ );
    int32_t result = 0;
    _Page _page;
    _page.assign(page, BLCKSZ);
    rpctransport->open();
    result = client->RpcFileWrite(_fd, _page, _amount, _seekpos, _wait_event_info);
    rpctransport->close();
    return result;
}

char* RpcFilePathName(const int _fd) {
    RpcInit();
    printf("[%s] function start \n", __func__ );
    _Path _return;

    char * filename = (char*) malloc(sizeof(char*)* 1024);

    rpctransport->open();
    client->RpcFilePathName(_return, _fd);
    _return.copy(filename, _return.length());
    rpctransport->close();
    return filename;
}

int RpcFileRead(char *buff, const int _fd, const int32_t _amount,  const int64_t _seekpos, const int32_t _wait_event_info) {
    RpcInit();
    printf("[%s] function start, and fd = %d \n", __func__ , _fd);
    fflush(stdout);
    _Page _return;
    rpctransport->open();
    client->RpcFileRead(_return, _fd, _amount, _seekpos, _wait_event_info);
    rpctransport->close();
    _return.copy(buff, BLCKSZ);
    return _return.length();
}

int32_t RpcFileTruncate(const int _fd, const int64_t _offset) {
    RpcInit();
    printf("[%s] function start \n", __func__ );
    int32_t result;

    rpctransport->open();
    result = client->RpcFileTruncate(_fd, _offset);
    rpctransport->close();
    return result;
}

int64_t RpcFileSize(const int _fd) {
    RpcInit();
    printf("[%s] function start \n", __func__ );
    int64_t result;
    rpctransport->open();
    result = client->RpcFileSize(_fd);
    rpctransport->close();
    return result;
}

int32_t RpcFilePrefetch(const int _fd, const int64_t _offset, const int32_t _amount, const int32_t wait_event_info) {
    RpcInit();
    printf("[%s] function start \n", __func__ );
    int32_t result;
    rpctransport->open();
    result = client->RpcFilePrefetch(_fd, _offset, _amount, wait_event_info);
    rpctransport->close();
    return result;
}

void RpcFileWriteback(const int _fd, const int64_t _offset, const int64_t nbytes, const int32_t wait_event_info) {
    RpcInit();
    printf("[%s] function start \n", __func__ );
    rpctransport->open();
    client->RpcFileWriteback(_fd, _offset, nbytes, wait_event_info);
    rpctransport->close();
    return;
}

int32_t RpcUnlink(const char* filepath) {
    RpcInit();
    printf("[%s] function start \n", __func__ );
    _Path _path;
    int32_t result = 0;

    _path.assign(filepath);

    rpctransport->open();
    result = client->RpcUnlink(_path);
    rpctransport->close();
    return result;
}

int32_t RpcFtruncate(const int _fd, const int64_t _offset) {
    RpcInit();
    printf("[%s] function start \n", __func__ );
    int32_t result = 0;

    rpctransport->open();
    result = client->RpcFileTruncate(_fd, _offset);
    rpctransport->close();
    return result;
}

void RpcInitFile(char* _return, const char* _path) {

}

int RpcOpenTransientFile(const char* filename, const int32_t _fileflags) {
    RpcInit();
    printf("[%s] function start \n", __func__ );
    _File result = 0;
    _Path _filename;

    _filename.assign(filename);

    rpctransport->open();
    result = client->RpcOpenTransientFile(_filename, _fileflags);
    rpctransport->close();
    return result;
}

int32_t RpcCloseTransientFile(const int _fd) {
    RpcInit();
    printf("[%s] function start \n", __func__ );

    int32_t result;
    rpctransport->open();
    result = client->RpcCloseTransientFile(_fd);
    rpctransport->close();
    return result;
}

int32_t RpcFileSync(const int _fd, const int32_t _wait_event_info) {
    RpcInit();
    printf("[%s] function start \n", __func__ );

    int32_t result;
    rpctransport->open();
    result = client->RpcFileSync(_fd, _wait_event_info);
    rpctransport->close();
    return result;
}

int32_t RpcPgPRead(const int _fd, char *p, const int32_t _amount, const int32_t _offset) {
    RpcInit();
    printf("[%s] function start, fd = %d \n", __func__ , _fd);

    int32_t result;
    _Page _return;
    rpctransport->open();
    client->RpcPgPRead(_return, _fd, _amount, _offset);
    rpctransport->close();
    _return.copy(p, _return.length());
    printf("[%s] return value = %d\n", __func__ , (int32_t)_return.length());
    return (int32_t)_return.length();
}

int32_t RpcPgPWrite(const int _fd, char *p, const int32_t _amount, const int32_t _offset) {
    RpcInit();
    printf("[%s] function start \n", __func__ );

    int32_t result;
    _Page _page;
    _page.assign(p, _amount);
    rpctransport->open();
    result = client->RpcPgPWrite(_fd, _page, _amount, _offset);
    rpctransport->close();
    return result;
}

int32_t RpcClose(const int _fd) {
    RpcInit();
    printf("[%s] function start \n", __func__ );

    int32_t result;
    rpctransport->open();
    result = client->RpcClose(_fd);
    rpctransport->close();
    return result;
}

int32_t RpcBasicOpenFile(char *path, int32_t _flags) {
    RpcInit();
    printf("[%s] function start \n", __func__ );

    int32_t result;
    _Path _path;
    _path.assign(path);
    rpctransport->open();
    result = client->RpcBasicOpenFile(_path, _flags);
    rpctransport->close();
    printf("[%s] result = %d\n", __func__ , result);
    return result;
}

int32_t RpcPgFdatasync(const int32_t _fd) {
    RpcInit();
    printf("[%s] function start \n", __func__ );

    int32_t result;
    rpctransport->open();
    result = client->RpcPgFdatasync(_fd);
    rpctransport->close();
    return result;
}


int32_t RpcPgFsyncNoWritethrough(const int32_t _fd) {
    RpcInit();
    printf("[%s] function start \n", __func__ );

    int32_t result;
    rpctransport->open();
    result = client->RpcPgFsyncNoWritethrough(_fd);
    rpctransport->close();
    return result;
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

//int main() {
//    RpcInit();
//    RpcFileClose(0);
//    return 0;
//}