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
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>

#include "access/xlog.h"
#include "access/xlogutils.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/md.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"
#include "storage/rpcclient.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/elog.h"
#include "utils/palloc.h"

#include <iostream>
#include <fstream>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

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

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace tutorial;

std::shared_ptr<TTransport> rpcsocket;
std::shared_ptr<TTransport> rpctransport;
std::shared_ptr<TProtocol> rpcprotocol;
DataPageAccessClient *client;

void TryRpcInitFile(_Page& _return, _Path& _path);

int64_t 
TryRpcKvNblocks(char * key, XLogRecPtr LSN)
{
	int trycount=0;
	int maxcount=3;
	_Path keystring;
	int64_t upperLSN = LSN >> 32;
	int64_t lowerLSN = LSN & 0x00000000ffffffff;
	int64_t	result;
	keystring.assign(key);
	do{
		try{
			rpctransport->open();
			result = client->RpcKvNblocks(keystring, upperLSN, lowerLSN);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);
			
			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
	return result;
}

void 
TryRpcKvRead(char * buf, Oid spcNode, Oid _dbNode, Oid _relNode, ForkNumber forknum, 
BlockNumber blocknum, XLogRecPtr LSN)
{
	int trycount=0;
	int maxcount=3;
	_Page _return;
	_Oid _spcNode = spcNode;
    _Oid _dbNode = dbNode;
    _Oid _relNode = relNode;
	int64_t upperLSN = LSN >> 32;
	int64_t lowerLSN = LSN & 0x00000000ffffffff;
	do{
		try{
			rpctransport->open();
			client->RpcKvRead(_return, _spcNode, _dbNode, _relNode, 
			(int32_t)forknum, (int64_t)blocknum, upperLSN, lowerLSN);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);
			
			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
	_return.copy(buf, BLCKSZ);
}

void
rpcinitfile(char * db_dir_raw, char * fp)
{
	char		mpath[MAXPGPATH];

	rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
	rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
	rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
	client = new DataPageAccessClient(rpcprotocol);
	
	_Page _page;
	_Path _path;
	_path.assign(fp);

	TryRpcInitFile(_page, _path);
	
	snprintf(mpath, sizeof(mpath), "%s/%s", db_dir_raw, fp);	

	std::ofstream ofile(mpath);

	ofile.write(&_page[0], _page.size());

	ofile.close();

	
	delete client;
}

void TryRpcInitFile(_Page& _return, _Path& _path)
{
	int trycount=0;
	int maxcount=3;
	do{
		try{
			rpctransport->open();
			client->RpcInitFile(_return, _path);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);
			
			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
};

File TryRpcOpenTransientFile(const char * filename, int fileflags) {
    int trycount=0;
	int maxcount=3;
	_Path _filename;
	_File result;
	_filename.assign(filename);
	do{
		try{
			rpctransport->open();
			result = client->RpcOpenTransientFile(_filename, fileflags);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);

			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
	return result;
}

int TryRpcCloseTransientFile(const File fd) {
    int trycount=0;
	int maxcount=3;
	do{
		try{
			rpctransport->open();
			client->RpcCloseTransientFile(fd);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);

			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
	return 0;
}

int TryRpcread(File fd,void * buf,  int size) {
    int trycount=0;
	int maxcount=3;
	_Page _buf;
	do{
		try{
			rpctransport->open();
			client->Rpcread(_buf, fd, size);
			rpctransport->close();
			trycount=maxcount;
		}catch(TException& tx){
			std::cout << "ERROR: " << tx.what() << std::endl;
			rpcsocket = std::make_shared<TSocket>("localhost", RPCPORT);
			rpctransport = std::make_shared<TBufferedTransport>(rpcsocket);
			rpcprotocol = std::make_shared<TBinaryProtocol>(rpctransport);
			delete client;
			client = new DataPageAccessClient(rpcprotocol);
			
			trycount++;
			printf("Try again %d\n", trycount);
		}
	}while(trycount < maxcount);
	return _buf.copy((char*)buf, size);
}