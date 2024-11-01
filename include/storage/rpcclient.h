//
// Created by pang65 on 6/19/22.
//

#ifndef THRIFT_TEST_RPCCLIENT_H
#define THRIFT_TEST_RPCCLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/bufmgr.h"



    void RpcInit(void);
    void RpcTransportClose(void);
    void RpcMdRead(char* buff, SMgrRelation reln, ForkNumber forknum, BlockNumber blknum);
    int32_t RpcMdExists(SMgrRelation reln, int32_t forknum);
    int32_t RpcMdNblocks(SMgrRelation reln, int32_t forknum);
    void RpcMdCreate(SMgrRelation reln, int32_t forknum, int32_t isRedo);
    void RpcMdExtend(SMgrRelation reln, int32_t forknum, int32_t blknum, char* buff, int32_t skipFsync);
    void RpcReadBuffer_common(char* buff, SMgrRelation reln, char relpersistence, ForkNumber forkNum,
                          BlockNumber blockNum, ReadBufferMode mode);
    void RpcMdTruncate(SMgrRelation reln, int32_t forknum, int32_t blknum);


    int32_t RpcRegisterSecondaryNode(bool primary, int64_t lsn);
    void RpcSecondaryNodeUpdatesLsn(int32_t node_id, int64_t lsn);

    void RpcShutdown(void);
    void RpcFileClose(const int _fd);
    void RpcTablespaceCreateDbspace(const int64_t _spcnode, const int64_t _dbnode, const bool isRedo);
    int RpcPathNameOpenFile(const char* _path, const int32_t _flag);
    int32_t RpcFileWrite(const int _fd, const char* page, const int32_t _amount, const int64_t _seekpos, const int32_t _wait_event_info);
    char* RpcFilePathName(const int _fd);
    int RpcFileRead(char *buff, const int _fd, const int32_t _amount,  const int64_t _seekpos, const int32_t _wait_event_info);
    int32_t RpcFileTruncate(const int _fd, const int64_t _offset);
    int64_t RpcFileSize(const int _fd);
    int32_t RpcFilePrefetch(const int _fd, const int64_t _offset, const int32_t _amount, const int32_t wait_event_info);
    void RpcFileWriteback(const int _fd, const int64_t _offset, const int64_t nbytes, const int32_t wait_event_info);
    int32_t RpcUnlink(const char* _path);
    int32_t RpcFtruncate(const int _fd, const int64_t _offset);
    void RpcInitFile(char* _return, const char* _path);
    int RpcOpenTransientFile(const char* _filename, const int32_t _fileflags);
    int RpcOpenTransientFileUnderPgData(const char* filename, const int32_t _fileflags); 
    int32_t RpcCloseTransientFile(const int _fd);
    int32_t RpcFileSync(const int _fd, const int32_t _wait_event_info);
    int32_t RpcPgPRead(const int _fd, char *p, const int32_t _amount, const int32_t _offset);
    int32_t RpcPgPWrite(const int _fd, char *p, const int32_t _amount, const int32_t _offset);
    int32_t RpcClose(const int _fd);
    int32_t RpcBasicOpenFile(char *path, int32_t _flags);
    int32_t RpcBasicOpenFileUnderPgData(char *path, int32_t _flags); 
    int32_t RpcPgFdatasync(const int32_t _fd);
    int32_t RpcPgFsyncNoWritethrough(const int32_t _fd);
    int32_t RpcLseek(const int32_t _fd, const int64_t _offset, const int32_t _flag);
    int RpcStat(const char* path,struct stat* _stat);
    int32_t RpcDirectoryIsEmpty(const char* path);
    int32_t RpcCopyDir(const char* _src, const char* _dst);
    int32_t RpcPgFsync(const int32_t _fd);
    int32_t RpcDurableUnlink(const char * filename, const int32_t _flag);
    int32_t RpcDurableRenameExcl(const char* oldFname, const char* newFname, const int32_t _elevel);
    int32_t RpcXLogWriteWithPosition(const int _fd, char *p, const int32_t _amount, const int32_t _offset, int startIdx, int blkNum, uint64_t* xlblocks, int xlblocksBufferNum, uint64_t  lsn);
    int RpcXLogFileInit(XLogSegNo logsegno, bool *use_existent, bool use_lock);
#ifdef __cplusplus
}
#endif


#endif //THRIFT_TEST_RPCCLIENT_H
