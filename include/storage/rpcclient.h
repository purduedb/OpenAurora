//
// Created by pang65 on 6/19/22.
//

#ifndef THRIFT_TEST_RPCCLIENT_H
#define THRIFT_TEST_RPCCLIENT_H

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/bufmgr.h"

#ifdef __cplusplus
extern "C" {
#endif

    void RpcInit(void);
    void RpcClose(void);
    void RpcMdRead(char* buff, SMgrRelation reln, ForkNumber forknum, BlockNumber blknum);
    int32_t RpcMdExists(SMgrRelation reln, int32_t forknum);
    int32_t RpcMdNblocks(SMgrRelation reln, int32_t forknum);
    void RpcMdCreate(SMgrRelation reln, int32_t forknum, int32_t isRedo);
    void RpcMdExtend(SMgrRelation reln, int32_t forknum, int32_t blknum, char* buff, int32_t skipFsync);
    void RpcReadBuffer_common(char* buff, SMgrRelation reln, char relpersistence, ForkNumber forkNum,
                          BlockNumber blockNum, ReadBufferMode mode);
#ifdef __cplusplus
}
#endif


#endif //THRIFT_TEST_RPCCLIENT_H
