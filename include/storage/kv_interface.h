//
// Created by pang65 on 10/31/22.
//
#ifndef SRC_KVSTORE_H
#define SRC_KVSTORE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <storage/buf_internals.h>
#include <access/xlogreader.h>

extern int KvPut(char *, char *, int);
extern void InitKvStore();
extern int KvGet(char *, char **, size_t* len);
extern void KvClose();
#ifdef DISABLED_FUNCTION
extern int KvGetInt(char *, int*);
extern int KvPutInt(char *, int);
#endif
extern int KvDelete(char *);
#ifdef DISABLED_FUNCTION
extern void KvPrefixCopyDir(char* , char* , const char* );

// If list exists, insert it to list. otherwise, create a new list
extern void InsertLsn2RocksdbList(BufferTag bufferTag, uint64_t lsn);
// this function will call InsertLsn2RocksdbList
//extern void ParseXLogBlocksLsn(XLogReaderState *record, int recordBlockId);

// Lsn list related
extern int GetListFromRocksdb(BufferTag bufferTag, uint64_t** listPointer, int* listSize);
extern void PutList2Rocksdb(BufferTag bufferTag, uint64_t* listPointer, int listSize);
#endif

// Page related
extern int GetPageFromRocksdb(BufferTag bufferTag, uint64_t lsn, char** pageContent);
extern void PutPage2Rocksdb(BufferTag bufferTag, uint64_t lsn, char* pageContent);
extern void DeletePageFromRocksdb(BufferTag bufferTag, uint64_t lsn);

// Xlog related
extern int PutXlogWithLsn(XLogRecPtr lsn, XLogRecord* record);
extern int GetXlogWithLsn(XLogRecPtr lsn, XLogRecord** record, size_t* record_size);

extern int FindListLowerBound(uint64_t* uintList, uint64_t targetLsn, uint64_t *foundLsn, uint64_t *foundPos);

#ifdef __cplusplus
}
#endif

#endif //SRC_KVSTORE_H
