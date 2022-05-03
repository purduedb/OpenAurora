//
// Created by pang65 on 2/12/22.
//

#ifndef SRC_KVSTORE_H
#define SRC_KVSTORE_H

extern int KvPut(char *, char *, int);
extern void InitKvStore();
extern int KvGet(char *, char **);
extern void KvClose();
extern int KvGetInt(char *, int*);
extern int KvPutInt(char *, int);
extern int KvDelete(char *);
extern void KvPrefixCopyDir(char* , char* , const char* );
extern void StartRocksDbWriteProcess();

#endif //SRC_KVSTORE_H
