//
// Created by pang65 on 2/12/22.
//

#ifndef SRC_KVSTORE_H
#define SRC_KVSTORE_H

#ifdef __cplusplus
extern "C" {
#endif

extern int KvPut(char *, char *, int);
extern void InitKvStore();
extern int KvGet(char *, char **);
extern void KvClose();
extern int KvGetInt(char *, int*);
extern int KvPutInt(char *, int);
extern int KvDelete(char *);
extern void KvPrefixCopyDir(char* , char* , const char* );

#ifdef __cplusplus
}
#endif

#endif //SRC_KVSTORE_H
