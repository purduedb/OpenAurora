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

extern int UnmarshalUnsignedLongListGetSize(char *p);
extern unsigned long * UnmarshalUnsignedLongListGetList (char *p);
extern void MarshalUnsignedLongList(const unsigned long *numList, int size, char **p);
extern int AddLargestElem2OrderedKVList(unsigned long **kvList, unsigned long newEle);
extern int FindLowerBound_UnsignedLong(const unsigned long *list, int listSize, unsigned long target);
#ifdef __cplusplus
}
#endif
#endif //SRC_KVSTORE_H
