//
// Created by rainman on 2023/3/14.
//

#ifndef DB2_PG_BUILTIN_SHMHT_H
#define DB2_PG_BUILTIN_SHMHT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "c.h"
#include "storage/relfilenode.h"

#define REL_SIZE_PARTITION_NUM 128
#define REL_SIZE_ESTIMATE_SIZE 16384

struct RelTag {
    RelFileNode reln;
    ForkNumber forkNumber;
};

typedef struct RelTag RelTag;

struct RelSizeLookupEntry {
    RelTag relTag;
    int relSize;
};

typedef struct RelSizeLookupEntry RelSizeLookupEntry;


extern Size RelSizeTableShmemSize();

// parameter $size is the desired entry size
extern void InitRelSizeTable();

extern uint32 RelSizeTableHashCode(RelTag *relTag);

// Return the relation size, if it doesn't exist, return -1
extern int RelSizeTableLookup(RelTag *relTag, uint32 hashcode);

extern int RelSizeTableInsert(RelTag *relTag, uint32 hashcode, int relSize);

extern void RelSizeTableDelete(RelTag *relTag, uint32 hashcode);

#ifdef __cplusplus
}
#endif

#endif //DB2_PG_BUILTIN_SHMHT_H
