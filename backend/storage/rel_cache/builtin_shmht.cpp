//#include "c.h"
#include <stdio.h>
#include <iostream>
#include "postgres.h"
#include "storage/builtin_shmht.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
//#include "miscadmin.h"
//#include "port/atomics.h"
//#include "storage/buf.h"
//#include "storage/latch.h"
//#include "storage/lwlock.h"
//#include "storage/shmem.h"
//#include "storage/smgr.h"
//#include "storage/spin.h"
//#include "utils/relcache.h"

static HTAB* SharedRelSizeHash;

Size RelSizeTableShmemSize() {
//    std::cout << __func__  << ", " << __LINE__ << std::endl;
//    fflush(stdout);
    return hash_estimate_size(REL_SIZE_ESTIMATE_SIZE, sizeof(RelSizeLookupEntry));
}

void
InitRelSizeTable()
{
//    std::cout << __func__  << ", " << __LINE__ << std::endl;
//    fflush(stdout);
    HASHCTL		info;

    MemSet(&info, 0, sizeof(info));
    /* assume no locking is needed yet */

    /* BufferTag maps to Buffer */
    info.keysize = sizeof(RelTag);
    info.entrysize = sizeof(RelSizeLookupEntry);
    info.num_partitions = REL_SIZE_PARTITION_NUM;

    SharedRelSizeHash = ShmemInitHash("Shared Relation Size Table",
                                  REL_SIZE_ESTIMATE_SIZE/8, REL_SIZE_ESTIMATE_SIZE/8,
                                  &info,
                                  HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
}

uint32
RelSizeTableHashCode(RelTag *tagPtr)
{
    return get_hash_value(SharedRelSizeHash, (void *) tagPtr);
}

int
RelSizeTableLookup(RelTag *tagPtr, uint32 hashcode)
{
    RelSizeLookupEntry *result;

//    std::cout << __func__ << " " << __LINE__ << ", lookup: " << tagPtr->reln.spcNode << "_" << tagPtr->reln.dbNode
//    << "_" << tagPtr->reln.relNode << "_" << tagPtr->forkNumber << std::endl;
//    printf("%s %d, lookup: %lu_%lu_%lu_%d\n", __func__ , __LINE__, tagPtr->reln.spcNode, tagPtr->reln.dbNode,
//           tagPtr->reln.relNode, tagPtr->forkNumber);
//    fflush(stdout);
    result = (RelSizeLookupEntry *)
            hash_search_with_hash_value(SharedRelSizeHash,
                                        (void *) tagPtr,
                                        hashcode,
                                        HASH_FIND,
                                        NULL);

//    if(result)
//        std::cout << __func__ << " " << __LINE__ << ", lookup: " << tagPtr->reln.spcNode << "_" << tagPtr->reln.dbNode
//                  << "_" << tagPtr->reln.relNode << "_" << tagPtr->forkNumber << " result=" << result->relSize << std::endl;
//    else
//        std::cout << __func__ << " " << __LINE__ << ", lookup: " << tagPtr->reln.spcNode << "_" << tagPtr->reln.dbNode
//                  << "_" << tagPtr->reln.relNode << "_" << tagPtr->forkNumber << " result=NULL" << std::endl;
//    fflush(stdout);

    if (!result)
        return -1;

    return result->relSize;
}

// Return -1 on successfully insertion
// Otherwise, if key has already existed, update the value and return the current value
int
RelSizeTableInsert(RelTag *tagPtr, uint32 hashcode, int relSize)
{
//    std::cout<< __func__  << " " << __LINE__ << " " << tagPtr->reln.spcNode << " " <<
//    tagPtr->reln.dbNode << " " << tagPtr->reln.relNode << " " << tagPtr->forkNumber << " relSize=" << relSize << std::endl;
//    fflush(stdout);
    RelSizeLookupEntry *result;
    bool		found;

    result = (RelSizeLookupEntry *)
            hash_search_with_hash_value(SharedRelSizeHash,
                                        (void *) tagPtr,
                                        hashcode,
                                        HASH_ENTER,
                                        &found);

//    std::cout<< __func__  << " " << __LINE__ << std::endl;
//    fflush(stdout);
    if (found)					/* found something already in the table */
    {
        result->relSize = relSize;
        return result->relSize;
    }

    result->relSize = relSize;

    return -1;
}

void
RelSizeTableDelete(RelTag *tagPtr, uint32 hashcode)
{
    RelSizeLookupEntry *result;

    result = (RelSizeLookupEntry *)
            hash_search_with_hash_value(SharedRelSizeHash,
                                        (void *) tagPtr,
                                        hashcode,
                                        HASH_REMOVE,
                                        NULL);

    if (!result)				/* shouldn't happen */
        elog(ERROR, "shared relation size hash table corrupted");
}