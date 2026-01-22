#ifndef PAGE_VERSION_TRACKER_H
#define PAGE_VERSION_TRACKER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "c.h"
#include "utils/hsearch.h"
#include "access/logindex_hashmap.h"
#include "access/xlog.h"

typedef struct HASHELEMENT_PVT {
    struct HASHELEMENT_PVT *link;
    uint32 hashvalue;
} HASHELEMENT_PVT;

typedef struct INDEX_ORDER_ITEM_PVT{
	HASHELEMENT_PVT* ptr;
	uint8_t slot;
} INDEX_ORDER_ITEM_PVT;

#define ITEMHEAD_SLOT_CNT_PVT 12
#define ITEMSEG_SLOT_CNT_PVT 16
#define SLOT_CNT_PVT (ITEMHEAD_SLOT_CNT_PVT >= ITEMSEG_SLOT_CNT_PVT ? ITEMHEAD_SLOT_CNT_PVT: ITEMSEG_SLOT_CNT_PVT)

typedef struct ITEMSEG_PVT{
	HASHELEMENT_PVT* next_seg;
	XLogRecPtr lsn[ITEMSEG_SLOT_CNT_PVT];
} ITEMSEG_PVT;

typedef struct ITEMHEAD_PVT{
	KeyType PageID;
	HASHELEMENT_PVT* next_item;
	HASHELEMENT_PVT* next_seg;
	HASHELEMENT_PVT* tail_seg;
	XLogRecPtr lsn[ITEMHEAD_SLOT_CNT_PVT];
} ITEMHEAD_PVT;

typedef union SEGMENT_ITEM_PVT{
	ITEMHEAD_PVT item_head;
	ITEMSEG_PVT item_seg;
} SEGMENT_ITEM_PVT;

typedef struct HASHHDR_PVT HASHHDR_PVT;

typedef struct HTAB_PVT HTAB_PVT;

typedef struct HASHCTL_PVT {
    long num_partitions; /* # partitions (must be power of 2) */
	long segment_cnt;
	long hashtable_cnt;
    long ffactor;        /* fill factor */
    Size keysize;        /* hash key length in bytes */
    Size entrysize;        /* total user element size in bytes */
    HashValueFunc hash;            /* hash function */
    HashCompareFunc match;        /* key comparison function */
    HashCopyFunc keycopy;        /* key copying function */
    HashAllocFunc alloc;        /* memory allocator */
    MemoryContext hcxt;            /* memory context to use for allocations */
    HASHHDR_PVT *hctl;            /* location of header in shared mem */
} HASHCTL_PVT;

typedef struct {
    HTAB_PVT *hashp;
    uint32 curBucket;        /* index of current bucket */
    HASHELEMENT_PVT *curEntry;        /* current entry in bucket */
} HASH_SEQ_STATUS_PVT;

extern HTAB_PVT *hash_create_pvt(const char *tabname, long nelem,
                         const HASHCTL_PVT *info, int flags);

extern void hash_destroy_pvt(HTAB_PVT *hashp);

extern void *hash_search_pvt(HTAB_PVT *hashp, const void *keyPtr, HASHACTION action,
                         bool *foundPtr, bool *head);

extern uint32 get_hash_value_pvt(HTAB_PVT *hashp, const void *keyPtr);

extern void *hash_search_with_hash_value_pvt(HTAB_PVT *hashp, const void *keyPtr,
                                         uint32 hashvalue, HASHACTION action,
                                         bool *foundPtr, bool *head);

extern bool hash_update_hash_key_pvt(HTAB_PVT *hashp, void *existingEntry,
                                 const void *newKeyPtr);

extern long hash_get_num_entries_pvt(HTAB_PVT *hashp);

extern void hash_freeze_pvt(HTAB_PVT *hashp);

extern Size hash_estimate_size_pvt(long hashtable_cnt, long segment_cnt);

extern Size hash_get_shared_size_pvt(HASHCTL_PVT *info, int flags);

extern void* hash_next_segment_pvt(void* item, bool head);

#ifdef __cplusplus
}
#endif
#endif							/* PAGE_VERSION_TRACKER_H */
