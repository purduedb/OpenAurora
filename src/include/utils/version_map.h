#ifndef VERSION_MAP_H
#define VERSION_MAP_H

#ifdef __cplusplus
extern "C" {
#endif

#include "c.h"
#include "utils/hsearch.h"
#include "access/logindex_hashmap.h"
#include "access/xlog.h"

typedef struct HASHELEMENT_VM {
    struct HASHELEMENT_VM *link;
    uint32 hashvalue;
} HASHELEMENT_VM;

typedef struct INDEX_ORDER_ITEM_VM{
	HASHELEMENT_VM* ptr;
	uint8_t slot;
} INDEX_ORDER_ITEM_VM;

#define ITEMHEAD_SLOT_CNT_VM 12
#define ITEMSEG_SLOT_CNT_VM 16
#define SLOT_CNT_VM (ITEMHEAD_SLOT_CNT_VM >= ITEMSEG_SLOT_CNT_VM ? ITEMHEAD_SLOT_CNT_VM: ITEMSEG_SLOT_CNT_VM)

typedef struct ITEMSEG_VM{
	HASHELEMENT_VM* next_seg;
	XLogRecPtr lsn[ITEMSEG_SLOT_CNT_VM];
} ITEMSEG_VM;

typedef struct ITEMHEAD_VM{
	KeyType PageID;
	HASHELEMENT_VM* next_item;
	HASHELEMENT_VM* next_seg;
	HASHELEMENT_VM* tail_seg;
	XLogRecPtr lsn[ITEMHEAD_SLOT_CNT_VM];
} ITEMHEAD_VM;

typedef union SEGMENT_ITEM_VM{
	ITEMHEAD_VM item_head;
	ITEMSEG_VM item_seg;
} SEGMENT_ITEM_VM;

typedef struct HASHHDR_VM HASHHDR_VM;

typedef struct HTAB_VM HTAB_VM;

typedef struct HASHCTL_VM {
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
    HASHHDR_VM *hctl;            /* location of header in shared mem */
} HASHCTL_VM;

typedef struct {
    HTAB_VM *hashp;
    uint32 curBucket;        /* index of current bucket */
    HASHELEMENT_VM *curEntry;        /* current entry in bucket */
} HASH_SEQ_STATUS_VM;

extern HTAB_VM *hash_create_vm(const char *tabname, long nelem,
                         HASHCTL_VM *info, int flags);

extern void hash_destroy_vm(HTAB_VM *hashp);

extern void *hash_search_vm(HTAB_VM *hashp, const void *keyPtr, HASHACTION action,
                         bool *foundPtr, bool *head);

extern uint32 get_hash_value_vm(HTAB_VM *hashp, const void *keyPtr);

extern void *hash_search_with_hash_value_vm(HTAB_VM *hashp, const void *keyPtr,
                                         uint32 hashvalue, HASHACTION action,
                                         bool *foundPtr, bool *head);

extern bool hash_update_hash_key_vm(HTAB_VM *hashp, void *existingEntry,
                                 const void *newKeyPtr);

extern long hash_get_num_entries_vm(HTAB_VM *hashp);

extern void hash_freeze_vm(HTAB_VM *hashp);

extern Size hash_estimate_size_vm(long hashtable_cnt, long segment_cnt);

extern Size hash_get_shared_size_vm(HASHCTL_VM *info, int flags);

extern void* hash_next_segment_vm(void* item, bool head);

#ifdef __cplusplus
}
#endif
#endif							/* VERSION_MAP_H */
