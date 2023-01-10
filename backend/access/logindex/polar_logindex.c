/*-------------------------------------------------------------------------
 *
 * polar_logindex.c
 *   Implementation of parse xlog states and replay.
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *    src/backend/access/logindex/polar_logindex.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/hash.h"
#include "access/polar_logindex.h"
#include "access/polar_logindex_internal.h"
#include "access/slru.h"
#include "access/transam.h"
#include "access/xlogdefs.h"
#include "access/heapam_xlog.h"
#include "access/visibilitymap.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "port/pg_crc32c.h"
#include "postmaster/startup.h"
#include "storage/ipc.h"
#include "utils/memutils.h"

static log_index_io_err_t       logindex_io_err = 0;
static int                      logindex_errno = 0;
logindex_snapshot_t logindexSnapShot;

static void log_index_insert_new_item(log_index_lsn_t *lsn_info, log_mem_table_t *table, uint32 key, log_seg_id_t new_item_id);
static void log_index_insert_new_seg(log_mem_table_t *table, log_seg_id_t head, log_seg_id_t seg_id, log_index_lsn_t *lsn_info);


static MemoryContext polar_redo_context = NULL;

static void
polar_clean_redo_context(int code, Datum arg)
{
    if (polar_redo_context)
    {
        MemoryContextDelete(polar_redo_context);
        polar_redo_context = NULL;
    }
}

MemoryContext
polar_get_redo_context(void)
{
    if (polar_redo_context == NULL)
    {
        polar_redo_context = AllocSetContextCreate(CurrentMemoryContext,
                                                   "polar working context",
                                                   ALLOCSET_DEFAULT_SIZES);

        before_shmem_exit(polar_clean_redo_context, 0);
    }

    return polar_redo_context;
}

// Xi: check whether snapshot has this state
// States includes: POLAR_LOGINDEX_STATE_INITIALIZED,
//                  POLAR_LOGINDEX_STATE_ADDING,
//                  POLAR_LOGINDEX_STATE_WRITABLE
bool
polar_logindex_check_state(log_index_snapshot_t *logindex_snapshot, uint32 state)
{
	return pg_atomic_read_u32(&logindex_snapshot->state) & state;
}

// Xi: If Context exists, return it. Otherwise, create and return it.
MemoryContext
polar_logindex_memory_context(void)
{
	static MemoryContext context = NULL;

	if (context == NULL)
	{
		context = AllocSetContextCreate(TopMemoryContext,
										"logindex snapshot mem context",
										ALLOCSET_DEFAULT_SIZES);
		MemoryContextAllowInCriticalSection(context, true);
	}

	return context;
}

XLogRecPtr
log_index_item_max_lsn(log_idx_table_data_t *table, log_item_head_t *item)
{
	log_item_seg_t *seg;

    //XI: if there is only head node, get the last record in suffix_lsn[] list
	if (item->head_seg == item->tail_seg)
		return LOG_INDEX_SEG_MAX_LSN(table, item);

    // XI: search the list tail item, that item has the max LSN
	seg = log_index_item_seg(table, item->tail_seg);
	Assert(seg != NULL);

	return LOG_INDEX_SEG_MAX_LSN(table, seg);
}

// XI: One snapShot has many logIndexMemTables
static Size
log_index_mem_tbl_shmem_size(uint64 logindex_mem_tbl_size)
{
	Size size = offsetof(log_index_snapshot_t, mem_table);
	size = add_size(size, mul_size(sizeof(log_mem_table_t), logindex_mem_tbl_size));

	size = MAXALIGN(size);

	/* The number of logindex memory table is at least 3 */
	if (logindex_mem_tbl_size < 3)
		elog(FATAL, "The number=%ld of logindex memory table is less than 3", logindex_mem_tbl_size);
	else
		ereport(LOG, (errmsg("The total log index memory table size is %ld", size)));

	return size;
}

static Size
log_index_lwlock_shmem_size(uint64 logindex_mem_tbl_size)
{
	Size size = mul_size(sizeof(LWLockMinimallyPadded), LOG_INDEX_LWLOCK_NUM(logindex_mem_tbl_size));

	return MAXALIGN(size);
}

Size
polar_logindex_shmem_size(uint64 logindex_mem_tbl_size, int bloom_blocks)
{
	Size size = 0;

	size = add_size(size, log_index_mem_tbl_shmem_size(logindex_mem_tbl_size));

//	size = add_size(size, log_index_bloom_shmem_size(bloom_blocks));

	size = add_size(size, log_index_lwlock_shmem_size(logindex_mem_tbl_size));

	return CACHELINEALIGN(size);
}

log_index_init_lwlock(log_index_snapshot_t *logindex_snapshot, int offset, int size, int tranche_id, const char *name)
{
	int i, j;

	LWLockRegisterTranche(tranche_id, name);

	for (i = offset, j = 0; j < size; i++, j++)
		LWLockInitialize(&(logindex_snapshot->lwlock_array[i].lock), tranche_id);
}

static XLogRecPtr
polar_logindex_snapshot_base_init(log_index_snapshot_t *logindex_snapshot, XLogRecPtr checkpoint_lsn)
{
//	log_index_meta_t *meta = &logindex_snapshot->meta;
	XLogRecPtr start_lsn = InvalidXLogRecPtr;
//
//	Assert(!polar_logindex_check_state(logindex_snapshot, POLAR_LOGINDEX_STATE_INITIALIZED));
//	Assert(!XLogRecPtrIsInvalid(checkpoint_lsn));

	LWLockAcquire(LOG_INDEX_IO_LOCK, LW_EXCLUSIVE);

	/*
	 * Reset logindex when we can not get correct logindex meta from storage
	 */
//	if (!log_index_get_meta(logindex_snapshot, meta))
//	{
//		polar_logindex_snapshot_remove_data(logindex_snapshot);
//		MemSet(meta, 0, sizeof(log_index_meta_t));
//	}

//	POLAR_LOG_LOGINDEX_META_INFO(meta);

    //XI: there are max_idx_table_id tables created, and the tables will be set inactive in mem_tbl_size size
//	logindex_snapshot_init_promoted_info(logindex_snapshot);
//	logindex_snapshot->max_idx_table_id = meta->max_idx_table_id;
//	LOG_INDEX_MEM_TBL_ACTIVE_ID = meta->max_idx_table_id % logindex_snapshot->mem_tbl_size;
    logindex_snapshot->max_idx_table_id = 0;
    LOG_INDEX_MEM_TBL_ACTIVE_ID = 0;

	logindex_snapshot->max_lsn = 0;
	MemSet(logindex_snapshot->mem_table, 0,
		   sizeof(log_mem_table_t) * logindex_snapshot->mem_tbl_size);

	/*
	 * If meta->start_lsn is invalid, we will not parse xlog and save it to logindex.
	 * RO will check logindex meta again when parse checkpoint xlog.
	 * RW will set logindex meta start lsn and save to storage when create new checkpoint.
	 * This will make ro and rw to create logindex from the same checkpoint.
	 */

//	if (!XLogRecPtrIsInvalid(meta->start_lsn)
//			&& meta->start_lsn < checkpoint_lsn)
//		start_lsn = meta->start_lsn;

	/*
	 * When we start to truncate lsn , latest_page_number may not be set up; insert a
	 * suitable value to bypass the sanity test in SimpleLruTruncate.
	 */
//	logindex_snapshot->bloom_ctl.shared->latest_page_number = UINT32_MAX;

	LWLockRelease(LOG_INDEX_IO_LOCK);

	/*
	 * When initialize log index snapshot, we load max table id's data to memory.
	 * When first insert lsn to memory table, need to check whether it already exists
	 * in previous table
	 */
//	polar_load_logindex_snapshot_from_storage(logindex_snapshot, checkpoint_lsn);

	pg_atomic_fetch_or_u32(&logindex_snapshot->state, POLAR_LOGINDEX_STATE_INITIALIZED);

	return start_lsn;
}

XLogRecPtr
polar_logindex_snapshot_init()
{
	XLogRecPtr start_lsn = InvalidXLogRecPtr;

//	if (!read_only)
//	{
//		char dir[MAXPGPATH];
//		snprintf(dir, MAXPGPATH, "%s/%s", POLAR_DATA_DIR(), logindex_snapshot->dir);
//		polar_validate_dir(dir);
//
//		pg_atomic_fetch_or_u32(&logindex_snapshot->state, POLAR_LOGINDEX_STATE_WRITABLE);
//	}

	start_lsn = polar_logindex_snapshot_base_init(logindexSnapShot, start_lsn);

	ereport(LOG, (errmsg("Init %s succeed", logindexSnapShot->dir)));

	return start_lsn;
}


static void
log_index_init_lwlock_array(log_index_snapshot_t *logindex_snapshot, const char *name, int tranche_id_begin, int tranche_id_end)
{
	int i = 0;
	int tranche_id = tranche_id_begin;

	Assert(logindex_snapshot->mem_tbl_size > 0);
	/*
	 * The tranche id defined for logindex must map the following call sequence.
	 * See the definition of LWTRANCE_WAL_LOGINDEX_BEGIN and LWTRANCHE_WAL_LOGINDEX_END as example
	 */
	snprintf(logindex_snapshot->trache_name[i], NAMEDATALEN, " %s_mem", name);
	log_index_init_lwlock(logindex_snapshot, LOG_INDEX_MEMTBL_LOCK_OFFSET, logindex_snapshot->mem_tbl_size,
						  tranche_id++, logindex_snapshot->trache_name[i]);

	snprintf(logindex_snapshot->trache_name[++i], NAMEDATALEN, " %s_hash", name);
	log_index_init_lwlock(logindex_snapshot, LOG_INDEX_HASH_LOCK_OFFSET, LOG_INDEX_MEM_TBL_HASH_LOCK_NUM,
						  tranche_id++, logindex_snapshot->trache_name[i]);

	snprintf(logindex_snapshot->trache_name[++i], NAMEDATALEN, " %s_io", name);
	log_index_init_lwlock(logindex_snapshot, LOG_INDEX_IO_LOCK_OFFSET, 1,
						  tranche_id++, logindex_snapshot->trache_name[i]);

	snprintf(logindex_snapshot->trache_name[++i], NAMEDATALEN, " %s_bloom", name);
	log_index_init_lwlock(logindex_snapshot, LOG_INDEX_BLOOM_LRU_LOCK_OFFSET, 1,
						  tranche_id, logindex_snapshot->trache_name[i]);

	Assert(tranche_id == tranche_id_end);
	Assert((i + 1) == LOG_INDEX_MAX_TRACHE);
}

static bool
log_index_page_precedes(int page1, int page2)
{
	return page1 < page2;
}

logindex_snapshot_t
polar_logindex_snapshot_shmem_init(const char *name, uint64 logindex_mem_tbl_size, int bloom_blocks, int tranche_id_begin, int tranche_id_end,
								   logindex_table_flushable table_flushable, void *extra_data)
{
#define LOGINDEX_SNAPSHOT_SUFFIX "_snapshot"
#define LOGINDEX_LOCK_SUFFIX "_lock"
#define LOGINDEX_BLOOM_SUFFIX "_bloom"

	logindex_snapshot_t logindex_snapshot = NULL;
	bool        found_snapshot;
	bool        found_locks;
	Size        size;
	char        item_name[POLAR_MAX_SHMEM_NAME];

	size = log_index_mem_tbl_shmem_size(logindex_mem_tbl_size);

	StaticAssertStmt(sizeof(log_item_head_t) == LOG_INDEX_TBL_SEG_SIZE,
					 "log_item_head_t size is not same as LOG_INDEX_MEM_TBL_SEG_SIZE");
	StaticAssertStmt(sizeof(log_item_seg_t) == LOG_INDEX_TBL_SEG_SIZE,
					 "log_item_seg_t size is not same as LOG_INDEX_MEM_TBL_SEG_SIZE");

	StaticAssertStmt(LOG_INDEX_FILE_TBL_BLOOM_SIZE > sizeof(log_file_table_bloom_t),
					 "LOG_INDEX_FILE_TBL_BLOOM_SIZE is not enough for log_file_table_bloom_t");

	snprintf(item_name, POLAR_MAX_SHMEM_NAME, "%s%s", name, LOGINDEX_SNAPSHOT_SUFFIX);

	logindex_snapshot = (logindex_snapshot_t)
						ShmemInitStruct(item_name, size, &found_snapshot);
	Assert(logindex_snapshot != NULL);

	snprintf(item_name, POLAR_MAX_SHMEM_NAME, "%s%s", name, LOGINDEX_LOCK_SUFFIX);

	/* Align lwlocks to cacheline boundary */
	logindex_snapshot->lwlock_array = (LWLockMinimallyPadded *)
									  ShmemInitStruct(item_name, log_index_lwlock_shmem_size(logindex_mem_tbl_size),
													  &found_locks);

	if (!IsUnderPostmaster)
	{
		Assert(!found_snapshot && !found_locks);
		logindex_snapshot->mem_tbl_size = logindex_mem_tbl_size;

		pg_atomic_init_u32(&logindex_snapshot->state, 0);

		log_index_init_lwlock_array(logindex_snapshot, name, tranche_id_begin, tranche_id_end);

		logindex_snapshot->max_allocated_seg_no = 0;
		logindex_snapshot->table_flushable = table_flushable;
		logindex_snapshot->extra_data = extra_data;

		SpinLockInit(LOG_INDEX_SNAPSHOT_LOCK);

		StrNCpy(logindex_snapshot->dir, name, NAMEDATALEN);
//		logindex_snapshot->segment_cache = NULL;
	}
	else
		Assert(found_snapshot && found_locks);

	logindex_snapshot->bloom_ctl.PagePrecedes = log_index_page_precedes;
	snprintf(item_name, POLAR_MAX_SHMEM_NAME, " %s%s", name, LOGINDEX_BLOOM_SUFFIX);

	/*
	 * Notice: When define tranche id for logindex, the last one is used for logindex bloom.
	 * See the definition between LWTRANCE_WAL_LOGINDEX_BEGIN and LWTRANCE_WAL_LOGINDEX_END
	 */
//	SimpleLruInit(&logindex_snapshot->bloom_ctl, item_name,
//				  bloom_blocks, 0,
//				  LOG_INDEX_BLOOM_LRU_LOCK, name,
//				  tranche_id_end, true);
	return logindex_snapshot;
}


void
polar_logindex_shmem_init(uint64 logindex_mem_tbl_size, int bloom_blocks) {
    logindexSnapShot = polar_logindex_snapshot_shmem_init("pg_logindex", logindex_mem_tbl_size, bloom_blocks, LWTRANCHE_WAL_LOGINDEX_BEGIN, LWTRANCHE_WAL_LOGINDEX_END,
                                                          NULL, NULL);
}

static bool
log_index_handle_update_v1_to_v2(log_index_meta_t *meta)
{
	if (polar_is_standby())
		return false;

	return true;
}


//XI: Read meta file("log_index_meta") from disk and validate data
bool
log_index_get_meta(log_index_snapshot_t *logindex_snapshot, log_index_meta_t *meta)
{
//	int         r;
//	char        meta_path[MAXPGPATH];
//	pg_crc32    crc;
//	int fd;
//
//	MemSet(meta, 0, sizeof(log_index_meta_t));
//
//	snprintf(meta_path, MAXPGPATH, "%s/%s/%s", POLAR_DATA_DIR(), logindex_snapshot->dir, LOG_INDEX_META_FILE);
//
//	if ((fd = PathNameOpenFile(meta_path, O_RDONLY | PG_BINARY, true)) < 0)
//		return false;
//
//
//	r = polar_file_pread(fd, (char *)meta, sizeof(log_index_meta_t), 0, WAIT_EVENT_LOGINDEX_META_READ);
//	logindex_errno = errno;
//
//	FileClose(fd);
//
//	if (r != sizeof(log_index_meta_t))
//	{
//		ereport(WARNING,
//				(errmsg("could not read file \"%s\": read %d of %d and errno=%d",
//						meta_path, r, (int) sizeof(log_index_meta_t), logindex_errno)));
//
//		return false;
//	}
//
//	crc = meta->crc;
//
//	if (meta->magic != LOG_INDEX_MAGIC)
//	{
//		POLAR_LOG_LOGINDEX_META_INFO(meta);
//		ereport(WARNING,
//				(errmsg("The magic number of meta file is incorrect, got %d, expect %d",
//						meta->magic, LOG_INDEX_MAGIC)));
//
//		return false;
//	}
//
//	meta->crc = 0;
//	meta->crc = log_index_calc_crc((unsigned char *)meta, sizeof(log_index_meta_t));
//
//	if (crc != meta->crc)
//	{
//		POLAR_LOG_LOGINDEX_META_INFO(meta);
//		ereport(WARNING,
//				(errmsg("The crc of file %s is incorrect, got %d but expect %d", meta_path,
//						crc, meta->crc)));
//
//		return false;
//	}
//
//	if (meta->version != LOG_INDEX_VERSION
//			&& !log_index_data_compatible(meta))
//	{
//		POLAR_LOG_LOGINDEX_META_INFO(meta);
//		ereport(WARNING,
//				(errmsg("The version is incorrect and incompatible, got %d, expect %d",
//						meta->version, LOG_INDEX_VERSION)));
//
//		return false;
//	}

	return true;
}

//XI: Return meta.start_lsn
XLogRecPtr
polar_logindex_start_lsn(logindex_snapshot_t logindex_snapshot)
{
	XLogRecPtr start_lsn = InvalidXLogRecPtr;

	if (logindex_snapshot != NULL)
	{
		LWLockAcquire(LOG_INDEX_IO_LOCK, LW_SHARED);
		start_lsn = logindex_snapshot->meta.start_lsn;
		LWLockRelease(LOG_INDEX_IO_LOCK);
	}

	return start_lsn;
}

// XI: iterate all the head with "key", (hash conflict)
// XI: if this page doesn't exist in this table, return value is NULL
static log_seg_id_t
log_index_mem_tbl_exists_page(BufferTag *tag,
							  log_idx_table_data_t *table, uint32 key)
{
    //XI: if the key doesn't exist in this table, then $exists is NULL
	log_seg_id_t    exists = LOG_INDEX_TBL_SLOT_VALUE(table, key);
	log_item_head_t *item;

    //XI: if the key doesn't exist in this page, then $item is NULL
	item = log_index_item_head(table, exists);

	while (item != NULL &&
			!BUFFERTAGS_EQUAL(item->tag, *tag))
	{
		exists = item->next_item;
		item = log_index_item_head(table, exists);
	}

	return exists;
}

//XI: Check whether node is full
//XI: Caller will depend on this to determine whether insert a new node
//XI: When the list's last node contains maximum records. (2 in head and 10 in segment)
static bool
log_index_mem_seg_full(log_mem_table_t *table, log_seg_id_t head)
{
	log_item_head_t *item;
	log_item_seg_t *seg;

	Assert(head != LOG_INDEX_TBL_INVALID_SEG);

	item = log_index_item_head(&table->data, head);

    // XI: the head list only has one head element
	if (item->tail_seg == head)
	{
		if (item->number == LOG_INDEX_ITEM_HEAD_LSN_NUM)
			return true;
	}
	else
	{
        // XI: The tail segments is No.($seg->number).
        // If it reached the maximum of this segment, return true
		seg = log_index_item_seg(&table->data, item->tail_seg);
		Assert(seg != NULL);

		if (seg->number == LOG_INDEX_ITEM_SEG_LSN_NUM)
			return true;
	}

	return false;
}

//XI: Insert a new page to LogIndex
//XI: Insert a new head into current active table
//XI: update active_table segment[] list (set new_item_id as a new head)
//XI: update active_table hash[] list, if no hash_conflict
static void
log_index_insert_new_item(log_index_lsn_t *lsn_info,
						  log_mem_table_t *table, uint32 key,
						  log_seg_id_t new_item_id)
{
    //XI: get segment[new_item_id] as new-head from the active table (already assigned in the caller by free_head++)
	log_item_head_t *new_item = log_index_item_head(&table->data, new_item_id);
	log_seg_id_t   *slot;

	Assert(key < LOG_INDEX_MEM_TBL_HASH_NUM);
    //XI: the hash[] will store the first segment_id inserting with same key
	slot = LOG_INDEX_TBL_SLOT(&table->data, key);

	new_item->head_seg = new_item_id;
	new_item->next_item = LOG_INDEX_TBL_INVALID_SEG;
	new_item->next_seg = LOG_INDEX_TBL_INVALID_SEG;
	new_item->tail_seg = new_item_id;
	memcpy(&(new_item->tag), lsn_info->tag, sizeof(BufferTag));
	new_item->number = 1;
	new_item->prev_page_lsn = lsn_info->prev_lsn;
    //XI: Set head Suffix LSN[0] as lsn_info.lsn's suffix LSN
	LOG_INDEX_INSERT_LSN_INFO(new_item, 0, lsn_info);

    //XI: If there are other pages have same key (in the same slot)
	if (*slot == LOG_INDEX_TBL_INVALID_SEG)
		*slot = new_item_id;
	else // XI: Create a new slot
	{
		new_item->next_item = *slot;
		*slot = new_item_id;
	}
}

//XI: Get table->seg[ $seg_id ] as a new segment node
//XI: add this new segment node in the head list
//XI: set $new_seg->suffix_lsn[0] = lsn_info.suffix_lsn
static void
log_index_insert_new_seg(log_mem_table_t *table, log_seg_id_t head,
						 log_seg_id_t seg_id, log_index_lsn_t *lsn_info)
{
	log_item_head_t *item = log_index_item_head(&table->data, head);
	log_item_seg_t *seg = log_index_item_seg(&table->data, seg_id);

	seg->head_seg = head;

    //XI: if the head's list have no other elements
    //      set head's next segment to seg_id
	if (item->tail_seg == head)
		item->next_seg = seg_id;
	else
	{
        // XI: Find the last segment of this head list
        //      and set last segment's next_seg as seg_id
		log_item_seg_t *pre_seg = log_index_item_seg(&table->data, item->tail_seg);

		if (pre_seg == NULL)
		{
//			POLAR_LOG_LOGINDEX_MEM_TABLE_INFO(table);
			ereport(PANIC, (errmsg("The log index table is corrupted, the segment %d is NULL;head=%d, seg_id=%d",
								   item->tail_seg, head, seg_id)));
		}

		pre_seg->next_seg = seg_id;
	}

	seg->prev_seg = item->tail_seg;
	item->tail_seg = seg_id;

    // XI: now this segment is tail, set its tail as InValid
	seg->next_seg = LOG_INDEX_TBL_INVALID_SEG;
    // XI: Why number is 1?
	seg->number = 1;
    // XI: set this segment's lsn[0] as lsn_info.lsn(suffix)
	LOG_INDEX_INSERT_LSN_INFO(seg, 0, lsn_info);
}

//XI: if the head list only has one head element, append it to head node
//XI: else, append it to tail segment node
//XI: Before this function, caller function will check whether this list is full
//XI: return value idx is the position in suffix LSN list
static uint8
log_index_append_lsn(log_mem_table_t *table, log_seg_id_t head, log_index_lsn_t *lsn_info)
{
	log_item_head_t *item;
	log_item_seg_t  *seg;
	uint8           idx;

	Assert(head != LOG_INDEX_TBL_INVALID_SEG);

	item = log_index_item_head(&table->data, head);

    //XI: if the head list has no segment element, add suffix lsn to the head
	if (item->tail_seg == head)
	{
		Assert(item->number < LOG_INDEX_ITEM_HEAD_LSN_NUM);
		idx = item->number;
		LOG_INDEX_INSERT_LSN_INFO(item, idx, lsn_info);
		item->number++;
	}
	else
	{ //XI: find the tail node and insert suffix LSN
		seg = log_index_item_seg(&table->data, item->tail_seg);
		Assert(seg != NULL);
		Assert(seg->number < LOG_INDEX_ITEM_SEG_LSN_NUM);
		idx = seg->number;
		LOG_INDEX_INSERT_LSN_INFO(seg, idx, lsn_info);
		seg->number++;
	}

	return idx;
}

//XI: find the free segment from the active table list
//XI: return value is the $free_head when inserting into the current ACTIVE_TABLE
//XI: LOOP all the active_table in the mem_table list
//      if table is new, use it and insert the first segment
//      if table is full or old(lsn doesn't match), loop to next mem_table and
//          set it as inactive talbe, also update global variable(active_table id)
//      otherwise, adopt the current active table, return (active_table->free_head++)
static log_seg_id_t
log_index_next_free_seg(log_index_snapshot_t *logindex_snapshot, XLogRecPtr lsn, log_mem_table_t **active_table)
{
	log_seg_id_t    dst = LOG_INDEX_TBL_INVALID_SEG;
	log_mem_table_t *active;
	int next_mem_id = -1;

    //XI: iterate all the table in mem_table list
	for (;;)
	{
        //XI: get current active table using snapshot->active_table(int)
		active = LOG_INDEX_MEM_TBL_ACTIVE();

		if (LOG_INDEX_MEM_TBL_STATE(active) == LOG_INDEX_MEM_TBL_STATE_ACTIVE)
		{
			/*
			 * 1. However when we get a new active table, we don't know its data.prefix_lsn,
			 * we assign InvalidXLogRecPtr lsn to data.prefix_lsn, so we should
			 * distinguish which table is new without prefix_lsn, and reassign it
			 * 2. If active table is full or
			 * new lsn prefix is different than this table's lsn prefix
			 * we will allocate new active memory table.
			 */
            //XI: TBL_IS_NEW-> if all the info in active-table is unsetted
			if (LOG_INDEX_MEM_TBL_IS_NEW(active))
			{
                //XI: set table's prefix_lsn with the current parameter lsn
				LOG_INDEX_MEM_TBL_SET_PREFIX_LSN(active, lsn);
                //XI: return table's free_head++
				dst = LOG_INDEX_MEM_TBL_UPDATE_FREE_HEAD(active);
			}
			else if (LOG_INDEX_MEM_TBL_FULL(active) ||    // XI: if free-head reached the maximum
					 !LOG_INDEX_SAME_TABLE_LSN_PREFIX(&active->data, lsn)) //XI: if this table has different table prefix lsn
			{
                // XI: set this table as InActive
				LOG_INDEX_MEM_TBL_SET_STATE(active, LOG_INDEX_MEM_TBL_STATE_INACTIVE);
                // XI: Loop to next active table, (ACTIVE_ID+1) % (mem_table_size)
				next_mem_id = LOG_INDEX_MEM_TBL_NEXT_ID(LOG_INDEX_MEM_TBL_ACTIVE_ID);
			}
			else //XI: table's free head++
				dst = LOG_INDEX_MEM_TBL_UPDATE_FREE_HEAD(active);
		}

		if (dst != LOG_INDEX_TBL_INVALID_SEG)
			return dst;

		if (next_mem_id != -1)
		{
            // XI: change to new active table
			active = LOG_INDEX_MEM_TBL(next_mem_id);
			*active_table = active;

			NOTIFY_LOGINDEX_BG_WORKER(logindex_snapshot->bg_worker_latch);
		}

//		pgstat_report_wait_start(WAIT_EVENT_LOGINDEX_WAIT_ACTIVE);
//		log_index_wait_active(logindex_snapshot, active, lsn);
//		pgstat_report_wait_end();

		if (next_mem_id != -1)
		{
			SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
			LOG_INDEX_MEM_TBL_ACTIVE_ID = next_mem_id;
			SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);
		}
	}

	/* never reach here */
	return LOG_INDEX_TBL_INVALID_SEG;
}

static void
log_index_update_min_segment(log_index_snapshot_t *logindex_snapshot)
{
	log_index_meta_t            *meta = &logindex_snapshot->meta;
	log_index_file_segment_t    *min_seg = &meta->min_segment_info;

	min_seg->segment_no++;
	min_seg->min_idx_table_id = min_seg->segment_no * LOG_INDEX_TABLE_NUM_PER_FILE + 1;

	if (LOG_INDEX_FILE_TABLE_SEGMENT_NO(meta->max_idx_table_id) == min_seg->segment_no)
	{
		min_seg->max_idx_table_id = meta->max_idx_table_id;
		min_seg->max_lsn = meta->max_lsn;
	}
	else
	{
		log_idx_table_data_t *table = palloc(sizeof(log_idx_table_data_t));
		min_seg->max_idx_table_id = (min_seg->segment_no + 1) * LOG_INDEX_TABLE_NUM_PER_FILE;

		if (log_index_read_table_data(logindex_snapshot, table, min_seg->max_idx_table_id, polar_trace_logindex(DEBUG4)) == false)
		{
//			POLAR_LOG_LOGINDEX_META_INFO(meta);
			ereport(PANIC,
					(errmsg("Failed to read log index which tid=%ld when truncate logindex",
							min_seg->max_idx_table_id)));
		}
		else
			min_seg->max_lsn = table->max_lsn;

		pfree(table);
	}


//	polar_log_index_write_meta(logindex_snapshot, &logindex_snapshot->meta, true);
}

static bool
log_index_truncate(log_index_snapshot_t *logindex_snapshot, XLogRecPtr lsn)
{
	log_index_meta_t            *meta = &logindex_snapshot->meta;
	log_index_file_segment_t    *min_seg = &meta->min_segment_info;
	uint64                      bloom_page;
	char                        path[MAXPGPATH];
	uint64                      min_segment_no;
	uint64                      max_segment_no;
	log_idx_table_id_t          max_unused_tid;

	LWLockAcquire(LOG_INDEX_IO_LOCK, LW_EXCLUSIVE);

	if (meta->crc == 0 || XLogRecPtrIsInvalid(meta->max_lsn)
			|| XLogRecPtrIsInvalid(min_seg->max_lsn)
			|| min_seg->max_lsn >= lsn)
	{
		LWLockRelease(LOG_INDEX_IO_LOCK);
		return false;
	}

	Assert(LOG_INDEX_FILE_TABLE_SEGMENT_NO(meta->max_idx_table_id) >= min_seg->segment_no);
	Assert(meta->max_idx_table_id >= min_seg->max_idx_table_id);

	/* Keep last saved segment file */
	if (LOG_INDEX_FILE_TABLE_SEGMENT_NO(meta->max_idx_table_id) == min_seg->segment_no)
	{
		LWLockRelease(LOG_INDEX_IO_LOCK);
		return false;
	}

	/*
	 * Update meta first. If meta update succeed but fail to remove files, we will not read these files.
	 * Otherwise if we remove files succeed but fail to update meta, we will fail to read file base on meta data.
	 */
	max_segment_no = LOG_INDEX_FILE_TABLE_SEGMENT_NO(meta->max_idx_table_id);
	min_segment_no = min_seg->segment_no;
	max_unused_tid = min_seg->max_idx_table_id;
	log_index_update_min_segment(logindex_snapshot);
	max_segment_no = Max(logindex_snapshot->max_allocated_seg_no, max_segment_no);
	LWLockRelease(LOG_INDEX_IO_LOCK);

	LOG_INDEX_FILE_TABLE_NAME(path, min_segment_no);
	bloom_page = LOG_INDEX_TBL_BLOOM_PAGE_NO(max_unused_tid);
	elog(LOG, "logindex truncate bloom id=%ld page=%ld", max_unused_tid, bloom_page);

	SimpleLruTruncate(&logindex_snapshot->bloom_ctl, bloom_page);

//	if (max_segment_no - min_segment_no < polar_max_logindex_files)
//	{
//		/* Rename unused file for next segment */
//		if (log_index_rename_segment_file(logindex_snapshot, path))
//			return true;
//	}

	durable_unlink(path, LOG);

	return true;
}

//XI: This function only return true when the last table's last LSN equal to parameter $lsn_info.lsn and page_tag matches
static bool
log_index_exists_in_saved_table(log_index_snapshot_t *logindex_snapshot, log_index_lsn_t *lsn_info)
{
//	uint32 mid;
//	log_mem_table_t *table;
//	uint32 i;
//	BufferTag      tag;
//	log_index_lsn_t saved_lsn;
//
//    //XI: Change to the previous active table in the mem_table list
//	SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
//	mid = LOG_INDEX_MEM_TBL_PREV_ID(LOG_INDEX_MEM_TBL_ACTIVE_ID);
//	SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);
//
//	table = LOG_INDEX_MEM_TBL(mid);
//
//	if (LOG_INDEX_MEM_TBL_STATE(table) != LOG_INDEX_MEM_TBL_STATE_FLUSHED)
//		return false;
//
//	CLEAR_BUFFERTAG(tag);
//
//	saved_lsn.tag = &tag;
//
//	for (i = table->data.last_order; i > 0; i--)
//	{
//        //XI: if the table's order[i-1]'s LSN is not what we expected, return false
//		if (log_index_get_order_lsn(&table->data, i - 1, &saved_lsn) != lsn_info->lsn)
//			return false;
//
//        //XI: if the LSN is we expected and the tags matches
//		if (BUFFERTAGS_EQUAL(*(lsn_info->tag), *(saved_lsn.tag)))
//			return true;
//	}

	return false;
}

//XI: find a free space to insert a LSN
//XI: if the current table is full, loop to another table in mem_table list, change active table
//XI: if the active-table doesn't have this page before, insert a new head node
//XI:   else (current table has this page and record is not full in the tail node), append lsn to tail node
//XI: Also, update table and snapshot's max-min LSN info.
void
log_index_insert_lsn(log_index_snapshot_t *logindex_snapshot, log_index_lsn_t *lsn_info,  uint32 key)
{
	log_mem_table_t     *active = NULL;
	bool                new_item;
	log_seg_id_t        head = LOG_INDEX_TBL_INVALID_SEG;;

	Assert(lsn_info->prev_lsn < lsn_info->lsn);

	active = LOG_INDEX_MEM_TBL_ACTIVE();

	/*
	 * 1. Logindex table state is atomic uint32, it's safe to change state without table lock
	 * 2. Only one process insert lsn, so it's safe to check exists page without hash lock
	 */
    //XI: if the page doesn't exist in active table, head==NULL.
	if (LOG_INDEX_MEM_TBL_STATE(active) == LOG_INDEX_MEM_TBL_STATE_ACTIVE &&
			LOG_INDEX_SAME_TABLE_LSN_PREFIX(&active->data, lsn_info->lsn))
		head = log_index_mem_tbl_exists_page(lsn_info->tag, &active->data, key);

	new_item = (head == LOG_INDEX_TBL_INVALID_SEG);

    // XI: if the page exists in the list and list isn't full
	if (!new_item && !log_index_mem_seg_full(active, head))
	{
		uint8 idx;
		log_item_head_t *item;

        //XI: append a lsn record to an already exists node (which is not full)
		LWLockAcquire(LOG_INDEX_HASH_LOCK(key), LW_EXCLUSIVE);
		idx = log_index_append_lsn(active, head, lsn_info);
		item = log_index_item_head(&active->data, head);
        //XI: update idx_mem_table's order list
        //XI: append "segID&0FFF | (idx<<12)&F000" to the order list
		LOG_INDEX_MEM_TBL_ADD_ORDER(&active->data, item->tail_seg, idx);
		LWLockRelease(LOG_INDEX_HASH_LOCK(key));
	}
	else
	{
		log_seg_id_t    dst;
		log_mem_table_t *old_active = active;

        //XI: dst is the free_head value in the current active_table
        //XI: in this function, the active table may change, which means old_active!=active
		dst = log_index_next_free_seg(logindex_snapshot, lsn_info->lsn, &active);

		Assert(dst != LOG_INDEX_TBL_INVALID_SEG);

		LWLockAcquire(LOG_INDEX_HASH_LOCK(key), LW_EXCLUSIVE);

        //XI: if the page doesn't exist in the current active table
        //      or page exists in this current active table, however this table($old_active) is full
        //      or both dost exists in this table and table($old_active) is full
        //    then insert a new head in the current active-table
		if (new_item || active != old_active)
			log_index_insert_new_item(lsn_info, active, key, dst);
		else //XI: find free space in old_active table, occupy table.seg[dst] as new segment node
			log_index_insert_new_seg(active, head, dst, lsn_info);

		LOG_INDEX_MEM_TBL_ADD_ORDER(&active->data, dst, 0);
		LWLockRelease(LOG_INDEX_HASH_LOCK(key));
	}

	SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
	active->data.max_lsn = Max(lsn_info->lsn, active->data.max_lsn);
	active->data.min_lsn = Min(lsn_info->lsn, active->data.min_lsn);
	logindex_snapshot->max_lsn = active->data.max_lsn;
	SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);
}

void
polar_logindex_add_lsn(BufferTag *tag, XLogRecPtr prev, XLogRecPtr lsn)
{
	uint32      key = LOG_INDEX_MEM_TBL_HASH_PAGE(tag);
	log_index_lsn_t lsn_info;

	Assert(tag != NULL);
	Assert(lsn > prev);


	lsn_info.tag = tag;
	lsn_info.lsn = lsn;
	lsn_info.prev_lsn = prev;

    //XI: if the state is not ADDING, maybe INITIALIZED??
	if (unlikely(!polar_logindex_check_state(logindexSnapShot, POLAR_LOGINDEX_STATE_ADDING)))
	{
		/* Insert logindex from meta->start_lsn, so We don't save logindex which lsn is less then meta->start_lsn */
//		if (lsn < meta->start_lsn)
//			return;

		/*
		 * If log index initialization is not finished
		 * we don't save lsn if it's less than max saved lsn,
		 * which means it's already in saved table
		 */
//		if (lsn < meta->max_lsn)
//			return;

		/*
		 * If lsn is equal to max saved lsn then
		 * we check whether the tag is in saved table
		 */
        //XI: this function check whether previous table (active_id-1)'s the last record
        //      if what we want (LSN matches and page_tag matches).
//		if (meta->max_lsn == lsn
//				&& log_index_exists_in_saved_table(logindex_snapshot, &lsn_info))
//			return;

		/*
		 * If we come here which means complete to check lsn overlap
		 * then we can save lsn to logindex memory table
		 */
		pg_atomic_fetch_or_u32(&logindexSnapShot->state, POLAR_LOGINDEX_STATE_ADDING);
		elog(LOG, "%s log index is insert from %lx", logindexSnapShot->dir, lsn);
	}

    //XI: Insert a new segment to active table (or append lsn to not full node),
    //      this process may include changing active table.
	log_index_insert_lsn(logindexSnapShot, &lsn_info, key);
}

//XI: Find a page in $table.
//  If found, return its head node
//  Otherwise, return NULL
log_item_head_t *
log_index_tbl_find(BufferTag *tag,
				   log_idx_table_data_t *table, uint32 key)
{
	log_seg_id_t    item_id;

	Assert(table != NULL);

	item_id = log_index_mem_tbl_exists_page(tag, table, key);
	return log_index_item_head(table, item_id);
}

//XI: return the logindex_snapshot->max_lsn with lock involved
XLogRecPtr
polar_get_logindex_snapshot_max_lsn(log_index_snapshot_t *logindex_snapshot)
{
	XLogRecPtr  max_lsn = InvalidXLogRecPtr;

	SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
	max_lsn = logindex_snapshot->max_lsn;
	SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);
	return max_lsn;
}

uint64
polar_logindex_mem_tbl_size(logindex_snapshot_t logindex_snapshot)
{
	return logindex_snapshot != NULL ? logindex_snapshot->mem_tbl_size : 0;
}

//XI: snapshot->max_tid - meta->max_tid
uint64
polar_logindex_used_mem_tbl_size(logindex_snapshot_t logindex_snapshot)
{
	log_idx_table_id_t mem_tbl_size = 0;

	if (logindex_snapshot != NULL)
	{
		LWLockAcquire(LOG_INDEX_IO_LOCK, LW_SHARED);
		SpinLockAcquire(LOG_INDEX_SNAPSHOT_LOCK);
		mem_tbl_size = logindex_snapshot->max_idx_table_id -
					   logindex_snapshot->meta.max_idx_table_id;
		SpinLockRelease(LOG_INDEX_SNAPSHOT_LOCK);
		LWLockRelease(LOG_INDEX_IO_LOCK);
	}

	return mem_tbl_size;
}

uint64
polar_logindex_convert_mem_tbl_size(uint64 mem_size)
{
	return (mem_size * 1024L * 1024L) / (sizeof(log_mem_table_t) + sizeof(LWLockMinimallyPadded));
}

//XI: if snapshot->meta.start_lsn is valid, return it
//XI: otherwise, if reach consistency in replica node, read meta from disk
XLogRecPtr
polar_logindex_check_valid_start_lsn(logindex_snapshot_t logindex_snapshot)
{
//	log_index_meta_t *meta = &logindex_snapshot->meta;
	XLogRecPtr start_lsn = InvalidXLogRecPtr;

//	LWLockAcquire(LOG_INDEX_IO_LOCK, LW_EXCLUSIVE);
//
//	if (!XLogRecPtrIsInvalid(meta->start_lsn))
//		start_lsn = meta->start_lsn;
//	else if (InRecovery)
//	{
//		/*
//		 * When reach consistency in replica node, read meta from storage is meta start lsn is invalid
//		 */
//		if (!polar_logindex_check_state(logindex_snapshot, POLAR_LOGINDEX_STATE_WRITABLE))
//		{
//			if (log_index_get_meta(logindex_snapshot, meta))
//				start_lsn = meta->start_lsn;
//			else
//				elog(FATAL, "Failed to read logindex meta from shared storage");
//		}
//	}
//
//	LWLockRelease(LOG_INDEX_IO_LOCK);

	return start_lsn;
}

//XI: set snapshot->meta.start_lsn and update disk meta
void
polar_logindex_set_start_lsn(logindex_snapshot_t logindex_snapshot, XLogRecPtr start_lsn)
{
	log_index_meta_t *meta = &logindex_snapshot->meta;

	Assert(!XLogRecPtrIsInvalid(start_lsn));

	LWLockAcquire(LOG_INDEX_IO_LOCK, LW_EXCLUSIVE);
	meta->start_lsn = start_lsn;
//	polar_log_index_write_meta(logindex_snapshot, meta, false);
	LWLockRelease(LOG_INDEX_IO_LOCK);
}

XLogRecPtr
polar_logindex_mem_table_max_lsn(struct log_mem_table_t *table)
{
	return table->data.max_lsn;
}


static void
polar_reset_blk(DecodedBkpBlock *blk)
{
    blk->in_use = false;
    blk->has_image = false;
    blk->has_data = false;
    blk->apply_image = false;
}

static void
polar_heap_save_vm_block(XLogReaderState *state, uint8 block_id, uint8 vm_block_id)
{
    DecodedBkpBlock *blk, *vm_blk;

    Assert(block_id <= XLR_MAX_BLOCK_ID);
    blk = &state->blocks[block_id];
    vm_blk = &state->blocks[vm_block_id];

    Assert(blk->in_use);
    Assert(!vm_blk->in_use);
    polar_reset_blk(vm_blk);

    vm_blk->in_use = true;
    vm_blk->rnode = blk->rnode;
    vm_blk->forknum = VISIBILITYMAP_FORKNUM;
    vm_blk->blkno = HEAPBLK_TO_MAPBLOCK(blk->blkno);

    state->max_block_id = Max(state->max_block_id, vm_block_id);
}

static void
polar_heap_update_save_vm_logindex(XLogReaderState *state, bool hotupdate)
{
    BlockNumber blkno_old, blkno_new;
    xl_heap_update *xlrec = (xl_heap_update *)(state->main_data);

    Assert(state->blocks[0].in_use);
    blkno_new = state->blocks[0].blkno;

    if (state->blocks[1].in_use)
    {
        /* HOT updates are never done across pages */
        Assert(!hotupdate);
        blkno_old = state->blocks[1].blkno;
    }
    else
        blkno_old = blkno_new;

    if (blkno_new != blkno_old)
    {
        if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
            polar_heap_save_vm_block(state, 1, 3);

        if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
            polar_heap_save_vm_block(state, 0, 2);
    }
    else
    {
        if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
            polar_heap_save_vm_block(state, 0, 2);
    }
}

static void
polar_xlog_queue_decode_heap(XLogReaderState *state)
{
    XLogRecord *rechdr = state->decoded_record;
    uint8 info = rechdr->xl_info & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK)
    {
		case XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE:
		{
			xl_heap_insert *xlrec = (xl_heap_insert *)(state->main_data);

			if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) {
				printf("%s %d, !!!!! need parse \n", __func__ , __LINE__);
				fflush(stdout);
			}

			break;

		}
		case XLOG_HEAP_INSERT:
        {
            xl_heap_insert *xlrec = (xl_heap_insert *)(state->main_data);

            if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
                polar_heap_save_vm_block(state, 0, 1);

            break;
        }

        case XLOG_HEAP_DELETE:
        {
            xl_heap_delete *xlrec = (xl_heap_delete *)(state->main_data);

            if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
                polar_heap_save_vm_block(state, 0, 1);

            break;
        }

        case XLOG_HEAP_UPDATE:
            polar_heap_update_save_vm_logindex(state, false);
            break;

        case XLOG_HEAP_HOT_UPDATE:
            polar_heap_update_save_vm_logindex(state, true);
            break;

        case XLOG_HEAP_LOCK:
        {
            xl_heap_lock *xlrec = (xl_heap_lock *)(state->main_data);

            if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
                polar_heap_save_vm_block(state, 0, 1);

            break;
        }
    }
}

static void
polar_xlog_queue_decode_heap2(XLogReaderState *state)
{
    XLogRecord *rechdr = state->decoded_record;
    uint8 info = rechdr->xl_info & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK)
    {
        case XLOG_HEAP2_MULTI_INSERT:
        {
            xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *)(state->main_data);

            if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
                polar_heap_save_vm_block(state, 0, 1);

            break;
        }

        case XLOG_HEAP2_LOCK_UPDATED:
        {
            xl_heap_lock_updated *xlrec = (xl_heap_lock_updated *)(state->main_data);

            if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
                polar_heap_save_vm_block(state, 0, 1);

            break;
        }

        default:
            break;
    }
}

void
polar_xlog_decode_data(XLogReaderState *state)
{
    XLogRecord *rechdr = state->decoded_record;

    switch (rechdr->xl_rmid)
    {
        case RM_HEAP_ID:
            polar_xlog_queue_decode_heap(state);
            break;

        case RM_HEAP2_ID:
            polar_xlog_queue_decode_heap2(state);
            break;

        default:
            break;
    }
}
