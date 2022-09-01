/*-------------------------------------------------------------------------
 *
 * polar_rel_size_cache.c
 *    
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
 *           src/backend/access/logindex/polar_rel_size_cache.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/polar_logindex.h"
#include "access/polar_rel_size_cache.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "utils/guc.h"

typedef enum
{
	NEXT,
	END
} search_state;

#define REL_SIZE_TABLE_INVALID_ID       (0)
#define REL_SIZE_TABLE_NEXT_ID(cache, id)      LOOP_NEXT_VALUE((id), (cache)->table_size)
#define REL_SIZE_TABLE_PREV_ID(cache, id)      LOOP_PREV_VALUE(i(id), (cache)->table_size)
#define REL_TABLE_IN_SHARED_MEM(cache, t)      (((const char *)(t)) >= &cache->table_data[0] && \
												((const char *)(t)) <= &cache->table_data[cache->table_size * POLAR_REL_CACHE_TABLE_SIZE - 1])
#define REL_SIZE_CACHE_TABLE(cache, i)         ((polar_rel_size_table_t *)(&(cache)->table_data[(i)*POLAR_REL_CACHE_TABLE_SIZE]))

Size
polar_rel_size_shmem_size(int blocks)
{
	Size size = 0;

	if (blocks <= 0)
		return size;

	size = offsetof(polar_rel_size_cache_data_t, table_size);

	size = add_size(size, mul_size(POLAR_REL_CACHE_TABLE_SIZE, blocks));

	return CACHELINEALIGN(size);
}

static void
polar_rel_size_remove_all(polar_rel_size_cache_t cache)
{
	char path[MAXPGPATH];

	snprintf(path, MAXPGPATH, "%s/%s", DataDir, cache->dir_name);
	rmtree(path, false);
}

//updated
static void
polar_check_rel_size_dir(polar_rel_size_cache_t cache)
{
	char path[MAXPGPATH];

	snprintf(path, MAXPGPATH, "%s/%s", DataDir, cache->dir_name);

//	polar_validate_dir(path);
}

static void
polar_init_rel_size_table(polar_rel_size_table_t *table, uint32 tid)
{
	MemSet(table, 0, POLAR_REL_CACHE_TABLE_SIZE);

	table->tid = tid;
	table->rel_tail = 0;
	table->db_tail = REL_INFO_TOTAL_SIZE;
	table->max_lsn = InvalidXLogRecPtr;
	table->min_lsn = InvalidXLogRecPtr;
}

polar_rel_size_cache_t
polar_rel_size_shmem_init(const char *name, int blocks)
{
	bool found;
	polar_rel_size_cache_t cache;

	if (blocks <= 0)
		return NULL;

	cache = (polar_rel_size_cache_t)ShmemInitStruct(name,
													polar_rel_size_shmem_size(blocks), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);

		MemSet(cache, 0, polar_rel_size_shmem_size(blocks));
		StrNCpy(cache->dir_name, name, MAX_REL_SIZE_CACHE_DIR_LEN);

		polar_check_rel_size_dir(cache);
		polar_rel_size_remove_all(cache);

		cache->table_size = blocks;
		/* Init the first relation size cache table */
		polar_init_rel_size_table(REL_SIZE_CACHE_TABLE(cache, 0), 1);
		cache->min_tid = 1;

		LWLockRegisterTranche(LWTRANCHE_RELATION_SIZE_CACHE, "polar_rel_size_cache");
		LWLockInitialize(POLAR_REL_SIZE_CACHE_LOCK(cache), LWTRANCHE_RELATION_SIZE_CACHE);
	}
	else
		Assert(found);

	return cache;
}

static void
polar_save_rel_size_table(polar_rel_size_cache_t cache, polar_rel_size_table_t *table)
{
	File fd;
	int ret;
	char path[MAXPGPATH];

	POLAR_REL_CACHE_TABLE_PATH(cache, path, table->tid);

	fd = PathNameOpenFile(path, O_CREAT | O_RDWR | PG_BINARY);

	if (fd < 0)
		elog(FATAL, "Could not open file \"%s\" for write, %m", path);

	ret = FileWrite(fd, (char *)table, POLAR_REL_CACHE_TABLE_SIZE, 0, WAIT_EVENT_REL_SIZE_CACHE_WRITE);

	if (ret != POLAR_REL_CACHE_TABLE_SIZE)
		elog(FATAL, "Failed to write file \"%s\", ret=%d, %m", path, ret);

	FileClose(fd);
}

static bool
polar_table_space_available(polar_rel_size_table_t *table, size_t size)
{
	return (table->rel_tail + size < table->db_tail);
}

static polar_rel_size_table_t *
polar_allocate_table_space(polar_rel_size_cache_t cache, size_t size)
{
	polar_rel_size_table_t *active_table;
	uint64 tid;

	active_table = REL_SIZE_CACHE_TABLE(cache, cache->active_mid);
	tid = active_table->tid;

	if (!polar_table_space_available(active_table, size))
	{
		cache->active_mid = LOOP_NEXT_VALUE(cache->active_mid, cache->table_size);

		active_table = REL_SIZE_CACHE_TABLE(cache, cache->active_mid);

		if (active_table->tid > 0)
			polar_save_rel_size_table(cache, active_table);

		polar_init_rel_size_table(active_table, tid + 1);
	}

	return active_table;
}

static void
polar_update_lsn_range(polar_rel_size_table_t *table, XLogRecPtr lsn)
{
	if (XLogRecPtrIsInvalid(table->max_lsn))
	{
		table->max_lsn = lsn;
		table->min_lsn = lsn;
	}
	else
	{
		Assert(lsn >= table->max_lsn);
		table->max_lsn = lsn;
	}
}

void
polar_record_db_state(polar_rel_size_cache_t cache, XLogRecPtr lsn, Oid spc, Oid db, polar_db_state_t state)
{
	polar_rel_size_table_t *active_table;
	int pos;

	polar_database_state_t db_state =
	{
		.lsn = lsn,
		.spc = spc,
		.db = db,
		.state = state
	};

	active_table = polar_allocate_table_space(cache, sizeof(polar_database_state_t));

	pos = active_table->db_tail - sizeof(polar_database_state_t);
	memcpy(&active_table->info[pos], &db_state, sizeof(polar_database_state_t));
	active_table->db_tail = pos;

	polar_update_lsn_range(active_table, lsn);

	if (unlikely(polar_enable_debug))
	{
		elog(LOG, "polar_record_db_state lsn=%lX,spc=%d,db=%d,state=%d",
			 lsn, spc, db, state);
	}
}

void
polar_record_db_state_with_lock(polar_rel_size_cache_t cache, XLogRecPtr lsn, Oid spc, Oid db, polar_db_state_t state)
{
	LWLockAcquire(POLAR_REL_SIZE_CACHE_LOCK(cache), LW_EXCLUSIVE);
	polar_record_db_state(cache, lsn, spc, db, state);
	LWLockRelease(POLAR_REL_SIZE_CACHE_LOCK(cache));
}

void
polar_record_rel_size(polar_rel_size_cache_t cache, XLogRecPtr lsn, RelFileNode *node, ForkNumber fork, BlockNumber rel_size)
{
	polar_rel_size_table_t *active_table;
	int pos;

	polar_relation_size_t rel =
	{
		.lsn = lsn,
		.node = *node,
		.fork = fork,
		.rel_size = rel_size
	};

	active_table = polar_allocate_table_space(cache, sizeof(polar_relation_size_t));

	pos = active_table->rel_tail;

	memcpy(&active_table->info[pos], &rel, sizeof(polar_relation_size_t));

	active_table->rel_tail = pos + sizeof(polar_relation_size_t);

	polar_update_lsn_range(active_table, lsn);

	if (unlikely(polar_enable_debug))
	{
		elog(LOG, "polar_record_rel_size lsn=%lX,spc=%d,db=%d,rel=%d,fork=%d,rel_size=%d",
			 lsn, node->dbNode, node->relNode, node->spcNode, fork, rel_size);
	}
}

void
polar_record_rel_size_with_lock(polar_rel_size_cache_t cache, XLogRecPtr lsn, RelFileNode *node, ForkNumber fork, BlockNumber rel_size)
{
	LWLockAcquire(POLAR_REL_SIZE_CACHE_LOCK(cache), LW_EXCLUSIVE);

	polar_record_rel_size(cache, lsn, node, fork, rel_size);

	LWLockRelease(POLAR_REL_SIZE_CACHE_LOCK(cache));
}

/*
 * Search database's state according to spec and db, final state will
 * be restored into result. POLAR_DB_NOTFOUND will be set inside result
 * when none of the databases matched.
 */
static void
polar_search_db_state(polar_rel_size_table_t *table, XLogRecPtr lsn, Oid spc,
					  Oid db, polar_rel_search_result_t *result)
{
	int pos;
	polar_database_state_t db_state;

	pos = table->db_tail;
	result->state = POLAR_DB_NOTFOUND;

	while (pos < REL_INFO_TOTAL_SIZE)
	{
		memcpy(&db_state, &table->info[pos], sizeof(polar_database_state_t));

		/*
		 * Relation size cache record the end lsn of XLOG record, while logindex
		 * record the start lsn of XLOG record
		 */
		if (db_state.lsn <= lsn)
			break;

		if (db_state.db == db && db_state.spc == spc)
		{
			result->lsn = db_state.lsn;
			result->state = db_state.state;
			break;
		}

		pos += sizeof(polar_database_state_t);
	}
}

static search_state
polar_search_truncate_info(polar_rel_size_cache_t cache, polar_rel_size_table_t *table,
						   XLogRecPtr lsn, BufferTag *tag, polar_rel_search_result_t *result)
{
	int pos;
	polar_relation_size_t rel;

	pos = table->rel_tail;

	while (pos > 0)
	{
		memcpy(&rel, &table->info[pos - sizeof(polar_relation_size_t)], sizeof(polar_relation_size_t));

		/*
		 * Relation size cache record the end lsn of XLOG record, while logindex
		 * record the start lsn of XLOG record
		 */
		if (rel.lsn <= lsn)
			return END;

		if (RelFileNodeEquals(tag->rnode, rel.node)
				&& tag->forkNum == rel.fork && tag->blockNum >= rel.rel_size)
		{
			result->lsn = rel.lsn;
			result->state = POLAR_DB_BLOCK_TRUNCATED;
			result->size = rel.rel_size;

			return END;
		}

		pos -= sizeof(polar_relation_size_t);
	}

	return table->tid > cache->min_tid ? NEXT : END;
}

static search_state
polar_check_table_rel_size(polar_rel_size_cache_t cache, polar_rel_size_table_t *table, XLogRecPtr lsn,
						   BufferTag *tag, polar_rel_search_result_t *result)
{
	if (lsn > table->max_lsn)
		return END;

	if (result->state == POLAR_DB_NOTFOUND)
		polar_search_db_state(table, lsn, tag->rnode.spcNode, tag->rnode.dbNode, result);

	switch (result->state)
	{
		case POLAR_DB_NEW:
		{
			if (!result->ignore_error && lsn < result->lsn)
				elog(PANIC, "Got unpexected value from relation size cache, check to replay " POLAR_LOG_BUFFER_TAG_FORMAT " lsn=%lX, but it's created when lsn=%lX", POLAR_LOG_BUFFER_TAG(tag), lsn, result->lsn);

			return polar_search_truncate_info(cache, table, lsn, tag, result);
		}

		case POLAR_DB_NOTFOUND:
			return polar_search_truncate_info(cache, table, lsn, tag, result);

		case POLAR_DB_DROPED:
			if (!result->ignore_error && lsn >= result->lsn)
				elog(PANIC, "Got unexpected value from relation size cache, check to replay " POLAR_LOG_BUFFER_TAG_FORMAT " lsn=%lX, but it's dropped when lsn=%lX", POLAR_LOG_BUFFER_TAG(tag), lsn, result->lsn);

			return END;

		default:
			break;
	}

	return END;
}

static polar_rel_size_table_t *
polar_get_rel_size_table(polar_rel_size_cache_t cache, uint64 tid)
{
	uint32 mid;
	polar_rel_size_table_t *table;
	char path[MAXPGPATH];
	File fd;
	int ret;

	mid = (tid - 1) % cache->table_size;
	table = REL_SIZE_CACHE_TABLE(cache, mid);

	if (table->tid == tid)
		return table;

	POLAR_REL_CACHE_TABLE_PATH(cache, path, tid);
	fd = PathNameOpenFile(path, O_RDONLY | PG_BINARY);

	if (fd < 0)
		elog(FATAL, "Could not open file \"%s\" for read, %m", path);

	table = palloc(POLAR_REL_CACHE_TABLE_SIZE);

	if (!table)
		elog(FATAL, "Unable to allocate memory which size is %d", POLAR_REL_CACHE_TABLE_SIZE);

	ret = FileRead(fd, (char *)table, POLAR_REL_CACHE_TABLE_SIZE, 0, WAIT_EVENT_REL_SIZE_CACHE_READ);

	if (ret != POLAR_REL_CACHE_TABLE_SIZE)
		elog(FATAL, "Failed to read from file \"%s\", ret=%d,%m", path, ret);

	FileClose(fd);

	return table;
}

/*
 * Check whether the lsn is suitable for the block or not and restore
 * the right start lsn for replaying into lsn_changed.
 * This is just internal function, caller should consider function
 * polar_check_rel_block_valid_only and polar_check_rel_block_valid_and_lsn.
 */
static bool
polar_check_rel_block_valid_internal(polar_rel_size_cache_t cache, XLogRecPtr lsn, BufferTag *tag,
									 XLogRecPtr *lsn_changed)
{
	polar_rel_size_table_t *table;
	polar_rel_search_result_t result;

	result.state = POLAR_DB_NOTFOUND;
	result.lsn = InvalidXLogRecPtr;
	if (lsn_changed == NULL)
		result.ignore_error = false;
	else
		result.ignore_error = true;

	table = REL_SIZE_CACHE_TABLE(cache, cache->active_mid);

	if (!XLogRecPtrIsInvalid(table->max_lsn))
	{
		while (table != NULL && polar_check_table_rel_size(cache, table, lsn, tag, &result) != END
				&& table->tid > cache->min_tid)
		{
			uint64 tid = table->tid;

			/*
			 * Call pfree if table is not in shared memory
			 */
			if (!REL_TABLE_IN_SHARED_MEM(cache, table))
				pfree(table);

			table = polar_get_rel_size_table(cache, tid - 1);
		}

		if (!REL_TABLE_IN_SHARED_MEM(cache, table))
			pfree(table);
	}

	switch (result.state)
	{
		case POLAR_DB_NOTFOUND:
			if (lsn_changed)
				*lsn_changed = lsn;

			return true;

		case POLAR_DB_NEW:
			if (lsn_changed)
				*lsn_changed = result.lsn;

			return true;

		case POLAR_DB_DROPED:
		case POLAR_DB_BLOCK_TRUNCATED:
			if (lsn_changed)
				*lsn_changed = result.lsn;

			return false;

		default:
			elog(PANIC, "Got unexpected state=%d when check block valid", result.state);
	}
}

/*
 * Only check whether the lsn is suitable for the block or not.
 * Usually, the caller make sure that the block will be modified
 * by record pointed by lsn.
 * Allow two cases for PANIC error:
 * (1) The lsn is lower than lsn of database's XLOG_DBASE_CREATE record.
 * (2) The lsn is higher than lsn of database's XLOG_DBASE_DROP record.
 *
 * Must acquire cache lock before call this function.
 */
bool
polar_check_rel_block_valid_only(polar_rel_size_cache_t cache, XLogRecPtr lsn, BufferTag *tag)
{
	return polar_check_rel_block_valid_internal(cache, lsn, tag, NULL);
}

/*
 * Check whether the lsn is suitable for the block or not and restore
 * the right start lsn for replaying into lsn_changed.
 * It is not like polar_check_rel_block_valid_only, there is no cases for
 * PANIC error. Becase it can return right start lsn when the parameter lsn
 * is not suitable for the block.
 *
 * Must acquire cache lock before call this function.
 */
bool
polar_check_rel_block_valid_and_lsn(polar_rel_size_cache_t cache, XLogRecPtr lsn, BufferTag *tag,
									XLogRecPtr *lsn_changed)
{
	return polar_check_rel_block_valid_internal(cache, lsn, tag, lsn_changed);
}

static void
polar_unlink_rel_size_table(polar_rel_size_cache_t cache, uint64 tid)
{
	char path[MAXPGPATH];
	POLAR_REL_CACHE_TABLE_PATH(cache, path, tid);

	if (unlink(path) < 0)
		elog(WARNING, "Could not remove file \"%s\":%m", path);
}

void
polar_truncate_rel_size_cache(polar_rel_size_cache_t cache, XLogRecPtr lsn)
{
	uint64 max_tid, tid;
	polar_rel_size_table_t *table;

	elog(LOG, "Truncate relation size cache from %lX", lsn);
	LWLockAcquire(POLAR_REL_SIZE_CACHE_LOCK(cache), LW_EXCLUSIVE);

	table = REL_SIZE_CACHE_TABLE(cache, cache->active_mid);
	max_tid = table->tid;
	tid = cache->min_tid;

	while (tid < max_tid)
	{
		XLogRecPtr tbl_max_lsn;

		table = polar_get_rel_size_table(cache, tid);
		tbl_max_lsn = table->max_lsn;

		if (!REL_TABLE_IN_SHARED_MEM(cache, table))
			pfree(table);

		if (lsn >= tbl_max_lsn)
		{
			if (REL_TABLE_IN_SHARED_MEM(cache, table))
				MemSet(table, 0, POLAR_REL_CACHE_TABLE_SIZE);
			else
				polar_unlink_rel_size_table(cache, tid);

			cache->min_tid++;
		}

		tid++;
	}

	LWLockRelease(POLAR_REL_SIZE_CACHE_LOCK(cache));
}
