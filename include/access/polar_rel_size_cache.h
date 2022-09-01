/*
 * polar_rel_size_cache.h
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 *
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
 *      src/include/access/polar_rel_size_cache.h
 */
#ifndef POLAR_REL_SIZE_CACHE_H
#define POLAR_REL_SIZE_CACHE_H

#include "storage/buf_internals.h"

#define POLAR_REL_CACHE_TABLE_PATH(instance, path, tid) \
	snprintf((path), MAXPGPATH, "%s/%s/%04lX", DataDir, (instance)->dir_name, (tid));

#define POLAR_REL_SIZE_CACHE_LOCK(cache)             (&((cache)->lock.lock))
#define POLAR_REL_CACHE_TABLE_SIZE (BLCKSZ)

#define REL_INFO_TOTAL_SIZE (POLAR_REL_CACHE_TABLE_SIZE - offsetof(polar_rel_size_table_t, info))
#define MAX_REL_SIZE_CACHE_DIR_LEN (32)

typedef enum
{
	POLAR_DB_NOTFOUND,
	POLAR_DB_NEW,
	POLAR_DB_BLOCK_TRUNCATED,
	POLAR_DB_DROPED
} polar_db_state_t;

typedef struct polar_database_state_t
{
	XLogRecPtr          lsn;    /* The XLOG lSN which drop database */
	Oid                 spc;    /* tablespace */
	Oid                 db;     /* database */
	polar_db_state_t      state;
} polar_database_state_t;

typedef struct polar_relation_size_t
{
	XLogRecPtr  lsn;        /* The XLOG LSN which truncate relation */
	RelFileNode node;       /* physical relation identifier */
	ForkNumber  fork;
	BlockNumber rel_size;   /* relation size after truncate */
} polar_relation_size_t;

typedef struct polar_rel_size_table_t
{
	uint64      tid;
	uint16      rel_tail;
	uint16      db_tail;
	XLogRecPtr  min_lsn;
	XLogRecPtr  max_lsn;
	char        info[FLEXIBLE_ARRAY_MEMBER];
} polar_rel_size_table_t;

typedef struct polar_rel_size_cache_data_t
{
	LWLockMinimallyPadded       lock;
	char                        dir_name[MAX_REL_SIZE_CACHE_DIR_LEN];
	uint32                      active_mid;
	uint32                      min_tid;
	uint32                      table_size;
	char                        table_data[FLEXIBLE_ARRAY_MEMBER];
} polar_rel_size_cache_data_t;

typedef polar_rel_size_cache_data_t *polar_rel_size_cache_t;

typedef struct polar_rel_search_result_t
{
	polar_db_state_t state;
	XLogRecPtr     lsn;
	BlockNumber    size;
	bool	ignore_error;
} polar_rel_search_result_t;


extern Size polar_rel_size_shmem_size(int blocks);
extern polar_rel_size_cache_t polar_rel_size_shmem_init(const char *name, int blocks);
extern void polar_record_db_state(polar_rel_size_cache_t cache, XLogRecPtr lsn, Oid spc, Oid db, polar_db_state_t state);
extern void polar_record_db_state_with_lock(polar_rel_size_cache_t cache, XLogRecPtr lsn, Oid spc, Oid db, polar_db_state_t state);
extern void polar_record_rel_size(polar_rel_size_cache_t cache, XLogRecPtr lsn, RelFileNode *node, ForkNumber fork,  BlockNumber rel_size);
extern void polar_record_rel_size_with_lock(polar_rel_size_cache_t cache, XLogRecPtr lsn, RelFileNode *node, ForkNumber fork,  BlockNumber rel_size);
extern bool polar_check_rel_block_valid_only(polar_rel_size_cache_t cache, XLogRecPtr lsn, BufferTag *tag);
extern bool polar_check_rel_block_valid_and_lsn(polar_rel_size_cache_t cache, XLogRecPtr lsn, BufferTag *tag, XLogRecPtr *lsn_changed);
extern void polar_truncate_rel_size_cache(polar_rel_size_cache_t cache, XLogRecPtr lsn);

#endif
