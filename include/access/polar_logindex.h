/*-------------------------------------------------------------------------
 *
 * polar_logindex.h
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
 *    src/include/access/polar_logindex.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_LOG_INDEX_H
#define POLAR_LOG_INDEX_H

#include "access/rmgr.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "storage/block.h"
#include "storage/buf_internals.h"
#include "storage/relfilenode.h"
#include "utils/guc.h"

typedef uint16  log_seg_id_t;
typedef uint64  log_idx_table_id_t;
typedef uint64  log_range_id_t;

#define LOG_INDEX_SUPPORT_NO_PREVIOUS_LSN
#define POLAR_MAX_SHMEM_NAME (128)

//#define POLAR_DATA_DIR() (POLAR_FILE_IN_SHARED_STORAGE() ? polar_datadir : DataDir)

//#define POLAR_FILE_PATH(path, orign) \
//	snprintf((path), MAXPGPATH, "%s/%s", POLAR_DATA_DIR(), (orign));

#define POLAR_READ_BUFFER_FOR_REDO(record, block_id, buffer) \
	XLogReadBufferForRedoExtended((record), (block_id),\
								  RBM_NORMAL, false, (buffer))


#define POLAR_INIT_BUFFER_FOR_REDO(record, block_id, buffer) \
	do { \
		*(buffer) = (!BufferIsValid(*buffer)) ? XLogInitBufferForRedo(record, block_id) : *(buffer); \
	} while (0)

struct log_mem_table_t;
struct log_index_iter_data_t;
typedef struct log_index_snapshot_t             *logindex_snapshot_t;
typedef struct log_index_page_iter_data_t       *log_index_page_iter_t;
typedef struct log_index_lsn_iter_data_t        *log_index_lsn_iter_t;

typedef bool (*logindex_table_flushable)(struct log_mem_table_t *, void *data);

typedef struct log_index_lsn_t
{
	BufferTag           *tag;
	XLogRecPtr          lsn;
	XLogRecPtr          prev_lsn;
} log_index_lsn_t;

typedef enum
{
	ITERATE_STATE_FORWARD,
	ITERATE_STATE_BACKWARD,
	ITERATE_STATE_FINISHED,
	ITERATE_STATE_HOLLOW,
	ITERATE_STATE_CORRUPTED
} log_index_iter_state_t;

extern Size polar_logindex_shmem_size(uint64 logindex_mem_tbl_size, int bloom_blocks);


extern void polar_logindex_shmem_init(uint64 logindex_mem_tbl_size, int bloom_blocks);
extern logindex_snapshot_t polar_logindex_snapshot_shmem_init(const char *name, uint64 logindex_mem_tbl_size,
															  int bloom_blocks, int tranche_id_begin, int tranche_id_end, logindex_table_flushable table_flushable, void *extra_data);

extern uint64 polar_logindex_convert_mem_tbl_size(uint64 mem_size);
extern void polar_logindex_create_local_cache(logindex_snapshot_t logindex_snapshot, const char *cache_name, uint32 max_segments);


extern MemoryContext polar_get_redo_context(void);

extern MemoryContext polar_logindex_memory_context(void);
extern uint64 polar_logindex_mem_tbl_size(logindex_snapshot_t logindex_snapshot);
extern uint64 polar_logindex_used_mem_tbl_size(logindex_snapshot_t logindex_snapshot);
extern void polar_logindex_set_start_lsn(logindex_snapshot_t logindex_snapshot, XLogRecPtr start_lsn);
extern XLogRecPtr polar_logindex_snapshot_init();
extern void polar_logindex_add_lsn(BufferTag *tag, XLogRecPtr prev, XLogRecPtr lsn);
extern bool polar_logindex_write_table(logindex_snapshot_t logindex_snapshot, struct log_mem_table_t *table);
extern bool polar_logindex_bg_write(logindex_snapshot_t logindex_snapshot);
extern XLogRecPtr polar_logindex_start_lsn(logindex_snapshot_t logindex_snapshot);

extern log_index_page_iter_t polar_logindex_create_page_iterator(logindex_snapshot_t logindex_snapshot, BufferTag *tag, XLogRecPtr min_lsn, XLogRecPtr max_lsn, bool before_promote);
extern void polar_logindex_release_page_iterator(log_index_page_iter_t iter);
extern log_index_lsn_t *polar_logindex_page_iterator_next(log_index_page_iter_t iter);
extern bool polar_logindex_page_iterator_end(log_index_page_iter_t iter);
extern log_index_iter_state_t polar_logindex_page_iterator_state(log_index_page_iter_t iter);
extern XLogRecPtr polar_logindex_page_iterator_max_lsn(log_index_page_iter_t iter);
extern XLogRecPtr polar_logindex_page_iterator_min_lsn(log_index_page_iter_t iter);
extern BufferTag  *polar_logindex_page_iterator_buf_tag(log_index_page_iter_t iter);

extern log_index_lsn_iter_t polar_logindex_create_lsn_iterator(logindex_snapshot_t logindex_snapshot, XLogRecPtr lsn);
extern void polar_logindex_release_lsn_iterator(log_index_lsn_iter_t iter);
extern log_index_lsn_t *polar_logindex_lsn_iterator_next(logindex_snapshot_t logindex_snapshot, log_index_lsn_iter_t iter);

extern void polar_logindex_truncate(logindex_snapshot_t logindex_snapshot, XLogRecPtr lsn);
extern bool polar_logindex_check_state(logindex_snapshot_t logindex_snapshot, uint32 state);

extern void polar_xlog_decode_data(XLogReaderState *state);

/*
 * POLAR: Return the newest byte position which all logindex info could be flushed before it.
 */
#define POLAR_LOGINDEX_FLUSHABLE_LSN() \
	(RecoveryInProgress() ? GetXLogReplayRecPtr(NULL) : GetFlushRecPtr())

#define POLAR_LOGINDEX_MASTER_WRITE_TEST (1 << 0)
#define POLAR_LOGINDEX_REPLICA_WRITE_TEST (1 << 1)
#define POLAR_LOGINDEX_WRITE_MASK (POLAR_LOGINDEX_MASTER_WRITE_TEST | POLAR_LOGINDEX_REPLICA_WRITE_TEST)

#define POLAR_UPDATE_BACKEND_LSN_INTERVAL (1 * 1000)

/* define logindex_snapshot bit state */
#define POLAR_LOGINDEX_STATE_INITIALIZED    (1U << 0)  /* The logindex snapshot is initialized */
#define POLAR_LOGINDEX_STATE_ADDING         (1U << 1)  /* Finish checking saved lsn, and it's ready to add new lsn */
#define POLAR_LOGINDEX_STATE_WRITABLE       (1U << 2)  /* It's enabled to write table to shared storage */

#define LOOP_NEXT_VALUE(id, size) \
	(((id) == ((size) - 1)) ? 0 : ((id) + 1))

#define LOOP_PREV_VALUE(id, size) \
	(((id) == 0) ? ((size) - 1) : ((id) -1))

#define POLAR_LOG_INDEX_ADD_LSN(logindex_snapshot, state, tag) \
	do { \
		polar_logindex_add_lsn((tag), InvalidXLogRecPtr, (state)->ReadRecPtr); \
	} while (0)

#define POLAR_GET_LOG_TAG(state, tag, block_id) \
	do { \
		RelFileNode rnode; \
		ForkNumber  forknum; \
		BlockNumber blkno; \
		if (!XLogRecGetBlockTag(state, block_id, &rnode, &forknum, &blkno)) \
			elog(PANIC, "Failed to locate blod_id %d, rmid=%d,info=%d", \
				 block_id, XLogRecGetRmid(state), XLogRecGetInfo(state)); \
		INIT_BUFFERTAG(tag, rnode, forknum, blkno); \
	} while (0)

#define NOTIFY_LOGINDEX_BG_WORKER(latch) \
	do { \
		if (latch) \
			SetLatch(latch); \
	} while (0)

extern void polar_logindex_flush_table(logindex_snapshot_t logindex_snapshot, XLogRecPtr checkpoint_lsn);
extern void polar_logindex_flush_active_table(logindex_snapshot_t logindex_snapshot);
extern XLogRecPtr polar_logindex_mem_table_max_lsn(struct log_mem_table_t *table);

extern XLogRecPtr polar_get_logindex_snapshot_max_lsn(logindex_snapshot_t logindex_snapshot);
/* Read logindex meta and get min lsn of logindex tables which are flushed to the storage */
extern XLogRecPtr polar_get_logindex_snapshot_storage_min_lsn(logindex_snapshot_t logindex_snapshot);

extern const char *polar_get_logindex_snapshot_dir(logindex_snapshot_t logindex_snapshot);

extern void polar_load_logindex_snapshot_from_storage(logindex_snapshot_t logindex_snapshot, XLogRecPtr start_lsn);
extern void polar_logindex_invalid_bloom_cache(logindex_snapshot_t logindex_snapshot, log_idx_table_id_t tid);

extern void polar_logindex_set_writer_latch(logindex_snapshot_t logindex_snapshot, struct Latch *latch);
extern void polar_logindex_online_promote(logindex_snapshot_t logindex_snapshot);
extern XLogRecPtr polar_logindex_check_valid_start_lsn(logindex_snapshot_t logindex_snapshot);
extern int polar_trace_logindex(int trace_level);
extern void polar_logindex_update_promoted_info(logindex_snapshot_t logindex_snapshot, XLogRecPtr last_replayed_lsn);
#endif
