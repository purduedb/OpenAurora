/*-------------------------------------------------------------------------
 *
 * polar_logindex_redo.h
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
 *    src/include/access/polar_logindex_redo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_LOGINDEX_REDO_H
#define POLAR_LOGINDEX_REDO_H

#include "access/polar_fullpage.h"
#include "access/polar_logindex.h"
#include "access/polar_mini_transaction.h"
#include "access/polar_queue_manager.h"
#include "access/polar_rel_size_cache.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "storage/buf_internals.h"
#include "storage/polar_procpool.h"
#include "polar_flashback/polar_flashback_log_internal.h"

extern int                  polar_logindex_mem_size;

typedef void (*polar_logindex_replay_end_callback)(Buffer, void *arg);

typedef struct polar_logindex_replay_end_t
{
	polar_logindex_replay_end_callback      func;
	Buffer                                  buffer;
	void                                    *arg;
} polar_logindex_replay_end_t;

typedef struct polar_logindex_promote_replay_t
{
	XLogRecPtr                          consist_lsn;
	XLogRecPtr                          min_recovery_lsn;
	XLogRecPtr                          mark_replayed_lsn;
} polar_logindex_promote_replay_t;

typedef struct polar_logindex_redo_ctl_data_t
{
	mini_trans_t            mini_trans;
	polar_fullpage_ctl_t    fullpage_ctl;
	logindex_snapshot_t     wal_logindex_snapshot;
	logindex_snapshot_t     fullpage_logindex_snapshot;
	polar_ringbuf_t         xlog_queue;
	polar_rel_size_cache_t  rel_size_cache;

	XLogRecPtr              xlog_replay_from; /* Record the start lsn we replayed from. */

	/*
	 * POLAR: set background process redo state
	 */
	pg_atomic_uint32 bg_redo_state;

	/*
	 * POLAR: The xlog which lsn < bg_replayed_lsn is replayed by background process
	 */
	XLogRecPtr  bg_replayed_lsn;

	/*
	 * POLAR: Record the start position of the last replayed xlog
	 */
	XLogRecPtr  last_replayed_read_ptr;

	XLogRecPtr  promote_oldest_lsn;

	slock_t            info_lck; /* locks shared variables shown above */

	struct  Latch       *bg_worker_latch;
	polar_task_sched_t  *parallel_sched;
} polar_logindex_redo_ctl_data_t;

typedef polar_logindex_redo_ctl_data_t *polar_logindex_redo_ctl_t;

typedef struct log_index_redo_t
{
	const char *rm_name;
	/* Used by master node to save logindex */
	void (*rm_polar_idx_save)(polar_logindex_redo_ctl_t instance, XLogReaderState *record);
	/* Used by replica node to parse and save logindex */
	bool (*rm_polar_idx_parse)(polar_logindex_redo_ctl_t instance, XLogReaderState *record);
	/* Used to replay XLOG */
	XLogRedoAction(*rm_polar_idx_redo)(polar_logindex_redo_ctl_t instance, XLogReaderState *record, BufferTag *tag, Buffer *buffer);
} log_index_redo_t;

typedef struct parallel_replay_task_node_t
{
	polar_task_node_t   task;
	BufferTag           tag;
	XLogRecPtr          lsn;
	XLogRecPtr          prev_lsn;
} parallel_replay_task_node_t;

typedef struct polar_logindex_bg_redo_ctl_t
{
	polar_logindex_redo_ctl_t   instance;
	log_index_lsn_iter_t        lsn_iter;    /* the iterator for lsn */
	log_index_lsn_t             *replay_page;   /* current iterator page need to do buffer only replay */
	int                         replay_batch_size; /* The maxium number of page to replay in one loop */
	XLogReaderState             *state;
	XLogRecPtr                  max_dispatched_lsn;     /* The max lsn value which dispatched to replay */
	polar_task_sched_ctl_t      *sched_ctl;
} polar_logindex_bg_redo_ctl_t;

extern polar_logindex_redo_ctl_t polar_logindex_redo_instance;

typedef enum
{
	POLAR_NOT_LOGINDEX_BG_PROC = 0, /* Indicate it's not related to logindex background process */
	POLAR_LOGINDEX_DISPATCHER,      /* Indicate it's logindex background dispatcher process */
	POLAR_LOGINDEX_PARALLEL_REPLAY, /* Indicate it's logindex background process to do parallel replay */
} polar_logindex_bg_proc_t;

/* POLAR: Flag set when in replaying and marking buffer dirty process */
extern polar_logindex_bg_proc_t polar_bg_replaying_process;

#define POLAR_IN_LOGINDEX_PARALLEL_REPLAY() (polar_bg_replaying_process == POLAR_LOGINDEX_PARALLEL_REPLAY)

/* POLAR: Parallel replay standby mode is off when the flashback log is enable in the standby node. */
//#define POLAR_ENABLE_PARALLEL_REPLAY_STANDBY_MODE() (polar_in_standby_mode() && polar_logindex_redo_instance && \
//	polar_enable_parallel_replay_standby_mode)
#define POLAR_ENABLE_PARALLEL_REPLAY_STANDBY_MODE() ( 1 )

//#define POLAR_IN_PARALLEL_REPLAY_STANDBY_MODE(ins) (polar_in_standby_mode() && ins && \
//	polar_get_bg_redo_state(ins) == POLAR_BG_PARALLEL_REPLAYING)
#define POLAR_IN_PARALLEL_REPLAY_STANDBY_MODE(ins) ( ins && \
	polar_get_bg_redo_state(ins) == POLAR_BG_PARALLEL_REPLAYING)

#define POLAR_LOGINDEX_ENABLE_XLOG_QUEUE() (polar_logindex_redo_instance && polar_logindex_redo_instance->xlog_queue)
#define POLAR_LOGINDEX_ENABLE_FULLPAGE() (polar_enable_fullpage_snapshot && polar_logindex_redo_instance && polar_logindex_redo_instance->fullpage_ctl)
#define POLAR_LOGINDEX_ENABLE_REL_SIZE_CACHE() (polar_logindex_redo_instance && polar_logindex_redo_instance->rel_size_cache)
#define POLAR_LOGINDEX_ENABLE_PAGE_OUTDATE() (polar_logindex_redo_instance)
#define POLAR_LOGINDEX_ENABLE_ONLINE_PROMOTE() (polar_enable_shared_storage_mode && polar_logindex_redo_instance && polar_in_replica_mode())


extern Size polar_logindex_redo_shmem_size(void);
extern void polar_logindex_redo_shmem_init(void);
extern MemoryContext polar_get_redo_context(void);

extern XLogRecPtr polar_logindex_redo_init(polar_logindex_redo_ctl_t instance, XLogRecPtr checkpoint_lsn, bool read_only);
extern void polar_logindex_redo_flush_data(polar_logindex_redo_ctl_t instance, XLogRecPtr checkpoint_lsn);
extern bool polar_logindex_redo_bg_flush_data(polar_logindex_redo_ctl_t instance);
extern bool polar_logindex_redo_bg_replay(polar_logindex_bg_redo_ctl_t *ctl, bool *can_hold);
extern XLogRecPtr polar_logindex_redo_get_min_replay_from_lsn(polar_logindex_redo_ctl_t instance, XLogRecPtr consist_lsn);
extern bool polar_logindex_parse_xlog(polar_logindex_redo_ctl_t instance, RmgrId rmid, XLogReaderState *state, XLogRecPtr redo_start_lsn, XLogRecPtr *mini_trans_lsn);
extern void polar_logindex_rw_save(polar_logindex_redo_ctl_t instance);
extern void polar_bg_redo_set_replayed_lsn(polar_logindex_redo_ctl_t instance, XLogRecPtr lsn);
extern XLogRecPtr polar_bg_redo_get_replayed_lsn(polar_logindex_redo_ctl_t instance);
extern void polar_set_last_replayed_read_ptr(polar_logindex_redo_ctl_t instance, XLogRecPtr lsn);
extern XLogRecPtr polar_get_last_replayed_read_ptr(polar_logindex_redo_ctl_t instance);
extern bool polar_logindex_require_backend_redo(polar_logindex_redo_ctl_t instance, ForkNumber fork_num, XLogRecPtr *replay_from);
extern void polar_logindex_redo_set_valid_info(polar_logindex_redo_ctl_t instance, XLogRecPtr end_of_log);
extern XLogRecPtr polar_logindex_redo_parse_start_lsn(polar_logindex_redo_ctl_t instance);

extern XLogRecord *polar_logindex_read_xlog(XLogReaderState *state, XLogRecPtr lsn);

extern void polar_logindex_remove_old_files(polar_logindex_redo_ctl_t instance);
extern void polar_logindex_remove_all(void);
extern XLogRecPtr polar_logindex_redo_start_lsn(polar_logindex_redo_ctl_t instance);
extern void polar_logindex_redo_online_promote(polar_logindex_redo_ctl_t instance);
extern void polar_logindex_redo_abort(polar_logindex_redo_ctl_t instance);

extern bool polar_logindex_io_lock_apply(polar_logindex_redo_ctl_t instance, BufferDesc *buf_hdr, XLogRecPtr replay_from, XLogRecPtr checkpoint_lsn);

extern XLogRecPtr polar_logindex_apply_page(polar_logindex_redo_ctl_t instance, XLogRecPtr start_lsn, XLogRecPtr end_lsn, BufferTag *tag, Buffer *buffer);
extern void polar_logindex_lock_apply_buffer(polar_logindex_redo_ctl_t instance, Buffer *buffer);
extern bool polar_logindex_lock_apply_page_from(polar_logindex_redo_ctl_t instance, XLogRecPtr start_lsn, BufferTag *tag, Buffer *buffer);
extern bool polar_logindex_restore_fullpage_snapshot_if_needed(polar_logindex_redo_ctl_t instance, BufferTag *tag, Buffer *buffer);
extern bool polar_enable_logindex_parse(void);

extern void polar_bg_redo_mark_logindex_ready(void);
extern XLogRecPtr polar_max_xlog_rec_ptr(XLogRecPtr a, XLogRecPtr b);
extern XLogRecPtr polar_min_xlog_rec_ptr(XLogRecPtr a, XLogRecPtr b);
extern void polar_log_page_iter_context(void);

extern void polar_logindex_apply_one_record(polar_logindex_redo_ctl_t instance, XLogReaderState *state, BufferTag *tag, Buffer *buffer);

extern void polar_logindex_save_lsn(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern void polar_logindex_save_block(polar_logindex_redo_ctl_t instance, XLogReaderState *state, uint8 block_id);

extern Buffer polar_logindex_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state, BufferTag *tag, bool get_cleanup_lock, polar_page_lock_t *page_lock);

extern Buffer polar_logindex_outdate_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state, BufferTag *tag, bool get_cleanup_lock, polar_page_lock_t *page_lock, bool vm_parse);

extern void polar_logindex_redo_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state, uint8 block_id);
extern void polar_logindex_cleanup_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state, uint8 block_id);
extern bool polar_xlog_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern void polar_xlog_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern XLogRedoAction polar_xlog_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *state,  BufferTag *tag, Buffer *buffer);
extern bool polar_storage_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record);
extern bool polar_heap2_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern void polar_heap2_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern XLogRedoAction polar_heap2_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *state,  BufferTag *tag, Buffer *buffer);
extern bool polar_heap_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern void polar_heap_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern XLogRedoAction polar_heap_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *state,  BufferTag *tag, Buffer *buffer);
extern bool polar_btree_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern void polar_btree_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern XLogRedoAction polar_btree_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *state,  BufferTag *tag, Buffer *buffer);
extern bool polar_hash_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern void polar_hash_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern XLogRedoAction polar_hash_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *state,  BufferTag *tag, Buffer *buffer);

extern bool polar_gin_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern void polar_gin_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern XLogRedoAction polar_gin_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *state, BufferTag *tag, Buffer *buffer);
extern bool polar_gist_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern void polar_gist_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern XLogRedoAction polar_gist_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *state,  BufferTag *tag, Buffer *buffer);
extern bool polar_seq_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern void polar_seq_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern XLogRedoAction polar_seq_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *state, BufferTag *tag, Buffer *buffer);
extern bool polar_spg_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern void polar_spg_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern XLogRedoAction polar_spg_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *state,  BufferTag *tag, Buffer *buffer);
extern bool polar_brin_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern void polar_brin_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern XLogRedoAction polar_brin_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *state, BufferTag *tag, Buffer *buffer);
extern bool polar_generic_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern void polar_generic_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *state);
extern XLogRedoAction polar_generic_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *state, BufferTag *tag, Buffer *buffer);

extern void polar_set_bg_redo_state(polar_logindex_redo_ctl_t instance, uint32 state);

extern void polar_logindex_wakeup_bg_replay(polar_logindex_redo_ctl_t instance, XLogRecPtr read_ptr, XLogRecPtr end_ptr);
extern XLogRecPtr polar_online_promote_fake_oldest_lsn(void);
extern void polar_logindex_bg_worker_main(void);
extern polar_task_sched_ctl_t *polar_create_parallel_replay_sched_ctl(polar_logindex_bg_redo_ctl_t *bg_ctl);

extern bool polar_need_do_bg_replay(polar_logindex_redo_ctl_t instance);
extern bool polar_logindex_bg_promoted(polar_logindex_redo_ctl_t instance);
extern polar_logindex_bg_redo_ctl_t *polar_create_bg_redo_ctl(polar_logindex_redo_ctl_t instance, bool enable_processes_pool);
extern void polar_release_bg_redo_ctl(polar_logindex_bg_redo_ctl_t *ctl);
extern void polar_reset_bg_replayed_lsn(polar_logindex_redo_ctl_t instance);
extern void polar_online_promote_data(polar_logindex_redo_ctl_t instance, TransactionId oldest_active_xid);
extern void polar_standby_promote_data(polar_logindex_redo_ctl_t instance);
extern void polar_wait_logindex_bg_stop_replay(polar_logindex_redo_ctl_t instance, Latch *latch);
extern XLogRecPtr polar_logindex_find_first_fpi(polar_logindex_redo_ctl_t instance, XLogRecPtr start_lsn, XLogRecPtr end_lsn,
						  BufferTag *tag, Buffer *buffer, bool apply);

#define POLAR_MAX_REPLAY_END_CALLBACK XLR_MAX_BLOCK_ID

#define POLAR_READ_MODE(buffer) (BufferIsValid(buffer) ? RBM_NORMAL_VALID : RBM_NORMAL)

#define POLAR_READ_BUFFER_FOR_REDO(record, block_id, buffer) \
	XLogReadBufferForRedoExtended((record), (block_id),\
								  POLAR_READ_MODE(*(buffer)), false, (buffer))

#define POLAR_INIT_BUFFER_FOR_REDO(record, block_id, buffer) \
	do { \
		*(buffer) = (!BufferIsValid(*buffer)) ? XLogInitBufferForRedo(record, block_id) : *(buffer); \
	} while (0)

#define POLAR_MINI_TRANS_REDO_PARSE(instance, record, block_id, tag, lock, buf) \
	do { \
		POLAR_GET_LOG_TAG((record), (tag), (block_id)); \
		lock = polar_logindex_mini_trans_lock((instance)->mini_trans, &(tag), LW_EXCLUSIVE, NULL); \
		buf = polar_logindex_parse((instance), (record), &(tag), false, &(lock)); \
	} while (0)

#define POLAR_MINI_TRANS_CLEANUP_PARSE(instance, record, block_id, tag, lock, buf) \
	do { \
		POLAR_GET_LOG_TAG((record), (tag), (block_id)); \
		lock = polar_logindex_mini_trans_lock((instance)->mini_trans, &(tag), LW_EXCLUSIVE, NULL); \
		buf = polar_logindex_parse((instance), (record), &(tag), true, &(lock)); \
	} while (0)


/*
 * POLAR: Flags for buffer redo state
 * Note: Must update POLAR_BUF_REDO_FLAG_MASK when delete or add more state flags
 */
#define POLAR_REDO_LOCKED               (1U << 1) /* redo state is locked */
#define POLAR_REDO_READ_IO_END          (1U << 2) /* Finish to read buffer content from storage */
#define POLAR_REDO_REPLAYING            (1U << 3) /* It's replaying buffer content */
#define POLAR_REDO_OUTDATE              (1U << 4) /* The buffer content is outdated */
#define POLAR_REDO_INVALIDATE           (1U << 5) /* This buffer is invalidating */
/* POLAR: Mask for redo flag */
#define POLAR_BUF_REDO_FLAG_MASK (0x003EU)
/*
 * POLAR: Flags for flashback log
 * Note: Must update POLAR_BUF_FLASHBACK_FLAG_MASK when delete or add more state flags
 */
#define POLAR_BUF_IN_FLOG_LIST         (1U << 31)
#define POLAR_BUF_INSERTING_FLOG       (1U << 30)
#define POLAR_BUF_FLOG_DISABLE         (1U << 29) /* Disable the flashback log for buffer, now just in recovery. */
#define POLAR_BUF_FLOG_LOST_CHECKED    (1U << 28) /* The buffer has been flashback log lost checked, just used in online promote mode */

/* POLAR: Mask for above */
#define POLAR_BUF_FLASHBACK_FLAG_MASK (0xF0000000U)

/*
 * POLAR: Background redo state
 */
#define POLAR_BG_REDO_NOT_START         (0)        /* Background process is not doing replaying */
#define POLAR_BG_RO_BUF_REPLAYING       (1)        /* Background process is replaying buffer which exists in buffer pool */
#define POLAR_BG_PARALLEL_REPLAYING     (2)        /* Background process is replaying in parallel and marking all buffer dirty */
#define POLAR_BG_WAITING_RESET          (3)        /* Background process is waiting for startup process to reset replay lsn for online promote */
#define POLAR_BG_ONLINE_PROMOTE         (4)        /* Background process is replaying and marking all buffer dirty in parallel from last consistent lsn */

/*
 * Functions for acquiring/releasing a shared buffer redo state's spinlock.
 */
extern uint32 polar_lock_redo_state(BufferDesc *desc);
inline static void
polar_unlock_redo_state(BufferDesc *desc, uint32 state)
{
	do
	{
		pg_write_barrier();
		pg_atomic_write_u32(&(desc->polar_redo_state), state & (~POLAR_REDO_LOCKED));
	}
	while (0);
}

inline static bool
polar_redo_check_state(BufferDesc *desc, uint32 state)
{
	pg_read_barrier();
	return pg_atomic_read_u32(&(desc->polar_redo_state)) & state;
}

#define POLAR_RECORD_DB_STATE(spc, db, state) \
	do {    \
		XLogRecPtr lsn; \
		if (POLAR_LOGINDEX_ENABLE_REL_SIZE_CACHE()) \
		{ \
			LWLockAcquire(POLAR_REL_SIZE_CACHE_LOCK(polar_logindex_redo_instance->rel_size_cache), LW_EXCLUSIVE); \
			lsn = InRecovery ? polar_get_replay_end_rec_ptr(NULL) : GetXLogInsertRecPtr(); \
			Assert(!XLogRecPtrIsInvalid(lsn)); \
			polar_record_db_state(polar_logindex_redo_instance->rel_size_cache, (lsn), (spc), (db), (state)); \
			LWLockRelease(POLAR_REL_SIZE_CACHE_LOCK(polar_logindex_redo_instance->rel_size_cache)); \
		} \
	} while (0)

#define POLAR_RECORD_REL_SIZE(node, fork, rel_size) \
	do { \
		XLogRecPtr lsn; \
		if (POLAR_LOGINDEX_ENABLE_REL_SIZE_CACHE()) \
		{ \
			LWLockAcquire(POLAR_REL_SIZE_CACHE_LOCK(polar_logindex_redo_instance->rel_size_cache), LW_EXCLUSIVE); \
			lsn = InRecovery ? polar_get_replay_end_rec_ptr(NULL) : GetXLogInsertRecPtr(); \
			Assert(!XLogRecPtrIsInvalid(lsn)); \
			polar_record_rel_size(polar_logindex_redo_instance->rel_size_cache, (lsn), (node), (fork), (rel_size)); \
			LWLockRelease(POLAR_REL_SIZE_CACHE_LOCK(polar_logindex_redo_instance->rel_size_cache)); \
		} \
	} while (0)

static inline uint32
polar_get_bg_redo_state(polar_logindex_redo_ctl_t instance)
{
	if (instance)
		return pg_atomic_read_u32(&instance->bg_redo_state);

	return POLAR_BG_REDO_NOT_START;
}

#define POLAR_LOGINDEX_MINI_TRANS_ADD_LSN(logindex_snapshot, mini_trans, trans_lock, state, tag) \
	do { \
		POLAR_LOG_INDEX_ADD_LSN((logindex_snapshot), (state), (tag)); \
		polar_logindex_mini_trans_set_page_added((mini_trans), (trans_lock)); \
	} while (0)

#define POLAR_LOGINDEX_BG_WORKER_NAME "polar logindex bg worker"

/*
 * In the future, if possible, it might be better to change bg_redo_state to a
 * flag-format state, where online promote state is independent from parallel
 * replay state and there are four possibilities for combining the two states. 
 * 
 * But for now, adding a state called POLAR_BG_PARALLEL_REPLAYING standing for
 * standby parallel replay and ro prewarm (in the future) affects the original
 * code least, and safety comes first.
 */
inline static bool
polar_bg_redo_state_is_parallel(polar_logindex_redo_ctl_t instance) 
{
	uint32 state = polar_get_bg_redo_state(instance);

	return state == POLAR_BG_ONLINE_PROMOTE ||
		   state == POLAR_BG_PARALLEL_REPLAYING;
}

#endif
