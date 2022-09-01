/*-------------------------------------------------------------------------
 * polar_async_ddl_lock_replay.h
 *	  Async ddl lock replay routines.
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
 *  src/include/access/polar_async_ddl_lock_replay.h
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_ASYNC_DDL_LOCK_REPLAY_H
#define POLAR_ASYNC_DDL_LOCK_REPLAY_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "storage/lwlock.h"
#include "storage/lockdefs.h"
#include "hash.h"

#define MAX_ASYNC_DDL_LOCK_REPLAY_WORKER_NUM 32
#define POLAR_ASYNC_DDL_LOCK_REPLAY_WORKER_NAME "polar async ddl lock replay worker"
#define POLAR_ASYNC_DDL_LOCK_REPLAY_FIRST_LOCK(tx) (tx->locks)
#define POLAR_ASYNC_DDL_LOCK_REPLAY_CURRENT_LOCK(tx) (tx->cur_lock)
#define POLAR_ASYNC_DDL_LOCK_REPLAY_NEXT_LOCK(lock) (lock->next)

#define POLAR_PENDING_LOCK_GET_STATE_STR(state) (((state)==POLAR_PENDING_LOCK_IDLE)?"IDLE":((state)==POLAR_PENDING_LOCK_GETTING?"GETTING":"GOT"))
#define POLAR_PENDING_TX_RELEASE_STATE_STR(state) (((state)==POLAR_PENDING_TX_UNKNOWN)?"UNKNOWN":"RELEASED")
#define POLAR_ASYNC_DDL_LOCK_REPLAY_LOG_LOCK(prefix, lock) elog(LOG, "async ddl lock replay: "prefix" lock: (%u, %u, %u), %lX, %zu, %s", \
			lock->xid, lock->dbOid, lock->relOid, lock->last_ptr, lock->rtime, POLAR_PENDING_LOCK_GET_STATE_STR(lock->state))
#define POLAR_ASYNC_DDL_LOCK_REPLAY_LOG_TX(prefix, tx) elog(LOG, "async ddl lock replay: "prefix" tx: %u, %s, %lX, %d, %d", \
			tx->xid, POLAR_PENDING_TX_RELEASE_STATE_STR(tx->commit_state), tx->last_ptr, tx->worker?tx->worker->id:-1, tx->worker?tx->worker->pid:-1)
#define POLAR_ASYNC_DDL_LOCK_REPLAY_LOG_TX_WITH_LOCK(prefix, tx) elog(LOG, "async ddl lock replay: "prefix" tx: %u, %s, %lX, %d, %d, lock: (%u, %u, %u), %lX, %zu, %s", \
			tx->xid, POLAR_PENDING_TX_RELEASE_STATE_STR(tx->commit_state), tx->last_ptr, tx->worker?tx->worker->id:-1, tx->worker?tx->worker->pid:-1, \
			tx->cur_lock->xid, tx->cur_lock->dbOid, tx->cur_lock->relOid, tx->cur_lock->last_ptr, tx->cur_lock->rtime, POLAR_PENDING_LOCK_GET_STATE_STR(tx->cur_lock->state))

typedef struct polar_async_ddl_lock_replay_worker_t polar_async_ddl_lock_replay_worker_t;
typedef struct polar_pending_lock polar_pending_lock;
typedef struct polar_pending_tx polar_pending_tx;

typedef enum polar_pending_lock_get_state
{
	POLAR_PENDING_LOCK_IDLE,	/* new added, need to be owned */
	POLAR_PENDING_LOCK_GETTING,	/* owned, and try to get */
	POLAR_PENDING_LOCK_GOT		/* get succeed */
} polar_pending_lock_get_state;

typedef struct polar_pending_lock
{
	XLogRecPtr last_ptr;	/* last_replayed_end_rec_ptr, which is fucking long */
	TimestampTz rtime;		/* rtime of startup replay this lock */

	TransactionId xid;		/* xid of holder of AccessExclusiveLock */
	Oid			dbOid;
	Oid			relOid;

	polar_pending_lock_get_state state;

	polar_pending_tx *tx;
	polar_pending_lock *next;
} polar_pending_lock;

typedef enum polar_pending_tx_commit_state
{
	POLAR_PENDING_TX_UNKNOWN,		/* still in getting */
	POLAR_PENDING_TX_RELEASED		/* commited/aborted */
} polar_pending_tx_commit_state;

typedef struct polar_pending_tx
{
	TransactionId	xid;
	polar_pending_tx_commit_state commit_state;/* is this transaction commited or aborted? */

	polar_pending_lock *head;		/* points to lock list head */
	polar_pending_lock *tail;		/* points to lock list tail */
	polar_pending_lock *cur_lock;	/* current lock trying to get */

	XLogRecPtr		last_ptr;		/* for quick accesss */
	LWLock			lock;			/* lock used for own this transaction */
	polar_pending_tx *next;
	polar_async_ddl_lock_replay_worker_t *worker;
} polar_pending_tx;

typedef struct polar_async_ddl_lock_replay_worker_handle_t
{
	int			slot;
	uint64		generation;
} polar_async_ddl_lock_replay_worker_handle_t;

typedef struct polar_async_ddl_lock_replay_worker_t
{
	int		id;
	int		pid;
	bool	working;

	polar_pending_tx *head;			/* points to transaction list head */
	polar_pending_tx *cur_tx;		/* points to current transaction trying to get */

	LWLock			lock;			/* Lock to transaction list, read on this by
									   current worker no need for this lock */
	polar_async_ddl_lock_replay_worker_handle_t handle;
} polar_async_ddl_lock_replay_worker_t;

typedef struct polar_async_ddl_lock_replay_ctl_t
{
	bool			working;
	HTAB			*entries;
	HTAB			*locks;

	LWLock			lock_tbl_lock;			/* Lock to pending lock table */
	LWLock			tx_tbl_lock;		/* Lock to pending transaction table */
	polar_async_ddl_lock_replay_worker_t workers[FLEXIBLE_ARRAY_MEMBER];
} polar_async_ddl_lock_replay_ctl_t;

extern polar_async_ddl_lock_replay_ctl_t *polar_async_ddl_lock_replay_ctl;
extern bool polar_enable_async_ddl_lock_replay_unit_test;

/* worker operation interface */
extern Size polar_async_ddl_lock_replay_shmem_size(void);
extern void polar_init_async_ddl_lock_replay(void);
extern bool polar_allow_async_ddl_lock_replay(void);
extern bool polar_launch_async_ddl_lock_replay_workers(void);
extern bool polar_stop_async_ddl_lock_replay_workers(void);
extern void polar_async_ddl_lock_replay_worker_main(Datum main_arg);

/* lock operation interface */
extern void polar_add_lock_to_pending_tbl(xl_standby_lock *lock,
										  XLogRecPtr last_ptr,
										  TimestampTz rtime);
extern XLogRecPtr polar_get_async_ddl_lock_replay_oldest_ptr(void);
extern bool polar_async_ddl_lock_replay_tx_is_replaying(TransactionId xid);
extern bool polar_async_ddl_lock_replay_lock_is_replaying(xl_standby_lock *lock);
extern void polar_async_ddl_lock_replay_release_one_tx(TransactionId xid);
extern void polar_async_ddl_lock_replay_release_all_tx(void);

/* exposed for unit test */
extern void polar_get_lock_by_tx(polar_pending_tx *tx, bool dotWait);
extern void polar_release_all_pending_tx(void);
extern polar_pending_tx *polar_own_pending_tx(void);
extern polar_async_ddl_lock_replay_worker_t **polar_async_ddl_lock_replay_get_myworker(void);

#endif /* !POLAR_ASYNC_DDL_LOCK_REPLAY_H */
