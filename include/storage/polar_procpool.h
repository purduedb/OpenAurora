/*----------------------------------------------------------------------------------------
 *
 * polar_procpool.h
 *  This list is used to manage free resources.
 *  The resources are allocated as array items,
 *  get the free item from polar_successor_list_pop
 *  and call polar_successor_list_push when the item is released.
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
 * src/backend/storage/polar_procpool.h
 * ---------------------------------------------------------------------------------------
 */
#ifndef POLAR_PROCPOOL_H
#define POLAR_PROCPOOL_H

#include "lib/ilist.h"
#include "port/atomics.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

typedef enum
{
	POLAR_TASK_NODE_IDLE = 0,
	POLAR_TASK_NODE_RUNNING,
	POLAR_TASK_NODE_HOLD,
	POLAR_TASK_NODE_FINISHED,
	POLAR_TASK_NODE_REMOVED
} polar_task_stat_t;

#define POLAR_TASK_NODE_PROC_MASK       (0xFFFF0000)
#define POLAR_TASK_NODE_STATUS_MASK     (0xFFFF)

#define POLAR_TASK_MAX_REPEAT_DELAY_TIMES (3)

#define POLAR_TASK_NODE_PROC(t)         (pg_atomic_read_u32(&(t)->task_status) >> 16)
#define POLAR_TASK_NODE_STATUS(t)       (pg_atomic_read_u32(&(t)->task_status) & POLAR_TASK_NODE_STATUS_MASK)

#define POLAR_TASK_NODE_INIT_STATUS(t)      (pg_atomic_init_u32(&(t)->task_status, 0))


#define POLAR_TASK_NAME_MAX_LEN         (128)

typedef struct polar_task_node_t
{
	SHM_QUEUE           depend_task;        /* Link task which depend on previous task */
	SHM_QUEUE           running_task;       /* Link task to the running queue */
	slock_t             lock;
	Latch               *next_latch;        /* Set this latch when task is finished */
	uint64              add_seq;
	uint64              finished_seq;
	pg_atomic_uint32    task_status;        /* The first 16bit indicates which sub process own this task;
                                               and the last 16bit indicates the status of this task */
} polar_task_node_t;

#define POLAR_RESET_TASK_NODE(node) \
	do { \
		SHMQueueInit(&(node)->depend_task);\
		SHMQueueInit(&(node)->running_task); \
		(node)->next_latch = NULL; \
		(node)->add_seq = 0; \
		(node)->finished_seq = 0; \
		POLAR_TASK_NODE_INIT_STATUS(node); \
	} while (0)

#define POLAR_UPDATE_TASK_NODE_PROC(t, p)    \
	do { \
		uint32 _status_ = pg_atomic_read_u32(&(t)->task_status); \
		_status_ = ((p) << 16) | (_status_ & POLAR_TASK_NODE_STATUS_MASK); \
		pg_atomic_write_u32(&(t)->task_status, _status_); \
	} while (0)

#define POLAR_UPDATE_TASK_NODE_STATUS(t, n) \
	do { \
		uint32 _status_ = pg_atomic_read_u32(&(t)->task_status); \
		_status_ = (_status_ & POLAR_TASK_NODE_PROC_MASK) | ((n) & POLAR_TASK_NODE_STATUS_MASK); \
		pg_atomic_write_u32(&(t)->task_status, _status_); \
	} while (0)

typedef struct polar_task_sched_t polar_task_sched_t;
typedef void (*polar_task_handle_startup)(void *run_arg);
typedef bool (*polar_task_handler)(polar_task_sched_t *, polar_task_node_t *);
typedef bool (*polar_task_handle_cleanup)(void *run_arg);
typedef void *(*polar_task_get_tag)(polar_task_node_t *);
typedef void (*polar_dispatcher_handle_finished)(polar_task_node_t *, void *);

typedef struct polar_task_sched_t
{
	char                        name[POLAR_TASK_NAME_MAX_LEN];
	PGPROC                      *dispatcher;            /* Point to dispatcher's PGPROC */
	uint32                      total_proc;
	Size                        task_node_size;
	Size                        task_queue_depth;

	polar_task_handle_startup   task_startup;
	polar_task_handler         task_handle;
	polar_task_handle_cleanup   task_cleanup;
	polar_task_get_tag          task_tag;
	void                        *run_arg;

	slock_t             lock;
	SHM_QUEUE           *running_queue_head; /* The address of the head in the running task queue */
	uint64              added_seq;
	pg_atomic_uint64            finished_seq;
	pg_atomic_uint32            enable_shutdown;
	polar_task_node_t           task_nodes[FLEXIBLE_ARRAY_MEMBER];
} polar_task_sched_t;

typedef struct polar_sub_proc_t
{
	BackgroundWorkerHandle  *handle;
	PGPROC                  *proc;
	polar_task_node_t       *task_head; /* The head of valid task in this ring */
	polar_task_node_t       *task_tail; /* The tail of valid task in this ring */
	bool                    ring_full;
	polar_task_node_t       *ring_task_nodes_begin; /* The start address of the ring */
	polar_task_node_t       *ring_task_nodes_end;   /* The end address of the ring */
	uint64                  running_tasks_num;      /* The number of tasks dispatched to this process */
} polar_sub_proc_t;

typedef struct polar_task_sched_ctl_t
{
	polar_task_sched_t              *sched;
	Size                            task_tag_size;
	HTAB                            *task_hash;         /* Record tasks dependency */
	uint32                          dst_proc;           /* Try to dispatch to this process for the next task */
	SHM_QUEUE                       *running_task_head; /* Point to the head of running task queue */
	SHM_QUEUE                       *running_task_tail; /* Point to the tail of running task queue */
	uint64                          added_task_num;     /* The number of succeed dispatched tasks */
	uint64                          fail_add_task_num;  /* The fail times when try to add task but the running queue is full */
	uint64                          finished_task_num;  /* The number of finished tasks */
	uint64                          hold_task_num;      /* Total number of tasks which status is hold when dispatch */
	uint64                          reset_task_num;     /* Total number reset task's status to be idle when it's finished */
	polar_dispatcher_handle_finished handle_finished;
	void                            *finished_arg;
	polar_sub_proc_t                sub_proc[FLEXIBLE_ARRAY_MEMBER];
} polar_task_sched_ctl_t;

extern Size polar_calc_task_sched_shmem_size(Size parallel_num, Size task_node_size, Size task_queue_depth);

extern polar_task_sched_t *polar_create_proc_task_sched(const char *sched_name, Size parallel_num, Size task_node_size, Size task_queue_depth, void *run_arg);

extern void polar_sched_reg_handler(polar_task_sched_t *sched, polar_task_handle_startup task_startup,  polar_task_handler task_handle, polar_task_handle_cleanup task_cleanup, polar_task_get_tag task_get_tag);

extern polar_task_sched_ctl_t *polar_create_task_sched_ctl(polar_task_sched_t *sched, Size task_tag_size, HashValueFunc hash, HashCompareFunc match);

extern void polar_sched_ctl_reg_handler(polar_task_sched_ctl_t *ctl, polar_dispatcher_handle_finished handle_finished, void *finished_arg);

extern polar_task_node_t *polar_sched_add_task(polar_task_sched_ctl_t *ctl, polar_task_node_t *task);
extern void polar_start_proc_pool(polar_task_sched_ctl_t *ctl);
extern void polar_release_task_sched_ctl(polar_task_sched_ctl_t *ctl);
extern int  polar_sched_remove_finished_task(polar_task_sched_ctl_t *ctl);
extern void polar_sub_task_main(Datum main_arg);
extern void polar_bg_quickdie(SIGNAL_ARGS);
extern void polar_bg_sigusr1_handler(SIGNAL_ARGS);
extern bool polar_sched_proc_is_any_started(polar_task_sched_ctl_t *ctl);

static inline void
polar_sched_enable_shutdown(polar_task_sched_t *sched, bool enable)
{
	pg_atomic_write_u32(&sched->enable_shutdown, enable ? 1 : 0);
}

static inline bool
polar_sched_shutdown_enabled(polar_task_sched_t *sched)
{
	return pg_atomic_read_u32(&sched->enable_shutdown) != 0;
}

static inline bool
polar_sched_empty_running_task(polar_task_sched_ctl_t *ctl)
{
	return ctl->running_task_head == NULL;
}

#define POLAR_SCHED_TASK_POINT(sched, task_begin, task_index) \
	((polar_task_node_t *)(((char *)task_begin) + (sched)->task_node_size * (task_index)))

#define POLAR_SCHED_TASK_ADVANCE(sched, proc, task) \
	do { \
		if (unlikely((task) == (proc)->ring_task_nodes_end)) \
			(task) = (proc)->ring_task_nodes_begin; \
		else \
			(task) = (polar_task_node_t *)(((char *)(task)) + (sched)->task_node_size); \
	} while (0)

#define POLAR_SCHED_RUNNING_QUEUE_HEAD(sched) ((polar_task_node_t *)(((char *)sched->running_queue_head) - \
																	 offsetof(polar_task_node_t, running_task)))
#endif
