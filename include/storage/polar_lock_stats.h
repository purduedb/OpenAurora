/*-------------------------------------------------------------------------
 *
 * polar_lock_stats.h
 *	  including lwlock stat and lock stat
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
 *    src/include/storage/polar_lock_stats.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_LOCK_STATS_H
#define POLAR_LOCK_STATS_H

#include "portability/instr_time.h"
#include "storage/lwlock.h"
#include "storage/lock.h"

/* lwlock tranches upper limit for stats */
#define POLAR_MAX_LWLOCK_TRANCHE 128

/* lwlock stat */
typedef struct polar_lwlock_stat
{
	uint64			sh_acquire_count;	  /* shared mode acquire count */
	uint64			ex_acquire_count;     /* exclude mode acquire count */
	uint64			block_count;		  /* lwlock block count */
	uint64			dequeue_self_count;   /* */
	uint64			wait_time; 			  /* total block time in micro second */
} polar_lwlock_stat;

/* all lwlocks stats */
typedef struct polar_all_lwlocks_stat
{
	polar_lwlock_stat lwlocks[POLAR_MAX_LWLOCK_TRANCHE];
} polar_all_lwlocks_stat;

/* regular lock stat */
typedef struct polar_regular_lock_stat
{
	uint64 lock_count;					/* lock acquire count */
	uint64 block_count;				    /* lock block count */
	uint64 fastpath_count;				/* fastpath lock count */
	uint64 wait_time;  					/* total block time in micro second */
} polar_regular_lock_stat;

/* all regular lock stats */
typedef struct polar_all_locks_stat
{
	polar_regular_lock_stat detail[LOCKTAG_LAST_TYPE + 1][MAX_LOCKMODES];
} polar_all_locks_stat;

/* block stat */
#define POLAR_LWLOCK_STAT_BLOCK(lock, is_blocking)								\
	do																			\
	{																			\
		if (!is_blocking)														\
		{																		\
			is_blocking = true;													\
			polar_lwlock_stat_block(lock);										\
		}																		\
	} while (0);


/* record start time */
#define POLAR_LOCK_STATS_TIME_START(start)   									\
	do										 									\
	{										 									\
		if (polar_enable_track_lock_timing && INSTR_TIME_IS_ZERO(start)) 		\
			INSTR_TIME_SET_CURRENT(start);   									\
	} while (0);

extern polar_lwlock_stat polar_lwlock_stat_local_summary;
extern polar_all_lwlocks_stat *polar_lwlocks_stat_array;
extern polar_all_locks_stat *polar_locks_stat_array;
extern polar_regular_lock_stat polar_lock_stat_local_summary[];

extern void polar_init_lwlock_local_stats(void);
extern void polar_lwlock_stat_acquire(LWLock *lock, LWLockMode mode);
extern void polar_lwlock_stat_block(LWLock *lock);
extern void polar_lwlock_stat_record_time(LWLock *lock, instr_time *start);

extern void polar_init_lock_local_stats(void);
extern void polar_lock_stat_lock(uint8 type, LOCKMODE mode);
extern void polar_lock_stat_block(uint8 type, LOCKMODE mode);
extern void polar_lock_stat_fastpath(uint8 type, LOCKMODE mode);
extern void polar_lock_stat_record_time(uint8 type, LOCKMODE mode, instr_time *start);

#endif  /* POLAR_LOCK_STATS_H */