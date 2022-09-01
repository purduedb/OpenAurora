/*-------------------------------------------------------------------------
 *
 * polar_io_fencing.h
 *	  Polardb shared storage I/O fencing code.
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
 * src/include/storage/polar_io_fencing.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_IO_FENCING_H
#define POLAR_IO_FENCING_H

#include <dirent.h>
#include <sys/stat.h>
#include "utils/resowner.h"

#define POLAR_SHARED_STORAGE_UNAVAILABLE "DEATH"
#define POLAR_RWID "RWID"
#define POLAR_RWID_LENGTH 28
/* The default interval time is 500 ms. */
#define POLAR_IO_FENCING_INTERVAL 500
#define USECS_PER_MILLISEC 1000L

/* POLAR: Used for polar IO fencing in order to determine whether we need to wait 2 * POLAR_IO_FENCING_INTERVAL ms or not. */
typedef struct polar_io_fencing_t
{
	/* see more about enum polar_io_fencing_state */
	pg_atomic_uint32 state;
	TimestampTz current_rwid_time;
} polar_io_fencing_t;

typedef enum polar_io_fencing_state
{
	POLAR_IO_FENCING_START = 0,
	POLAR_IO_FENCING_WAIT,
	POLAR_IO_FENCING_NORMAL
} polar_io_fencing_state;

/* make sure that 512 is divisible by the length of struct. */
typedef struct RWID
{
	int hostid;
	char random_id[POLAR_RWID_LENGTH];
} RWID;
extern RWID polar_rwid;
extern bool polar_enable_io_fencing;

extern bool polar_shared_storage_is_available(void);
extern void polar_hold_shared_storage(bool force_hold);
extern void polar_mark_shared_storage_unavailable(void);
extern void polar_check_double_write(void);
extern polar_io_fencing_t *polar_io_fencing_get_instance(void);
extern void polar_usleep(TimestampTz start, TimestampTz microsec);
#define POLAR_IO_FENCING_SET_STATE(instance, value)						\
	do																	\
	{																	\
		if (instance)													\
		{																\
			if (value == POLAR_IO_FENCING_WAIT)							\
				(instance)->current_rwid_time = GetCurrentTimestamp();	\
			pg_atomic_write_u32(&(instance)->state, value);				\
		}																\
	}while(0)

#define POLAR_IO_FENCING_RESET_STATE(instance)								\
	do																		\
	{																		\
		if (instance)														\
		{																	\
			(instance)->current_rwid_time = 0;								\
			pg_atomic_init_u32(&(instance)->state, POLAR_IO_FENCING_START);	\
		}																	\
	}while(0)

#define POLAR_IO_FENCING_GET_STATE(instance) \
	(instance ? pg_atomic_read_u32(&(instance)->state) : POLAR_IO_FENCING_START)

/*
 * POLAR: In case of other RW could not find us, we neeed to wait for at least
 * 2 * POLAR_IO_FENCING_INTERVAL ms. If it is OK during this interval time, we believe
 * that there's no other RW running at the same time.
 */
#define POLAR_IO_FENCING_WAIT_FOR(instance)										\
	do																			\
	{																			\
		if (instance)															\
			polar_usleep((instance)->current_rwid_time,							\
						 2 * POLAR_IO_FENCING_INTERVAL * USECS_PER_MILLISEC);	\
	}while(0)

#endif
