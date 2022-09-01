/*-------------------------------------------------------------------------
 *
 * px_timeout.h
 *	  Definitions for pxtimeout.c utilities.
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
 *	  src/include/px/px_timeout.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PXTIMEOUT_H
#define PXTIMEOUT_H

#include "utils/faultinjector.h"

extern volatile sig_atomic_t px_wait_lock_alert;
extern int px_wait_lock_timer_id;

#define IS_VALID_TIMER_ID(timer_id) \
	(timer_id >= USER_TIMEOUT && timer_id < MAX_TIMEOUTS)

extern void px_wait_lock_timeout_handler(void);
extern void px_cancel_wait_lock(void);

/*
 * Fault Injection: only used for multi-session testing.
 * After a PX query acquires its necessary locks, hang it for specific seconds.
 */
#define INJECT_PX_HANG_FOR_SECOND(fault_name, db_name, table_name, sec)		\
	if (FaultInjector_TriggerFaultIfSet(fault_name, db_name, table_name)	\
			== FaultInjectorTypeEnable) {									\
		TimestampTz t_timeout = GetCurrentTimestamp() + USECS_PER_SEC * sec;\
		while (GetCurrentTimestamp() < t_timeout)							\
			CHECK_FOR_INTERRUPTS();											\
	}

#endif
