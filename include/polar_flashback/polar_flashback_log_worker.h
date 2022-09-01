/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_worker.h
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
 *     src/include/polar_flashback/polar_flashback_log_worker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_WORKER_H
#define POLAR_FLASHBACK_LOG_WORKER_H
#include "postgres.h"

#define FLOG_BG_WORKER_NAME "polar flashback log bg worker"
#define FLOG_BG_WORKER_TYPE "polar flashback log"

#define FLOG_LIST_BG_WORKER_NAME "polar flashback log list bg worker"
#define FLOG_LIST_BG_WORKER_TYPE "polar flashback log list"

#define FLOG_START_BGWRITER_AND_INSERTER \
	do {                                                \
		if (FlogBgInserterPID == 0)                     \
			FlogBgInserterPID = StartFlogBgInserter();  \
		if (FlogBgWriterPID == 0)                       \
			FlogBgWriterPID = StartFlogBgWriter();      \
	} while (0)

extern void polar_flog_bgwriter_main(void);
extern void polar_flog_bginserter_main(void);

#endif
