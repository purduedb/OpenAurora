/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_insert.h
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
 *    src/include/polar_flashback/polar_flashback_log_insert.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_INSERT_H
#define POLAR_FLASHBACK_LOG_INSERT_H
#include "polar_flashback/polar_flashback_log_index_queue.h"
#include "polar_flashback/polar_flashback_log_mem.h"
#include "polar_flashback/polar_flashback_log_record.h"

typedef struct flog_insert_context
{
	BufferTag   *buf_tag;
	Page        origin_page;
	XLogRecPtr  redo_lsn;
	RmgrId      rmgr;
	uint8       info;
} flog_insert_context;

extern polar_flog_rec_ptr polar_flog_insert_into_buffer(flog_buf_ctl_t buf_ctl,
														flog_index_queue_ctl_t queue_ctl, flog_insert_context insert_context);
extern polar_flog_rec_ptr polar_insert_buf_flog_rec(flog_buf_ctl_t buf_ctl,
													flog_index_queue_ctl_t queue_ctl, BufferTag *tag, XLogRecPtr redo_lsn, XLogRecPtr fbpoint_lsn, uint8 info, Page origin_page, bool from_origin_buf);

#endif
