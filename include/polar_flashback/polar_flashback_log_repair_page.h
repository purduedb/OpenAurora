/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_repair_page.h
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
 *    src/include/polar_flashback/polar_flashback_log_repair_page.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_REPAIR_PAGE_H
#define POLAR_FLASHBACK_LOG_REPAIR_PAGE_H
#include "access/polar_logindex.h"
#include "polar_flashback/polar_flashback_log.h"
#include "storage/buf_internals.h"

extern bool polar_can_flog_repair(flog_ctl_t instance, BufferDesc *buf_hdr, bool has_redo_action);
extern void polar_repair_partial_write(flog_ctl_t instance, BufferDesc *bufHdr);
#endif
