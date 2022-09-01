/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_decoder.h
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
 *   src/include/polar_flashback/polar_flashback_log_decoder.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_LOG_DECODER_H
#define POLAR_FLASHBACK_LOG_DECODER_H
#include "polar_flashback/polar_flashback_log.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"

extern bool polar_get_origin_page(flog_ctl_t instance, BufferTag *tag, Page page,
		polar_flog_rec_ptr start_ptr, polar_flog_rec_ptr end_ptr, XLogRecPtr *replay_start_lsn);
extern bool polar_flashback_buffer(flog_ctl_t instance, Buffer *buf, BufferTag *tag,
		polar_flog_rec_ptr start_ptr, polar_flog_rec_ptr end_ptr, XLogRecPtr start_lsn,
		XLogRecPtr end_lsn, int elevel, bool apply_fpi);
#endif
