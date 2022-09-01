/*-------------------------------------------------------------------------
 *
 * px_snapshot.h
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
 *    src/include/px/px_snapshot.h
 *
 *-------------------------------------------------------------------------
 */

#include "utils/snapmgr.h"

extern void pxsn_log_snapshot(Snapshot snapshot, const char *func);
extern void pxsn_set_snapshot(Snapshot snapshot);
extern void pxsn_set_oldest_snapshot(Snapshot snapshot);
extern void pxsn_set_serialized_snapshot(const char *sdsnapshot, int size);
extern char *pxsn_get_serialized_snapshot(void);
extern int	pxsn_get_serialized_snapshot_size(void);
extern char *pxsn_get_serialized_snapshot_data(void);
