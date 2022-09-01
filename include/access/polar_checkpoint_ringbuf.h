/*-------------------------------------------------------------------------
 *
 * polar_checkpoint_ringbuf.h
 *	  PolarDB Check code.
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
 *      src/include/access/polar_checkpoint_ringbuf.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_CHECKPOINT_RINGBUF
#define POLAR_CHECKPOINT_RINGBUF

#include "catalog/pg_control.h"
#include "storage/lwlock.h"

#define POLAR_CHECKPOINT_RINGBUF_CAPACITY 5

typedef struct polar_checkpoint_ringbuf_t {
	XLogRecPtr recptrs[POLAR_CHECKPOINT_RINGBUF_CAPACITY];      /* checkpoint start pointer array */
	XLogRecPtr endptrs[POLAR_CHECKPOINT_RINGBUF_CAPACITY];      /* checkpoint end pointer array */
	CheckPoint checkpoints[POLAR_CHECKPOINT_RINGBUF_CAPACITY];  /* checkpoint array */
	LWLock     lock;                                            /* lock used in ringbuf */
	int        head;                                            /* head index in ringbuf */
	int        size;                                            /* size of ringbuf */
} polar_checkpoint_ringbuf_t;

extern void polar_checkpoint_ringbuf_init(polar_checkpoint_ringbuf_t *ringbuf);
extern void polar_checkpoint_ringbuf_shrink(polar_checkpoint_ringbuf_t *ringbuf, XLogRecPtr replayed);
extern void polar_checkpoint_ringbuf_insert(polar_checkpoint_ringbuf_t *ringbuf, XLogRecPtr checkpointRecPtr, XLogRecPtr checkpointEndPtr, const CheckPoint *checkpoint);
extern void polar_checkpoint_ringbuf_front(polar_checkpoint_ringbuf_t *ringbuf, XLogRecPtr *checkpointRecPtr, XLogRecPtr *checkpointEndPtr, CheckPoint *checkpoint);


#endif