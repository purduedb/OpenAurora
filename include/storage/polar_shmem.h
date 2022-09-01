/*-------------------------------------------------------------------------
 *
 * polar_shmem.h
 *	  polar persisted shared memory management structures
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
 *      src/include/storage/polar_shmem.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_SHMEM_H
#define POLAR_SHMEM_H

#include "storage/pg_shmem.h"
#include "storage/shmem.h"

typedef enum PolarPersistedShmem
{
	POLAR_BUFFER_POOL_CTL = 0,
	POLAR_BUFFER_DESCS,
	POLAR_BUFFER_BLOCKS,
	POLAR_PERSISTED_SHMEM_NUM
} PolarPersistedShmem;

typedef struct	PolarShmemInfo
{
	const char	*name;			/* string name */
	void		*location;		/* location in shared mem */
	Size		size;			/* # bytes allocated for the structure */
} PolarShmemInfo;

extern bool polar_shmem_reused;

extern void polar_init_shmem_access(void *seghdr);
extern void *polar_shmem_init_struct(const char *name, Size size, bool *foundPtr);
extern bool	polar_persisted_buffer_pool_enabled(const char *name);
extern PolarShmemInfo *polar_get_persisted_shmem_info(void);
extern PGShmemHeader *polar_get_persisted_shmem_seg_hdr(void);
#endif							/* POLAR_SHMEM_H */
