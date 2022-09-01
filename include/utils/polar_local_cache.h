/*-------------------------------------------------------------------------
 *
 * polar_local_cache.h
 *      Manage local file cache, which is the file cache of shared storage.
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
 *
 * IDENTIFICATION
 *      src/include/utils/polar_local_cache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_LOCAL_CACHE_H
#define POLAR_LOCAL_CACHE_H

#include "storage/lwlock.h"
#include "utils/hsearch.h"
#include "utils/polar_successor_list.h"

#define POLAR_CACHE_TRASH_DIR "polar_cache_trash"

#define POLAR_LOCAL_CACHE_MAX_NAME_LEN      (64)
#define POLAR_LOCAL_CACHE_HASH_LEN          (96)

#define POLAR_UNKNOWN_CACHE_OFFSET          (-1)
#define POLAR_UNKNOWN_CACHE_SIZE            (-1)

typedef void (*polar_segment_file_path)(char *full_path, uint64 segno);

/* Macro defined for polar_io_segment status */
#define POLAR_CACHE_SEG_OCCUPIED                (1 << 31)
#define POLAR_CACHE_SEG_DIRTY                   (1 << 30)
#define POLAR_CACHE_SEG_SET_MIN_OFFSET          (1 << 29)
#define POLAR_CACHE_SEG_READABLE                (1 << 28)
#define POLAR_CACHE_SEG_WRITE_IN_PROGRESS       (1 << 27)
#define POLAR_CACHE_SEG_STATUS_MASK             (0xFFF00000)
#define POLAR_CACHE_SEG_READ_MASK               (0x000FFFFF)

typedef struct polar_io_segment
{
	uint64      segno;
	uint32      status;
	uint32      min_write_offset;
} polar_io_segment;

/* Macro defined for polar_local_cache_data io_permission */
#define POLAR_CACHE_LOCAL_FILE_READ     (1 << 0)
#define POLAR_CACHE_LOCAL_FILE_WRITE    (1 << 1)
#define POLAR_CACHE_SHARED_FILE_READ    (1 << 2)
#define POLAR_CACHE_SHARED_FILE_WRITE   (1 << 3)
#define POLAR_CACHE_IO_PERMISSION_MASK  (0x0F)

typedef struct polar_local_cache_data
{
	char                        dir_name[POLAR_LOCAL_CACHE_MAX_NAME_LEN];
	char                        trache_name[POLAR_LOCAL_CACHE_MAX_NAME_LEN];
	Size                        segment_size;   /* The max size of one segment file */
	polar_io_segment            *io_seg_items; /* Save segment number which is in io progress to this array */
	uint32                      max_segments;   /* The size of io_seg_items array */
	LWLockPadded                *ctl_lock;
	polar_successor_list        *free_items_list;  /* Unused items of io_seg_items array */
	char                        hash_name[POLAR_LOCAL_CACHE_HASH_LEN];
	HTAB                        *hash_io_seg;  /* Hash map of segment to index of io_seg_items */
	polar_segment_file_path     seg_path;         /* Convert segment to segment file path */
	uint32                      io_permission;      /* See macro defined for io_permission for more detail */
	uint64                      local_flushed_times;     /* Increase everytime when call polar_local_cache_flush_all and POLAR_CACHE_SHARED_FILE_WRITE is not permitted*/
} polar_local_cache_data;

typedef polar_local_cache_data *polar_local_cache;

typedef struct polar_io_seg_entry
{
	uint64  segno;
	int     index;
} polar_io_seg_entry;

typedef enum
{
	POLAR_CACHE_OPEN_FAILED,
	POLAR_CACHE_SEEK_FAILED,
	POLAR_CACHE_READ_FAILED,
	POLAR_CACHE_WRITE_FAILED,
	POLAR_CACHE_SYNC_FAILED,
	POLAR_CACHE_UNLINK_FAILED,
	POLAR_CACHE_STAT_FAILED
} polar_io_cache_errcause;

typedef enum
{
	POLAR_INVALID_CACHE_FILE,
	POLAR_LOCAL_CACHE_FILE,
	POLAR_LOCAL_CACHE_META,
	POLAR_SHARED_STORAGE_FILE
} polar_cache_file_type;

typedef struct polar_cache_io_error
{
	uint64                  segno;
	polar_cache_file_type   file_type;
	off_t                   offset;
	ssize_t                 size;
	ssize_t                 io_return;
	int                     save_errno;
	polar_io_cache_errcause errcause;
} polar_cache_io_error;

#define POLAR_SEGMENT_IO_SUCCESS(io_error)      ((io_error)->save_errno == 0)
#define POLAR_SEGMENT_NOT_EXISTS(io_error)      ((io_error)->save_errno == ENOENT)
#define POLAR_SEGMENT_BEYOND_END(io_error)      ((io_error)->save_errno == ERANGE)

#define POLAR_LOCAL_CACHE_LOCK(cache) (&((cache)->ctl_lock->lock))

#define POLAR_LOCAL_FILE_PATH(path, cache, segno) \
	do { \
		snprintf(path, sizeof(path), "%s/", (cache)->dir_name); \
		(cache)->seg_path((path)+strlen(path), (segno)); \
	} while (0)

#define POLAR_SHARED_FILE_PATH(path, cache, segno) \
	do { \
		snprintf(path, sizeof(path), "%s/%s/", polar_datadir, (cache)->dir_name); \
		(cache)->seg_path((path)+strlen(path), (segno)); \
	} while (0)

#define POLAR_LOCAL_CACHE_META_PATH(path, cache, flushed_times) \
	do { \
		snprintf(path, sizeof(path), "%s/readonly_flushed_seg_%04lX", (cache)->dir_name, (flushed_times)); \
	} while (0)

/* POLAR: Return true mean continue scan, otherwise break. */
typedef bool (*polar_local_cache_scan_callback)(polar_local_cache cache, char *cache_file, uint64 segno);

Size polar_local_cache_shmem_size(uint32 max_segments);
polar_local_cache polar_create_local_cache(const char *cache_name, const char *dir_name,
										   uint32 max_segments, Size segment_size,
										   int tranche_id, uint32 io_permission, polar_segment_file_path seg_path);

bool polar_local_cache_read(polar_local_cache cache, uint64 segno, uint32 offset, char *buf, uint32 size, polar_cache_io_error *io_error);

bool polar_local_cache_write(polar_local_cache cache, uint64 segno, uint32 offset, char *buf, uint32 size, polar_cache_io_error *io_error);

bool polar_local_cache_flush_all(polar_local_cache cache, polar_cache_io_error *io_error);

bool polar_local_cache_remove(polar_local_cache cache, uint64 segno, polar_cache_io_error *io_error);

void polar_local_cache_report_error(polar_local_cache cache, polar_cache_io_error *io_error, int log_level);

void polar_local_cache_dir(polar_local_cache cache, char *path, size_t size);

bool polar_local_cache_set_io_permission(polar_local_cache cache, uint32 io_permission, polar_cache_io_error *io_error);

void polar_local_cache_scan(polar_local_cache cache, polar_local_cache_scan_callback callback, uint64 segno);

/* Used by local cache test module, which create hash table not in postmaster process */
void polar_local_cache_attach_hashmap(polar_local_cache cache);

bool polar_local_cache_move_trash(const char *dir_name);

bool polar_local_cache_empty_trash(void);
#endif
