/*-------------------------------------------------------------------------
 *
 * polar_local_cache.c
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
 *      src/backend/utils/cache/polar_local_cache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/polar_backtrace.h"
#include "utils/polar_local_cache.h"


#define POLAR_CACHE_IO_IN_PROGRESS(seg)         ((seg)->status & (POLAR_CACHE_SEG_READ_MASK | POLAR_CACHE_SEG_WRITE_IN_PROGRESS))
#define POLAR_CACHE_IS_OCCUPIED_SEGMENT(seg)    ((seg)->status & POLAR_CACHE_SEG_OCCUPIED)
#define POLAR_CACHE_IS_CLEAN_SEGMENT(seg)       (!((seg)->status & POLAR_CACHE_SEG_DIRTY))
#define POLAR_CACHE_WRITE_IN_PROGRESS(seg)      ((seg)->status & POLAR_CACHE_SEG_WRITE_IN_PROGRESS)
#define POLAR_CACHE_SHARED_READING(seg)         ((!((seg)->status & POLAR_CACHE_SEG_READABLE)) && \
												 (((seg)->status & POLAR_CACHE_SEG_READ_MASK) > 0))
#define POLAR_CACHE_REACH_READ_LIMIT(seg)       (((seg)->status & POLAR_CACHE_SEG_READ_MASK) == POLAR_CACHE_SEG_READ_MASK)

#define POLAR_LOG_IO_ERROR(__segno, __file_type, __offset, __size, __io_return, __errcause) \
	do { \
		if (io_error) \
		{ \
			io_error->segno = (__segno); \
			io_error->file_type = (__file_type); \
			io_error->offset = (__offset); \
			io_error->size = (__size); \
			io_error->io_return = (__io_return); \
			io_error->save_errno = errno; \
			io_error->errcause = (__errcause); \
		} \
	} while (0)

typedef enum polar_copy_direction
{
	LOCAL_TO_SHARED,
	SHARED_TO_LOCAL
} polar_copy_direction;

static bool polar_local_cache_flush(polar_local_cache cache, polar_io_segment *io_seg, polar_cache_io_error *io_error);

static void
polar_default_convert_seg2str(char *buf, uint64 segno)
{
	sprintf(buf, "%04lX", segno);
}

static bool
polar_record_local_flushed_seg(polar_local_cache cache, uint64 segno, polar_cache_io_error *io_error)
{
	char path[MAXPGPATH];
	int fd;
	ssize_t write_size;

	POLAR_LOCAL_CACHE_META_PATH(path, cache, cache->local_flushed_times);

	pgstat_report_wait_start(WAIT_EVENT_CACHE_LOCAL_OPEN);
	fd = OpenTransientFile(path, O_RDWR | O_CREAT | O_APPEND | PG_BINARY);
	pgstat_report_wait_end();

	if (fd < 0)
	{
		POLAR_LOG_IO_ERROR(segno, POLAR_LOCAL_CACHE_META, POLAR_UNKNOWN_CACHE_OFFSET,
						   POLAR_UNKNOWN_CACHE_SIZE, fd, POLAR_CACHE_OPEN_FAILED);
		LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));

		return false;
	}

	pgstat_report_wait_start(WAIT_EVENT_CACHE_LOCAL_WRITE);
	write_size = pg_pwrite(fd, (void *)&segno, sizeof(segno), 0);
	pgstat_report_wait_end();

	if (write_size != sizeof(segno))
	{
		POLAR_LOG_IO_ERROR(segno, POLAR_LOCAL_CACHE_META, POLAR_UNKNOWN_CACHE_OFFSET,
						   POLAR_UNKNOWN_CACHE_SIZE, write_size, POLAR_CACHE_WRITE_FAILED);
	}

	CloseTransientFile(fd);

	return write_size == sizeof(segno);
}

static Size
polar_local_cache_size(uint32 max_segments)
{
	Size sz = MAXALIGN(sizeof(polar_local_cache_data));

	sz += MAXALIGN(sizeof(LWLockPadded)); /* ctl_lock */
	sz += MAXALIGN(max_segments * sizeof(polar_io_segment)); /* io_seg_items[] */
	sz += MAXALIGN(POLAR_SUCCESSOR_LIST_SIZE(max_segments)); /* free_items_list */

	return sz;
}

Size
polar_local_cache_shmem_size(uint32 max_segments)
{
	Size sz = polar_local_cache_size(max_segments);

	sz += hash_estimate_size(max_segments, sizeof(polar_io_seg_entry));

	return MAXALIGN(sz);
}

static int
polar_segment_compare(const void *seg1, const void *seg2,  size_t size)
{
	uint64 *left = (uint64 *) seg1;
	uint64 *right = (uint64 *) seg2;

	return *left == *right ? 0 : 1;
}

void
polar_local_cache_attach_hashmap(polar_local_cache cache)
{
	HASHCTL info;

	memset(&info, 0, sizeof(info));

	info.keysize = sizeof(uint64);
	info.entrysize = sizeof(polar_io_seg_entry);
	info.match = polar_segment_compare;

	cache->hash_io_seg = ShmemInitHash(cache->hash_name, cache->max_segments,
									   cache->max_segments, &info, HASH_ELEM | HASH_BLOBS | HASH_COMPARE);

}

static void
polar_local_cache_hash_init(polar_local_cache cache, const char *name)
{
#define LOCAL_CACHE_HASH_SUFFIX " local cache hash"

	snprintf(cache->hash_name, POLAR_LOCAL_CACHE_HASH_LEN, "%s%s", name, LOCAL_CACHE_HASH_SUFFIX);
	cache->hash_name[POLAR_LOCAL_CACHE_HASH_LEN - 1] = '\0';
	polar_local_cache_attach_hashmap(cache);
}

static void
polar_validate_data_dir(const char *dir_name)
{
//	struct stat st;
//	char dir[MAXPGPATH];
//	size_t len = strlen(dir_name);
//	char *p;
//
//	StrNCpy(dir, dir_name, MAXPGPATH);
//
//	if (dir[len - 1] != '/')
//	{
//		Assert(len < MAXPGPATH);
//		dir[len] = '/';
//		dir[len + 1] = '\0';
//	}
//
//	for (p = strchr(dir + 1, '/'); p != NULL; p = strchr(p + 1, '/'))
//	{
//		*p = '\0';
//
//		if (stat(dir, &st) == 0)
//		{
//			if (!S_ISDIR(st.st_mode))
//				ereport(FATAL,
//						(errmsg("required directory \"%s\" does not exist",
//								dir)));
//		}
//		else
//		{
//			if (polar_make_pg_directory(dir) < 0)
//				ereport(FATAL,
//						(errmsg("could not create missing directory \"%s\": %m",
//								dir)));
//		}
//
//		*p = '/';
//	}
}

polar_local_cache
polar_create_local_cache(const char *cache_name, const char *dir_name,
						 uint32 max_segments, Size segment_size,
						 int tranche_id, uint32 io_permission, polar_segment_file_path seg_path)
{
#define LOCAL_CACHE_SUFFIX "_local_cache"
	polar_local_cache cache;
	bool found;
	char *name;
	size_t size = strlen(cache_name) + strlen(LOCAL_CACHE_SUFFIX) + 1;

	name = palloc(size);
	snprintf(name, size, "%s%s", cache_name, LOCAL_CACHE_SUFFIX);

	cache = (polar_local_cache) ShmemInitStruct(name, polar_local_cache_size(max_segments), &found);

	if (!IsUnderPostmaster)
	{
		char *ptr = (char *)cache;
		Size offset = MAXALIGN(sizeof(polar_local_cache_data));

		Assert(!found);
		memset(cache, 0, sizeof(polar_local_cache_data));

		StrNCpy(cache->dir_name, dir_name, sizeof(cache->dir_name));
		StrNCpy(cache->trache_name, name, sizeof(cache->trache_name));

		cache->ctl_lock = (LWLockPadded *)(ptr + offset);
		offset += MAXALIGN(sizeof(LWLockPadded));

		cache->io_seg_items = (polar_io_segment *)(ptr + offset);
		MemSet(cache->io_seg_items, 0, max_segments * sizeof(polar_io_segment));
		offset += MAXALIGN(max_segments * sizeof(polar_io_segment));

		cache->free_items_list = polar_successor_list_init((void *)(ptr + offset), max_segments);
		offset += MAXALIGN(POLAR_SUCCESSOR_LIST_SIZE(max_segments));

		Assert(offset == polar_local_cache_size(max_segments));

		if (seg_path == NULL)
			cache->seg_path = polar_default_convert_seg2str;
		else
			cache->seg_path = seg_path;

		cache->max_segments = max_segments;
		cache->segment_size = segment_size;

		cache->io_permission = io_permission & POLAR_CACHE_IO_PERMISSION_MASK;
		cache->local_flushed_times = 1;

		LWLockRegisterTranche(tranche_id, cache->trache_name);
		LWLockInitialize(POLAR_LOCAL_CACHE_LOCK(cache), tranche_id);

		polar_local_cache_hash_init(cache, cache_name);
	}
	else
		Assert(found);

	pfree(name);

	return cache;
}

static int
polar_cache_file_open(polar_local_cache cache, polar_cache_file_type file_type, uint64 segno,
					  uint32 offset, int file_flags, polar_cache_io_error *io_error)
{
	char path[MAXPGPATH];
	int fd;
	int event;
	off_t seek_ret;

	switch (file_type)
	{
		case POLAR_LOCAL_CACHE_FILE:
			POLAR_LOCAL_FILE_PATH(path, cache, segno);
			break;

		case POLAR_SHARED_STORAGE_FILE:
			POLAR_SHARED_FILE_PATH(path, cache, segno);
			break;

		default:
			POLAR_LOG_BACKTRACE();
			elog(FATAL, "Incorrect filetype=%d for %s", file_type, __func__);
	}

	event = (file_type == POLAR_LOCAL_CACHE_FILE) ? WAIT_EVENT_CACHE_LOCAL_OPEN : WAIT_EVENT_CACHE_SHARED_OPEN;

	pgstat_report_wait_start(event);
	fd = OpenTransientFile(path, file_flags);
	pgstat_report_wait_end();

	if (fd < 0)
	{
		POLAR_LOG_IO_ERROR(segno, file_type, POLAR_UNKNOWN_CACHE_OFFSET, POLAR_UNKNOWN_CACHE_SIZE,
						   fd, POLAR_CACHE_OPEN_FAILED);

		return -1;
	}

	event = (file_type == POLAR_LOCAL_CACHE_FILE) ? WAIT_EVENT_CACHE_LOCAL_LSEEK : WAIT_EVENT_CACHE_SHARED_LSEEK;
	pgstat_report_wait_start(WAIT_EVENT_CACHE_LOCAL_LSEEK);
	seek_ret = lseek(fd, (int) offset, SEEK_SET);
	pgstat_report_wait_end();

	if (seek_ret < 0)
	{
		POLAR_LOG_IO_ERROR(segno, file_type, offset, POLAR_UNKNOWN_CACHE_SIZE,
						   seek_ret, POLAR_CACHE_SEEK_FAILED);
		return -1;
	}

	return fd;
}

static ssize_t
polar_cache_file_read(polar_local_cache cache, polar_cache_file_type file_type, uint64 segno,
					  uint32 offset, char *buf, uint32 size, polar_cache_io_error *io_error)
{
	int fd;
	ssize_t read_size;
	int event;

	MemSet(io_error, 0, sizeof(polar_cache_io_error));
	fd = polar_cache_file_open(cache, file_type, segno, offset, O_RDWR | PG_BINARY, io_error);

	if (fd < 0)
		return -1;

	event = (file_type == POLAR_LOCAL_CACHE_FILE) ? WAIT_EVENT_CACHE_LOCAL_READ : WAIT_EVENT_CACHE_SHARED_READ;
	pgstat_report_wait_start(event);
	read_size = pg_pread(fd, buf, size, 0);
	pgstat_report_wait_end();

	if (read_size != size)
		POLAR_LOG_IO_ERROR(segno, file_type, offset, size, read_size, POLAR_CACHE_READ_FAILED);

	CloseTransientFile(fd);

	return read_size;
}

static ssize_t
polar_cache_file_write(polar_local_cache cache, polar_cache_file_type file_type, uint64 segno,
					   uint32 offset, char *buf, uint32 size, polar_cache_io_error *io_error)
{
	int fd;
	ssize_t write_size = 0;
	int event;

	MemSet(io_error, 0, sizeof(polar_cache_io_error));

	fd = polar_cache_file_open(cache, file_type, segno, offset, O_RDWR | O_CREAT | PG_BINARY, io_error);

	if (fd < 0)
		return -1;

	event = (file_type == POLAR_LOCAL_CACHE_FILE) ? WAIT_EVENT_CACHE_LOCAL_WRITE : WAIT_EVENT_CACHE_SHARED_WRITE;
	pgstat_report_wait_start(event);
	write_size = pg_pwrite(fd, buf, size, 0);
	pgstat_report_wait_end();

	if (write_size != size)
		POLAR_LOG_IO_ERROR(segno, file_type, offset, size, write_size, POLAR_CACHE_WRITE_FAILED);
	else if (file_type == POLAR_SHARED_STORAGE_FILE)
	{
		int ret;

		/* sync file data when is shared storage file */
		pgstat_report_wait_start(WAIT_EVENT_CACHE_SHARED_SYNC);
		ret = pg_fsync(fd);
		pgstat_report_wait_end();

		if (ret < 0)
		{
			POLAR_LOG_IO_ERROR(segno, file_type, POLAR_UNKNOWN_CACHE_OFFSET, POLAR_UNKNOWN_CACHE_SIZE,
							   ret, POLAR_CACHE_SYNC_FAILED);

			CloseTransientFile(fd);
			return -1;
		}
	}

	CloseTransientFile(fd);

	return write_size;
}

static ssize_t
polar_cache_copy(polar_local_cache cache, polar_copy_direction direct, uint64 segno,
				 size_t from_offset, char *seg_buf, polar_cache_io_error *io_error)
{
	ssize_t     write_size;
	ssize_t     read_size;
	polar_cache_file_type from, to;

	if (direct == LOCAL_TO_SHARED)
	{
		from = POLAR_LOCAL_CACHE_FILE;
		to = POLAR_SHARED_STORAGE_FILE;
	}
	else
	{
		from = POLAR_SHARED_STORAGE_FILE;
		to = POLAR_LOCAL_CACHE_FILE;
	}

	read_size = polar_cache_file_read(cache, from, segno, from_offset, seg_buf, cache->segment_size - from_offset, io_error);

	if (read_size < 0)
		return read_size;

	write_size = polar_cache_file_write(cache, to, segno, from_offset, seg_buf, read_size, io_error);

	if (write_size != read_size)
	{
		elog(LOG, "Copy size for local cache %s is mismatch, direct=%d, segno=%ld, offset=%ld, write_size=%ld, read_size=%ld",
			 cache->dir_name, direct, segno, from_offset, write_size, read_size);

		/*
		 * Remove local file cache if we failed to copy data from shared storage to local file.
		 * Otherwise we will get inconsistent data
		 */
		if (direct == SHARED_TO_LOCAL)
		{
			char path[MAXPGPATH];

			POLAR_LOCAL_FILE_PATH(path, cache, segno);
			unlink(path);
		}
	}

	return read_size;
}

static bool
polar_cache_copy_local_not_exists(polar_local_cache cache, uint64 segno, polar_cache_io_error *io_error)
{
	char path[MAXPGPATH];
	struct stat st;
	char *seg_buf;
	ssize_t copy_size;

	POLAR_LOCAL_FILE_PATH(path, cache, segno);

	if (stat(path, &st) == 0)
	{
		if (!S_ISREG(st.st_mode))
			elog(FATAL, "%s exists, but is not regular file,st_mode=%d", path, st.st_mode);

		return true;
	}

	seg_buf = palloc(cache->segment_size);
	copy_size = polar_cache_copy(cache, SHARED_TO_LOCAL, segno, 0, seg_buf, io_error);
	pfree(seg_buf);

	return (copy_size >= 0 && POLAR_SEGMENT_IO_SUCCESS(io_error)) || POLAR_SEGMENT_NOT_EXISTS(io_error);
}

static bool
polar_cache_read(polar_local_cache cache, uint64 segno, uint32 offset,
				 char *buf,  uint32 size, polar_cache_io_error *io_error)
{
	char        *seg_buf;
	ssize_t     local_read_size;
	ssize_t     shared_read_size;

	local_read_size = polar_cache_file_read(cache, POLAR_LOCAL_CACHE_FILE, segno, offset, buf, size, io_error);

	if (local_read_size == size)
		return true;
	else if (POLAR_SEGMENT_IO_SUCCESS(io_error))
	{
		if (!(cache->io_permission & POLAR_CACHE_SHARED_FILE_READ)
				|| (local_read_size + offset) == cache->segment_size)
		{
			/* Set ERANGE error when read to the end of file */
			errno = ERANGE;
			POLAR_LOG_IO_ERROR(segno, POLAR_LOCAL_CACHE_FILE, offset, size,
							   local_read_size, POLAR_CACHE_READ_FAILED);
		}
	}

	if (!POLAR_SEGMENT_IO_SUCCESS(io_error) && !POLAR_SEGMENT_NOT_EXISTS(io_error))
		return false;

	if (!(cache->io_permission & POLAR_CACHE_SHARED_FILE_READ))
		return false;

	MemSet(io_error, 0, sizeof(polar_cache_io_error));

	seg_buf = palloc(cache->segment_size);

	shared_read_size = polar_cache_copy(cache, SHARED_TO_LOCAL, segno, 0, seg_buf, io_error);

	if (shared_read_size < 0 || !POLAR_SEGMENT_IO_SUCCESS(io_error))
	{
		pfree(seg_buf);
		return false;
	}

	if (shared_read_size < offset + size)
	{
		errno = ERANGE;
		POLAR_LOG_IO_ERROR(segno, POLAR_SHARED_STORAGE_FILE, offset, size,
						   shared_read_size, POLAR_CACHE_READ_FAILED);
		pfree(seg_buf);
		return false;
	}

	memcpy(buf, seg_buf + offset, size);

	pfree(seg_buf);

	return true;
}

static bool
polar_shared_file_flush(polar_local_cache cache, polar_io_segment *seg, polar_cache_io_error *io_error)
{
	ssize_t copy_size;
	char *buf;

	buf = palloc(cache->segment_size);

	copy_size = polar_cache_copy(cache, LOCAL_TO_SHARED, seg->segno, seg->min_write_offset, buf, io_error);

	pfree(buf);

	return copy_size >= 0 && POLAR_SEGMENT_IO_SUCCESS(io_error);
}

/*
 * Evict io segment in the following order
 * 1. Evict io segment which is clean
 * 2. Evict io segment which is in io progrss
 * 3. Flush dirty segment
 * We should acquire cache lock before call this function
 */
static bool
polar_evict_io_segment(polar_local_cache cache, polar_cache_io_error *io_error)
{
	bool flushed = true;

	while (POLAR_SUCCESSOR_LIST_EMPTY(cache->free_items_list) && flushed)
	{
		bool io_in_progress = false;
		bool evicted = false;
		polar_io_segment *io_seg;
		int i;

		for (i = 0; i < cache->max_segments; i++)
		{
			io_seg = &cache->io_seg_items[i];

			/* Evict segment which complete io read and it's clean */
			if (POLAR_CACHE_IO_IN_PROGRESS(io_seg))
				io_in_progress = true;
			else if (POLAR_CACHE_IS_OCCUPIED_SEGMENT(io_seg) && POLAR_CACHE_IS_CLEAN_SEGMENT(io_seg))
			{
				if (hash_search(cache->hash_io_seg, (void *) & (io_seg->segno),
								HASH_REMOVE, NULL) == NULL)
					elog(FATAL, "polar local cache hash table for %s is corrupted", cache->dir_name);

				io_seg->status = 0;
				polar_successor_list_push(cache->free_items_list, i);
				evicted = true;
			}
		}

		if (evicted)
			break;

		if (io_in_progress)
		{
			LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));
			LWLockAcquire(POLAR_LOCAL_CACHE_LOCK(cache), LW_EXCLUSIVE);
			continue;
		}

		/* Reach here means all segments are dirty and no io in progress, so we choose the first segment to write */
		io_seg = &cache->io_seg_items[0];

		flushed = polar_local_cache_flush(cache, io_seg, io_error);
	}

	return !POLAR_SUCCESSOR_LIST_EMPTY(cache->free_items_list);
}

static polar_io_seg_entry *
polar_create_new_seg_entry(polar_local_cache cache, uint64 segno, polar_cache_io_error *io_error)
{
	int index;
	polar_io_seg_entry *entry;
	bool found;

	if (POLAR_SUCCESSOR_LIST_EMPTY(cache->free_items_list)
			&& !polar_evict_io_segment(cache, io_error))
		return NULL;

	entry = hash_search(cache->hash_io_seg, (void *)&segno, HASH_ENTER, &found);

	if (!found)
	{
		index = polar_successor_list_pop(cache->free_items_list);

		if (index == POLAR_SUCCESSOR_LIST_NIL || cache->io_seg_items[index].status != 0)
			elog(FATAL, "%s local cache free list is corrupted", cache->dir_name);

		entry->index = index;
		cache->io_seg_items[index].segno = segno;
		cache->io_seg_items[index].status = POLAR_CACHE_SEG_OCCUPIED;
		cache->io_seg_items[index].min_write_offset = 0;
	}

	return entry;
}

bool
polar_local_cache_read(polar_local_cache cache, uint64 segno,
					   uint32 offset, char *buf, uint32 size, polar_cache_io_error *io_error)
{
	polar_io_seg_entry *entry;
	bool ret;
	polar_io_segment *io_seg = NULL;

	Assert(cache->io_permission & POLAR_CACHE_LOCAL_FILE_READ);
	memset(io_error, 0, sizeof(polar_cache_io_error));

	for (;;)
	{
		LWLockAcquire(POLAR_LOCAL_CACHE_LOCK(cache), LW_EXCLUSIVE);
		entry = hash_search(cache->hash_io_seg, (void *)&segno, HASH_FIND, NULL);

		if (entry == NULL)
			entry = polar_create_new_seg_entry(cache, segno, io_error);

		if (!entry)
		{
			LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));
			elog(LOG, "Failed to find entry for %ld in %s local cache", segno, cache->dir_name);
			return false;
		}

		io_seg = &cache->io_seg_items[entry->index];
		Assert(io_seg->segno == segno);

		/*
		 * 1. Wait when write in progress.
		 * 2. We need one process to read segment from pfs to local fs, so wait when read in progress and local segment is not readable.
		 * 3. Wait if  the reference number of read in progress reach the max limit. It should never happen
		 */
		if (POLAR_CACHE_WRITE_IN_PROGRESS(io_seg) || POLAR_CACHE_SHARED_READING(io_seg)
				|| POLAR_CACHE_REACH_READ_LIMIT(io_seg))
		{
			LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));
			continue;
		}

		HOLD_INTERRUPTS();

		/* Increase read refenece */
		io_seg->status++;
		LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));
		break;
	}

	ret = polar_cache_read(cache, io_seg->segno, offset, buf, size, io_error);

	LWLockAcquire(POLAR_LOCAL_CACHE_LOCK(cache), LW_EXCLUSIVE);
	/* Decrease read reference */
	io_seg->status--;

	if (ret)
		io_seg->status |= POLAR_CACHE_SEG_READABLE;

	LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));

	RESUME_INTERRUPTS();

	return ret;
}

bool
polar_local_cache_write(polar_local_cache cache, uint64 segno,
						uint32 offset, char *buf, uint32 size, polar_cache_io_error *io_error)
{
	polar_io_seg_entry *entry;
	ssize_t write_size = -1;
	polar_io_segment *io_seg = NULL;
	polar_cache_file_type file_type;

	memset(io_error, 0, sizeof(polar_cache_io_error));

	if (cache->io_permission & POLAR_CACHE_LOCAL_FILE_WRITE)
		file_type = POLAR_LOCAL_CACHE_FILE;
	else if (cache->io_permission & POLAR_CACHE_SHARED_FILE_WRITE)
		file_type = POLAR_SHARED_STORAGE_FILE;
	else
		elog(FATAL, "Write is not allowed for local cache %s, permission=%x", cache->dir_name, cache->io_permission);

	for (;;)
	{
		LWLockAcquire(POLAR_LOCAL_CACHE_LOCK(cache), LW_EXCLUSIVE);
		entry = hash_search(cache->hash_io_seg, (void *)&segno, HASH_FIND, NULL);

		if (entry == NULL)
			entry = polar_create_new_seg_entry(cache, segno, io_error);

		if (!entry)
		{
			LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));
			elog(LOG, "Failed to get entry for segno=%ld in local cache %s", segno, cache->dir_name);
			return false;
		}

		io_seg = &cache->io_seg_items[entry->index];
		Assert(io_seg->segno == segno);

		if (POLAR_CACHE_IO_IN_PROGRESS(io_seg))
		{
			LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));
			continue;
		}

		/* Protect io_seg->status in this process */
		HOLD_INTERRUPTS();
		io_seg->status |= (POLAR_CACHE_SEG_WRITE_IN_PROGRESS | POLAR_CACHE_SEG_DIRTY);

		/* Set mininum offset to write */
		if (!(io_seg->status & POLAR_CACHE_SEG_SET_MIN_OFFSET))
		{
			io_seg->status |= POLAR_CACHE_SEG_SET_MIN_OFFSET;
			io_seg->min_write_offset = offset;
		}
		else
			io_seg->min_write_offset = Min(io_seg->min_write_offset, offset);

		LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));
		break;
	}

	/*
	 * If write to local file then we must chek if it already exists,
	 * otherwise try to copy segment from shared to local
	 */
	if (file_type == POLAR_LOCAL_CACHE_FILE)
	{
		if (polar_cache_copy_local_not_exists(cache, segno, io_error))
			write_size = polar_cache_file_write(cache, file_type, io_seg->segno, offset, buf, size, io_error);
	}
	else
		write_size = polar_cache_file_write(cache, file_type, io_seg->segno, offset, buf, size, io_error);

	LWLockAcquire(POLAR_LOCAL_CACHE_LOCK(cache), LW_EXCLUSIVE);
	io_seg->status &= (~POLAR_CACHE_SEG_WRITE_IN_PROGRESS);

	if (write_size == size && file_type == POLAR_SHARED_STORAGE_FILE)
		io_seg->status &= ~(POLAR_CACHE_SEG_DIRTY);

	LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));

	RESUME_INTERRUPTS();

	if (write_size != size)
		elog(LOG, "%s size mismatch, write_size=%ld and size=%d", __func__, write_size, size);

	return write_size == size;
}

/*
 * Fulsh one segment file and we should acquire the cache lock before call this function
 */
static bool
polar_local_cache_flush(polar_local_cache cache, polar_io_segment *io_seg, polar_cache_io_error *io_error)
{
	Assert(cache->io_permission & (POLAR_CACHE_LOCAL_FILE_WRITE | POLAR_CACHE_SHARED_FILE_WRITE));

	for (;;)
	{
		bool flushed;

		if (!POLAR_CACHE_IS_OCCUPIED_SEGMENT(io_seg))
			return true;

		if (POLAR_CACHE_IS_CLEAN_SEGMENT(io_seg))
			return true;

		if (POLAR_CACHE_WRITE_IN_PROGRESS(io_seg))
		{
			LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));
			LWLockAcquire(POLAR_LOCAL_CACHE_LOCK(cache), LW_EXCLUSIVE);
			continue;
		}

		/* Hold interrupts to make sure io_seg->status is changed in this process */
		HOLD_INTERRUPTS();

		io_seg->status |= POLAR_CACHE_SEG_WRITE_IN_PROGRESS;

		if (!(cache->io_permission & POLAR_CACHE_SHARED_FILE_WRITE))
			flushed = polar_record_local_flushed_seg(cache, io_seg->segno, io_error);
		else
		{
			Assert(cache->io_permission & POLAR_CACHE_LOCAL_FILE_WRITE);

			LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));
			flushed = polar_shared_file_flush(cache, io_seg, io_error);
			LWLockAcquire(POLAR_LOCAL_CACHE_LOCK(cache), LW_EXCLUSIVE);
		}

		if (flushed)
			io_seg->status &= ~(POLAR_CACHE_SEG_DIRTY | POLAR_CACHE_SEG_WRITE_IN_PROGRESS);
		else
			io_seg->status &= ~(POLAR_CACHE_SEG_WRITE_IN_PROGRESS);

		RESUME_INTERRUPTS();

		return flushed;
	}
}

static uint64
polar_local_cache_update_flushed_times(polar_local_cache cache)
{
	uint64 dst_remove_flushed_times = 0;

	if (cache->io_permission & POLAR_CACHE_SHARED_FILE_WRITE)
		return 0;

	/*
	 * We update local_flushed_times when do checkpoint, and we
	 * keep last 3 times flushed  lists.
	 * These flushed lists are used when do online promote.
	 * We copy files from local cache to shared storage base on these flush lists.
	 */
	if (cache->local_flushed_times > 2)
		dst_remove_flushed_times = cache->local_flushed_times - 2;

	cache->local_flushed_times++;

	return dst_remove_flushed_times;
}

bool
polar_local_cache_flush_all(polar_local_cache cache, polar_cache_io_error *io_error)
{
	int i;
	uint64 dst_remove_flushed_times = 0;
	int  flushed_seg = 0;
	bool flushed_all;
	int *need_flush;

	memset(io_error, 0, sizeof(polar_cache_io_error));

	Assert(cache->io_permission & (POLAR_CACHE_LOCAL_FILE_READ | POLAR_CACHE_LOCAL_FILE_WRITE));

	need_flush = palloc(sizeof(int) * cache->max_segments);

	for (i = 0; i < cache->max_segments; i++)
		need_flush[i] = i;

	do
	{
		flushed_all = true;
		LWLockAcquire(POLAR_LOCAL_CACHE_LOCK(cache), LW_EXCLUSIVE);

		for (i = 0; i < cache->max_segments; i++)
		{
			polar_io_segment *io_seg;

			if (need_flush[i] == -1)
				continue;

			io_seg = &cache->io_seg_items[i];

			if (!POLAR_CACHE_IS_OCCUPIED_SEGMENT(io_seg)
					|| POLAR_CACHE_IS_CLEAN_SEGMENT(io_seg))
			{
				need_flush[i] = -1;
				continue;
			}

			if (POLAR_CACHE_WRITE_IN_PROGRESS(io_seg))
			{
				/* Wait for previous io opertion to end. We will check this segment in next loop */
				LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));
				LWLockAcquire(POLAR_LOCAL_CACHE_LOCK(cache), LW_EXCLUSIVE);
				flushed_all = false;
				continue;
			}

			if (!polar_local_cache_flush(cache, io_seg, io_error))
			{
				polar_local_cache_report_error(cache, io_error, ERROR);
				flushed_all = false;
			}
			else
			{
				flushed_seg++;
				need_flush[i] = -1;
			}
		}

		if (flushed_all && flushed_seg > 0)
			dst_remove_flushed_times = polar_local_cache_update_flushed_times(cache);

		LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));
	}
	while (!flushed_all && io_error->save_errno == 0);

	if (flushed_all && dst_remove_flushed_times != 0)
	{
		char path[MAXPGPATH];

		/* We only keep the segments flushed list of the last flushed time */
		POLAR_LOCAL_CACHE_META_PATH(path, cache, dst_remove_flushed_times);
		unlink(path);
	}

	pfree(need_flush);
	return flushed_all;
}

static bool
polar_cache_file_unlink(polar_local_cache cache, polar_cache_file_type file_type, uint64 segno, polar_cache_io_error *io_error)
{
	char path[MAXPGPATH];

	switch (file_type)
	{
		case POLAR_LOCAL_CACHE_FILE:
			POLAR_LOCAL_FILE_PATH(path, cache, segno);
			break;

		case POLAR_SHARED_STORAGE_FILE:
			POLAR_SHARED_FILE_PATH(path, cache, segno);
			break;

		default:
			POLAR_LOG_BACKTRACE();
			elog(FATAL, "Incorrect filetype=%d for %s", file_type, __func__);

	}

	if (unlink(path) < 0 && errno != ENOENT)
	{
		POLAR_LOG_IO_ERROR(segno, file_type, POLAR_UNKNOWN_CACHE_OFFSET, POLAR_UNKNOWN_CACHE_SIZE,
						   -1, POLAR_CACHE_UNLINK_FAILED);

		return false;
	}

	return true;
}

bool
polar_local_cache_remove(polar_local_cache cache, uint64 segno, polar_cache_io_error *io_error)
{
	polar_io_seg_entry *entry;

	Assert(cache->io_permission & (POLAR_CACHE_LOCAL_FILE_WRITE | POLAR_CACHE_LOCAL_FILE_READ));

	memset(io_error, 0, sizeof(polar_cache_io_error));

	for (;;)
	{
		LWLockAcquire(POLAR_LOCAL_CACHE_LOCK(cache), LW_EXCLUSIVE);
		entry = hash_search(cache->hash_io_seg, (void *)&segno, HASH_FIND, NULL);

		if (entry)
		{
			polar_io_segment *io_seg = &cache->io_seg_items[entry->index];

			/* wait when io in progress */
			if (POLAR_CACHE_IO_IN_PROGRESS(io_seg))
			{
				LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));
				continue;
			}

			/* Protect io_seg data which in shared memory */
			HOLD_INTERRUPTS();

			/* flush to shared storage when segment is dirty */
			if (!POLAR_CACHE_IS_CLEAN_SEGMENT(io_seg) &&
					(cache->io_permission & POLAR_CACHE_SHARED_FILE_WRITE))
			{
				bool flushed;

				io_seg->status |= POLAR_CACHE_SEG_WRITE_IN_PROGRESS;
				LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));

				flushed = polar_shared_file_flush(cache, io_seg, io_error);
				LWLockAcquire(POLAR_LOCAL_CACHE_LOCK(cache), LW_EXCLUSIVE);

				if (flushed)
					io_seg->status &= ~(POLAR_CACHE_SEG_WRITE_IN_PROGRESS | POLAR_CACHE_SEG_DIRTY);
				else
				{
					io_seg->status &= ~POLAR_CACHE_SEG_WRITE_IN_PROGRESS;
					LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));

					RESUME_INTERRUPTS();
					return false;
				}
			}

			cache->io_seg_items[entry->index].status = 0;
			polar_successor_list_push(cache->free_items_list, entry->index);

			if (hash_search(cache->hash_io_seg, (void *)&segno, HASH_REMOVE, NULL) == NULL)
				elog(FATAL, "The local cache hash table for %s is corrupted", cache->dir_name);

			RESUME_INTERRUPTS();
		}

		LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));
		break;
	}

	if ((cache->io_permission & POLAR_CACHE_SHARED_FILE_WRITE)
			&& !polar_cache_file_unlink(cache, POLAR_SHARED_STORAGE_FILE, segno, io_error))
		return false;

	Assert(cache->io_permission & POLAR_CACHE_LOCAL_FILE_WRITE);

	return polar_cache_file_unlink(cache, POLAR_LOCAL_CACHE_FILE, segno, io_error);
}

void
polar_local_cache_report_error(polar_local_cache cache, polar_cache_io_error *io_error, int log_level)
{
	char file_path[MAXPGPATH];

	POLAR_LOG_BACKTRACE();

	switch (io_error->file_type)
	{
		case POLAR_LOCAL_CACHE_FILE:
			POLAR_LOCAL_FILE_PATH(file_path, cache, io_error->segno);
			break;

		case POLAR_LOCAL_CACHE_META:
			POLAR_LOCAL_CACHE_META_PATH(file_path, cache, cache->local_flushed_times);
			break;

		case POLAR_SHARED_STORAGE_FILE:
			POLAR_SHARED_FILE_PATH(file_path, cache, io_error->segno);
			break;

		default:
			POLAR_LOG_BACKTRACE();
			elog(log_level < FATAL ? FATAL : log_level, "Incorret local cache filetype=%d", io_error->file_type);
	}

	elog(WARNING, "Failed to do io for local cache, file_type=%d, io_permission=%X",
		 io_error->file_type, cache->io_permission);

	switch (io_error->errcause)
	{
		case POLAR_CACHE_OPEN_FAILED:
			elog(log_level, "Failed to open file %s, errno=%d", file_path, io_error->save_errno);
			break;

		case POLAR_CACHE_SEEK_FAILED:
			elog(log_level, "Failed to seek file %s from offset=%ld and size=%ld, errno=%d, io_return=%ld",
				 file_path, io_error->offset, io_error->size, io_error->save_errno, io_error->io_return);
			break;

		case POLAR_CACHE_READ_FAILED:
			elog(log_level, "Failed to read file %s from offset=%ld and size=%ld, errno=%d, io_return=%ld",
				 file_path, io_error->offset, io_error->size, io_error->save_errno, io_error->io_return);
			break;

		case POLAR_CACHE_WRITE_FAILED:
			elog(log_level, "Failed to write file %s from offset=%ld and size=%ld, errno=%d, io_return=%ld",
				 file_path, io_error->offset, io_error->size, io_error->save_errno, io_error->io_return);
			break;

		case POLAR_CACHE_SYNC_FAILED:
			elog(log_level, "Failed to sync file %s, errno=%d", file_path, io_error->save_errno);
			break;

		case POLAR_CACHE_UNLINK_FAILED:
			elog(log_level, "Failed to unlink file %s, errno=%d", file_path, io_error->save_errno);
			break;

		default:
			elog(PANIC, "Unknown local cache error cause %d", io_error->errcause);
			break;
	}
}

/*
 * Copy local flushed files to shared storage
 */
static bool
polar_copy_local_flushed(polar_local_cache cache, uint64 flushed_times, polar_cache_io_error *io_error)
{
	char path[MAXPGPATH];
	int fd;
	uint64 segno;
	char *seg_buf;
	bool succeed = true;

	POLAR_LOCAL_CACHE_META_PATH(path, cache, flushed_times);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);

	if (fd < 0)
	{
		POLAR_LOG_IO_ERROR(-1, POLAR_LOCAL_CACHE_META, POLAR_UNKNOWN_CACHE_OFFSET,
						   POLAR_UNKNOWN_CACHE_SIZE, fd, POLAR_CACHE_OPEN_FAILED);

		return POLAR_SEGMENT_NOT_EXISTS(io_error);
	}

	seg_buf = palloc(cache->segment_size);

	while (pg_pread(fd, &segno, sizeof(uint64), 0) > 0)
	{
		if (polar_cache_copy(cache, LOCAL_TO_SHARED, segno, 0, seg_buf, io_error) < 0
				|| !POLAR_SEGMENT_IO_SUCCESS(io_error))
		{
			/*
			 * The segment recored in the flushed list could be unlinked.
			 * So we ignore this segment if it's not exists
			 */
			if (POLAR_SEGMENT_NOT_EXISTS(io_error))
			{
				char file_path[MAXPGPATH];
				POLAR_LOCAL_FILE_PATH(file_path, cache, segno);

				elog(LOG, "%s is not exists, which may be unlinked", file_path);
			}
			else
			{
				succeed = false;
				break;
			}
		}

		if (unlikely(polar_enable_debug))
			elog(LOG, "copy %s %ld from local to shared", cache->dir_name, segno);
	}

	pfree(seg_buf);
	CloseTransientFile(fd);

	return succeed;
}

bool
polar_local_cache_set_io_permission(polar_local_cache cache, uint32 io_permission, polar_cache_io_error *io_error)
{
	uint32 old_permission;
	bool succeed = true;

	MemSet(io_error, 0, sizeof(polar_cache_io_error));

	LWLockAcquire(POLAR_LOCAL_CACHE_LOCK(cache), LW_EXCLUSIVE);
	old_permission = cache->io_permission;
	cache->io_permission = io_permission & POLAR_CACHE_IO_PERMISSION_MASK;

	if (!(old_permission & POLAR_CACHE_SHARED_FILE_WRITE) && (cache->io_permission & POLAR_CACHE_SHARED_FILE_WRITE))
	{
		/*
		 * Copy flushed segments by the last 2 checkpoint
		 */
		if (succeed && cache->local_flushed_times > 2)
			succeed = polar_copy_local_flushed(cache, cache->local_flushed_times - 2, io_error);

		if (succeed && cache->local_flushed_times > 1)
			succeed = polar_copy_local_flushed(cache, cache->local_flushed_times - 1, io_error);

		/* Copy flushed segments which have been evicted recently */
		if (succeed)
			succeed = polar_copy_local_flushed(cache, cache->local_flushed_times, io_error);
	}

	LWLockRelease(POLAR_LOCAL_CACHE_LOCK(cache));

	return succeed;
}

bool
polar_local_cache_move_trash(const char *dir_name)
{
	uint32 i;
	bool moved = false;

	polar_validate_data_dir(POLAR_CACHE_TRASH_DIR);

	/*
	 * Move dir with number as suffix to the trash dir.
	 * Update the suffix number if previous moved dir was not deleted.
	 * We only try 5 times, otherwise it means the instance is crashed during the restarting.
	 * If we move the same dir to the trash dir more than 3 times, we will force to empty the tash
	 */
	for (i = 0; i < 5; i++)
	{
		char new_path[MAXPGPATH];

		snprintf(new_path, MAXPGPATH, "%s/%s_%04X", POLAR_CACHE_TRASH_DIR, dir_name, i);

		if (rename(dir_name, new_path) == 0 || errno == ENOENT)
		{
//			elog(LOG, "rename %s to local cache trash %s, errno=%d", dir_name, new_path, errno);
			moved = true;
			break;
		}

//		if (errno != ENOTEMPTY && errno != EEXIST)
//			elog(WARNING, "Failed to move %s to local trash dir %s, errno=%d",
//				 dir_name, POLAR_CACHE_TRASH_DIR, errno);

		/* Force to clean trash dir if move same dir to the trash dir too much times */
		if (i == 3)
		{
			elog(LOG, "Force to clean trash dir because move the same dir %s to the trash dir too much times", dir_name);
			rmtree(POLAR_CACHE_TRASH_DIR, false);
		}
	}

	/* Create new dir after move it to trash */
	if (moved)
		polar_validate_data_dir(dir_name);

	return moved;
}

bool
polar_local_cache_empty_trash(void)
{
	return rmtree(POLAR_CACHE_TRASH_DIR, false);
}

//updated
void
polar_local_cache_scan(polar_local_cache cache, polar_local_cache_scan_callback callback, uint64 segno)
{
    Assert(0);
//	DIR     *cldir;
//	struct dirent *clde;
//
//	cldir = polar_allocate_dir(cache->dir_name);
//
//	while ((clde = ReadDir(cldir, cache->dir_name)) != NULL)
//	{
//		if (!callback(cache, clde->d_name, segno))
//			break;
//	}
//
//	FreeDir(cldir);
}
