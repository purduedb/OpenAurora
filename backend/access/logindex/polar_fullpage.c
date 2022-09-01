/*-------------------------------------------------------------------------
 *
 * polar_fullpage.c
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
 *           src/backend/access/logindex/polar_fullpage.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>
#include "miscadmin.h"

#include "access/polar_logindex.h"
#include "access/polar_fullpage.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "replication/walreceiver.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

static bool polar_fullpage_online_promote = false;

static int open_fullpage_file = -1;
static uint64 open_fullpage_file_seg_no = -1;

static void remove_old_fullpage_files(polar_fullpage_ctl_t ctl);
static uint64 polar_xlog_read_fullpage_no(logindex_snapshot_t logindex_snapshot, XLogRecPtr lsn);

/*
 * Initialization of shared memory for fullpage control
 */
Size
polar_fullpage_shmem_size(void)
{
	return CACHELINEALIGN(sizeof(polar_fullpage_ctl_data_t));
}

//updated
polar_fullpage_ctl_t
polar_fullpage_shmem_init(const char *name, polar_ringbuf_t queue, logindex_snapshot_t logindex_snapshot)
{
#define FULLPAGE_SUFFIX "_fp_ctl"
	bool    found = false;
	polar_fullpage_ctl_t ctl;
	char        item_name[POLAR_MAX_SHMEM_NAME];

	snprintf(item_name, POLAR_MAX_SHMEM_NAME, "%s%s", name, FULLPAGE_SUFFIX);

	ctl = (polar_fullpage_ctl_t)
		  ShmemInitStruct(item_name, polar_fullpage_shmem_size(), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		memset(ctl, 0, sizeof(polar_fullpage_ctl_data_t));

		pg_atomic_init_u64(&ctl->max_fullpage_no, 0);

		/* Init fullpage lock */
		LWLockRegisterTranche(LWTRANCHE_FULLPAGE_FILE, "fullpage_file_lock");
		LWLockInitialize(&ctl->file_lock.lock, LWTRANCHE_FULLPAGE_FILE);
		StrNCpy(ctl->name, name, MAX_FULLPAGE_DIR_NAME_LEN);
		pg_atomic_init_u32(&ctl->procno, INVALID_PGPROCNO);

		ctl->logindex_snapshot = logindex_snapshot;
		ctl->queue = queue;

		/* Validate that dir to save fullpage files is created */
		if (!polar_in_replica_mode())
		{
			char dir[MAXPGPATH];
			snprintf(dir, MAXPGPATH, "%s/%s", POLAR_DATA_DIR(), ctl->name);
//			polar_validate_dir(dir);
		}
	}
	else
		Assert(found);

	return ctl;
}

/*
 * When initializing, we can only get max_fullpage_no from wal file, because we
 * don't save max_fullpage_no in meta file for compatibility.
 * If wal file is removed, it assume fullpage file isn't needed by anyone, just
 * reset fullpage_no to 0
 */
void
polar_logindex_calc_max_fullpage_no(polar_fullpage_ctl_t ctl)
{
	char        path[MAXPGPATH] = {0};
	uint64      fullpage_no = 0;
	struct stat statbuf;
	XLogSegNo   xlog_seg_no = 0;
	XLogRecPtr lsn = InvalidXLogRecPtr;

	/* set max_fullpage_no when logindex snapshot state is writable */
	if (!ctl || !polar_logindex_check_state(ctl->logindex_snapshot, POLAR_LOGINDEX_STATE_WRITABLE))
		return;

	/* if max_fullpage_no has been set, nothing to do */
	if (pg_atomic_read_u64(&ctl->max_fullpage_no) > 0)
	{
		elog(LOG, "redo done at max_fullpage_no = %ld", pg_atomic_read_u64(&ctl->max_fullpage_no));
		return;
	}

	lsn = polar_get_logindex_snapshot_max_lsn(ctl->logindex_snapshot);

	if (XLogRecPtrIsInvalid(lsn))
		return;

	/* check wal file is exist or not */
	XLByteToSeg(lsn, xlog_seg_no, wal_segment_size);
	XLogFilePath(path, ThisTimeLineID, xlog_seg_no, wal_segment_size);

	/* wal file has been removed olready, we cannot get fullpage_no by lsn */
	if (lstat(path, &statbuf) < 0)
	{
		pg_atomic_write_u64(&ctl->max_fullpage_no, 0);
		return;
	}
	else
	{
		fullpage_no = polar_xlog_read_fullpage_no(ctl->logindex_snapshot, lsn);
		pg_atomic_write_u64(&ctl->max_fullpage_no, fullpage_no);
	}

	elog(LOG, "redo done at max_fullpage_no = %ld", pg_atomic_read_u64(&ctl->max_fullpage_no));
}

/*
 * POLAR: we update max_fullpage_no when replay wal, we guarantee that fullpage_no
 * keep the same order with wal lsn, so last fullpage_no must be max_fullpage_no
 */
void
polar_update_max_fullpage_no(polar_fullpage_ctl_t ctl, uint64 fullpage_no)
{
	pg_atomic_write_u64(&ctl->max_fullpage_no, fullpage_no);
}

/*
 * Install a new FULLPAGE segment file as a current or future log segment.
 */
static bool
install_fullpage_file_segment(polar_fullpage_ctl_t ctl, uint64 *seg_no,
							  const char *tmppath, uint64 max_segno)
{
	char        path[MAXPGPATH];
	struct stat stat_buf;

	FULLPAGE_SEG_FILE_NAME(ctl, path, *seg_no);

	/*
	 * We want to be sure that only one process does this at a time.
	 */
	LWLockAcquire(LOG_INDEX_FULLPAGE_FILE_LOCK(ctl), LW_EXCLUSIVE);

	/* Find a free slot to put it in */
	while (stat(path, &stat_buf) == 0)
	{
		if ((*seg_no) >= max_segno)
		{
			/* Failed to find a free slot within specified range */
			LWLockRelease(LOG_INDEX_FULLPAGE_FILE_LOCK(ctl));
			return false;
		}

		(*seg_no)++;
		FULLPAGE_SEG_FILE_NAME(ctl, path, *seg_no);
	}

	/*
	 * Perform the rename using link if available, paranoidly trying to avoid
	 * overwriting an existing file (there shouldn't be one).
	 */
	//updated
	if (durable_rename(tmppath, path, LOG) != 0)
	{
		LWLockRelease(LOG_INDEX_FULLPAGE_FILE_LOCK(ctl));
		/* durable_link_or_rename already emitted log message */
		return false;
	}

	LWLockRelease(LOG_INDEX_FULLPAGE_FILE_LOCK(ctl));

	return true;
}

//UPDATED From polar_fd
#define POLAR_BUFFER_ALIGN_LEN			(4096)
#define POLAR_BUFFER_EXTEND_SIZE(LEN)	(LEN + POLAR_BUFFER_ALIGN_LEN)
#define POLAR_BUFFER_ALIGN(LEN)			TYPEALIGN(POLAR_BUFFER_ALIGN_LEN, LEN)
static void
fill_fullpage_file_zero_pages(int fd, char *tmppath)
{
#define ONE_MB (1024 * 1024L)
	char        *data;
	char	*aligned_data;
	int     nbytes = 0;

	data = palloc0(POLAR_BUFFER_EXTEND_SIZE(ONE_MB));
	aligned_data = (char *) POLAR_BUFFER_ALIGN(data);

	pgstat_report_wait_start(WAIT_EVENT_FULLPAGE_FILE_INIT_WRITE);

	for (nbytes = 0; nbytes < FULLPAGE_SEGMENT_SIZE; nbytes += ONE_MB)
	{
		int     rc = 0;

		errno = 0;
		rc = (int) pg_pwrite(fd, aligned_data, ONE_MB, nbytes);

		if (rc != ONE_MB)
		{
			int         save_errno = errno;

			/*
			 * If we fail to make the file, delete it to release disk space
			 */
			unlink(tmppath);

			close(fd);

			/* if write didn't set errno, assume problem is no disk space */
			errno = save_errno ? save_errno : ENOSPC;

			pfree(data);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", tmppath)));
		}
	}

	pgstat_report_wait_end();

	pfree(data);

	if (pg_fsync(fd) != 0)
	{
		int         save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", tmppath)));
	}

	return;
}

/*
 * Create a new FULLPAGE file segment, or open a pre-existing one.
 */
int
polar_fullpage_file_init(polar_fullpage_ctl_t ctl, uint64 fullpage_no)
{
	char        path[MAXPGPATH];
	char        tmppath[MAXPGPATH];
	uint64  installed_segno;
	uint64  max_segno;
	int         fd;
	char        polar_tmppath[MAXPGPATH];

	FULLPAGE_FILE_NAME(ctl, path, fullpage_no);

	/*
	 * Try to use existent file (polar worker maker may have created it already)
	 */

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY );

	if (fd < 0)
	{
		if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", path)));
	}
	else
		return fd;

	/*
	 * Initialize an empty (all zeroes) segment.  NOTE: it is possible that
	 * another process is doing the same thing.  If so, we will end up
	 * pre-creating an extra log segment.  That seems OK, and better than
	 * holding the lock throughout this lengthy process.
	 */
	elog(LOG, "creating and filling new FULLPAGE file");

	snprintf(polar_tmppath, MAXPGPATH, "%s/fullpagetemp.%d", ctl->name, (int) getpid());
//	polar_make_file_path_level2(tmppath, polar_tmppath);
    snprintf(tmppath, MAXPGPATH, "%s", polar_tmppath);
	unlink(tmppath);

	/* do not use get_sync_bit() here --- want to fsync only at end of fill */
	fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);

	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmppath)));

	/* POLAR: File allocate, juse change file metadata once */
	if (posix_fallocate(fd, 0, FULLPAGE_SEGMENT_SIZE) != 0)
		elog(ERROR, "polar_fallocate fail in polar_fullpage_file_init");

	fill_fullpage_file_zero_pages(fd, tmppath);

	if (close(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tmppath)));

	/*
	 * Now move the segment into place with its final name.
	 *
	 * If caller didn't want to use a pre-existing file, get rid of any
	 * pre-existing file.  Otherwise, cope with possibility that someone else
	 * has created the file while we were filling ours: if so, use ours to
	 * pre-create a future log segment.
	 */
	installed_segno = FULLPAGE_FILE_SEG_NO(fullpage_no);

	max_segno = FULLPAGE_FILE_SEG_NO(fullpage_no) + polar_fullpage_keep_segments;

	if (!install_fullpage_file_segment(ctl, &installed_segno, tmppath, max_segno))
	{
		/*
		 * No need for any more future segments, or install_fullpage_file_segment()
		 * failed to rename the file into place. If the rename failed, opening
		 * the file below will fail.
		 */
		unlink(tmppath);
	}

	/* Now open original target segment (might not be file I just made) */
	fd = BasicOpenFile(path, O_RDWR | PG_BINARY);

	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	elog(LOG, "done creating and filling new FULLPAGE file %s", path);

	return fd;
}


/*
 * Open a fullpage file segment for read or write.
 *
 */
static int
fullpage_file_open(polar_fullpage_ctl_t ctl, uint64 fullpage_no)
{
	char        path[MAXPGPATH];
	int         fd;

	FULLPAGE_FILE_NAME(ctl, path, fullpage_no);

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY);

	if (fd >= 0)
		return fd;

	if (errno != ENOENT) /* unexpected failure? */
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	return polar_fullpage_file_init(ctl, fullpage_no);
}

static uint64
polar_log_fullpage_begin(polar_fullpage_ctl_t ctl)
{
	/*
	 * fullpage_no must keep the same order with lsn.
	 */
	LWLockAcquire(LOG_INDEX_FULLPAGE_FILE_LOCK(ctl), LW_EXCLUSIVE);

	return pg_atomic_fetch_add_u64(&ctl->max_fullpage_no, 1);
}

static void
polar_log_fullpage_end(polar_fullpage_ctl_t ctl)
{
	LWLockRelease(LOG_INDEX_FULLPAGE_FILE_LOCK(ctl));
}

/*
 * POLAR: write old version page to fullpage file
 */
static void
polar_write_fullpage(polar_fullpage_ctl_t ctl, Page page, uint64 fullpage_no)
{
	if (open_fullpage_file < 0)
	{
		open_fullpage_file = fullpage_file_open(ctl, fullpage_no);
		open_fullpage_file_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);
	}
	else if (open_fullpage_file_seg_no != FULLPAGE_FILE_SEG_NO(fullpage_no))
	{
		close(open_fullpage_file);
		open_fullpage_file = fullpage_file_open(ctl, fullpage_no);
		open_fullpage_file_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);
	}

	pg_pwrite(open_fullpage_file, (void *)page, BLCKSZ, FULLPAGE_FILE_OFFSET(fullpage_no));
}

/*
 * POLAR: read old version page from fullpage file
 */
void
polar_read_fullpage(polar_fullpage_ctl_t ctl, Page page, uint64 fullpage_no)
{
	if (open_fullpage_file < 0)
	{
		open_fullpage_file = fullpage_file_open(ctl, fullpage_no);
		open_fullpage_file_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);
	}
	else if (open_fullpage_file_seg_no != FULLPAGE_FILE_SEG_NO(fullpage_no))
	{
		close(open_fullpage_file);
		open_fullpage_file = fullpage_file_open(ctl, fullpage_no);
		open_fullpage_file_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);
	}

	pg_pread(open_fullpage_file, (void *)page, BLCKSZ, FULLPAGE_FILE_OFFSET(fullpage_no));
}

/*
 * Preallocate fullpage files
 */
void
polar_prealloc_fullpage_files(polar_fullpage_ctl_t ctl)
{
	uint64      max_fullpage_no = 0;
	int     file = 0;
	int     count = 0;

	if (!ctl)
		return ;

	max_fullpage_no = pg_atomic_read_u64(&ctl->max_fullpage_no);

	while (count < 3)
	{
		file = polar_fullpage_file_init(ctl, max_fullpage_no);
		close(file);
		max_fullpage_no += FULLPAGE_NUM_PER_FILE;
		count++;
	}
}


/*
 * Recycle or remove a fullpage file that's no longer needed.
 */
void
polar_remove_old_fullpage_file(polar_fullpage_ctl_t ctl, const char *segname, uint64 min_fullpage_seg_no)
{
	struct stat statbuf;
	uint64 end_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(pg_atomic_read_u64(&ctl->max_fullpage_no));
	uint64 recycle_seg_no = min_fullpage_seg_no + polar_fullpage_keep_segments;

	/*
	 * Before deleting the file, see if it can be recycled as a future fullpage
	 * segment.
	 */
	if (end_fullpage_seg_no <= recycle_seg_no &&
			lstat(segname, &statbuf) == 0 && S_ISREG(statbuf.st_mode) &&
			install_fullpage_file_segment(ctl, &end_fullpage_seg_no, segname,
										  recycle_seg_no))
	{
		/* POLAR: force log */
		ereport(LOG,
				(errmsg("recycled fullpage file \"%s\"",
						segname)));
	}
	else
	{
		/* No need for any more future segments... */
		int         rc;

		/* POLAR: force log */
		ereport(LOG,
				(errmsg("removing fullpage file \"%s\"",
						segname)));

		rc = durable_unlink(segname, LOG);

		if (rc != 0)
		{
			/* Message already logged by durable_unlink() */
			return;
		}
	}
}

/*
 * Recycle or remove all fullpage files older to passed segno.
 * It's a extern function
 */
void
polar_remove_old_fullpage_files(polar_fullpage_ctl_t ctl, XLogRecPtr min_lsn)
{
	polar_logindex_truncate(ctl->logindex_snapshot, min_lsn);
	remove_old_fullpage_files(ctl);
}

static uint64
polar_xlog_read_fullpage_no(logindex_snapshot_t logindex_snapshot, XLogRecPtr lsn)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char       *errormsg;
	uint64  fullpage_no = 0;

//	xlogreader = XLogReaderAllocate(wal_segment_size, &read_local_xlog_page,
//									NULL);
    xlogreader =
            XLogReaderAllocate(wal_segment_size, NULL,
                               XL_ROUTINE(.page_read = &read_local_xlog_page,
                                          .segment_open = NULL,
                                          .segment_close = wal_segment_close),
                               NULL);

	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	XLogBeginRead(xlogreader, lsn);
	record = XLogReadRecord(xlogreader, &errormsg);

	if (record == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read fullpage wal state from WAL at %X/%X",
						(uint32)(lsn >> 32),
						(uint32) lsn)));

	if (XLogRecGetRmid(xlogreader) != RM_XLOG_ID ||
			(XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK) != XLOG_FPSI)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("expected fullpage wal state data is not present in WAL at %X/%X",
						(uint32)(lsn >> 32),
						(uint32) lsn)));

	/* get fullpage_no from record */
	memcpy(&fullpage_no, XLogRecGetData(xlogreader), sizeof(uint64));
	XLogReaderFree(xlogreader);
	return fullpage_no;
}

/*
 * Recycle or remove all fullpage files older to passed segno.
 */
static void
remove_old_fullpage_files(polar_fullpage_ctl_t ctl)
{
	char        path[MAXPGPATH] = {0};
	uint64      min_fullpage_seg_no = 0;
	uint64      fullpage_no = 0;
	struct stat statbuf;
	XLogSegNo   xlog_seg_no = 0;
	XLogRecPtr  min_lsn = InvalidXLogRecPtr;

	if (polar_in_replica_mode())
		return;

	min_lsn = polar_get_logindex_snapshot_storage_min_lsn(ctl->logindex_snapshot);

	/* check wal file is exist or not */
	XLByteToSeg(min_lsn, xlog_seg_no, wal_segment_size);
	XLogFilePath(path, ThisTimeLineID, xlog_seg_no, wal_segment_size);

	/* wal file has been removed already, we cannot get fullpage_no by lsn */
	if (lstat(path, &statbuf) < 0)
		return;

	fullpage_no = polar_xlog_read_fullpage_no(ctl->logindex_snapshot, min_lsn);
	min_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);

	while (min_fullpage_seg_no > 0)
	{
		/* Must remove segments older than min_fullpage_seg_no */
		min_fullpage_seg_no--;

		FULLPAGE_SEG_FILE_NAME(ctl, path, min_fullpage_seg_no);

		/* File has been removed already */
		if (lstat(path, &statbuf) < 0)
			break;

		elog(LOG, "attempting to remove FULLPAGE segments older than log file %s",
			 path);
		polar_remove_old_fullpage_file(ctl, path, min_fullpage_seg_no);
	}
}

/*
 * POLAR: write a WAL record containing a full snapshot image of a page. Caller is
 * responsible for writing the page to disk after calling this routine.
 */
//XI: updated
XLogRecPtr
polar_log_fullpage_snapshot_image(polar_fullpage_ctl_t ctl, Buffer buffer, XLogRecPtr oldest_apply_lsn)
{
	SMgrRelation smgr = NULL;
	int         flags;
	RelFileNode rnode;
	ForkNumber  forkNum;
	BlockNumber blkno;
	XLogRecPtr  recptr;
	BufferDesc *buf_hdr = GetBufferDescriptor(buffer - 1);
	uint64      fullpage_no = 0;
	static char *fullpage = NULL;

	/*
	 * We allocate the old page space once and use it over on each subsequent
	 * call.
	 */
	if (fullpage == NULL)
		fullpage = MemoryContextAlloc(TopMemoryContext, BLCKSZ);

	Assert(!polar_in_replica_mode());

	flags = REGBUF_NO_IMAGE;
	BufferGetTag(buffer, &rnode, &forkNum, &blkno);

	/*
	 * If we have copy buffer, and copy buffer can be flushed,
	 * just treat copy buffer as fullpage, so we can avoid reading
	 * older page from storage
	 */
	if (buf_hdr->copy_buffer &&
			polar_copy_buffer_get_lsn(buf_hdr->copy_buffer) <= oldest_apply_lsn)
		memcpy(fullpage, (char *)CopyBufHdrGetBlock(buf_hdr->copy_buffer), BLCKSZ);
	else
	{
		/* Find smgr relation for buffer */
		smgr = smgropen(rnode, InvalidBackendId);

		smgrread(smgr, forkNum, blkno, (char *) fullpage);

		/* check for garbage data, decrypt page if it has beed encrypted */
        if (!PageIsVerified((Page) fullpage, blkno))
//		if (!PageIsVerified((Page) fullpage, forkNum, blkno, smgr))
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid page in block %u of relation %s",
							blkno,
							relpath(smgr->smgr_rnode, forkNum))));
		}
	}

	/*
	 * POLAR: get fullpage write lock, only one process can
	 * write fullpage wal record at the same time, we expected
	 * that fullpage_no keep the same order with lsn, it's used
	 * for easily cleaning fullpage file
	 * NOTE: we assume that XLogInsert is more faster then smgrread
	 * and polar_write_fullpage
	 */
	fullpage_no = polar_log_fullpage_begin(ctl);

	XLogBeginInsert();
	XLogRegisterBuffer(0, buffer, flags);
	XLogRegisterData((char *)(&fullpage_no), sizeof(uint64));

	recptr = XLogInsert(RM_XLOG_ID, XLOG_FPSI);

	/* POLAR: release fullpage write lock */
	polar_log_fullpage_end(ctl);

	polar_write_fullpage(ctl, fullpage, fullpage_no);
	return recptr;
}

/*
 * POLAR: this function is only used for polar worker pop record from queue
 */
static XLogRecord *
polar_fullpage_bgworker_xlog_recv_queue_pop(polar_fullpage_ctl_t ctl, polar_ringbuf_ref_t *ref,
											XLogReaderState *state, char **errormsg)
{
    Assert(0);
    return InvalidXLogRecPtr;
}

/*
 * POLAR: polar worker replay fullpage wal record, only replica
 * can do this
 */
// updated
void
polar_bgworker_fullpage_snapshot_replay(polar_fullpage_ctl_t ctl)
{
	XLogRecord *record;
	char       *errormsg = NULL;
	TimestampTz last_load_logindex_time = 0;
	static polar_ringbuf_ref_t ref = { .slot = -1} ;
	static XLogReaderState *xlogreader = NULL;

	/* wait util reach consistency mode */
	if (!HotStandbyActive())
		return;

	if (unlikely(ref.slot == -1))
	{
		if (polar_fullpage_online_promote)
			return;

//		POLAR_XLOG_QUEUE_NEW_REF(&ref, ctl->queue, true, "polar_fullpage_xlog_queue");
	}

	/* Set up XLOG reader facility */
	if (!xlogreader)
//		xlogreader = XLogReaderAllocate(wal_segment_size, NULL, NULL);
        xlogreader =
            XLogReaderAllocate(wal_segment_size, NULL,
                               XL_ROUTINE(.page_read = &read_local_xlog_page,
                                          .segment_open = NULL,
                                          .segment_close = wal_segment_close),
                               NULL);

	/*no cover begin*/
	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	/*no cover end*/

	for (;;)
	{

		record = polar_fullpage_bgworker_xlog_recv_queue_pop(ctl, &ref, xlogreader, &errormsg);

		if (record && record->xl_rmid == RM_XLOG_ID && record->xl_info == XLOG_FPSI)
		{
			BufferTag tag;
			POLAR_GET_LOG_TAG(xlogreader, tag, 0);
			POLAR_LOG_INDEX_ADD_LSN(ctl->logindex_snapshot, xlogreader, &tag);
		}

		if (record == NULL)
		{
			TimestampTz now = GetCurrentTimestamp();
			XLogRecPtr last_replay_end_ptr = GetXLogReplayRecPtr(NULL);

			CHECK_FOR_INTERRUPTS();

			if (polar_fullpage_get_online_promote())
			{
				polar_ringbuf_release_ref(&ref);
				ref.slot = -1;
				break;
			}

			if (errormsg != NULL)
			{
				elog(WARNING, "Failed to read xlog for fullpage, errmsg=%s", errormsg);
				break;
			}

			/*
			 * We load logindex from storage in following cases:
			 * 1. read from file instead of queue
			 * 2. wal receiver hang or lost for a long time(1s?)
			 * 3. periodly load logindex from storage(1s)
			 */
//			if (TimestampDifferenceExceeds(
//						polar_get_walrcv_last_msg_receipt_time(), now, 1000) ||
//					TimestampDifferenceExceeds(
//						last_load_logindex_time, now, 1000))
//			{
//				last_load_logindex_time = now;
//				polar_load_logindex_snapshot_from_storage(ctl->logindex_snapshot, InvalidXLogRecPtr);
//			}

			/*
			 * POLAR: when read from wal file instead of queue maybe very slow.
			 * we use replay_lsn for polar worker catch up with starup replay
			 * when replay fullpage wal, because polar worker don't need to
			 * replay wal lsn less then replay_lsn, the future page fullpage
			 * snapshot wal lsn must be bigger then replay_lsn
			 */

			if (xlogreader->EndRecPtr < last_replay_end_ptr)
				xlogreader->EndRecPtr = last_replay_end_ptr;

			break;
		}
	}
}

void
polar_fullpage_bgworker_wait_notify(polar_fullpage_ctl_t ctl)
{
	if (ctl)
		pg_atomic_write_u32(&ctl->procno, MyProc->pgprocno);
}

void
polar_fullpage_bgworker_wakeup(polar_fullpage_ctl_t ctl)
{
	int procno;

	if (ctl)
	{
		procno = pg_atomic_read_u32(&ctl->procno);

		if (procno != INVALID_PGPROCNO)
		{
			pg_atomic_write_u32(&ctl->procno, INVALID_PGPROCNO);
			/*
			 * Not acquiring ProcArrayLock here which is slightly icky. It's
			 * actually fine because procLatch isn't ever freed, so we just can
			 * potentially set the wrong process' (or no process') latch.
			 */
			SetLatch(&ProcGlobal->allProcs[procno].procLatch);
		}
	}
}

void
polar_fullpage_set_online_promote(bool online_promote)
{
	polar_fullpage_online_promote = online_promote;
}

bool
polar_fullpage_get_online_promote(void)
{
	return polar_fullpage_online_promote;
}

