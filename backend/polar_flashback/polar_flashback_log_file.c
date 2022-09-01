/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_file.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_log_file.c
 *
 *-------------------------------------------------------------------------
 */
#include <dirent.h>
#include <unistd.h>

#include "postgres.h"

#include "access/xlog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "polar_flashback/polar_flashback_log_file.h"
#include "port.h"
#include "storage/fd.h"
#include "utils/guc.h"

#define FLOG_TMP_FNAME "flogtmp."

//#define flog_dir_full_path(ctl, path) \
//	polar_make_file_path_level2(path, polar_get_flog_dir(ctl))

#define flog_dir_full_path(ctl, path) \
    snprintf(ctl, MAXPGPATH, "%s", path)

#define flog_file_full_path(ctl, path, file_name) \
	polar_make_file_path_level3(path, polar_get_flog_dir(ctl), file_name)

/* GUCs */
int polar_flashback_log_keep_segments;

static bool
is_file_exist(const char *path)
{
	struct stat st;

	if ((stat(path, &st) == 0) && S_ISREG(st.st_mode))
		return true;
	else
		return false;
}

/* Get flashback log file path. */
static void
flog_file_path_from_seg(char *path, const char *dir, uint64 seg_no, int segsz_bytes)
{
	char seg_file_name[FLOG_MAX_FNAME_LEN];

	get_flog_fname(seg_file_name, seg_no, segsz_bytes, FLOG_DEFAULT_TIMELINE);
	polar_make_file_path_level3(path, dir, seg_file_name);
}

/*
 * Install a new flashback log file as a current or future segment.
 */
static bool
install_flog_seg(flog_buf_ctl_t ctl, uint64 *segno, char *tmppath, bool find_free, uint64 max_segno)
{
	char        path[MAXPGPATH];
	struct stat stat_buf;
	LWLock *lock = &ctl->init_lock;

	flog_file_path_from_seg(path, polar_get_flog_dir(ctl), *segno, POLAR_FLOG_SEG_SIZE);

	/*
	 * We want to be sure that only one process does this at a time.
	 */
	LWLockAcquire(lock, LW_EXCLUSIVE);

	if (!find_free)
	{
		/* Force installation: get rid of any pre-existing segment file */
		durable_unlink(path, DEBUG1);
	}
	else
	{
		/* Find a free slot to put it in */
		while (stat(path, &stat_buf) == 0)
		{
			if ((*segno) >= max_segno)
			{
				/* Failed to find a free slot within specified range */
				LWLockRelease(lock);
				return false;
			}

			(*segno)++;
			flog_file_path_from_seg(path, polar_get_flog_dir(ctl), *segno, POLAR_FLOG_SEG_SIZE);
		}
	}

	/*
	 * Perform the rename using link if available, paranoidly trying to avoid
	 * overwriting an existing file (there shouldn't be one).
	 */
	//updated
	if (durable_rename(tmppath, path, LOG) != 0)
	{
		LWLockRelease(lock);
		/* durable_link_or_rename already emitted log message */
		return false;
	}

	LWLockRelease(lock);
	return true;
}

/*
 * Recycle or remove a log file that's no longer needed.
 *
 * endptr is current (or recent) end of flashback log. It is used to determine
 * whether we want to recycle rather than delete no-longer-wanted log files.
 * If lastredoptr is not known, pass invalid, and the function will recycle,
 * somewhat arbitrarily, 10 future segments.
 */
static void
remove_old_flog_file(flog_buf_ctl_t ctl, const char *segname, polar_flog_rec_ptr endptr)
{
	char        path[MAXPGPATH];
	struct stat statbuf;
	uint64  endlogSegNo;
	uint64  recycleSegNo;

	/*
	 * Initialize info about where to try to recycle to.
	 */
	endlogSegNo = flog_ptr_to_seg(endptr, POLAR_FLOG_SEG_SIZE);

	/* POLAR: Just reuse polar_flashback_log_keep_segments segments */
	recycleSegNo = endlogSegNo + polar_flashback_log_keep_segments;

	flog_file_full_path(ctl, path, segname);

	/*
	 * Before deleting the file, see if it can be recycled as a future log
	 * segment. Only recycle normal files.
	 */
	if (endlogSegNo <= recycleSegNo &&
			lstat(path, &statbuf) == 0 && S_ISREG(statbuf.st_mode) &&
			install_flog_seg(ctl, &endlogSegNo, path,
							 true, recycleSegNo))
	{
		/* POLAR: force log */
		ereport(LOG,
				(errmsg("recycled flashback log file \"%s\"",
						segname)));
		endlogSegNo++;
	}
	else
	{
		/* No need for any more future segments... */
		ereport(LOG,
				(errmsg("removing flashback log file \"%s\"",
						segname)));

		/* Message already logged by durable_unlink() */
		durable_unlink(path, LOG);
	}
}

static void
keep_flog_seg(uint64 *segno)
{
	/* compute limit for polar_flashback_log_keep_segments */
	if (polar_flashback_log_keep_segments > 0)
	{
		/* avoid underflow, don't go below 1 */
		if (*segno <= polar_flashback_log_keep_segments)
			*segno = 1;
		else
			*segno = *segno - polar_flashback_log_keep_segments;
	}
}

/*
 * Recycle or remove all log files older or equal to passed segno.
 *
 * endptr is current (or recent) end of flashback log used to determine
 * whether we want to recycle rather than delete no-longer-wanted log files.
 */
//updated
static void
truncate_flog_files(flog_buf_ctl_t ctl, uint64 segno, polar_flog_rec_ptr endptr)
{
//	DIR        *xldir;
//	struct dirent *xlde;
//	char        lastoff[FLOG_MAX_FNAME_LEN];
//	char        polar_path[MAXPGPATH];
//
//	Assert(!polar_in_replica_mode());
//	flog_dir_full_path(ctl, polar_path);
//
//	/*
//	 * Construct a filename of the last segment to be kept. The timeline ID
//	 * doesn't matter, we ignore that in the comparison. (During recovery,
//	 * ThisTimeLineID isn't set, so we can't use that.)
//	 */
//	get_flog_fname(lastoff, segno, POLAR_FLOG_SEG_SIZE, FLOG_DEFAULT_TIMELINE);
//
//	elog(LOG, "attempting to remove flashback log segments older than log file %s",
//		 lastoff);
//
//	xldir = polar_allocate_dir(polar_path);
//
//	while ((xlde = ReadDir(xldir, polar_path)) != NULL)
//	{
//		/* Ignore files that are not flashback log segments */
//		if (!is_flashback_log_file(xlde->d_name))
//			continue;
//
//		if (strcmp(xlde->d_name, lastoff) <= 0)
//			remove_old_flog_file(ctl, xlde->d_name, endptr);
//	}
//
//	FreeDir(xldir);
}

/*
 * POLAR: Truncate the flashback log files before the checkpoint ptr.
 */
void
polar_truncate_flog_before(flog_buf_ctl_t ctl, polar_flog_rec_ptr ptr)
{
	uint64 seg_no;
	polar_flog_rec_ptr recptr;

	/* Get the current flashback log write result (end of flashback log in disk) */
	recptr = polar_get_flog_write_result(ctl);

	seg_no = flog_ptr_to_seg(ptr, POLAR_FLOG_SEG_SIZE);

	/* There is only one file, just return */
	if (seg_no == 0)
		return;

	keep_flog_seg(&seg_no);
	seg_no--;
	truncate_flog_files(ctl, seg_no, recptr);
}

/*
 * Read 'count' bytes from flashback log into 'buf',
 * starting at location 'startptr'.
 *
 * Will open, and keep open, one segment stored in the static file
 * descriptor 'sendFile'. This means if polar_flashback_log_read is
 * used once, there will always be one descriptor left open until
 * the process ends, but never more than one.
 *
 */
void
polar_flog_read(char *buf, int segsize, polar_flog_rec_ptr startptr,
				Size count, const char *dir)
{
	char       *p;
	polar_flog_rec_ptr  recptr;
	Size        nbytes;
	/* state maintained across calls */
	static int  open_file = -1;
	static uint64 open_segno = 0;

	Assert(segsize == POLAR_FLOG_SEG_SIZE);

	p = buf;
	recptr = startptr;
	nbytes = count;

	while (nbytes > 0)
	{
		uint32      startoff;
		int         segbytes;
		int         readbytes;

		startoff = FLOG_SEGMENT_OFFSET(recptr, segsize);

		/* Do we need to switch to a different xlog segment? */
		if (open_file < 0 || !ptr_in_flog_seg(recptr, open_segno, segsize))
		{
			char        path[MAXPGPATH];

			if (open_file >= 0)
				close(open_file);

			open_segno = flog_ptr_to_seg(recptr, segsize);

			flog_file_path_from_seg(path, dir, open_segno, segsize);

			open_file = BasicOpenFile(path, O_RDWR | PG_BINARY, true);

			if (open_file < 0)
			{
				/*no cover begin*/
				if (errno == ENOENT)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("requested flashback log segment %s has already been removed",
									path)));
				else
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not open file \"%s\": %m",
									path)));

				/*no cover end*/
			}
		}

		/* How many bytes are within this segment? */
		if (nbytes > (segsize - startoff))
			segbytes = segsize - startoff;
		else
			segbytes = nbytes;

		pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_LOG_READ);
		readbytes = polar_pread(open_file, p, segbytes, startoff);
		pgstat_report_wait_end();

		if (readbytes <= 0)
		{
			char        path[MAXPGPATH];
			int         save_errno = errno;

			flog_file_path_from_seg(path, dir, open_segno, segsize);
			errno = save_errno;
			/*no cover line*/
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from log segment %s, offset %u, length %lu: %m",
							path, startoff, (unsigned long) segbytes)));
		}

		/* Update state for read */
		recptr += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}
}

/*
 * Converts a "usable byte position" to polar_flog_rec_ptr. A usable byte position
 * is the position starting from the beginning of flashback log, excluding all flashback log
 * page headers.
 */
polar_flog_rec_ptr
polar_flog_pos2ptr(uint64 bytepos)
{
	uint64      fullsegs;
	uint64      fullpages;
	uint64      bytesleft;
	uint32      seg_offset;
	polar_flog_rec_ptr  result;

	fullsegs = bytepos / USABLE_BYTES_IN_SEG;
	bytesleft = bytepos % USABLE_BYTES_IN_SEG;

	if (bytesleft < POLAR_FLOG_BLCKSZ - FLOG_LONG_PHD_SIZE)
	{
		/* fits on first page of segment */
		seg_offset = bytesleft + FLOG_LONG_PHD_SIZE;
	}
	else
	{
		/* account for the first page on segment with long header */
		seg_offset = POLAR_FLOG_BLCKSZ;
		bytesleft -= POLAR_FLOG_BLCKSZ - FLOG_LONG_PHD_SIZE;

		fullpages = bytesleft / USABLE_BYTES_IN_PAGE;
		bytesleft = bytesleft % USABLE_BYTES_IN_PAGE;

		seg_offset += fullpages * POLAR_FLOG_BLCKSZ + bytesleft + FLOG_SHORT_PHD_SIZE;
	}

	flog_seg_offset_to_ptr(fullsegs, seg_offset, POLAR_FLOG_SEG_SIZE, result);
	return result;
}

/*
 * Like polar_flog_pos2ptr, but if the position is at a page boundary,
 * returns a pointer to the beginning of the page (ie. before page header),
 * not to where the first flashbck log record on that page would go to. This is used
 * when converting a pointer to the end of a record.
 */
polar_flog_rec_ptr
polar_flog_pos2endptr(uint64 bytepos)
{
	uint64      fullsegs;
	uint64      fullpages;
	uint64      bytesleft;
	uint32      seg_offset;
	polar_flog_rec_ptr  result;

	fullsegs = bytepos / USABLE_BYTES_IN_SEG;
	bytesleft = bytepos % USABLE_BYTES_IN_SEG;

	if (bytesleft < POLAR_FLOG_BLCKSZ - FLOG_LONG_PHD_SIZE)
	{
		/* fits on first page of segment */
		if (bytesleft == 0)
			seg_offset = 0;
		else
			seg_offset = bytesleft + FLOG_LONG_PHD_SIZE;
	}
	else
	{
		/* account for the first page on segment with long header */
		seg_offset = POLAR_FLOG_BLCKSZ;
		bytesleft -= POLAR_FLOG_BLCKSZ - FLOG_LONG_PHD_SIZE;

		fullpages = bytesleft / USABLE_BYTES_IN_PAGE;
		bytesleft = bytesleft % USABLE_BYTES_IN_PAGE;

		if (bytesleft == 0)
			seg_offset += fullpages * POLAR_FLOG_BLCKSZ + bytesleft;
		else
			seg_offset += fullpages * POLAR_FLOG_BLCKSZ + bytesleft + FLOG_SHORT_PHD_SIZE;
	}

	flog_seg_offset_to_ptr(fullsegs, seg_offset, POLAR_FLOG_SEG_SIZE, result);
	return result;
}

/*
 * Convert an flashback log ptr to a "usable byte position".
 */
uint64
polar_flog_ptr2pos(polar_flog_rec_ptr ptr)
{
	uint64      fullsegs;
	uint32      fullpages;
	uint32      offset;
	uint64      result;

	fullsegs = flog_ptr_to_seg(ptr, POLAR_FLOG_SEG_SIZE);

	fullpages = (FLOG_SEGMENT_OFFSET(ptr, POLAR_FLOG_SEG_SIZE)) / POLAR_FLOG_BLCKSZ;
	offset = ptr % POLAR_FLOG_BLCKSZ;

	if (fullpages == 0)
	{
		result = fullsegs * USABLE_BYTES_IN_SEG;

		if (offset > 0)
		{
			Assert(offset >= FLOG_LONG_PHD_SIZE);
			result += offset - FLOG_LONG_PHD_SIZE;
		}
	}
	else
	{
		result = fullsegs * USABLE_BYTES_IN_SEG +
				 (POLAR_FLOG_BLCKSZ - FLOG_LONG_PHD_SIZE) + /* account for first page */
				 (fullpages - 1) * USABLE_BYTES_IN_PAGE;    /* full pages */

		if (offset > 0)
		{
			Assert(offset >= FLOG_SHORT_PHD_SIZE);
			result += offset - FLOG_SHORT_PHD_SIZE;
		}
	}

	return result;
}

/*
 * Read flashback log control file and return false when there is
 * no control file data. otherwise, return true.
 *
 * NB: We don't mark the control file non-exist a error but waring and return
 * false.
 */
bool
polar_read_flog_ctl_file(flog_buf_ctl_t ctl, flog_ctl_file_data_t *ctl_file_data)
{
	char ctl_file_path[MAXPGPATH];
	int fd;
	int read_len;
	pg_crc32c   crc;
	int rc;

	flog_file_full_path(ctl, ctl_file_path, FLOG_CTL_FILE);

	/* The control file may be non-exist */
	if (!is_file_exist(ctl_file_path))
	{
		elog(WARNING, "Can't find %s", ctl_file_path);
		return false;
	}

	fd = BasicOpenFile(ctl_file_path, O_RDWR | PG_BINARY, true);

	if (fd < 0)
		/*no cover line*/
		ereport(FATAL,
				(errcode_for_file_access(),
				 (errmsg("could not open file \"%s\": %m", ctl_file_path))));

	/* Read data */
	pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_LOG_CTL_FILE_READ);
	read_len = pg_pread(fd, ctl_file_data, sizeof(flog_ctl_file_data_t), 0);
	pgstat_report_wait_end();

	if (read_len != sizeof(flog_ctl_file_data_t))
		/*no cover line*/
		ereport(FATAL,
				(errcode_for_file_access(),
				 (errmsg("could not read from file \"%s\": %m", ctl_file_path))));

	rc = close(fd);

	if (rc)
		/*no cover line*/
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", ctl_file_path)));

	/* Verify CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) ctl_file_data, offsetof(flog_ctl_file_data_t, crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(crc, ctl_file_data->crc))
		/*no cover line*/
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("calculated CRC checksum does not match value stored in file \"%s\"",
						ctl_file_path)));

	return true;
}

/*
 * POLAR: write flashback log control file by checkpoint.
 * Caller must hold the ctl_file_lock.
 */
void
polar_write_flog_ctl_file(flog_buf_ctl_t ctl)
{
	flog_ctl_file_data_t flashback_log_ctl_file;
	char            ctl_file_path[MAXPGPATH];
	int             fd;

	flashback_log_ctl_file.version_no = FLOG_CTL_FILE_VERSION;

	if (unlikely(ctl->buf_state == FLOG_BUF_SHUTDOWNED))
		flashback_log_ctl_file.version_no |= FLOG_SHUTDOWNED;

	flashback_log_ctl_file.fbpoint_info = ctl->fbpoint_info;
	flashback_log_ctl_file.max_seg_no = ctl->max_seg_no;
	flashback_log_ctl_file.tli = ctl->tli;

	INIT_CRC32C(flashback_log_ctl_file.crc);
	COMP_CRC32C(flashback_log_ctl_file.crc, &flashback_log_ctl_file,
				offsetof(flog_ctl_file_data_t, crc));
	FIN_CRC32C(flashback_log_ctl_file.crc);

	flog_file_full_path(ctl, ctl_file_path, FLOG_CTL_FILE);
	fd = BasicOpenFile(ctl_file_path, O_RDWR | O_CREAT | PG_BINARY, true);

	if (fd < 0)
	{
		/*no cover line*/
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						ctl_file_path)));
		return;
	}

	pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_LOG_CTL_FILE_WRITE);

	if (pg_pwrite(fd, &flashback_log_ctl_file, sizeof(flog_ctl_file_data_t), 0) != sizeof(flog_ctl_file_data_t))
		/*no cover line*/
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m",
						ctl_file_path)));

	pgstat_report_wait_end();

	pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_LOG_CTL_FILE_SYNC);

	if (pg_fsync(fd) != 0)
		/*no cover line*/
		ereport(PANIC,
				(errcode_for_file_access(),
				 (errmsg("could not sync file \"%s\": %m",
						 ctl_file_path))));

	pgstat_report_wait_end();

	if (close(fd))
		/*no cover line*/
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", ctl_file_path)));

	if (unlikely(polar_flashback_log_debug))
	{
		char		ckpttime_str[128];
		time_t		time_tmp;
		const char *strftime_fmt = "%c";

		time_tmp = (time_t) flashback_log_ctl_file.fbpoint_info.wal_info.fbpoint_time;
		strftime(ckpttime_str, sizeof(ckpttime_str), strftime_fmt,
				 localtime(&time_tmp));

		elog(LOG, "The flashback log control info now is: "
			 "next flashback log point in the last flashback point begining: %X/%X, "
			 "next flashback log point in the last flashback point end: %X/%X, "
			 "previous flashback log point of the last flashback point end: %X/%X, "
			 "max segment no: %lu, "
			 "current flashback point WAL lsn: %X/%X, "
			 "previous flashback point WAL lsn: %X/%X, "
			 "current flashback point time: %s",
			 (uint32)(flashback_log_ctl_file.fbpoint_info.flog_start_ptr >> 32),
			 (uint32) flashback_log_ctl_file.fbpoint_info.flog_start_ptr,
			 (uint32)(flashback_log_ctl_file.fbpoint_info.flog_end_ptr >> 32),
			 (uint32) flashback_log_ctl_file.fbpoint_info.flog_end_ptr,
			 (uint32)(flashback_log_ctl_file.fbpoint_info.flog_end_ptr_prev >> 32),
			 (uint32) flashback_log_ctl_file.fbpoint_info.flog_end_ptr_prev,
			 flashback_log_ctl_file.max_seg_no,
			 (uint32)(flashback_log_ctl_file.fbpoint_info.wal_info.fbpoint_lsn >> 32),
			 (uint32) flashback_log_ctl_file.fbpoint_info.wal_info.fbpoint_lsn,
			 (uint32)(flashback_log_ctl_file.fbpoint_info.wal_info.prior_fbpoint_lsn >> 32),
			 (uint32) flashback_log_ctl_file.fbpoint_info.wal_info.prior_fbpoint_lsn,
			 ckpttime_str);
	}
}

/*
 * POLAR: update the flashback log checkpoint information and write the flashback log
 * control file to disk.
 *
 * This is only called by checkpointer process.
 */
void
polar_flush_fbpoint_info(flog_buf_ctl_t ctl,
						  polar_flog_rec_ptr flashback_log_ptr_ckp_start,
						  polar_flog_rec_ptr flashback_log_ptr_ckp_end,
						  polar_flog_rec_ptr flashback_log_ptr_ckp_end_prev)
{
	LWLockAcquire(&ctl->ctl_file_lock, LW_EXCLUSIVE);
	ctl->fbpoint_info.flog_start_ptr = flashback_log_ptr_ckp_start;
	ctl->fbpoint_info.flog_end_ptr = flashback_log_ptr_ckp_end;
	ctl->fbpoint_info.flog_end_ptr_prev = flashback_log_ptr_ckp_end_prev;
	ctl->fbpoint_info.wal_info = ctl->wal_info;
	polar_write_flog_ctl_file(ctl);
	LWLockRelease(&ctl->ctl_file_lock);
}

void
polar_flush_flog_max_seg_no(flog_buf_ctl_t ctl, uint64 seg_no)
{
	uint64 max_seg_no = 0;

	LWLockAcquire(&ctl->ctl_file_lock, LW_EXCLUSIVE);
	max_seg_no = ctl->max_seg_no;

	if (max_seg_no == POLAR_INVALID_FLOG_SEGNO
			|| max_seg_no < seg_no)
	{
		ctl->max_seg_no = seg_no;
		polar_write_flog_ctl_file(ctl);
	}

	LWLockRelease(&ctl->ctl_file_lock);
}

/* POLAR: Validate the flashback log dir */
//updated
void
polar_validate_flog_dir(flog_buf_ctl_t ctl)
{
	char path[MAXPGPATH];

	flog_dir_full_path(ctl, path);
//	polar_validate_dir(path);
}

/*
 * POLAR: Open a pre-existing logfile segment for writing.
 */
int
polar_flog_file_open(uint64 segno, const char *dir)
{
	char        path[MAXPGPATH];
	int         fd;

	flog_file_path_from_seg(path, dir, segno, POLAR_FLOG_SEG_SIZE);

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY, true);

	if (fd < 0)
		/*no cover line*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open flashback log file \"%s\": %m", path)));

	return fd;
}

/*
 * POLAR: Create a new flashback log file segment, or open a pre-existing one.
 *
 * logsegno: identify segment to be created/opened.
 *
 * *use_existent: if true, OK to use a pre-existing file (else, any
 * pre-existing file will be deleted).  On return, true if a pre-existing
 * file was used.
 *
 * Returns FD of opened file.
 *
 * Note: errors here are ERROR not PANIC because we might or might not be
 * inside a critical section (eg, during checkpoint there is no reason to
 * take down the system on failure).  They will promote to PANIC if we are
 * in a critical section.
 */
int
polar_flog_file_init(flog_buf_ctl_t ctl, uint64 logsegno, bool *use_existent)
{
	char        path[MAXPGPATH];
	char        tmppath[MAXPGPATH];
	uint64  installed_segno;
	uint64  max_segno;
	int         fd;
	char        tmpfile_name[FLOG_MAX_FNAME_LEN];

	flog_file_path_from_seg(path, polar_get_flog_dir(ctl), logsegno, POLAR_FLOG_SEG_SIZE);

	/*
	 * Try to use existent file (polar worker may have created it already)
	 */
	if (*use_existent)
	{
		fd = BasicOpenFile(path, O_RDWR | PG_BINARY, true);

		if (fd < 0)
		{
			/*no cover begin*/
			if (errno != ENOENT)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m", path)));

			/*no cover end*/
		}
		else
			return fd;
	}

	/*
	 * Initialize an empty (all zeroes) segment.  NOTE: it is possible that
	 * another process is doing the same thing.  If so, we will end up
	 * pre-creating an extra log segment.  That seems OK, and better than
	 * holding the lock throughout this lengthy process.
	 */
	elog(DEBUG2, "creating and filling new flashback log file");

	snprintf(tmpfile_name, FLOG_MAX_FNAME_LEN, FLOG_TMP_FNAME "%d", (int) getpid());
	flog_file_full_path(ctl, tmppath, tmpfile_name);
	unlink(tmppath);

	fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, true);

	if (fd < 0)
		/*no cover line*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmppath)));

	/*
	 * Zero-fill the file.  We have to do this the hard way to ensure that all
	 * the file space has really been allocated --- on platforms that allow
	 * "holes" in files, just seeking to the end doesn't allocate intermediate
	 * space.  This way, we know that we have all the space and (after the
	 * fsync below) that all the indirect blocks are down on disk.  Therefore,
	 * fdatasync(2) or O_DSYNC will be sufficient to sync future writes to the
	 * log file.
	 */

	polar_fill_segment_file_zero(fd, tmppath, POLAR_FLOG_SEG_SIZE,
								 WAIT_EVENT_FLASHBACK_LOG_INIT_WRITE, WAIT_EVENT_FLASHBACK_LOG_INIT_SYNC, "flashback log");

	if (close(fd))
		/*no cover line*/
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
	installed_segno = logsegno;

	max_segno = logsegno + polar_flashback_log_keep_segments;

	if (!install_flog_seg(ctl, &installed_segno, tmppath,
						  *use_existent, max_segno))
	{
		/*
		 * No need for any more future segments, or polar_install_flashback_log_seg()
		 * failed to rename the file into place. If the rename failed, opening
		 * the file below will fail.
		 */
		/*no cover line*/
		unlink(tmppath);
	}

	/* Set flag to tell caller there was no existent file */
	*use_existent = false;

	/* Now open original target segment (might not be file I just made) */
	fd = BasicOpenFile(path, O_RDWR | PG_BINARY, true);

	if (fd < 0)
		/*no cover line*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	if (unlikely(polar_flashback_log_debug))
		elog(LOG, "done creating and filling new flashback log file %s", path);

	return fd;
}

/*
 * POLAR: Preallocate flashback log files beyond last endpoint.
 */
void
polar_prealloc_flog_files(flog_buf_ctl_t ctl, int num)
{
	polar_flog_rec_ptr  request_ptr = polar_get_flog_write_request(ctl);
	uint64   _log_seg_no;
	int         lf;
	bool        use_existent;
	int     count = 0;

	_log_seg_no = flog_ptr_to_seg(request_ptr, POLAR_FLOG_SEG_SIZE);

	while (count < num)
	{
		use_existent = true;
		lf = polar_flog_file_init(ctl, _log_seg_no, &use_existent);
		close(lf);
		_log_seg_no++;
		count++;
	}
}

/* POLAR: Check the flashback log of the segment no exists */
bool
polar_is_flog_file_exist(const char *dir, polar_flog_rec_ptr ptr, int elevel)
{
	char        path[MAXPGPATH];
	uint64      segno;

	segno = flog_ptr_to_seg(ptr, POLAR_FLOG_SEG_SIZE);
	flog_file_path_from_seg(path, dir, segno, POLAR_FLOG_SEG_SIZE);

	if (is_file_exist(path))
		return true;

	/*no cover begin*/
	ereport(elevel, (errcode_for_file_access(),
					 errmsg("The flashback log file %s is not found", path)));
	return false;
	/*no cover end*/
}

void
polar_flog_clean_dir_internal(const char *dir_path)
{
	struct stat st;

	if (stat(dir_path, &st) == 0 && S_ISDIR(st.st_mode))
	{
		rmtree(dir_path, true);
		elog(LOG, "Remove the directory %s while the polar_enable_flashback_log is off.", dir_path);
	}
}

/* POLAR: Remove all the flashback log files and keep the flashback log dir. */
void
polar_flog_remove_all(flog_buf_ctl_t ctl)
{
	char        path[MAXPGPATH];

	flog_dir_full_path(ctl, path);
	polar_flog_clean_dir_internal(path);
}

/* POLAR: Write a new line contain flashback log switch pointer info to the history file */
void
polar_write_flog_history_file(const char *dir, TimeLineID tli, polar_flog_rec_ptr switch_ptr,
							  polar_flog_rec_ptr next_ptr)
{
#define MAX_INFO_LEN 64

	char        path[MAXPGPATH];
	char        tmp_path[MAXPGPATH];
	int         fd;
	int         tmp_fd;
	bool        found = false;
	char        ptr_info[MAX_INFO_LEN];
	int         nbytes;
	char        buffer[BLCKSZ];

	polar_make_file_path_level3(path, dir, FLOG_HISTORY_FILE);
	polar_make_file_path_level3(tmp_path, dir, FLOG_TMP_HISTORY_FILE);
	unlink(tmp_path);

	/* Create new tmp history file */
	tmp_fd = OpenTransientFile(tmp_path, O_RDWR | O_CREAT | O_TRUNC);

	if (tmp_fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmp_path)));

	if (is_file_exist(path))
	{
		fd = BasicOpenFile(path, O_RDONLY);

		if (fd < 0)
			/*no cover line*/
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m",
							path)));

		for (;;)
		{
			errno = 0;
			pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_LOG_HISTORY_FILE_READ);
			nbytes = (int) pg_pread(fd, buffer, sizeof(buffer));
			pgstat_report_wait_end();

			if (nbytes < 0 || errno != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m", path)));

			if (nbytes == 0)
				break;

			errno = 0;
			pgstat_report_wait_start(WAIT_EVENT_TIMELINE_HISTORY_WRITE);

			if ((int) pg_pwrite(tmp_fd, buffer, nbytes, 0) != nbytes)
			{
				int         save_errno = errno;

				/*
				 * If we fail to make the file, delete it to release disk
				 * space
				 */
				unlink(tmp_path);

				/*
				 * if write didn't set errno, assume problem is no disk space
				 */
				errno = save_errno ? save_errno : ENOSPC;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to file \"%s\": %m", tmp_path)));
			}

			pgstat_report_wait_end();
		}

		close(fd);
	}

	snprintf(ptr_info, sizeof(ptr_info),
			 "%s%u\t%08X/%08X\t%08X/%08X\n",
			 found ? "\n" : "", tli,
			 (uint32)(switch_ptr >> 32), (uint32)(switch_ptr),
			 (uint32)(next_ptr >> 32), (uint32)(next_ptr));
	nbytes = strlen(ptr_info);
	pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_LOG_HISTORY_FILE_WRITE);

	if (pg_pwrite(tmp_fd, ptr_info, nbytes, 0) != nbytes)
		/*no cover line*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m",
						tmp_path)));

	pgstat_report_wait_end();

	if (close(tmp_fd))
		/*no cover line*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));

	/*
	 * As the rename is atomic operation, if any problem occurs after this
	 * at worst it can lose the new switch ptr and next ptr.
	 */
	durable_rename(tmp_path, path, ERROR);
}

/*
 * POLAR: Try to read a flashback log history file.
 * Return the list of switch ptr flog_history_entry.
 */
List *
polar_read_flog_history_file(const char *dir)
{
	char        path[MAXPGPATH];
	char        fline[MAXPGPATH];
	polar_flog_rec_ptr switch_ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr last_ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr next_ptr = POLAR_INVALID_FLOG_REC_PTR;
	List *result = NIL;
	int history_file_fd;

	polar_make_file_path_level3(path, dir, FLOG_HISTORY_FILE);

	if (!is_file_exist(path))
		return result;

	history_file_fd = BasicOpenFile(path, O_RDWR, true);

	if (history_file_fd < 0)
		/*no cover line*/
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open file \"%s\": %m", path)));

	pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_LOG_HISTORY_FILE_READ);

	while (polar_read_line(history_file_fd, fline, sizeof(fline)) > 0)
	{
		uint32      switchpoint_hi;
		uint32      switchpoint_lo;
		uint32      nextpoint_hi;
		uint32      nextpoint_lo;
		TimeLineID  tli;
		int         nfields;
		char       *ptr;
		flog_history_entry *entry;

		/* skip leading whitespace and check for # comment */
		for (ptr = fline; *ptr; ptr++)
		{
			if (!isspace((unsigned char) *ptr))
				break;
		}

		if (*ptr == '\0' || *ptr == '#')
			continue;

		nfields = sscanf(fline, "%u\t%08X/%08X\t%08X/%08X", &tli, &switchpoint_hi, &switchpoint_lo,
						 &nextpoint_hi, &nextpoint_lo);

		if (nfields != HISTORY_FILE_FIELDS)
		{
			/*no cover line*/
			ereport(FATAL,
					(errmsg("syntax error in flashback log history file: %s", fline),
					 errhint("Expected a flashback log switch point and next point location.")));
		}

		switch_ptr = ((polar_flog_rec_ptr)(switchpoint_hi)) << 32 |
					 (polar_flog_rec_ptr) switchpoint_lo;
		next_ptr = ((polar_flog_rec_ptr)(nextpoint_hi)) << 32 |
				   (polar_flog_rec_ptr) nextpoint_lo;

		if (FLOG_REC_PTR_IS_INVAILD(next_ptr) ||
				switch_ptr > next_ptr)
			/*no cover line*/
			ereport(FATAL, (errmsg("invalid data in flashback log history file: %s", fline),
							errhint("switch point and next point must be valid and next point"
									" is larger than or equal to switch point.")));

		if (last_ptr && last_ptr > switch_ptr)
			/*no cover line*/
			ereport(FATAL,
					(errmsg("invalid data in flashback log history file: %s", fline),
					 errhint("switch point location must be in increasing sequence.")));

		entry = (flog_history_entry *) palloc(sizeof(flog_history_entry));
		entry->tli = tli;
		entry->switch_ptr = switch_ptr;
		entry->next_ptr = next_ptr;
		/* Build list with newest item first */
		result = lcons(entry, result);
		last_ptr = switch_ptr;
		/* we ignore the remainder of each line */
	}

	pgstat_report_wait_end();

	return result;
}

/* Make the page ptr to first record ptr in flashback log */
polar_flog_rec_ptr
convert_to_first_valid_ptr(polar_flog_rec_ptr page_ptr)
{
	polar_flog_rec_ptr valid_ptr = page_ptr;

	if (valid_ptr % POLAR_FLOG_SEG_SIZE == 0)
		valid_ptr += FLOG_LONG_PHD_SIZE;
	else if (valid_ptr % POLAR_FLOG_BLCKSZ == 0)
		valid_ptr += FLOG_SHORT_PHD_SIZE;

	return valid_ptr;
}

/* POLAR: Get the next valid flashback log pointer */
polar_flog_rec_ptr
polar_get_next_flog_ptr(polar_flog_rec_ptr ptr, uint32 log_len)
{
	polar_flog_rec_ptr next_ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr pos;

	Assert(log_len != 0);
	pos = polar_flog_ptr2pos(ptr) + MAXALIGN(log_len);
	next_ptr = polar_flog_pos2ptr(pos);

	if (unlikely(polar_flashback_log_debug))
	{
		elog(LOG, "The flashback log point after %X/%X is %X/%X",
			 (uint32)(ptr >> 32), (uint32)(ptr),
			 (uint32)(next_ptr >> 32), (uint32)(next_ptr));
	}

	return next_ptr;
}

/* POLAR: Remove all the temporary flashback log in the startup */
void
polar_remove_tmp_flog_file(const char *dir)
{
//	char        path[MAXPGPATH];
//	DIR       *flashback_log_dir;
//	struct dirent *flashback_log_de;
//
//	polar_make_file_path_level2(path, dir);
//	flashback_log_dir = polar_allocate_dir(path);
//
//	/*no cover begin*/
//	if (flashback_log_dir == NULL)
//		return;
//
//	/*no cover end*/
//
//	while ((flashback_log_de = ReadDir(flashback_log_dir, path)) != NULL)
//	{
//		char        file_path[MAXPGPATH];
//
//		if (strncmp(flashback_log_de->d_name, FLOG_TMP_FNAME,
//					strlen(FLOG_TMP_FNAME)) == 0)
//		{
//			/*no cover begin*/
//			snprintf(file_path, MAXPGPATH, "%s/%s", path, flashback_log_de->d_name);
//			elog(LOG, "Remove the temporary flashback log %s", flashback_log_de->d_name);
//			durable_unlink(file_path, LOG);
//			/*no cover end*/
//		}
//	}
//
//	FreeDir(flashback_log_dir);
}
