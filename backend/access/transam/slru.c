/*-------------------------------------------------------------------------
 *
 * slru.c
 *		Simple LRU buffering for transaction status logfiles
 *
 * We use a simple least-recently-used scheme to manage a pool of page
 * buffers.  Under ordinary circumstances we expect that write
 * traffic will occur mostly to the latest page (and to the just-prior
 * page, soon after a page transition).  Read traffic will probably touch
 * a larger span of pages, but in any case a fairly small number of page
 * buffers should be sufficient.  So, we just search the buffers using plain
 * linear search; there's no need for a hashtable or anything fancy.
 * The management algorithm is straight LRU except that we will never swap
 * out the latest page (since we know it's going to be hit again eventually).
 *
 * We use a control LWLock to protect the shared data structures, plus
 * per-buffer LWLocks that synchronize I/O for each buffer.  The control lock
 * must be held to examine or modify any shared state.  A process that is
 * reading in or writing out a page buffer does not hold the control lock,
 * only the per-buffer lock for the buffer it is working on.
 *
 * "Holding the control lock" means exclusive lock in all cases except for
 * SimpleLruReadPage_ReadOnly(); see comments for SlruRecentlyUsed() for
 * the implications of that.
 *
 * When initiating I/O on a buffer, we acquire the per-buffer lock exclusively
 * before releasing the control lock.  The per-buffer lock is released after
 * completing the I/O, re-acquiring the control lock, and updating the shared
 * state.  (Deadlock is not possible here, because we never try to initiate
 * I/O when someone else is already doing I/O on the same buffer.)
 * To wait for I/O to complete, release the control lock, acquire the
 * per-buffer lock in shared mode, immediately release the per-buffer lock,
 * reacquire the control lock, and then recheck state (since arbitrary things
 * could have happened while we didn't have the lock).
 *
 * As with the regular buffer manager, it is possible for another process
 * to re-dirty a page that is currently being written out.  This is handled
 * by re-setting the page's page_dirty flag.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/slru.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/slru.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#define SlruFileName(ctl, path, seg) \
	snprintf(path, MAXPGPATH, "%s/%04X", (ctl)->Dir, seg)

/*
 * During SimpleLruFlush(), we will usually not need to write/fsync more
 * than one or two physical files, but we may need to write several pages
 * per file.  We can consolidate the I/O requests by leaving files open
 * until control returns to SimpleLruFlush().  This data structure remembers
 * which files are open.
 */
#define MAX_FLUSH_BUFFERS	16

typedef struct SlruFlushData
{
	int			num_files;		/* # files actually open */
	int			fd[MAX_FLUSH_BUFFERS];	/* their FD's */
	int			segno[MAX_FLUSH_BUFFERS];	/* their log seg#s */
} SlruFlushData;

typedef struct SlruFlushData *SlruFlush;

/*
 * Macro to mark a buffer slot "most recently used".  Note multiple evaluation
 * of arguments!
 *
 * The reason for the if-test is that there are often many consecutive
 * accesses to the same page (particularly the latest page).  By suppressing
 * useless increments of cur_lru_count, we reduce the probability that old
 * pages' counts will "wrap around" and make them appear recently used.
 *
 * We allow this code to be executed concurrently by multiple processes within
 * SimpleLruReadPage_ReadOnly().  As long as int reads and writes are atomic,
 * this should not cause any completely-bogus values to enter the computation.
 * However, it is possible for either cur_lru_count or individual
 * page_lru_count entries to be "reset" to lower values than they should have,
 * in case a process is delayed while it executes this macro.  With care in
 * SlruSelectLRUPage(), this does little harm, and in any case the absolute
 * worst possible consequence is a nonoptimal choice of page to evict.  The
 * gain from allowing concurrent reads of SLRU pages seems worth it.
 */
#define SlruRecentlyUsed(shared, slotno)	\
	do { \
		int		new_lru_count = (shared)->cur_lru_count; \
		if (new_lru_count != (shared)->page_lru_count[slotno]) { \
			(shared)->cur_lru_count = ++new_lru_count; \
			(shared)->page_lru_count[slotno] = new_lru_count; \
		} \
	} while (0)

/* Saved info for SlruReportIOError */
typedef enum
{
	SLRU_OPEN_FAILED,
	SLRU_SEEK_FAILED,
	SLRU_READ_FAILED,
	SLRU_WRITE_FAILED,
	SLRU_FSYNC_FAILED,
	SLRU_CLOSE_FAILED
} SlruErrorCause;

static SlruErrorCause slru_errcause;
static int	slru_errno;

int n_polar_slru_stats = 0;
const polar_slru_stat *polar_slru_stats[POLAR_SLRU_STATS_NUM];


static void SimpleLruZeroLSNs(SlruCtl ctl, int slotno);
static void SimpleLruWaitIO(SlruCtl ctl, int slotno);
static void SlruInternalWritePage(SlruCtl ctl, int slotno, SlruFlush fdata);
static bool SlruPhysicalReadPage(SlruCtl ctl, int pageno, int slotno);
static bool SlruPhysicalWritePage(SlruCtl ctl, int pageno, int slotno,
								  SlruFlush fdata);
static void SlruReportIOError(SlruCtl ctl, int pageno, TransactionId xid);
static int	SlruSelectLRUPage(SlruCtl ctl, int pageno);

static bool SlruScanDirCbDeleteCutoff(SlruCtl ctl, char *filename,
									  int segpage, void *data);
static void SlruInternalDeleteSegment(SlruCtl ctl, char *filename);

/* POLAR */
static void polar_slru_file_name_by_seg(SlruCtl ctl, char *path, int seg);
static void polar_slru_file_name_by_name(SlruCtl ctl, char *path, char *filename);
static void polar_slru_file_dir(SlruCtl ctl, char *path);
static bool polar_slru_local_cache_read_page(SlruCtl ctl, int pageno, int slotno);
static bool polar_slru_local_cache_write_page(SlruCtl ctl, int pageno, int slotno);

#define SlruFileName(a,b,c)			polar_slru_file_name_by_seg(a,b,c)

/* POLAR: hash table */
#define SlruHashSlots(nslots)       (nslots * 2)
#define VICTIM_WINDOW				128      /* victim slot window */
static inline void polar_slru_set_page_status(SlruShared shared, int slotno, SlruPageStatus pagestatus);
static void polar_slru_hash_init(SlruShared shared, int nslots, const char *name);
/*
 * Initialization of shared memory
 */

Size
SimpleLruShmemSize(int nslots, int nlsns)
{
	Size		sz;

	/* we assume nslots isn't so large as to risk overflow */
	sz = MAXALIGN(sizeof(SlruSharedData));
	sz += MAXALIGN(nslots * sizeof(char *));	/* page_buffer[] */
	sz += MAXALIGN(nslots * sizeof(SlruPageStatus));	/* page_status[] */
	sz += MAXALIGN(nslots * sizeof(bool));	/* page_dirty[] */
	sz += MAXALIGN(nslots * sizeof(int));	/* page_number[] */
	sz += MAXALIGN(nslots * sizeof(int));	/* page_lru_count[] */
	sz += MAXALIGN(nslots * sizeof(LWLockPadded));	/* buffer_locks[] */

	if (nlsns > 0)
		sz += MAXALIGN(nslots * nlsns * sizeof(XLogRecPtr));	/* group_lsn[] */

	return BUFFERALIGN(sz) + BLCKSZ * nslots;
}

/*
 * Initialize, or attach to, a simple LRU cache in shared memory.
 *
 * ctl: address of local (unshared) control structure.
 * name: name of SLRU.  (This is user-visible, pick with care!)
 * nslots: number of page slots to use.
 * nlsns: number of LSN groups per page (set to zero if not relevant).
 * ctllock: LWLock to use to control access to the shared control structure.
 * subdir: PGDATA-relative subdirectory that will contain the files.
 * tranche_id: LWLock tranche ID to use for the SLRU's per-buffer LWLocks.
 */
void
SimpleLruInit(SlruCtl ctl, const char *name, int nslots, int nlsns,
			  LWLock *ctllock, const char *subdir, int tranche_id)
{
	SlruShared	shared;
	bool		found;

	shared = (SlruShared) ShmemInitStruct(name,
										  SimpleLruShmemSize(nslots, nlsns),
										  &found);

	if (!IsUnderPostmaster)
	{
		/* Initialize locks and shared memory area */
		char	   *ptr;
		Size		offset;
		int			slotno;

		Assert(!found);

		memset(shared, 0, sizeof(SlruSharedData));

		shared->ControlLock = ctllock;

		shared->num_slots = nslots;
		shared->lsn_groups_per_page = nlsns;

		shared->cur_lru_count = 0;

		/* shared->latest_page_number will be set later */

		shared->slru_stats_idx = pgstat_slru_index(name);

		ptr = (char *) shared;
		offset = MAXALIGN(sizeof(SlruSharedData));
		shared->page_buffer = (char **) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(char *));
		shared->page_status = (SlruPageStatus *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(SlruPageStatus));
		shared->page_dirty = (bool *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(bool));
		shared->page_number = (int *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(int));
		shared->page_lru_count = (int *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(int));

		/* Initialize LWLocks */
		shared->buffer_locks = (LWLockPadded *) (ptr + offset);
		offset += MAXALIGN(nslots * sizeof(LWLockPadded));

		if (nlsns > 0)
		{
			shared->group_lsn = (XLogRecPtr *) (ptr + offset);
			offset += MAXALIGN(nslots * nlsns * sizeof(XLogRecPtr));
		}

		ptr += BUFFERALIGN(offset);
		for (slotno = 0; slotno < nslots; slotno++)
		{
			LWLockInitialize(&shared->buffer_locks[slotno].lock,
							 tranche_id);

			shared->page_buffer[slotno] = ptr;
			shared->page_status[slotno] = SLRU_PAGE_EMPTY;
			shared->page_dirty[slotno] = false;
			shared->page_lru_count[slotno] = 0;
			ptr += BLCKSZ;
		}

		/* Should fit to estimated shmem size */
		Assert(ptr - (char *) shared <= SimpleLruShmemSize(nslots, nlsns));
	}
	else
		Assert(found);

	/*
	 * Initialize the unshared control struct, including directory path. We
	 * assume caller set PagePrecedes.
	 */
	ctl->shared = shared;
	ctl->do_fsync = true;		/* default behavior */
	StrNCpy(ctl->Dir, subdir, sizeof(ctl->Dir));
}

/*
 * Initialize (or reinitialize) a page to zeroes.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
int
SimpleLruZeroPage(SlruCtl ctl, int pageno)
{
	SlruShared	shared = ctl->shared;
	int			slotno;

	/* Find a suitable buffer slot for the page */
	slotno = SlruSelectLRUPage(ctl, pageno);
	Assert(shared->page_status[slotno] == SLRU_PAGE_EMPTY ||
		   (shared->page_status[slotno] == SLRU_PAGE_VALID &&
			!shared->page_dirty[slotno]) ||
		   shared->page_number[slotno] == pageno);

	/* Mark the slot as containing this page */
	shared->page_number[slotno] = pageno;
	shared->page_status[slotno] = SLRU_PAGE_VALID;
	shared->page_dirty[slotno] = true;
	SlruRecentlyUsed(shared, slotno);

	/* Set the buffer to zeroes */
	MemSet(shared->page_buffer[slotno], 0, BLCKSZ);

	/* Set the LSNs for this new page to zero */
	SimpleLruZeroLSNs(ctl, slotno);

	/* Assume this page is now the latest active page */
	shared->latest_page_number = pageno;

	/* update the stats counter of zeroed pages */
	pgstat_count_slru_page_zeroed(shared->slru_stats_idx);

	return slotno;
}

/*
 * Zero all the LSNs we store for this slru page.
 *
 * This should be called each time we create a new page, and each time we read
 * in a page from disk into an existing buffer.  (Such an old page cannot
 * have any interesting LSNs, since we'd have flushed them before writing
 * the page in the first place.)
 *
 * This assumes that InvalidXLogRecPtr is bitwise-all-0.
 */
static void
SimpleLruZeroLSNs(SlruCtl ctl, int slotno)
{
	SlruShared	shared = ctl->shared;

	if (shared->lsn_groups_per_page > 0)
		MemSet(&shared->group_lsn[slotno * shared->lsn_groups_per_page], 0,
			   shared->lsn_groups_per_page * sizeof(XLogRecPtr));
}

/*
 * Wait for any active I/O on a page slot to finish.  (This does not
 * guarantee that new I/O hasn't been started before we return, though.
 * In fact the slot might not even contain the same page anymore.)
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static void
SimpleLruWaitIO(SlruCtl ctl, int slotno)
{
	SlruShared	shared = ctl->shared;

	/* See notes at top of file */
	LWLockRelease(shared->ControlLock);
	LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_SHARED);
	LWLockRelease(&shared->buffer_locks[slotno].lock);
	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);

	/*
	 * If the slot is still in an io-in-progress state, then either someone
	 * already started a new I/O on the slot, or a previous I/O failed and
	 * neglected to reset the page state.  That shouldn't happen, really, but
	 * it seems worth a few extra cycles to check and recover from it. We can
	 * cheaply test for failure by seeing if the buffer lock is still held (we
	 * assume that transaction abort would release the lock).
	 */
	if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
		shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS)
	{
		if (LWLockConditionalAcquire(&shared->buffer_locks[slotno].lock, LW_SHARED))
		{
			/* indeed, the I/O must have failed */
			if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS)
				shared->page_status[slotno] = SLRU_PAGE_EMPTY;
			else				/* write_in_progress */
			{
				shared->page_status[slotno] = SLRU_PAGE_VALID;
				shared->page_dirty[slotno] = true;
			}
			LWLockRelease(&shared->buffer_locks[slotno].lock);
		}
	}
}

/*
 * Find a page in a shared buffer, reading it in if necessary.
 * The page number must correspond to an already-initialized page.
 *
 * If write_ok is true then it is OK to return a page that is in
 * WRITE_IN_PROGRESS state; it is the caller's responsibility to be sure
 * that modification of the page is safe.  If write_ok is false then we
 * will not return the page until it is not undergoing active I/O.
 *
 * The passed-in xid is used only for error reporting, and may be
 * InvalidTransactionId if no specific xid is associated with the action.
 *
 * Return value is the shared-buffer slot number now holding the page.
 * The buffer's LRU access info is updated.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
int
SimpleLruReadPage(SlruCtl ctl, int pageno, bool write_ok,
				  TransactionId xid)
{
	SlruShared	shared = ctl->shared;

	/* Outer loop handles restart if we must wait for someone else's I/O */
	for (;;)
	{
		int			slotno;
		bool		ok;

		/* See if page already is in memory; if not, pick victim slot */
		slotno = SlruSelectLRUPage(ctl, pageno);

		/* Did we find the page in memory? */
		if (shared->page_number[slotno] == pageno &&
			shared->page_status[slotno] != SLRU_PAGE_EMPTY)
		{
			/*
			 * If page is still being read in, we must wait for I/O.  Likewise
			 * if the page is being written and the caller said that's not OK.
			 */
			if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
				(shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS &&
				 !write_ok))
			{
				SimpleLruWaitIO(ctl, slotno);
				/* Now we must recheck state from the top */
				continue;
			}
			/* Otherwise, it's ready to use */
			SlruRecentlyUsed(shared, slotno);

			/* update the stats counter of pages found in the SLRU */
			pgstat_count_slru_page_hit(shared->slru_stats_idx);

			return slotno;
		}

		/* We found no match; assert we selected a freeable slot */
		Assert(shared->page_status[slotno] == SLRU_PAGE_EMPTY ||
			   (shared->page_status[slotno] == SLRU_PAGE_VALID &&
				!shared->page_dirty[slotno]));

		/* Mark the slot read-busy */
		shared->page_number[slotno] = pageno;
		shared->page_status[slotno] = SLRU_PAGE_READ_IN_PROGRESS;
		shared->page_dirty[slotno] = false;

		/* Acquire per-buffer lock (cannot deadlock, see notes at top) */
		LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_EXCLUSIVE);

		/* Release control lock while doing I/O */
		LWLockRelease(shared->ControlLock);

		/* Do the read */
		ok = SlruPhysicalReadPage(ctl, pageno, slotno);

		/* Set the LSNs for this newly read-in page to zero */
		SimpleLruZeroLSNs(ctl, slotno);

		/* Re-acquire control lock and update page state */
		LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);

		Assert(shared->page_number[slotno] == pageno &&
			   shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS &&
			   !shared->page_dirty[slotno]);

		shared->page_status[slotno] = ok ? SLRU_PAGE_VALID : SLRU_PAGE_EMPTY;

		LWLockRelease(&shared->buffer_locks[slotno].lock);

		/* Now it's okay to ereport if we failed */
		if (!ok)
			SlruReportIOError(ctl, pageno, xid);

		SlruRecentlyUsed(shared, slotno);

		/* update the stats counter of pages not found in SLRU */
		pgstat_count_slru_page_read(shared->slru_stats_idx);

		return slotno;
	}
}

/*
 * Find a page in a shared buffer, reading it in if necessary.
 * The page number must correspond to an already-initialized page.
 * The caller must intend only read-only access to the page.
 *
 * The passed-in xid is used only for error reporting, and may be
 * InvalidTransactionId if no specific xid is associated with the action.
 *
 * Return value is the shared-buffer slot number now holding the page.
 * The buffer's LRU access info is updated.
 *
 * Control lock must NOT be held at entry, but will be held at exit.
 * It is unspecified whether the lock will be shared or exclusive.
 */
int
SimpleLruReadPage_ReadOnly(SlruCtl ctl, int pageno, TransactionId xid)
{
	SlruShared	shared = ctl->shared;
	int			slotno;

	/* Try to find the page while holding only shared lock */
	LWLockAcquire(shared->ControlLock, LW_SHARED);

	/* See if page is already in a buffer */
	for (slotno = 0; slotno < shared->num_slots; slotno++)
	{
		if (shared->page_number[slotno] == pageno &&
			shared->page_status[slotno] != SLRU_PAGE_EMPTY &&
			shared->page_status[slotno] != SLRU_PAGE_READ_IN_PROGRESS)
		{
			/* See comments for SlruRecentlyUsed macro */
			SlruRecentlyUsed(shared, slotno);

			/* update the stats counter of pages found in the SLRU */
			pgstat_count_slru_page_hit(shared->slru_stats_idx);

			return slotno;
		}
	}

	/* No luck, so switch to normal exclusive lock and do regular read */
	LWLockRelease(shared->ControlLock);
	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);

	return SimpleLruReadPage(ctl, pageno, true, xid);
}

/*
 * Write a page from a shared buffer, if necessary.
 * Does nothing if the specified slot is not dirty.
 *
 * NOTE: only one write attempt is made here.  Hence, it is possible that
 * the page is still dirty at exit (if someone else re-dirtied it during
 * the write).  However, we *do* attempt a fresh write even if the page
 * is already being written; this is for checkpoints.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static void
SlruInternalWritePage(SlruCtl ctl, int slotno, SlruFlush fdata)
{
	SlruShared	shared = ctl->shared;
	int			pageno = shared->page_number[slotno];
	bool		ok;

	/* If a write is in progress, wait for it to finish */
	while (shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS &&
		   shared->page_number[slotno] == pageno)
	{
		SimpleLruWaitIO(ctl, slotno);
	}

	/*
	 * Do nothing if page is not dirty, or if buffer no longer contains the
	 * same page we were called for.
	 */
	if (!shared->page_dirty[slotno] ||
		shared->page_status[slotno] != SLRU_PAGE_VALID ||
		shared->page_number[slotno] != pageno)
		return;

	/*
	 * Mark the slot write-busy, and clear the dirtybit.  After this point, a
	 * transaction status update on this page will mark it dirty again.
	 */
	shared->page_status[slotno] = SLRU_PAGE_WRITE_IN_PROGRESS;
	shared->page_dirty[slotno] = false;

	/* Acquire per-buffer lock (cannot deadlock, see notes at top) */
	LWLockAcquire(&shared->buffer_locks[slotno].lock, LW_EXCLUSIVE);

	/* Release control lock while doing I/O */
	LWLockRelease(shared->ControlLock);

	/* Do the write */
	ok = SlruPhysicalWritePage(ctl, pageno, slotno, fdata);

	/* If we failed, and we're in a flush, better close the files */
	if (!ok && fdata)
	{
		int			i;

		for (i = 0; i < fdata->num_files; i++)
			CloseTransientFile(fdata->fd[i]);
	}

	/* Re-acquire control lock and update page state */
	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);

	Assert(shared->page_number[slotno] == pageno &&
		   shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS);

	/* If we failed to write, mark the page dirty again */
	if (!ok)
		shared->page_dirty[slotno] = true;

	shared->page_status[slotno] = SLRU_PAGE_VALID;

	LWLockRelease(&shared->buffer_locks[slotno].lock);

	/* Now it's okay to ereport if we failed */
	if (!ok)
		SlruReportIOError(ctl, pageno, InvalidTransactionId);
}

/*
 * Wrapper of SlruInternalWritePage, for external callers.
 * fdata is always passed a NULL here.
 */
void
SimpleLruWritePage(SlruCtl ctl, int slotno)
{
	SlruInternalWritePage(ctl, slotno, NULL);
}

/*
 * Return whether the given page exists on disk.
 *
 * A false return means that either the file does not exist, or that it's not
 * large enough to contain the given page.
 */
bool
SimpleLruDoesPhysicalPageExist(SlruCtl ctl, int pageno)
{
	int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
	int			offset = rpageno * BLCKSZ;
	char		path[MAXPGPATH];
	int			fd;
	bool		result;
	off_t		endpos;

	/* update the stats counter of checked pages */
	pgstat_count_slru_page_exists(ctl->shared->slru_stats_idx);

	SlruFileName(ctl, path, segno);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		/* expected: file doesn't exist */
		if (errno == ENOENT)
			return false;

		/* report error normally */
		slru_errcause = SLRU_OPEN_FAILED;
		slru_errno = errno;
		SlruReportIOError(ctl, pageno, 0);
	}

	if ((endpos = lseek(fd, 0, SEEK_END)) < 0)
	{
		slru_errcause = SLRU_SEEK_FAILED;
		slru_errno = errno;
		SlruReportIOError(ctl, pageno, 0);
	}

	result = endpos >= (off_t) (offset + BLCKSZ);

	if (CloseTransientFile(fd) != 0)
	{
		slru_errcause = SLRU_CLOSE_FAILED;
		slru_errno = errno;
		return false;
	}

	return result;
}

/*
 * Physical read of a (previously existing) page into a buffer slot
 *
 * On failure, we cannot just ereport(ERROR) since caller has put state in
 * shared memory that must be undone.  So, we return false and save enough
 * info in static variables to let SlruReportIOError make the report.
 *
 * For now, assume it's not worth keeping a file pointer open across
 * read/write operations.  We could cache one virtual file pointer ...
 */
static bool
SlruPhysicalReadPage(SlruCtl ctl, int pageno, int slotno)
{
	SlruShared	shared = ctl->shared;
	int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
	int			offset = rpageno * BLCKSZ;
	char		path[MAXPGPATH];
	int			fd;

	SlruFileName(ctl, path, segno);

	/*
	 * In a crash-and-restart situation, it's possible for us to receive
	 * commands to set the commit status of transactions whose bits are in
	 * already-truncated segments of the commit log (see notes in
	 * SlruPhysicalWritePage).  Hence, if we are InRecovery, allow the case
	 * where the file doesn't exist, and return zeroes instead.
	 */
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno != ENOENT || !InRecovery)
		{
			slru_errcause = SLRU_OPEN_FAILED;
			slru_errno = errno;
			return false;
		}

		ereport(LOG,
				(errmsg("file \"%s\" doesn't exist, reading as zeroes",
						path)));
		MemSet(shared->page_buffer[slotno], 0, BLCKSZ);
		return true;
	}

	if (lseek(fd, (off_t) offset, SEEK_SET) < 0)
	{
		slru_errcause = SLRU_SEEK_FAILED;
		slru_errno = errno;
		CloseTransientFile(fd);
		return false;
	}

	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_SLRU_READ);
	if (read(fd, shared->page_buffer[slotno], BLCKSZ) != BLCKSZ)
	{
		pgstat_report_wait_end();
		slru_errcause = SLRU_READ_FAILED;
		slru_errno = errno;
		CloseTransientFile(fd);
		return false;
	}
	pgstat_report_wait_end();

	if (CloseTransientFile(fd) != 0)
	{
		slru_errcause = SLRU_CLOSE_FAILED;
		slru_errno = errno;
		return false;
	}

	return true;
}

/*
 * Physical write of a page from a buffer slot
 *
 * On failure, we cannot just ereport(ERROR) since caller has put state in
 * shared memory that must be undone.  So, we return false and save enough
 * info in static variables to let SlruReportIOError make the report.
 *
 * For now, assume it's not worth keeping a file pointer open across
 * independent read/write operations.  We do batch operations during
 * SimpleLruFlush, though.
 *
 * fdata is NULL for a standalone write, pointer to open-file info during
 * SimpleLruFlush.
 */
static bool
SlruPhysicalWritePage(SlruCtl ctl, int pageno, int slotno, SlruFlush fdata)
{
	SlruShared	shared = ctl->shared;
	int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
	int			offset = rpageno * BLCKSZ;
	char		path[MAXPGPATH];
	int			fd = -1;

	/* update the stats counter of written pages */
	pgstat_count_slru_page_written(shared->slru_stats_idx);

	/*
	 * Honor the write-WAL-before-data rule, if appropriate, so that we do not
	 * write out data before associated WAL records.  This is the same action
	 * performed during FlushBuffer() in the main buffer manager.
	 */
	if (shared->group_lsn != NULL)
	{
		/*
		 * We must determine the largest async-commit LSN for the page. This
		 * is a bit tedious, but since this entire function is a slow path
		 * anyway, it seems better to do this here than to maintain a per-page
		 * LSN variable (which'd need an extra comparison in the
		 * transaction-commit path).
		 */
		XLogRecPtr	max_lsn;
		int			lsnindex,
					lsnoff;

		lsnindex = slotno * shared->lsn_groups_per_page;
		max_lsn = shared->group_lsn[lsnindex++];
		for (lsnoff = 1; lsnoff < shared->lsn_groups_per_page; lsnoff++)
		{
			XLogRecPtr	this_lsn = shared->group_lsn[lsnindex++];

			if (max_lsn < this_lsn)
				max_lsn = this_lsn;
		}

		if (!XLogRecPtrIsInvalid(max_lsn))
		{
			/*
			 * As noted above, elog(ERROR) is not acceptable here, so if
			 * XLogFlush were to fail, we must PANIC.  This isn't much of a
			 * restriction because XLogFlush is just about all critical
			 * section anyway, but let's make sure.
			 */
			START_CRIT_SECTION();
			XLogFlush(max_lsn);
			END_CRIT_SECTION();
		}
	}

	/*
	 * During a Flush, we may already have the desired file open.
	 */
	if (fdata)
	{
		int			i;

		for (i = 0; i < fdata->num_files; i++)
		{
			if (fdata->segno[i] == segno)
			{
				fd = fdata->fd[i];
				break;
			}
		}
	}

	if (fd < 0)
	{
		/*
		 * If the file doesn't already exist, we should create it.  It is
		 * possible for this to need to happen when writing a page that's not
		 * first in its segment; we assume the OS can cope with that. (Note:
		 * it might seem that it'd be okay to create files only when
		 * SimpleLruZeroPage is called for the first page of a segment.
		 * However, if after a crash and restart the REDO logic elects to
		 * replay the log from a checkpoint before the latest one, then it's
		 * possible that we will get commands to set transaction status of
		 * transactions that have already been truncated from the commit log.
		 * Easiest way to deal with that is to accept references to
		 * nonexistent files here and in SlruPhysicalReadPage.)
		 *
		 * Note: it is possible for more than one backend to be executing this
		 * code simultaneously for different pages of the same file. Hence,
		 * don't use O_EXCL or O_TRUNC or anything like that.
		 */
		SlruFileName(ctl, path, segno);
		fd = OpenTransientFile(path, O_RDWR | O_CREAT | PG_BINARY);
		if (fd < 0)
		{
			slru_errcause = SLRU_OPEN_FAILED;
			slru_errno = errno;
			return false;
		}

		if (fdata)
		{
			if (fdata->num_files < MAX_FLUSH_BUFFERS)
			{
				fdata->fd[fdata->num_files] = fd;
				fdata->segno[fdata->num_files] = segno;
				fdata->num_files++;
			}
			else
			{
				/*
				 * In the unlikely event that we exceed MAX_FLUSH_BUFFERS,
				 * fall back to treating it as a standalone write.
				 */
				fdata = NULL;
			}
		}
	}

	if (lseek(fd, (off_t) offset, SEEK_SET) < 0)
	{
		slru_errcause = SLRU_SEEK_FAILED;
		slru_errno = errno;
		if (!fdata)
			CloseTransientFile(fd);
		return false;
	}

	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_SLRU_WRITE);
	if (write(fd, shared->page_buffer[slotno], BLCKSZ) != BLCKSZ)
	{
		pgstat_report_wait_end();
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		slru_errcause = SLRU_WRITE_FAILED;
		slru_errno = errno;
		if (!fdata)
			CloseTransientFile(fd);
		return false;
	}
	pgstat_report_wait_end();

	/*
	 * If not part of Flush, need to fsync now.  We assume this happens
	 * infrequently enough that it's not a performance issue.
	 */
	if (!fdata)
	{
		pgstat_report_wait_start(WAIT_EVENT_SLRU_SYNC);
		if (ctl->do_fsync && pg_fsync(fd) != 0)
		{
			pgstat_report_wait_end();
			slru_errcause = SLRU_FSYNC_FAILED;
			slru_errno = errno;
			CloseTransientFile(fd);
			return false;
		}
		pgstat_report_wait_end();

		if (CloseTransientFile(fd) != 0)
		{
			slru_errcause = SLRU_CLOSE_FAILED;
			slru_errno = errno;
			return false;
		}
	}

	return true;
}

/*
 * Issue the error message after failure of SlruPhysicalReadPage or
 * SlruPhysicalWritePage.  Call this after cleaning up shared-memory state.
 */
static void
SlruReportIOError(SlruCtl ctl, int pageno, TransactionId xid)
{
	int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
	int			offset = rpageno * BLCKSZ;
	char		path[MAXPGPATH];

	SlruFileName(ctl, path, segno);
	errno = slru_errno;
	switch (slru_errcause)
	{
		case SLRU_OPEN_FAILED:
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not access status of transaction %u", xid),
					 errdetail("Could not open file \"%s\": %m.", path)));
			break;
		case SLRU_SEEK_FAILED:
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not access status of transaction %u", xid),
					 errdetail("Could not seek in file \"%s\" to offset %u: %m.",
							   path, offset)));
			break;
		case SLRU_READ_FAILED:
			if (errno)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not access status of transaction %u", xid),
						 errdetail("Could not read from file \"%s\" at offset %u: %m.",
								   path, offset)));
			else
				ereport(ERROR,
						(errmsg("could not access status of transaction %u", xid),
						 errdetail("Could not read from file \"%s\" at offset %u: read too few bytes.", path, offset)));
			break;
		case SLRU_WRITE_FAILED:
			if (errno)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not access status of transaction %u", xid),
						 errdetail("Could not write to file \"%s\" at offset %u: %m.",
								   path, offset)));
			else
				ereport(ERROR,
						(errmsg("could not access status of transaction %u", xid),
						 errdetail("Could not write to file \"%s\" at offset %u: wrote too few bytes.",
								   path, offset)));
			break;
		case SLRU_FSYNC_FAILED:
			ereport(data_sync_elevel(ERROR),
					(errcode_for_file_access(),
					 errmsg("could not access status of transaction %u", xid),
					 errdetail("Could not fsync file \"%s\": %m.",
							   path)));
			break;
		case SLRU_CLOSE_FAILED:
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not access status of transaction %u", xid),
					 errdetail("Could not close file \"%s\": %m.",
							   path)));
			break;
		default:
			/* can't get here, we trust */
			elog(ERROR, "unrecognized SimpleLru error cause: %d",
				 (int) slru_errcause);
			break;
	}
}

/*
 * Select the slot to re-use when we need a free slot.
 *
 * The target page number is passed because we need to consider the
 * possibility that some other process reads in the target page while
 * we are doing I/O to free a slot.  Hence, check or recheck to see if
 * any slot already holds the target page, and return that slot if so.
 * Thus, the returned slot is *either* a slot already holding the pageno
 * (could be any state except EMPTY), *or* a freeable slot (state EMPTY
 * or CLEAN).
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
SlruSelectLRUPage(SlruCtl ctl, int pageno)
{
	SlruShared	shared = ctl->shared;

	/* Outer loop handles restart after I/O */
	for (;;)
	{
		int			slotno;
		int			cur_count;
		int			bestvalidslot = 0;	/* keep compiler quiet */
		int			best_valid_delta = -1;
		int			best_valid_page_number = 0; /* keep compiler quiet */
		int			bestinvalidslot = 0;	/* keep compiler quiet */
		int			best_invalid_delta = -1;
		int			best_invalid_page_number = 0;	/* keep compiler quiet */

		/* See if page already has a buffer assigned */
		for (slotno = 0; slotno < shared->num_slots; slotno++)
		{
			if (shared->page_number[slotno] == pageno &&
				shared->page_status[slotno] != SLRU_PAGE_EMPTY)
				return slotno;
		}

		/*
		 * If we find any EMPTY slot, just select that one. Else choose a
		 * victim page to replace.  We normally take the least recently used
		 * valid page, but we will never take the slot containing
		 * latest_page_number, even if it appears least recently used.  We
		 * will select a slot that is already I/O busy only if there is no
		 * other choice: a read-busy slot will not be least recently used once
		 * the read finishes, and waiting for an I/O on a write-busy slot is
		 * inferior to just picking some other slot.  Testing shows the slot
		 * we pick instead will often be clean, allowing us to begin a read at
		 * once.
		 *
		 * Normally the page_lru_count values will all be different and so
		 * there will be a well-defined LRU page.  But since we allow
		 * concurrent execution of SlruRecentlyUsed() within
		 * SimpleLruReadPage_ReadOnly(), it is possible that multiple pages
		 * acquire the same lru_count values.  In that case we break ties by
		 * choosing the furthest-back page.
		 *
		 * Notice that this next line forcibly advances cur_lru_count to a
		 * value that is certainly beyond any value that will be in the
		 * page_lru_count array after the loop finishes.  This ensures that
		 * the next execution of SlruRecentlyUsed will mark the page newly
		 * used, even if it's for a page that has the current counter value.
		 * That gets us back on the path to having good data when there are
		 * multiple pages with the same lru_count.
		 */
		cur_count = (shared->cur_lru_count)++;
		for (slotno = 0; slotno < shared->num_slots; slotno++)
		{
			int			this_delta;
			int			this_page_number;

			if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
				return slotno;
			this_delta = cur_count - shared->page_lru_count[slotno];
			if (this_delta < 0)
			{
				/*
				 * Clean up in case shared updates have caused cur_count
				 * increments to get "lost".  We back off the page counts,
				 * rather than trying to increase cur_count, to avoid any
				 * question of infinite loops or failure in the presence of
				 * wrapped-around counts.
				 */
				shared->page_lru_count[slotno] = cur_count;
				this_delta = 0;
			}
			this_page_number = shared->page_number[slotno];
			if (this_page_number == shared->latest_page_number)
				continue;
			if (shared->page_status[slotno] == SLRU_PAGE_VALID)
			{
				if (this_delta > best_valid_delta ||
					(this_delta == best_valid_delta &&
					 ctl->PagePrecedes(this_page_number,
									   best_valid_page_number)))
				{
					bestvalidslot = slotno;
					best_valid_delta = this_delta;
					best_valid_page_number = this_page_number;
				}
			}
			else
			{
				if (this_delta > best_invalid_delta ||
					(this_delta == best_invalid_delta &&
					 ctl->PagePrecedes(this_page_number,
									   best_invalid_page_number)))
				{
					bestinvalidslot = slotno;
					best_invalid_delta = this_delta;
					best_invalid_page_number = this_page_number;
				}
			}
		}

		/*
		 * If all pages (except possibly the latest one) are I/O busy, we'll
		 * have to wait for an I/O to complete and then retry.  In that
		 * unhappy case, we choose to wait for the I/O on the least recently
		 * used slot, on the assumption that it was likely initiated first of
		 * all the I/Os in progress and may therefore finish first.
		 */
		if (best_valid_delta < 0)
		{
			SimpleLruWaitIO(ctl, bestinvalidslot);
			continue;
		}

		/*
		 * If the selected page is clean, we're set.
		 */
		if (!shared->page_dirty[bestvalidslot])
			return bestvalidslot;

		/*
		 * Write the page.
		 */
		SlruInternalWritePage(ctl, bestvalidslot, NULL);

		/*
		 * Now loop back and try again.  This is the easiest way of dealing
		 * with corner cases such as the victim page being re-dirtied while we
		 * wrote it.
		 */
	}
}

/*
 * Flush dirty pages to disk during checkpoint or database shutdown
 */
void
SimpleLruFlush(SlruCtl ctl, bool allow_redirtied)
{
	SlruShared	shared = ctl->shared;
	SlruFlushData fdata;
	int			slotno;
	int			pageno = 0;
	int			i;
	bool		ok;

	/* update the stats counter of flushes */
	pgstat_count_slru_flush(shared->slru_stats_idx);

	/*
	 * Find and write dirty pages
	 */
	fdata.num_files = 0;

	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);

	for (slotno = 0; slotno < shared->num_slots; slotno++)
	{
		SlruInternalWritePage(ctl, slotno, &fdata);

		/*
		 * In some places (e.g. checkpoints), we cannot assert that the slot
		 * is clean now, since another process might have re-dirtied it
		 * already.  That's okay.
		 */
		Assert(allow_redirtied ||
			   shared->page_status[slotno] == SLRU_PAGE_EMPTY ||
			   (shared->page_status[slotno] == SLRU_PAGE_VALID &&
				!shared->page_dirty[slotno]));
	}

	LWLockRelease(shared->ControlLock);

	/*
	 * Now fsync and close any files that were open
	 */
	ok = true;
	for (i = 0; i < fdata.num_files; i++)
	{
		pgstat_report_wait_start(WAIT_EVENT_SLRU_FLUSH_SYNC);
		if (ctl->do_fsync && pg_fsync(fdata.fd[i]) != 0)
		{
			slru_errcause = SLRU_FSYNC_FAILED;
			slru_errno = errno;
			pageno = fdata.segno[i] * SLRU_PAGES_PER_SEGMENT;
			ok = false;
		}
		pgstat_report_wait_end();

		if (CloseTransientFile(fdata.fd[i]) != 0)
		{
			slru_errcause = SLRU_CLOSE_FAILED;
			slru_errno = errno;
			pageno = fdata.segno[i] * SLRU_PAGES_PER_SEGMENT;
			ok = false;
		}
	}
	if (!ok)
		SlruReportIOError(ctl, pageno, InvalidTransactionId);
}

/*
 * Remove all segments before the one holding the passed page number
 *
 * All SLRUs prevent concurrent calls to this function, either with an LWLock
 * or by calling it only as part of a checkpoint.  Mutual exclusion must begin
 * before computing cutoffPage.  Mutual exclusion must end after any limit
 * update that would permit other backends to write fresh data into the
 * segment immediately preceding the one containing cutoffPage.  Otherwise,
 * when the SLRU is quite full, SimpleLruTruncate() might delete that segment
 * after it has accrued freshly-written data.
 */
void
SimpleLruTruncate(SlruCtl ctl, int cutoffPage)
{
	SlruShared	shared = ctl->shared;
	int			slotno;

	/* update the stats counter of truncates */
	pgstat_count_slru_truncate(shared->slru_stats_idx);

	/*
	 * The cutoff point is the start of the segment containing cutoffPage.
	 */
	cutoffPage -= cutoffPage % SLRU_PAGES_PER_SEGMENT;

	/*
	 * Scan shared memory and remove any pages preceding the cutoff page, to
	 * ensure we won't rewrite them later.  (Since this is normally called in
	 * or just after a checkpoint, any dirty pages should have been flushed
	 * already ... we're just being extra careful here.)
	 */
	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);

restart:;

	/*
	 * While we are holding the lock, make an important safety check: the
	 * planned cutoff point must be <= the current endpoint page. Otherwise we
	 * have already wrapped around, and proceeding with the truncation would
	 * risk removing the current segment.
	 */
	if (ctl->PagePrecedes(shared->latest_page_number, cutoffPage))
	{
		LWLockRelease(shared->ControlLock);
		ereport(LOG,
				(errmsg("could not truncate directory \"%s\": apparent wraparound",
						ctl->Dir)));
		return;
	}

	for (slotno = 0; slotno < shared->num_slots; slotno++)
	{
		if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
			continue;
		if (!ctl->PagePrecedes(shared->page_number[slotno], cutoffPage))
			continue;

		/*
		 * If page is clean, just change state to EMPTY (expected case).
		 */
		if (shared->page_status[slotno] == SLRU_PAGE_VALID &&
			!shared->page_dirty[slotno])
		{
			shared->page_status[slotno] = SLRU_PAGE_EMPTY;
			continue;
		}

		/*
		 * Hmm, we have (or may have) I/O operations acting on the page, so
		 * we've got to wait for them to finish and then start again. This is
		 * the same logic as in SlruSelectLRUPage.  (XXX if page is dirty,
		 * wouldn't it be OK to just discard it without writing it?  For now,
		 * keep the logic the same as it was.)
		 */
		if (shared->page_status[slotno] == SLRU_PAGE_VALID)
			SlruInternalWritePage(ctl, slotno, NULL);
		else
			SimpleLruWaitIO(ctl, slotno);
		goto restart;
	}

	LWLockRelease(shared->ControlLock);

	/* Now we can remove the old segment(s) */
	(void) SlruScanDirectory(ctl, SlruScanDirCbDeleteCutoff, &cutoffPage);
}

/*
 * Delete an individual SLRU segment, identified by the filename.
 *
 * NB: This does not touch the SLRU buffers themselves, callers have to ensure
 * they either can't yet contain anything, or have already been cleaned out.
 */
static void
SlruInternalDeleteSegment(SlruCtl ctl, char *filename)
{
	char		path[MAXPGPATH];

	snprintf(path, MAXPGPATH, "%s/%s", ctl->Dir, filename);
	ereport(DEBUG2,
			(errmsg("removing file \"%s\"", path)));
	unlink(path);
}

/*
 * Delete an individual SLRU segment, identified by the segment number.
 */
void
SlruDeleteSegment(SlruCtl ctl, int segno)
{
	SlruShared	shared = ctl->shared;
	int			slotno;
	char		path[MAXPGPATH];
	bool		did_write;

	/* Clean out any possibly existing references to the segment. */
	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
restart:
	did_write = false;
	for (slotno = 0; slotno < shared->num_slots; slotno++)
	{
		int			pagesegno = shared->page_number[slotno] / SLRU_PAGES_PER_SEGMENT;

		if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
			continue;

		/* not the segment we're looking for */
		if (pagesegno != segno)
			continue;

		/* If page is clean, just change state to EMPTY (expected case). */
		if (shared->page_status[slotno] == SLRU_PAGE_VALID &&
			!shared->page_dirty[slotno])
		{
			shared->page_status[slotno] = SLRU_PAGE_EMPTY;
			continue;
		}

		/* Same logic as SimpleLruTruncate() */
		if (shared->page_status[slotno] == SLRU_PAGE_VALID)
			SlruInternalWritePage(ctl, slotno, NULL);
		else
			SimpleLruWaitIO(ctl, slotno);

		did_write = true;
	}

	/*
	 * Be extra careful and re-check. The IO functions release the control
	 * lock, so new pages could have been read in.
	 */
	if (did_write)
		goto restart;

	snprintf(path, MAXPGPATH, "%s/%04X", ctl->Dir, segno);
	ereport(DEBUG2,
			(errmsg("removing file \"%s\"", path)));
	unlink(path);

	LWLockRelease(shared->ControlLock);
}

/*
 * SlruScanDirectory callback
 *		This callback reports true if there's any segment prior to the one
 *		containing the page passed as "data".
 */
bool
SlruScanDirCbReportPresence(SlruCtl ctl, char *filename, int segpage, void *data)
{
	int			cutoffPage = *(int *) data;

	cutoffPage -= cutoffPage % SLRU_PAGES_PER_SEGMENT;

	if (ctl->PagePrecedes(segpage, cutoffPage))
		return true;			/* found one; don't iterate any more */

	return false;				/* keep going */
}

/*
 * SlruScanDirectory callback.
 *		This callback deletes segments prior to the one passed in as "data".
 */
static bool
SlruScanDirCbDeleteCutoff(SlruCtl ctl, char *filename, int segpage, void *data)
{
	int			cutoffPage = *(int *) data;

	if (ctl->PagePrecedes(segpage, cutoffPage))
		SlruInternalDeleteSegment(ctl, filename);

	return false;				/* keep going */
}

/*
 * SlruScanDirectory callback.
 *		This callback deletes all segments.
 */
bool
SlruScanDirCbDeleteAll(SlruCtl ctl, char *filename, int segpage, void *data)
{
	SlruInternalDeleteSegment(ctl, filename);

	return false;				/* keep going */
}

/*
 * Scan the SimpleLru directory and apply a callback to each file found in it.
 *
 * If the callback returns true, the scan is stopped.  The last return value
 * from the callback is returned.
 *
 * The callback receives the following arguments: 1. the SlruCtl struct for the
 * slru being truncated; 2. the filename being considered; 3. the page number
 * for the first page of that file; 4. a pointer to the opaque data given to us
 * by the caller.
 *
 * Note that the ordering in which the directory is scanned is not guaranteed.
 *
 * Note that no locking is applied.
 */
bool
SlruScanDirectory(SlruCtl ctl, SlruScanCallback callback, void *data)
{
	bool		retval = false;
	DIR		   *cldir;
	struct dirent *clde;
	int			segno;
	int			segpage;

	cldir = AllocateDir(ctl->Dir);
	while ((clde = ReadDir(cldir, ctl->Dir)) != NULL)
	{
		size_t		len;

		len = strlen(clde->d_name);

		if ((len == 4 || len == 5 || len == 6) &&
			strspn(clde->d_name, "0123456789ABCDEF") == len)
		{
			segno = (int) strtol(clde->d_name, NULL, 16);
			segpage = segno * SLRU_PAGES_PER_SEGMENT;

			elog(DEBUG2, "SlruScanDirectory invoking callback on %s/%s",
				 ctl->Dir, clde->d_name);
			retval = callback(ctl, clde->d_name, segpage, data);
			if (retval)
				break;
		}
	}
	FreeDir(cldir);

	return retval;
}

/*
 * POLAR: Extend from SlruFileName
 */
static void
polar_slru_file_name_by_seg(SlruCtl ctl, char *path, int seg)
{
//    if (POLAR_SLRU_FILE_IN_SHARED_STORAGE())
//        snprintf(path, MAXPGPATH, "%s/%s/%04X", polar_datadir, (ctl)->Dir, seg);
//    else
        snprintf(path, MAXPGPATH, "%s/%04X", (ctl)->Dir, seg);

    return;
}

static void
polar_slru_file_dir(SlruCtl ctl, char *path)
{
//    if (POLAR_SLRU_FILE_IN_SHARED_STORAGE())
//        snprintf(path, MAXPGPATH, "%s/%s", polar_datadir, (ctl)->Dir);
//    else
        snprintf(path, MAXPGPATH, "%s", (ctl)->Dir);

    return;
}

static void
polar_slru_file_name_by_name(SlruCtl ctl, char *path, char *filename)
{
//    if (POLAR_SLRU_FILE_IN_SHARED_STORAGE())
//        snprintf(path, MAXPGPATH, "%s/%s/%s", polar_datadir, ctl->Dir, filename);
//    else
        snprintf(path, MAXPGPATH, "%s/%s", ctl->Dir, filename);

    return;
}

/*
 * For primary, some slru files, such as pg_xact, pg_commit_ts and pg_multixact,
 * are on the shared storage, for replica, all slru files are on the local storage.
 */
bool
polar_slru_file_in_shared_storage(bool in_shared_storage)
{
//    if (polar_in_replica_mode() || !polar_enable_shared_storage_mode)
//        return false;
//    else
//        return in_shared_storage;
    return false;
}

/*
 * POLAR: Scan SimpleLru and force to invalid specific page
 */
void
polar_slru_invalid_page(SlruCtl ctl, int pageno)
{
    SlruShared	shared = ctl->shared;
    int			slotno;

    LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);
    if (polar_enable_slru_hash_index)
    {
        for (;;)
        {
            polar_slru_hash_entry *entry;
            entry = hash_search(shared->polar_hash_index, (void *) &pageno,
                                HASH_FIND, NULL);

            if (entry != NULL)
            {
                slotno = entry->slotno;

                if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
                    shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS)
                {
                    SimpleLruWaitIO(ctl, slotno);
                    /* POLAR: Now we must recheck from the top */
                    continue;
                }

                Assert(shared->page_status[slotno] == SLRU_PAGE_EMPTY ||
                       (shared->page_status[slotno] == SLRU_PAGE_VALID &&
                        !shared->page_dirty[slotno]));

                if (shared->page_status[slotno] != SLRU_PAGE_EMPTY)
                {
                    if (hash_search(shared->polar_hash_index,
                                    (void *) &(shared->page_number[slotno]),
                                    HASH_REMOVE, NULL) == NULL)
                        elog(FATAL, "slru hash table corrupted");

                    polar_successor_list_push(shared->polar_free_list, slotno);
                }

                polar_slru_set_page_status(shared, slotno, SLRU_PAGE_EMPTY);
                shared->page_dirty[slotno] = false;
                shared->page_lru_count[slotno] = 0;
            }

            break;
        }
    }
    else
    {
        for (slotno = 0; slotno < shared->num_slots; slotno++)
        {
            if (shared->page_number[slotno] == pageno)
            {
                if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
                    shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS)
                {
                    SimpleLruWaitIO(ctl, slotno);
                    slotno = 0;
                    /* POLAR: Now we must recheck from the top */
                    continue;
                }

                Assert(shared->page_status[slotno] == SLRU_PAGE_EMPTY ||
                       (shared->page_status[slotno] == SLRU_PAGE_VALID &&
                        !shared->page_dirty[slotno]));

                polar_slru_set_page_status(shared, slotno, SLRU_PAGE_EMPTY);
                shared->page_dirty[slotno] = false;
                shared->page_lru_count[slotno] = 0;
                break;
            }
        }
    }

    LWLockRelease(shared->ControlLock);
}

/*
 * POLAR: Wrapper of SlruInternalWritePage, for external callers to append data to segment file.
 * fdata is always passed a NULL here.
 * If we know the segment file does not exists then set update to be false, otherwise set to be true.
 */
//Updated
void
polar_slru_append_page(SlruCtl ctl, int slotno, bool update)
{
//    SlruInternalWritePage(ctl, slotno, NULL, update);
    SlruInternalWritePage(ctl, slotno, NULL);
}

/*
 * POLAR:Check whether page exists in file
 */
bool
polar_slru_page_physical_exists(SlruCtl ctl, int pageno)
{
    int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
    int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    int			offset = rpageno * BLCKSZ;
    char		path[MAXPGPATH];
    struct stat fst;

    SlruFileName(ctl, path, segno);

    if (stat(path, &fst) < 0)
        return false;

    return fst.st_size >= (offset + BLCKSZ);
}

static inline void
polar_slru_set_page_status(SlruShared shared, int slotno, SlruPageStatus pagestatus)
{
    polar_slru_stat *stat = &shared->stat;
    SlruPageStatus oldstatus = shared->page_status[slotno];

    if (oldstatus != pagestatus)
    {
        stat->n_page_status_stat[oldstatus]--;
        stat->n_page_status_stat[pagestatus]++;
    }

    shared->page_status[slotno] = pagestatus;
}

static void
polar_slru_hash_init(SlruShared shared, int nslots, const char *name)
{
#define POLAR_SLRU_HASH_NAME " slru hash index"
    HASHCTL 	info;
    char *hash_name;
    size_t name_size = strlen(name) + strlen(POLAR_SLRU_HASH_NAME) + 1;

    hash_name = palloc(name_size);
    memset(&info, 0, sizeof(info));

    info.keysize = sizeof(int);
    info.entrysize = sizeof(polar_slru_hash_entry);

    snprintf(hash_name, name_size, "%s%s", name, POLAR_SLRU_HASH_NAME);

    shared->polar_hash_index = ShmemInitHash(hash_name,
                                             nslots, nslots, &info,
                                             HASH_ELEM | HASH_BLOBS);
    pfree(hash_name);
}

void
polar_slru_reg_local_cache(SlruCtl ctl, polar_local_cache cache)
{
    SlruShared shared = ctl->shared;

    shared->polar_cache = cache;
}

static bool
polar_slru_local_cache_read_page(SlruCtl ctl, int pageno, int slotno)
{
    SlruShared	shared = ctl->shared;
    int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
    int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    int			offset = rpageno * BLCKSZ;
    polar_cache_io_error io_error;

    if (!polar_local_cache_read(shared->polar_cache, segno, offset,
                                shared->page_buffer[slotno], BLCKSZ, &io_error))
    {
        /*
           * if we are InRecovery, allow the case where the file doesn't exist and where
           * the page beyond the end of the file. (see notes in SlruPhysicalWritePage).
           */
        if ((!POLAR_SEGMENT_NOT_EXISTS(&io_error) &&
             !POLAR_SEGMENT_BEYOND_END(&io_error)) ||
            !InRecovery)
        {
            polar_local_cache_report_error(shared->polar_cache, &io_error, WARNING);
//            slru_errcause = SLRU_CACHE_READ_FAILED;
            return false;
        }

        polar_local_cache_report_error(shared->polar_cache, &io_error, LOG);
        MemSet(shared->page_buffer[slotno], 0, BLCKSZ);
    }

    return true;
}

static bool
polar_slru_local_cache_write_page(SlruCtl ctl, int pageno, int slotno)
{
    SlruShared	shared = ctl->shared;
    int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
    int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    int			offset = rpageno * BLCKSZ;
    polar_cache_io_error io_error;

    if (!polar_local_cache_write(shared->polar_cache, segno,
                                 offset, shared->page_buffer[slotno], BLCKSZ, &io_error))
    {
        polar_local_cache_report_error(shared->polar_cache, &io_error, WARNING);
//        slru_errcause = SLRU_CACHE_WRITE_FAILED;
        return false;
    }

    return true;
}

static void
polar_slru_stats_init(void)
{
    MemSet(polar_slru_stats, 0, sizeof(polar_slru_stat *) * POLAR_SLRU_STATS_NUM);
    n_polar_slru_stats = 0;
}

void
polar_slru_init(void)
{
    if (IsUnderPostmaster)
        return;

    polar_slru_stats_init();
}

/* POLAR: promote slru if local cache exists, copy data from local storage to shared storage */
void
polar_slru_promote(SlruCtl ctl)
{
    polar_local_cache cache = ctl->shared->polar_cache;
    uint32 io_permission = POLAR_CACHE_LOCAL_FILE_READ | POLAR_CACHE_LOCAL_FILE_WRITE |
                           POLAR_CACHE_SHARED_FILE_READ | POLAR_CACHE_SHARED_FILE_WRITE;
    polar_cache_io_error io_error;

    if (!cache)
        elog(FATAL, "There's no local cache, so we can not promote");

    if (!polar_local_cache_set_io_permission(cache, io_permission, &io_error))
        polar_local_cache_report_error(cache, &io_error, FATAL);
    ctl->shared->polar_file_in_shared_storage = true;
}

/* POLAR: remove slru local cache file */
void
polar_slru_remove_local_cache_file(SlruCtl ctl)
{
    SlruShared shared = ctl->shared;
    if (shared && shared->polar_cache)
        polar_local_cache_move_trash(shared->polar_cache->dir_name);
}