/*-------------------------------------------------------------------------
 *
 * fd.h
 *	  Virtual file descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/fd.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * calls:
 *
 *	File {Close, Read, Write, Size, Sync}
 *	{Path Name Open, Allocate, Free} File
 *
 * These are NOT JUST RENAMINGS OF THE UNIX ROUTINES.
 * Use them for all file activity...
 *
 *	File fd;
 *	fd = PathNameOpenFile("foo", O_RDONLY);
 *
 *	AllocateFile();
 *	FreeFile();
 *
 * Use AllocateFile, not fopen, if you need a stdio file (FILE*); then
 * use FreeFile, not fclose, to close it.  AVOID using stdio for files
 * that you intend to hold open for any length of time, since there is
 * no way for them to share kernel file descriptors with other files.
 *
 * Likewise, use AllocateDir/FreeDir, not opendir/closedir, to allocate
 * open directories (DIR*), and OpenTransientFile/CloseTransientFile for an
 * unbuffered file descriptor.
 *
 * If you really can't use any of the above, at least call AcquireExternalFD
 * or ReserveExternalFD to report any file descriptors that are held for any
 * length of time.  Failure to do so risks unnecessary EMFILE errors.
 */
#ifndef FD_H
#define FD_H

#include <dirent.h>


typedef int File;


/* GUC parameter */
extern PGDLLIMPORT int max_files_per_process;
extern PGDLLIMPORT bool data_sync_retry;

/*
 * This is private to fd.c, but exported for save/restore_backend_variables()
 */
extern int	max_safe_fds;

/*
 * On Windows, we have to interpret EACCES as possibly meaning the same as
 * ENOENT, because if a file is unlinked-but-not-yet-gone on that platform,
 * that's what you get.  Ugh.  This code is designed so that we don't
 * actually believe these cases are okay without further evidence (namely,
 * a pending fsync request getting canceled ... see ProcessSyncRequests).
 */
#ifndef WIN32
#define FILE_POSSIBLY_DELETED(err)	((err) == ENOENT)
#else
#define FILE_POSSIBLY_DELETED(err)	((err) == ENOENT || (err) == EACCES)
#endif

#ifdef __cplusplus
extern "C" {
#elif
extern {
#endif

/*
 * prototypes for functions in fd.c
 */

/* Operations on virtual Files --- equivalent to Unix kernel file ops */
File PathNameOpenFile(const char *fileName, int fileFlags);
File PathNameOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode);
File OpenTemporaryFile(bool interXact);
void FileClose(File file);
int	FilePrefetch(File file, off_t offset, int amount, uint32 wait_event_info);
int	FileRead(File file, char *buffer, int amount, off_t offset, uint32 wait_event_info);
int	FileSync(File file, uint32 wait_event_info);
int	FileWrite(File file, char *buffer, int amount, off_t offset, uint32 wait_event_info);
off_t FileSize(File file);
int	FileTruncate(File file, off_t offset, uint32 wait_event_info);
void FileWriteback(File file, off_t offset, off_t nbytes, uint32 wait_event_info);
char *FilePathName(File file);
int	FileGetRawDesc(File file);
int	FileGetRawFlags(File file);
mode_t FileGetRawMode(File file);

/* Operations used for sharing named temporary files */
File PathNameCreateTemporaryFile(const char *name, bool error_on_failure);
File PathNameOpenTemporaryFile(const char *name);
bool PathNameDeleteTemporaryFile(const char *name, bool error_on_failure);
void PathNameCreateTemporaryDir(const char *base, const char *name);
void PathNameDeleteTemporaryDir(const char *name);
void TempTablespacePath(char *path, Oid tablespace);

/* Operations that allow use of regular stdio --- USE WITH CAUTION */
FILE *AllocateFile(const char *name, const char *mode);
int	FreeFile(FILE *file);

/* Operations that allow use of pipe streams (popen/pclose) */
FILE *OpenPipeStream(const char *command, const char *mode);
int	ClosePipeStream(FILE *file);

/* Operations to allow use of the <dirent.h> library routines */
DIR *AllocateDir(const char *dirname);
struct dirent *ReadDir(DIR *dir, const char *dirname);
struct dirent *ReadDirExtended(DIR *dir, const char *dirname,
									  int elevel);
int	FreeDir(DIR *dir);

/* Operations to allow use of a plain kernel FD, with automatic cleanup */
int	OpenTransientFile(const char *fileName, int fileFlags);
int	OpenTransientFilePerm(const char *fileName, int fileFlags, mode_t fileMode);
int	CloseTransientFile(int fd);

/* If you've really really gotta have a plain kernel FD, use this */
int	BasicOpenFile(const char *fileName, int fileFlags);
int	BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode);

/* Use these for other cases, and also for long-lived BasicOpenFile FDs */
bool AcquireExternalFD(void);
void ReserveExternalFD(void);
void ReleaseExternalFD(void);

/* Make a directory with default permissions */
int	MakePGDirectory(const char *directoryName);

/* Miscellaneous support routines */
void InitFileAccess(void);
void set_max_safe_fds(void);
void closeAllVfds(void);
void SetTempTablespaces(Oid *tableSpaces, int numSpaces);
bool TempTablespacesAreSet(void);
int	GetTempTablespaces(Oid *tableSpaces, int numSpaces);
Oid	GetNextTempTableSpace(void);
void AtEOXact_Files(bool isCommit);
void AtEOSubXact_Files(bool isCommit, SubTransactionId mySubid,
							  SubTransactionId parentSubid);
void RemovePgTempFiles(void);
void RemovePgTempFilesInDir(const char *tmpdirname, bool missing_ok,
								   bool unlink_all);
bool looks_like_temp_rel_name(const char *name);

int	pg_fsync(int fd);
int	pg_fsync_no_writethrough(int fd);
int	pg_fsync_writethrough(int fd);
int	pg_fdatasync(int fd);
void pg_flush_data(int fd, off_t offset, off_t amount);
void fsync_fname(const char *fname, bool isdir);
int	fsync_fname_ext(const char *fname, bool isdir, bool ignore_perm, int elevel);
int	durable_rename(const char *oldfile, const char *newfile, int loglevel);
int	durable_unlink(const char *fname, int loglevel);
int	durable_rename_excl(const char *oldfile, const char *newfile, int loglevel);
void SyncDataDirectory(void);
int	data_sync_elevel(int elevel);

#ifdef __cplusplus
}
#elif
}
#endif

/* Filename components */
#define PG_TEMP_FILES_DIR "pgsql_tmp"
#define PG_TEMP_FILE_PREFIX "pgsql_tmp"

#endif							/* FD_H */
