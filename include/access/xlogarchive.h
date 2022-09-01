/*------------------------------------------------------------------------
 *
 * xlogarchive.h
 *		Prototypes for WAL archives in the backend
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/access/xlogarchive.h
 *
 *------------------------------------------------------------------------
 */

#ifndef XLOG_ARCHIVE_H
#define XLOG_ARCHIVE_H

#include "access/xlogdefs.h"

extern bool RestoreArchivedFile(char *path, const char *xlogfname,
								const char *recovername, off_t expectedSize,
								bool cleanupEnabled);
extern void ExecuteRecoveryCommand(const char *command, const char *commandName,
								   bool failOnSignal);
extern void KeepFileRestoredFromArchive(const char *path, const char *xlogfname);
extern void XLogArchiveNotify(const char *xlog);
extern void XLogArchiveNotifySeg(XLogSegNo segno);
extern void XLogArchiveForceDone(const char *xlog);
extern bool XLogArchiveCheckDone(const char *xlog);
extern bool XLogArchiveIsBusy(const char *xlog);
extern bool XLogArchiveIsReady(const char *xlog);
extern bool XLogArchiveIsReadyOrDone(const char *xlog);
extern void XLogArchiveCleanup(const char *xlog);

#ifndef FRONTEND
extern void polar_backup_history_file_path(char *path, TimeLineID tli, XLogSegNo logSegNo, XLogRecPtr startpoint, int wal_segsz_bytes);
extern void polar_status_file_path(char *path, const char *xlog, char *suffix);
extern void polar_tl_history_file_path(char *path, TimeLineID tli);
extern void polar_xLog_file_path(char *path, TimeLineID tli, XLogSegNo logSegNo, int wal_segsz_bytes);
extern XLogRecData *polar_get_main_data_head(void);
extern uint32 polar_get_main_data_len(void);
extern void polar_set_main_data(void *data, uint32 len);
extern void polar_reset_main_data(void);
#endif

#endif							/* XLOG_ARCHIVE_H */
