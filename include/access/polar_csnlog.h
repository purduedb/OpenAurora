/*
 * polar_csnlog.h
 *
 * Commit-Sequence-Number log.
 *
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *
 * src/include/access/polar_clog.h
 */
#ifndef POLAR_CSNLOG_H
#define POLAR_CSNLOG_H

#include "postgres.h"
#include "access/xlogreader.h"
#include "port/atomics.h"
#include "utils/polar_local_cache.h"

typedef struct polar_csnlog_ub_stat
{
 	pg_atomic_uint64 t_all_fetches;
 	pg_atomic_uint64 t_ub_fetches;
 	pg_atomic_uint64 t_ub_hits;
} polar_csnlog_ub_stat; 

/* share memory related */
extern Size polar_csnlog_shmem_buffers(void);
extern Size polar_csnlog_shmem_size(void);
extern void polar_csnlog_shmem_init(void);

/* system admin related */
extern void polar_csnlog_bootstrap(void);
extern void polar_csnlog_startup(TransactionId oldest_active_xid);
extern void polar_csnlog_shutdown(void);

/* core method */
extern void polar_csnlog_set_csn(TransactionId xid, int nsubxids,
								 TransactionId *subxids, CommitSeqNo csn, XLogRecPtr lsn);
extern CommitSeqNo polar_csnlog_get_csn(TransactionId xid);

extern CommitSeqNo polar_csnlog_get_upperbound_csn(TransactionId xid);
extern void polar_csnlog_count_upperbound_fetch(int t_all_fetches, 
												int t_ub_fetches, int t_ub_hits);
extern polar_csnlog_ub_stat* polar_csnlog_get_upperbound_stat_ptr(void);

extern void polar_csnlog_set_parent(TransactionId xid, TransactionId parent);
extern TransactionId polar_csnlog_get_parent(TransactionId xid);

/* auxiliary method */
extern TransactionId polar_csnlog_get_next_active_xid(TransactionId start,
													  TransactionId end);
extern void polar_csnlog_get_running_xids(TransactionId start, TransactionId end, CommitSeqNo snapshot_csn,
                                   int max_xids, int *nxids, TransactionId *xids, bool *overflowed);

extern TransactionId polar_csnlog_get_top(TransactionId xid);

/* storage related */
extern void polar_csnlog_validate_dir(void);
extern void polar_csnlog_remove_all(void);
extern void polar_csnlog_extend(TransactionId newest_xid, bool write_wal);
extern void polar_csnlog_truncate(TransactionId oldest_xid);
extern void polar_csnlog_checkpoint(void);

extern void polar_csnlog_zero_page_redo(int pageno);
extern void polar_csnlog_truncate_redo(int pageno);

/* do online promote for csnlog */
extern void polar_promote_csnlog(TransactionId oldest_active_xid);

/* POLAR: remove csnlog local cache file */
extern void polar_remove_csnlog_local_cache_file(void);
#endif							/* POLAR_CSNLOG_H */
