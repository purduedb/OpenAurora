/*-------------------------------------------------------------------------
 *
 * polar_csn_mvcc_vars.h
 *	  polar csn mvcc related share global vars
 *
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *
 * src/include/access/polar_mvccvars.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_MVCCVARS_H
#define POLAR_MVCCVARS_H

#include "port/atomics.h"

/*
 * Polar csn based mvcc related global share variables
 * We pad high-contended csn field to PG_CACHE_LINE_SIZE for ARM
 */
typedef struct polar_csn_mvcc_var_cache
{
    /* 
     * The XID of the oldest transaction that's still in-progress. 
     * (Or rather, the oldest XID among all still in-progress
	 * transactions; it's not necessarily the one that started first).
	 * Must hold ProcArrayLock in shared mode, and use atomic ops, to update.
     */
    union {
	    pg_atomic_uint32 polar_oldest_active_xid;
        char             pad_for_oldest_active_xid[PG_CACHE_LINE_SIZE];
    };

	/*
	 * The CSN which will be assigned to next committed transaction.
	 * It is protected by CommitSeqNoLock.
     * Use atomic ops to update.
	 */
	union {
        pg_atomic_uint64 polar_next_csn;
        char             pad_for_next_csn[PG_CACHE_LINE_SIZE];
    };

    /* 
     * The highest XID that has committed. 
     * Anything great than this is seen by still in-progress by everyone. 
     * Use atomic ops to update.
     * 
     * This var is same with latestCompletedXid in VariableCache,
     * but we are atomic and can be updated without procarray lock
     */
    union {
        pg_atomic_uint32 polar_latest_completed_xid;
        char             pad_for_latest_completed_xid[PG_CACHE_LINE_SIZE];
    };
} polar_csn_mvcc_var_cache;

extern void polar_csn_mvcc_var_cache_shmem_init(void);
extern void polar_csn_mvcc_var_cache_set(TransactionId oldest_active_xid, CommitSeqNo next_csn, TransactionId latest_completed_xid);

extern PGDLLIMPORT polar_csn_mvcc_var_cache *polar_shmem_csn_mvcc_var_cache;

#endif   /* POLAR_MVCCVARS_H */