/*-------------------------------------------------------------------------
 *
 * px_sreh.h
 *	  Routines for single row error handling
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * IDENTIFICATION
 *	    src/include/px/px_sreh.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PXSREH_H
#define PXSREH_H

#include "fmgr.h"
#include "utils/memutils.h"

/*
 * All the Single Row Error Handling state is kept here.
 * When an error happens and we are in single row error handling
 * mode this struct is updated and handed to the single row
 * error handling manager (pxsreh.c).
 */
typedef struct PxSreh
{
	/* bad row information */
	char	   *errmsg;			/* the error message for this bad data row */
	char	   *rawdata;		/* the bad data row */
	char	   *relname;		/* target relation */
	int64		linenumber;		/* line number of error in original file */
	uint64		processed;		/* num logical input rows processed so far */
	bool		is_server_enc;	/* was bad row converted to server encoding? */

	/* reject limit state */
	int			rejectlimit;	/* SEGMENT REJECT LIMIT value */
	int64		rejectcount;	/* how many were rejected so far */
	bool		is_limit_in_rows;	/* ROWS = true, PERCENT = false */

	MemoryContext badrowcontext;	/* per-badrow evaluation context */
	char		filename[MAXPGPATH];	/* "uri [filename]" */

	bool		log_to_file;	/* or log into file? */
	Oid			relid;			/* parent relation id */
} PxSreh;

extern void ReportSrehResults(PxSreh *pxsreh, uint64 total_rejected);


#endif							/* PXSREH_H */
