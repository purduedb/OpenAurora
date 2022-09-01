/*-------------------------------------------------------------------------
 *
 * px_dispatchresult.h
 *	  Routines for processing dispatch results.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * IDENTIFICATION
 *	    src/include/px/px_dispatchresult.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PXDISPATCHRESULT_H
#define PXDISPATCHRESULT_H

#include "commands/tablecmds.h"
#include "utils/hsearch.h"

#include "px/px_disp.h"

struct pg_result;				/* PGresult ... #include "libpq-fe.h" */
struct PxWorkerDescriptor;	/* #include "px/px_conn.h" */
struct StringInfoData;			/* #include "lib/stringinfo.h" */
struct PQExpBufferData;			/* #include "libpq-int.h" */

typedef struct PxPgResults
{
	struct pg_result **pg_results;
	int			numResults;
} PxPgResults;

/*
 * PxDispatchResults_SliceInfo:
 * An entry in a PxDispatchResults object's slice map.
 * Used to find the PxDispatchResult objects for a gang
 * of PXs given their slice index.
 */
typedef struct PxDispatchResults_SliceInfo
{
	int			resultBegin;
	int			resultEnd;
} PxDispatchResults_SliceInfo;

/*
 * PxDispatchResult:
 * Struct for holding the result information
 * for a command dispatched by PxCommandDispatch to
 * a single segdb.
 */
typedef struct PxDispatchResult
{
	/*
	 * libpq connection to PX process; * reset to NULL after end of thread
	 */
	struct PxWorkerDescriptor *pxWorkerDesc;

	/* owner of this PxDispatchResult */
	struct PxDispatchResults *meleeResults;

	/*
	 * index of this entry within results->resultArray
	 */
	int			meleeIndex;

	/*
	 * ERRCODE_xxx (sqlstate encoded as an int) of first error, or 0.
	 */
	int			errcode;

	/*
	 * index of last entry in resultbuf with resultStatus == PGRES_TUPLES_OK
	 * or PGRES_COMMAND_OK (command ended successfully); or -1. Pass to
	 * pxconn_getResult().
	 */
	int			okindex;

	/*
	 * array of ptr to PGresult
	 */
	struct PQExpBufferData *resultbuf;

	/* string of messages; or NULL */
	struct PQExpBufferData *error_message;

	/* true => PQsendCommand done */
	bool		hasDispatched;

	/* true => busy in dispatch thread */
	volatile bool	stillRunning;

	/* type of signal sent */
	DispatchWaitMode sentSignal;

	/*
	 * true => got any of these errors: ERRCODE_PX_OPERATION_CANCELED
	 * ERRCODE_QUERY_CANCELED
	 */
	bool		wasCanceled;

	/* num rows rejected in SREH mode */
	int			numrowsrejected;

	/* num rows completed in COPY FROM ON SEGMENT */
	int			numrowscompleted;
} PxDispatchResult;

/*
 * PxDispatchResults:
 * A collection of PxDispatchResult objects to hold and summarize
 * the results of dispatching a command or plan to one or more Gangs.
 */
typedef struct PxDispatchResults
{
	/*
	 * Array of PxDispatchResult objects, one per PX
	 */
	PxDispatchResult *resultArray;

	/*
	 * num of assigned slots (num of PXs) 0 <= resultCount <= resultCapacity
	 */
	int			resultCount;

	/* size of resultArray (total #slots) */
	int			resultCapacity;

	/*
	 * index of the resultArray entry for the PX that was first to report an
	 * error; or -1 if no error.
	 */
	volatile int iFirstError;

	/*
	 * ERRCODE_xxx (sqlstate encoded as an int) of the first error, or 0.
	 */
	volatile int errcode;

	/* true => stop remaining PXs on err */
	bool		cancelOnError;

	/*
	 * Map: sliceIndex => resultArray index
	 */
	PxDispatchResults_SliceInfo *sliceMap;

	/* num of slots in sliceMap */
	int			sliceCapacity;
} PxDispatchResults;


/*
 * Create a PxDispatchResult object, appending it to the
 * resultArray of a given PxDispatchResults object.
 */
PxDispatchResult *pxdisp_makeResult(struct PxDispatchResults *meleeResults,
				   struct PxWorkerDescriptor *pxWorkerDesc,
				   int sliceIndex);

/*
 * Destroy a PxDispatchResult object.
 */
void
			pxdisp_termResult(PxDispatchResult *dispatchResult);

/*
 * Reset a PxDispatchResult object for possible reuse.
 */
void
			pxdisp_resetResult(PxDispatchResult *dispatchResult);

/*
 * Take note of an error.
 * 'errcode' is the ERRCODE_xxx value for setting the client's SQLSTATE.
 * NB: This can be called from a dispatcher thread, so it must not use
 * palloc/pfree or elog/ereport because they are not thread safe.
 */
void pxdisp_seterrcode(int errcode,		/* ERRCODE_xxx or 0 */
				   int resultIndex, /* -1 if no PGresult */
				   PxDispatchResult *dispatchResult);

/*
 * Format a message, printf-style, and append to the error_message buffer.
 * Also write it to stderr if logging is enabled for messages of the
 * given severity level 'elevel' (for example, DEBUG1; or 0 to suppress).
 * 'errcode' is the ERRCODE_xxx value for setting the client's SQLSTATE.
 * NB: This can be called from a dispatcher thread, so it must not use
 * palloc/pfree or elog/ereport because they are not thread safe.
 */
void
pxdisp_appendMessageNonThread(PxDispatchResult *dispatchResult,
								int errcode,
								const char *fmt,
								...)
pg_attribute_printf(3, 4);

void
pxdisp_appendMessageThread(PxDispatchResult *dispatchResult,
								int errcode,
								const char *fmt,
								...)
pg_attribute_printf(3, 4);


/*
 * Store a PGresult object ptr in the result buffer.
 * NB: Caller must not PQclear() the PGresult object.
 */
void pxdisp_appendResult(PxDispatchResult *dispatchResult,
					 struct pg_result *res);

/*
 * Return the i'th PGresult object ptr (if i >= 0), or
 * the n+i'th one (if i < 0), or NULL (if i out of bounds).
 * NB: Caller must not PQclear() the PGresult object.
 */
struct pg_result *pxdisp_getPGresult(PxDispatchResult *dispatchResult, int i);

/*
 * Return the number of PGresult objects in the result buffer.
 */
int
			pxdisp_numPGresult(PxDispatchResult *dispatchResult);

/*
 * Construct an ErrorData from the dispatch results.
 */
ErrorData  *pxdisp_dumpDispatchResult(PxDispatchResult *dispatchResult);

/*
 * Format a PxDispatchResults object.
 * Appends error messages to caller's StringInfo buffer.
 * Returns ERRCODE_xxx if some error was found, or 0 if no errors.
 * Before calling this function, you must call PxCheckDispatchResult().
 */
void pxdisp_dumpDispatchResults(struct PxDispatchResults *gangResults,
							ErrorData **pxError);

extern ErrorData *pxdisp_get_PXerror(struct pg_result *pgresult);

/*
 * Return sum of the cmdTuples values from PxDispatchResult
 * entries that have a successful PGresult.  If sliceIndex >= 0,
 * uses only the entries belonging to the specified slice.
 */
int64
			pxdisp_sumCmdTuples(PxDispatchResults *results, int sliceIndex);

/*
 * If several tuples were eliminated/rejected from the result because of
 * bad data formatting (this is currenly only possible in external tables
 * with single row error handling) - sum up the total rows rejected from
 * all PX's and notify the client.
 */
void
			pxdisp_sumRejectedRows(PxDispatchResults *results);

/*
 * max of the lastOid values returned from the PXs
 */
Oid
			pxdisp_maxLastOid(PxDispatchResults *results, int sliceIndex);

/*
 * Return ptr to first resultArray entry for a given sliceIndex.
 */
PxDispatchResult *pxdisp_resultBegin(PxDispatchResults *results, int sliceIndex);

/*
 * Return ptr to last+1 resultArray entry for a given sliceIndex.
 */
PxDispatchResult *pxdisp_resultEnd(PxDispatchResults *results, int sliceIndex);

/*
 * used in the interconnect on the dispatcher to avoid error-cleanup deadlocks.
 */
bool
			pxdisp_checkResultsErrcode(struct PxDispatchResults *meeleResults);

/*
 * pxdisp_makeDispatchResults:
 * Will be freed in function pxdisp_destroyDispatcherState by deleting the
 * memory context.
 */
void pxdisp_makeDispatchResults(struct PxDispatcherState *ds,
							int sliceCapacity,
							bool cancelOnError);

/* Px adaptive scan */
extern volatile bool px_adps_dispatch_wait;
extern pg_atomic_uint32 px_adps_eno;
void px_check_adps_thread_error(void);
void px_set_adps_thread_error(int eno);
void px_reset_adps_thread_error(void);

#endif							/* PXDISPATCHRESULT_H */
