/*-------------------------------------------------------------------------
 *
 * px_disp.h
 * routines for dispatching commands from the dispatcher process
 * to the qExec processes.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * IDENTIFICATION
 *	    src/include/px/px_disp.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PXDISP_H
#define PXDISP_H

#include "nodes/pg_list.h"
#include "utils/resowner.h"

#define PX_MOTION_LOST_CONTACT_STRING "Interconnect error master lost contact with segment."

struct PxDispatchResults;		/* #include "px/px_dispatchresult.h" */
struct PxPgResults;
struct Gang;					/* #include "px/px_gang.h" */
struct ResourceOwnerData;
enum GangType;

/*
 * Types of message to PX when we wait for it.
 */
typedef enum DispatchWaitMode
{
	DISPATCH_WAIT_NONE = 0,		/* wait until PX fully completes */
	DISPATCH_WAIT_FINISH,		/* send query finish */
	DISPATCH_WAIT_CANCEL		/* send query cancel */
}			DispatchWaitMode;

typedef struct PxDispatcherState
{
	List	   *allocatedGangs;
	struct PxDispatchResults *primaryResults;
	void	   *dispatchParams;
	int			largestGangSize;
	bool		forceDestroyGang;
	bool		isExtendedQuery;
#ifdef USE_ASSERT_CHECKING
	bool		isGangDestroying;
#endif
} PxDispatcherState;

typedef struct DispatcherInternalFuncs
{
	bool		(*checkForCancel) (struct PxDispatcherState *ds);
	int			(*getWaitSocketFd) (struct PxDispatcherState *ds);
	void	   *(*makeDispatchParams) (int maxSlices, int largestGangSize, char *queryText, int queryTextLen);
	void		(*checkResults) (struct PxDispatcherState *ds, DispatchWaitMode waitMode);
	void		(*dispatchToGang) (struct PxDispatcherState *ds, struct Gang *gp, int sliceIndex);
	void		(*waitDispatchFinish) (struct PxDispatcherState *ds);

} DispatcherInternalFuncs;

typedef struct dispatcher_handle_t
{
	struct PxDispatcherState *dispatcherState;

	ResourceOwner owner;		/* owner of this handle */
	struct dispatcher_handle_t *next;
	struct dispatcher_handle_t *prev;
} dispatcher_handle_t;

extern dispatcher_handle_t *open_dispatcher_handles;

/*--------------------------------------------------------------------*/
/*
 * pxdisp_dispatchToGang:
 * Send the strCommand SQL statement to the subset of all segdbs in the cluster
 * specified by the gang parameter. cancelOnError indicates whether an error
 * occurring on one of the qExec segdbs should cause all still-executing commands to cancel
 * on other qExecs. Normally this would be true. The commands are sent over the libpq
 * connections that were established during pxlink_setup.
 *
 * The caller must provide a PxDispatchResults object having available
 * resultArray slots sufficient for the number of PXs to be dispatched:
 * i.e., resultCapacity - resultCount >= gp->size. This function will
 * assign one resultArray slot per PX of the Gang, paralleling the Gang's
 * db_descriptors array. Success or failure of each PX will be noted in
 * the PX's PxDispatchResult entry; but before examining the results, the
 * caller must wait for execution to end by calling PxCheckDispatchResult().
 *
 * The PxDispatchResults object owns some malloc'ed storage, so the caller
 * must make certain to free it by calling pxdisp_destroyDispatcherState().
 *
 * When dispatchResults->cancelOnError is false, strCommand is to be
 * dispatched to every connected gang member if possible, despite any
 * cancellation requests, PX errors, connection failures, etc.
 *
 * NB: This function should return normally even if there is an error.
 * It should not longjmp out via elog(ERROR, ...), ereport(ERROR, ...),
 * PG_THROW, CHECK_FOR_INTERRUPTS, etc.
 */
void pxdisp_dispatchToGang(struct PxDispatcherState *ds,
					   struct Gang *gp,
					   int sliceIndex);

/*
 * pxdisp_waitDispatchFinish:
 *
 * For asynchronous dispatcher, we have to wait all dispatch to finish before we move on to query execution,
 * otherwise we may get into a deadlock situation, e.g, gather motion node waiting for data,
 * while segments waiting for plan.
 */
void
			pxdisp_waitDispatchFinish(struct PxDispatcherState *ds);

/*
 * PxCheckDispatchResult:
 *
 * Waits for completion of threads launched by pxdisp_dispatchToGang().
 *
 * PXs that were dispatched with 'cancelOnError' true and are not yet idle
 * will be canceled/finished according to waitMode.
 */
void
			pxdisp_checkDispatchResult(struct PxDispatcherState *ds, DispatchWaitMode waitMode);

/*
 * pxdisp_getDispatchResults:
 *
 * Block until all PXs return results or report errors.
 *
 * Return Values:
 *   Return NULL If one or more PXs got Error in which case pxErrorMsg contain
 *   PX error messages and pxErrorCode the thrown ERRCODE.
 */
struct PxDispatchResults *pxdisp_getDispatchResults(struct PxDispatcherState *ds, ErrorData **pxError);

/*
 * PxDispatchHandleError
 *
 * When caller catches an error, the PG_CATCH handler can use this
 * function instead of pxdisp_finishCommand to wait for all PXs
 * to finish, clean up, and report PX errors if appropriate.
 * This function should be called only from PG_CATCH handlers.
 *
 * This function destroys and frees the given PxDispatchResults objects.
 * It is a no-op if both PxDispatchResults ptrs are NULL.
 *
 * On return, the caller is expected to finish its own cleanup and
 * exit via PG_RE_THROW().
 */
void
			PxDispatchHandleError(struct PxDispatcherState *ds);

void
			pxdisp_cancelDispatch(PxDispatcherState *ds);

/*
 * Allocate memory and initialize PxDispatcherState.
 *
 * Call pxdisp_destroyDispatcherState to free it.
 */
PxDispatcherState *pxdisp_makeDispatcherState(bool isExtendedQuery);

/*
 * Free memory in PxDispatcherState
 *
 * Free the PPXxpBufferData allocated in libpq.
 * Free dispatcher memory context.
 */
void		pxdisp_destroyDispatcherState(PxDispatcherState *ds);

void pxdisp_makeDispatchParams(PxDispatcherState *ds,
						   int maxSlices,
						   char *queryText,
						   int queryTextLen);

bool		pxdisp_checkForCancel(PxDispatcherState *ds);
int			pxdisp_getWaitSocketFd(PxDispatcherState *ds);

char	   *segmentsToContentStr(List *segments);
void AtAbort_DispatcherState(void);
void pxdisp_cleanupDispatcherHandle(const struct ResourceOwnerData * owner);

/* POLAR px: pq_thread interactive*/
void pxdisp_createPqThread(void);
void pxdisp_startPqThread(PxDispatcherState* ds);
void pxdisp_finishPqThread(void);
bool pxdisp_isDsThreadRuning(void);
/* POLAR px end*/
#endif							/* PXDISP_H */
