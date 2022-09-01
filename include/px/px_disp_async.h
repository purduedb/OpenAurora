/*-------------------------------------------------------------------------
 *
 * px_disp_async.h
 * routines for asynchronous implementation of dispatching commands
 * to the qExec processes.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * IDENTIFICATION
 *	    src/include/px/px_disp_async.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PXDISP_ASYNC_H
#define PXDISP_ASYNC_H

extern DispatcherInternalFuncs DispatcherAsyncFuncs;

typedef struct PxDispatchCmdAsync
{
	/*
	 * dispatchResultPtrArray: Array[0..dispatchCount-1] of PxDispatchResult*
	 * Each PxDispatchResult object points to a PxWorkerDescriptor
	 * that dispatcher will send the command to.
	 */
	struct PxDispatchResult **dispatchResultPtrArray;

	/* Number of px workers dispatched */
	int			dispatchCount;

	/*
	 * Depending on this mode, we may send query cancel or query finish
	 * message to PX while we are waiting it to complete.  NONE means we
	 * expect PX to complete without any instruction.
	 */
	volatile	DispatchWaitMode waitMode;

	/*
	 * Text information to dispatch: The format is type(1 byte) + length(size
	 * of int) + content(n bytes)
	 */
	char	   *query_text;
	int			query_text_len;

} PxDispatchCmdAsync;

#endif
