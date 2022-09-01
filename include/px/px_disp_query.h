/*-------------------------------------------------------------------------
 *
 * px_disp_query.h
 *	  routines for dispatching command string or plan to the qExec processes.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * IDENTIFICATION
 *	  src/include/px/px_disp_query.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PXDISP_QUERY_H
#define PXDISP_QUERY_H

#include "lib/stringinfo.h"		/* StringInfo */
#include "nodes/params.h"
#include "nodes/pg_list.h"
#include "executor/execdesc.h"

/*
 * indicate whether an error occurring on one of the qExec segdbs should cause all still-executing
 * commands to cancel on other qExecs, normally this would be true.
 */
#define DF_CANCEL_ON_ERROR 0x1
/*
 * indicate whether the command to be dispatched should be done inside of a global transaction.
 */
#define DF_NEED_TWO_PHASE 0x2
/*
 * indicate whether the command should be dispatched to qExecs along with a snapshot.
 */
#define DF_WITH_SNAPSHOT  0x4

struct QueryDesc;
struct PxDispatcherState;
struct PxPgResults;

/* Compose and dispatch the POLARPX commands corresponding to a plan tree
 * within a complete parallel plan.
 *
 * The PxDispatchResults objects allocated for the plan are
 * returned in *pPrimaryResults
 * The caller, after calling PxCheckDispatchResult(), can
 * examine the PxDispatchResults objects, can keep them as
 * long as needed, and ultimately must free them with
 * pxdisp_destroyDispatchState() prior to deallocation
 * of the caller's memory context.
 *
 * NB: Callers should use PG_TRY()/PG_CATCH() if needed to make
 * certain that the PxDispatchResults objects are destroyed by
 * pxdisp_destroyDispatchState() in case of error.
 * To wait for completion, check for errors, and clean up, it is
 * suggested that the caller use pxdisp_finishCommand().
 */
void PxDispatchPlan(struct QueryDesc *queryDesc,
				bool planRequiresTxn,
				bool cancelOnError);

extern ParamListInfo deserializeParamListInfo(const char *str, int slen);

void px_log_querydesc(QueryDispatchDesc *ddesc);
#endif							/* PXDISP_QUERY_H */
