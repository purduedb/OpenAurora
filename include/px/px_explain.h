/*-------------------------------------------------------------------------
 *
 * px_explain.h
 *    Functions supporting the Greenplum EXPLAIN ANALYZE command
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * IDENTIFICATION
 *	    src/include/px/px_explain.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PXEXPLAIN_H
#define PXEXPLAIN_H

#include "executor/instrument.h"        /* instr_time */

struct PxDispatchResults;              /* #include "px/px_dispatchresult.h" */
struct PlanState;                       /* #include "nodes/execnodes.h" */
struct QueryDesc;                       /* #include "executor/execdesc.h" */

struct PxExplain_ShowStatCtx;          /* private, in "px/px_explain.c" */

typedef struct
{
	double		vmax;			/* maximum value of statistic */
	double		vsum;			/* sum of values */
	int			vcnt;			/* count of values > 0 */
	int			imax;			/* id of 1st observation having maximum value */
} PxExplain_Agg;

static inline void
pxexplain_agg_init0(PxExplain_Agg *agg)
{
    agg->vmax = 0;
    agg->vsum = 0;
    agg->vcnt = 0;
    agg->imax = 0;
}

static inline bool
pxexplain_agg_upd(PxExplain_Agg *agg, double v, int id)
{
    if (v > 0)
    {
        agg->vsum += v;
        agg->vcnt++;

        if (v > agg->vmax ||
            agg->vcnt == 0)
        {
            agg->vmax = v;
            agg->imax = id;
            return true;
        }
    }
    return false;
}

static inline double
pxexplain_agg_avg(PxExplain_Agg *agg)
{
    return (agg->vcnt > 0) ? agg->vsum / agg->vcnt
                           : 0;
}


/*
 * pxexplain_localExecStats
 *    Called by qDisp to build NodeSummary and SliceSummary blocks
 *    containing EXPLAIN ANALYZE statistics for a root slice that
 *    has been executed locally in the qDisp process.  Attaches these
 *    structures to the PlanState nodes' Instrumentation objects for
 *    later use by cdbexplain_showExecStats().
 *
 * 'planstate' is the top PlanState node of the slice.
 * 'showstatctx' is a PxExplain_ShowStatCtx object which was created by
 *      calling pxexplain_showExecStatsBegin().
 */
void
pxexplain_localExecStats(struct PlanState                 *planstate,
                          struct PxExplain_ShowStatCtx    *showstatctx);

/*
 * pxexplain_sendExecStats
 *    Called by qExec process to send EXPLAIN ANALYZE statistics to qDisp.
 *    On the qDisp, libpq will attach this data to the PGresult object.
 */
void
pxexplain_sendExecStats(struct QueryDesc *queryDesc);

/*
 * pxexplain_recvExecStats
 *    Called by qDisp to transfer a slice's EXPLAIN ANALYZE statistics
 *    from the CdbDispatchResults structures to the PlanState tree.
 *    Recursively does the same for slices that are descendants of the
 *    one specified.
 *
 * 'showstatctx' is a PxExplain_ShowStatCtx object which was created by
 *      calling pxexplain_showExecStatsBegin().
 */
void
pxexplain_recvExecStats(struct PlanState              *planstate,
                         struct PxDispatchResults     *dispatchResults,
                         int                            sliceIndex,
                         struct PxExplain_ShowStatCtx *showstatctx);

/*
 * pxexplain_showExecStatsBegin
 *    Called by qDisp process to create a PxExplain_ShowStatCtx structure
 *    in which to accumulate overall statistics for a query.
 *
 * 'explaincxt' is a MemoryContext from which to allocate the ShowStatCtx as
 *      well as any needed buffers and the like.  The explaincxt ptr is saved
 *      in the ShowStatCtx.  The caller is expected to reset or destroy the
 *      explaincxt not too long after calling cdbexplain_showExecStatsEnd(); so
 *      we don't bother to pfree() memory that we allocate from this context.
 * 'querystarttime' is the timestamp of the start of the query, in a
 *      platform-dependent format.
 */
struct PxExplain_ShowStatCtx *
pxexplain_showExecStatsBegin(struct QueryDesc *queryDesc,
                              instr_time        querystarttime);



#endif   /* PXEXPLAIN_H */
