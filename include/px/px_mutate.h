/*-------------------------------------------------------------------------
 *
 * px_mutate.h
 *	  Definitions for pxmutate.c utilities.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/px/px_mutate.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PXMUTATE_H
#define PXMUTATE_H

#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "optimizer/px_walkers.h"

extern void collect_shareinput_producers(PlannerInfo *root, Plan *plan);
extern Plan *replace_shareinput_targetlists(PlannerInfo *root, Plan *plan);
extern Plan *apply_shareinput_xslice(Plan *plan, PlannerInfo *root);

extern Plan *replace_shareinput_targetlists(PlannerInfo *root, Plan *plan);

extern int32 pxhash_const_list(List *plConsts, int iSegments, Oid *hashfuncs);

extern Node *exec_make_plan_constant(struct PlannedStmt *stmt, EState *estate,
						bool is_SRI, List **cursorPositions);
extern void remove_subquery_in_RTEs(Node *node);

extern bool contains_outer_params(Node *node, void *context);

extern Node *makePXWorkerIndexFilterExpr(int segid);

#endif							/* PXMUTATE_H */
