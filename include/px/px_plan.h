/*-------------------------------------------------------------------------
 *
 * px_plan.h
 *    Definitions for pxplan.c utilities.
 *
 * Portions Copyright (c) 2004-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/px/px_plan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PXPLAN_H
#define PXPLAN_H

#include "nodes/relation.h"
#include "optimizer/px_walkers.h"

extern Node *plan_tree_mutator(Node *node, Node *(*mutator) (), void *context, bool recurse_into_subplans);

#endif							/* PXPLAN_H */
