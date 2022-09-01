/*-------------------------------------------------------------------------
 *
 * px_llize.h
 *    Definitions for parallelizing a PostgreSQL sequential plan tree.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/px/px_llize.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PXLLIZE_H
#define PXLLIZE_H

#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"

extern bool is_plan_node(Node *node);

#endif							/* PXLLIZE_H */
