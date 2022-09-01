/*-------------------------------------------------------------------------
 *
 * px_def.h
 *	 Definitions for use anywhere
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 *
 * IDENTIFICATION
 *	    src/include/px/px_def.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PXDEF_H
#define PXDEF_H

/*
 * PxVisitOpt
 *      Some tree walkers use these codes to direct the traversal.
 */
typedef enum
{
	PxVisit_Walk = 1,			/* proceed in normal sequence */
	PxVisit_Skip,				/* no more calls for current node or its kids */
	PxVisit_Stop,				/* break out of traversal, no more callbacks */
	PxVisit_Failure,			/* break out of traversal, no more callbacks */
	PxVisit_Success			/* break out of traversal, no more callbacks */
}			PxVisitOpt;

#endif							/* PXDEF_H */
