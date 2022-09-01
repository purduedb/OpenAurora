/*-------------------------------------------------------------------------
 *
 * px_gang_async.h
 *	  Routines for asynchronous implementation of creating gang.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * IDENTIFICATION
 *    src/include/px/px_gang_async.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PXGANG_ASYNC_H
#define PXGANG_ASYNC_H

#include "px/px_gang.h"

extern Gang *pxgang_createGang_async(List *segments, SegmentType segmentType);

#endif
