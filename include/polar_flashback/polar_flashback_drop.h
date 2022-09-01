/*-------------------------------------------------------------------------
 *
 * polar_flashback_drop.h
 *
 *
 * Copyright (c) 2020-2021, Alibaba-inc PolarDB Group
 *
 * src/include/polar_flashback/polar_flashback_drop.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_FLASHBACK_DROP_H
#define POLAR_FLASHBACK_DROP_H



#include "nodes/params.h"
#include "nodes/plannodes.h"

#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/queryenvironment.h"

#define RECYCLEBINNAME "recyclebin"
#define LENGTH 30

extern void polar_flashback_drop(PlannedStmt *pstmt,
								 const char *queryString,
								 ProcessUtilityContext context,
								 ParamListInfo params,
								 QueryEnvironment *queryEnv,
								 DestReceiver *dest,
								 char *completionTag);

extern void polar_flashback_recover_table(PlannedStmt *pstmt,
								  		  const char *queryString,
								  		  ProcessUtilityContext context,
								  		  ParamListInfo params,
								  		  QueryEnvironment *queryEnv,
								  		  DestReceiver *dest,
								  		  char *completionTag);

extern bool polar_flashback_drop_process_utility(PlannedStmt *pstmt,
													const char *queryString,
													ProcessUtilityContext context,
													ParamListInfo params,
													QueryEnvironment *queryEnv,
													DestReceiver *dest,
													char *completionTag,
													bool ishook);

#endif
