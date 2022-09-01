/*-------------------------------------------------------------------------
 *
 * polar_sql_time_stat.h
 *	  including sql time stat
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *    src/include/utils/polar_sql_time_stat.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POLAR_SQL_TIME_STAT_H
#define POLAR_SQL_TIME_STAT_H

#include "postgres.h"

#include "portability/instr_time.h"

typedef enum sqlLifeStage
{
	SQL_PARSE = 0,
	SQL_ANALYZE,
	QUERY_REWRITE,
	SQL_PLAN,
	SQL_EXECUTE
}sqlLifeStage;

/* sql time stat */
typedef struct polar_sql_time_stat
{
	uint64			parse_time;	  	/* parse time in micro second */
	uint64			analyze_time;   /* analyze time in micro second */
	uint64			rewrite_time;   /* rewrite time in micro second */
	uint64			plan_time;		/* plan time in micro second */
	uint64			execute_time;   /* execute time in micro second */
} polar_sql_time_stat;

extern polar_sql_time_stat polar_sql_time_stat_local_summary;
extern void polar_init_sql_time_local_stats(void);
extern void polar_sql_stat_set_time(void);
extern void polar_sql_stat_record_time(sqlLifeStage sql_stage);

#endif  /* POLAR_SQL_TIME_STAT_H */