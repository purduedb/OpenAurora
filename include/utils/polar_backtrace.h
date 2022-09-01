/*----------------------------------------------------------------------------------
 *
 * polar_backtrace.h
 *      Print callstack of the current process
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
 *      src/include/utils/polar_backtrace.h
 *
 *----------------------------------------------------------------------------------
 */

#ifndef POLAR_BACKTRACE_H
#define POLAR_BACKTRACE_H

#include <execinfo.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define POLAR_BACKTRACE_DEPTH (100)
#define POLAR_MAX_BACKTRACE_LEN (1024)

#define POLAR_LOG_BACKTRACE() \
	{ \
		void *stack_address[POLAR_BACKTRACE_DEPTH]; \
		char stack[POLAR_MAX_BACKTRACE_LEN]; \
		char *p = stack; \
		char **symbol = NULL; \
		int i = 0, nsym; \
		int size = POLAR_MAX_BACKTRACE_LEN - 1; \
		\
		nsym = backtrace(stack_address, POLAR_BACKTRACE_DEPTH); \
		if (nsym > 0) \
			symbol = backtrace_symbols(stack_address, nsym); \
		\
		while (i < nsym && size > 0) \
		{\
			int ret = snprintf(p, size, "%s\n", symbol[i++]); \
			size -= ret; \
			p += ret;\
		} \
		if (symbol) \
			free(symbol); \
		elog(LOG, "%s", stack); \
	}

#endif
