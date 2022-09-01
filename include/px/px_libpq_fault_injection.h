/*-------------------------------------------------------------------------
 *
 * px_libpq_fault_injection.h
 *	  px_adps_coordinator_thread fault injection
 *	  SIMPLE_FAULT_INJECTOR can't work in thread
 *	  so make some random error
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
 *     src/include/px/px_libpq_fault_injection.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PXLIBPQFAULTINJECTION_H
#define PXLIBPQFAULTINJECTION_H

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

/* POLAR px */
#include "utils/guc.h"
#include "utils/faultinjector.h"

#include <stdlib.h>
#include <pthread.h>
#include <errno.h>

/* ---------------------------
 * random fault in thread
 * ---------------------------
*/
#define RANDOM_FAULT_MAX 100

/*
 * testmode_poll
 * 		poll function with faults injected.
 */
__attribute__((unused))
static int
testmode_poll(const char *caller_name, struct pollfd fds[], nfds_t nfds,
			  int timeout)
{
	if (px_enable_tcp_testmode)
	{
		int			fault_type;
		fault_type = random() % RANDOM_FAULT_MAX;

		switch (fault_type)
		{
			case 0:
				write_log("inject fault to poll: EINTR");
				errno = EINTR;
				return -1;

			case 1:
				write_log("inject fault to poll: EFAULT");
				errno = EFAULT;
				return -1;

			default:
				break;
		}
	}
	return poll(fds, nfds, timeout);
}

/*
 * testmode_malloc
 * 		malloc with faults injected.
 */
__attribute__((unused))
static void *
testmode_malloc(const char *caller_name, size_t size)
{
	void	   *ret;
	if (px_enable_tcp_testmode)
	{
		int			fault_type;
		fault_type = random() % RANDOM_FAULT_MAX;
		if (fault_type == 0)
		{
			write_log("inject fault to malloc in %s", caller_name);
			errno = ENOMEM;
			return NULL;
		}
	}
	ret = malloc(size);
	return ret;
}

__attribute__((unused))
static int
testmode_pqPacketSend(PGconn *conn, char pack_type, const void *buf, size_t buf_len)
{
	if (px_enable_tcp_testmode)
	{
		int			fault_type;
		fault_type = random() % RANDOM_FAULT_MAX;
		if (fault_type == 0)
		{
			write_log("inject fault to pqPacketSend");
			pqDropConnection(conn, true);
			return STATUS_ERROR;
		}
	}
	return pqPacketSend(conn, pack_type, buf, buf_len);
}

__attribute__((unused))
static int
testmode_pqFlushNonBlocking(PGconn *conn)
{
	if (px_enable_tcp_testmode)
	{
		int			fault_type;
		fault_type = random() % RANDOM_FAULT_MAX;
		if (fault_type == 0)
		{
			write_log("inject fault to pqFlushNonBlocking");
			pqDropConnection(conn, true);
			return STATUS_ERROR;
		}
	}
	return pqFlushNonBlocking(conn);
}

__attribute__((unused))
static int
testmode_PQconsumeInput(PGconn *conn)
{
	if (px_enable_tcp_testmode)
	{
		int			fault_type;
		fault_type = random() % RANDOM_FAULT_MAX;
		if (fault_type == 0)
		{
			write_log("inject fault to PQconsumeInput");
			pqDropConnection(conn, true);
			return 0;
		}
	}
	return PQconsumeInput(conn);
}

__attribute__((unused))
static ConnStatusType
testmode_PQstatus(PGconn *conn)
{
	if (px_enable_tcp_testmode)
	{
		int			fault_type;
		fault_type = random() % RANDOM_FAULT_MAX;
		if (fault_type == 0)
		{
			write_log("inject fault to PQstatus");
			pqDropConnection(conn, true);
			return CONNECTION_BAD;
		}
	}
	return PQstatus(conn);
}

__attribute__((unused))
static ExecStatusType
testmode_PQresultStatus(const PGresult *res)
{
	if (px_enable_tcp_testmode)
	{
		int			fault_type;
		fault_type = random() % RANDOM_FAULT_MAX;
		if (fault_type == 0)
		{
			write_log("inject fault to PQresultStatus");
			return PGRES_FATAL_ERROR;
		}
	}
	return PQresultStatus(res);
}

/* ------------------------------
 * special fault by fault inject
 * ------------------------------
*/

/*
 * testmode_pthread_create
 * 		pthread_create with faults injected.
 */
__attribute__((unused))
static int
testmode_pthread_create(const char *caller_name, pthread_t *thread,
						const pthread_attr_t *attr, void *(*start_routine)(void *), void *arg)
{
	if (SIMPLE_FAULT_INJECTOR("px_pthread_create") == FaultInjectorTypeEnable)
	{
		elog(DEBUG5, "inject fault to pthread_create");
		return ENOMEM;
	}
	return pthread_create(thread, attr, start_routine, arg);
}

__attribute__((unused))
static int
testmode_pq_flush()
{
	if (SIMPLE_FAULT_INJECTOR("px_pq_flush") == FaultInjectorTypeEnable)
	{
		elog(DEBUG5, "inject fault to pq_flush");
		return EOF;
	}
	return pq_flush();
}

__attribute__((unused))
static int
testmode_pq_getbyte()
{
	if (SIMPLE_FAULT_INJECTOR("px_pq_getbyte") == FaultInjectorTypeEnable)
	{
		elog(DEBUG5, "inject fault to pq_getbyte");
		return EOF;
	}
	return pq_getbyte();
}

__attribute__((unused))
static int
testmode_pq_getmessage(StringInfo s, int len)
{
	if (SIMPLE_FAULT_INJECTOR("px_pq_getmessage") == FaultInjectorTypeEnable)
	{
		elog(DEBUG5, "inject fault to pq_getmessage");
		return EOF;
	}
	return pq_getmessage(s, len);
}

#undef poll
#undef malloc
#undef pthread_create
#undef pq_flush
#undef pq_getbyte
#undef pq_getmessage
#undef pqPacketSend
#undef pqFlushNonBlocking
#undef PQconsumeInput
#undef PQstatus
#undef PQresultStatus

#define poll(fds, nfds, timeout) \
	testmode_poll(PG_FUNCNAME_MACRO, fds, nfds, timeout)

#define malloc(size) \
	testmode_malloc(PG_FUNCNAME_MACRO, size)

#define pthread_create(thread, attr, start_routine, arg) \
	testmode_pthread_create(PG_FUNCNAME_MACRO, thread, attr, start_routine, arg)

#define pq_flush() \
    testmode_pq_flush()

#define pq_getbyte() \
    testmode_pq_getbyte()

#define pq_getmessage(s, len) \
    testmode_pq_getmessage(s, len)

#define pqPacketSend(conn, type, buf, len) \
    testmode_pqPacketSend(conn, type, buf, len)

#define pqFlushNonBlocking(conn) \
	testmode_pqFlushNonBlocking(conn)

#define PQconsumeInput(conn) \
	testmode_PQconsumeInput(conn)

#define PQstatus(conn) \
	testmode_PQstatus(conn)

#define PQresultStatus(res) \
	testmode_PQresultStatus(res)

#endif
