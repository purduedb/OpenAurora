/*-------------------------------------------------------------------------
 *
 * polar_logindex_bg_worker.c
 *  
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
 *    src/backend/access/logindex/polar_logindex_bg_worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/polar_logindex.h"
#include "access/polar_logindex_redo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procarray.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/polar_coredump.h"
#include "utils/resowner.h"

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t shutdown_requested = false;
static volatile sig_atomic_t polar_online_promote_req = false;
/* Signal handlers */


/* SIGHUP: set flag to re-read config file at next convenient time */
static void
bg_sighup_handler(SIGNAL_ARGS)
{
	int         save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGTERM: set flag to shutdown and exit */
static void
bg_shutdown_handler(SIGNAL_ARGS)
{
	int         save_errno = errno;

	shutdown_requested = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* POLAR: SIGUSR2 : used for online promote */
static void
online_promote_trigger(SIGNAL_ARGS)
{
	int save_errno = errno;

	polar_online_promote_req = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


static void
bgworker_handle_online_promote(polar_logindex_redo_ctl_t instance)
{
	XLogRecPtr bg_lsn;

	/*
	 * Only run polar_logindex_redo_online_promote when promoting replica.
	 *
	 * Standby promoting doesn't need it to change logindex state, which is
	 * originally writable.
	 */
	polar_logindex_redo_online_promote(instance);
	bg_lsn = polar_bg_redo_get_replayed_lsn(instance);

	elog(LOG, "Before online promote bg_replayed_lsn=%lX", bg_lsn);

	polar_set_bg_redo_state(instance, POLAR_BG_WAITING_RESET);

	/* POLAR: Notify startup process that background replay state is changed */
	WakeupRecovery();
}

static void
set_logindex_bg_worker_latch(polar_logindex_redo_ctl_t instance)
{
	Assert(MyLatch);

	instance->bg_worker_latch = MyLatch;
	polar_logindex_set_writer_latch(instance->wal_logindex_snapshot, MyLatch);

	if (instance->fullpage_logindex_snapshot)
		polar_logindex_set_writer_latch(instance->fullpage_logindex_snapshot, MyLatch);
}

static polar_logindex_bg_redo_ctl_t *
create_logindex_bg_redo_ctl(polar_logindex_redo_ctl_t instance)
{
	polar_logindex_bg_redo_ctl_t *bg_redo_ctl = NULL;

	do
	{
		uint32 state;

		if (!polar_need_do_bg_replay(polar_logindex_redo_instance))
			break;

		state = polar_get_bg_redo_state(polar_logindex_redo_instance);

		switch (state)
		{
			case POLAR_BG_RO_BUF_REPLAYING:
			{
				bg_redo_ctl = polar_create_bg_redo_ctl(polar_logindex_redo_instance, false);
				polar_bg_replaying_process = POLAR_LOGINDEX_DISPATCHER;
				break;
			}

			case POLAR_BG_PARALLEL_REPLAYING:
			case POLAR_BG_ONLINE_PROMOTE:
			{
				bg_redo_ctl = polar_create_bg_redo_ctl(polar_logindex_redo_instance, true);
				polar_bg_replaying_process = POLAR_LOGINDEX_DISPATCHER;
				break;
			}

			case POLAR_BG_REDO_NOT_START:
			{
				if (polar_online_promote_req)
					return bg_redo_ctl;

				/* Else run the next case to WaitLatch and then check whether startup set new background redo state */
			}

			case POLAR_BG_WAITING_RESET:
			{
				int rc = WaitLatch(MyLatch,
								   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
								   100 /* ms */, WAIT_EVENT_LOGINDEX_BG_MAIN);

				if (rc & WL_POSTMASTER_DEATH)
					exit(1);

				ResetLatch(MyLatch);
				break;
			}

			default:
				elog(PANIC, "Got unexpected bg_redo_state=%d", state);
		}

		CHECK_FOR_INTERRUPTS();
	}
	while (bg_redo_ctl == NULL && !shutdown_requested);

	return bg_redo_ctl;
}

//updated
void
polar_logindex_bg_worker_main(void)
{

}
