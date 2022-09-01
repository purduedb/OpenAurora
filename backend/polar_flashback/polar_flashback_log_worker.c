/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_worker.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_log_worker.c
 *
 *-------------------------------------------------------------------------
 */
#include <unistd.h>

#include "postgres.h"

#include "access/xlog.h"
#include "pgstat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "polar_flashback/polar_flashback_log.h"
#include "polar_flashback/polar_flashback_log_index.h"
#include "polar_flashback/polar_flashback_log_worker.h"
#include "polar_flashback/polar_flashback_log_list.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/polar_procpool.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/polar_coredump.h"
#include "utils/resowner.h"

/* GUCs */
int polar_flashback_log_bgwrite_delay;
int polar_flashback_log_insert_list_delay;
int polar_flashback_log_insert_list_max_num;
int polar_flashback_log_flush_max_size;

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t shutdown_requested = false;
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

void
polar_flog_bgwriter_main(void)
{
	sigjmp_buf  local_sigjmp_buf;
	MemoryContext bgworker_context;

	/*
	 * Properly accept or ignore signals the postmaster might send us.
	 *
	 * bgwriter doesn't participate in ProcSignal signalling, but a SIGUSR1
	 * handler is still needed for latch wakeups.
	 */
	pqsignal(SIGHUP, bg_sighup_handler);    /* set flag to read config file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, bg_shutdown_handler); /* shutdown */
	pqsignal(SIGQUIT, polar_bg_quickdie); /* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, polar_bg_sigusr1_handler);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

	/* POLAR : register for coredump print */
#ifndef _WIN32
#ifdef SIGILL
	pqsignal(SIGILL, polar_program_error_handler);
#endif
#ifdef SIGSEGV
	pqsignal(SIGSEGV, polar_program_error_handler);
#endif
#ifdef SIGBUS
	pqsignal(SIGBUS, polar_program_error_handler);
#endif
#endif  /* _WIN32 */
	/* POLAR: end */

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/*
	 * Create a resource owner to keep track of our resources (currently only
	 * buffer pins).
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "FlogBgWriter");

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	bgworker_context = AllocSetContextCreate(TopMemoryContext,
											 "Flashback Log Background Writer",
											 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(bgworker_context);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/*no cover begin*/
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * These operations are really just a minimal subset of
		 * AbortTransaction().  We don't have very many resources to worry
		 * about in bgwriter, but we do have LWLocks, buffers, and temp files.
		 */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		AbortBufferIO();
		UnlockBuffers();
		/* buffer pins are released here: */
		ResourceOwnerRelease(CurrentResourceOwner,
							 RESOURCE_RELEASE_BEFORE_LOCKS,
							 false, true);
		/* we needn't bother with the other ResourceOwnerRelease phases */
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(bgworker_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(bgworker_context);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);

		/*
		 * Close all open files after any error.  This is helpful on Windows,
		 * where holding deleted files open causes various strange errors.
		 * It's not clear we need it elsewhere, but shouldn't hurt.
		 */
		smgrcloseall();

		/* Report wait end here, when there is no further possibility of wait */
		pgstat_report_wait_end();
		/*no cover end*/
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	elog(LOG, "PolarDB start flashback log background writer success");

	for (;;)
	{
		int rc;
		polar_flog_rec_ptr ptr_expected;
		flog_buf_ctl_t buf_ctl = flog_instance->buf_ctl;
		logindex_snapshot_t snapshot = flog_instance->logindex_snapshot;
		flog_index_queue_ctl_t queue_ctl = flog_instance->queue_ctl;

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (shutdown_requested)
		{
			elog(LOG, "PolarDB exit flashback log background writer");
			/*
			 * From here on, elog(ERROR) should end with exit(1), not send
			 * control back to the sigsetjmp block above
			 */
			ExitOnAnyError = true;
			/* Normal exit is here */
			proc_exit(0);       /* done */
		}

		/* Wait for the startup of the flashback log */
		if (polar_has_flog_startup(flog_instance))
		{
			/* Recover the flashback service */
			if (!polar_is_flog_ready(flog_instance))
				polar_recover_flog(flog_instance);

			ptr_expected = polar_flog_flush_bg(buf_ctl);

			if (!FLOG_REC_PTR_IS_INVAILD(ptr_expected))
			{
				polar_flog_index_insert(snapshot, queue_ctl, buf_ctl, ptr_expected, ANY);
				polar_logindex_flush_table(snapshot, ptr_expected);
			}
		}

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   polar_flashback_log_bgwrite_delay /* ms */, WAIT_EVENT_FLOG_WRITE_BG_MAIN);

		/*
		 * Emergency bailout if postmaster has died. This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			exit(1);
	}
}

void
polar_flog_bginserter_main(void)
{
	sigjmp_buf  local_sigjmp_buf;
	MemoryContext bgworker_context;
	flog_buf_ctl_t buf_ctl = flog_instance->buf_ctl;
	flog_list_ctl_t list_ctl = flog_instance->list_ctl;
	flog_index_queue_ctl_t queue_ctl = flog_instance->queue_ctl;

	/*
	 * Properly accept or ignore signals the postmaster might send us.
	 *
	 * bgwriter doesn't participate in ProcSignal signalling, but a SIGUSR1
	 * handler is still needed for latch wakeups.
	 */
	pqsignal(SIGHUP, bg_sighup_handler);    /* set flag to read config file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, bg_shutdown_handler); /* shutdown */
	pqsignal(SIGQUIT, polar_bg_quickdie); /* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, polar_bg_sigusr1_handler);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

	/* POLAR : register for coredump print */
#ifndef _WIN32
#ifdef SIGILL
	pqsignal(SIGILL, polar_program_error_handler);
#endif
#ifdef SIGSEGV
	pqsignal(SIGSEGV, polar_program_error_handler);
#endif
#ifdef SIGBUS
	pqsignal(SIGBUS, polar_program_error_handler);
#endif
#endif  /* _WIN32 */
	/* POLAR: end */

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/*
	 * Create a resource owner to keep track of our resources (currently only
	 * buffer pins).
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "FlogBgInserter");

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	bgworker_context = AllocSetContextCreate(TopMemoryContext,
											 "Flashback Log Background Inserter",
											 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(bgworker_context);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/*no cover begin*/
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * These operations are really just a minimal subset of
		 * AbortTransaction().  We don't have very many resources to worry
		 * about in bgwriter, but we do have LWLocks, buffers, and temp files.
		 */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		AbortBufferIO();
		UnlockBuffers();
		/* buffer pins are released here: */
		ResourceOwnerRelease(CurrentResourceOwner,
							 RESOURCE_RELEASE_BEFORE_LOCKS,
							 false, true);
		/* we needn't bother with the other ResourceOwnerRelease phases */
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(bgworker_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(bgworker_context);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);

		/*
		 * Close all open files after any error.  This is helpful on Windows,
		 * where holding deleted files open causes various strange errors.
		 * It's not clear we need it elsewhere, but shouldn't hurt.
		 */
		smgrcloseall();

		/* Report wait end here, when there is no further possibility of wait */
		pgstat_report_wait_end();
		/*no cover end*/
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	elog(LOG, "PolarDB start flashback log background inserter success");

	for (;;)
	{
		int rc;
		int head;
		int tail;
		int insert_num;

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (shutdown_requested)
		{
			elog(LOG, "PolarDB exit flashback log background inserter");
			/*
			 * From here on, elog(ERROR) should end with exit(1), not send
			 * control back to the sigsetjmp block above
			 */
			ExitOnAnyError = true;
			/* Normal exit is here */
			proc_exit(0);       /* done */
		}

		insert_num = 0;
		do
		{
			polar_insert_flog_rec_from_list_bg(list_ctl, buf_ctl, queue_ctl);
			polar_flog_get_async_list_info(list_ctl, &head, &tail);
			insert_num++;
		}
		while (head != NOT_IN_FLOG_LIST && !shutdown_requested &&
				insert_num <= polar_flashback_log_insert_list_max_num);

		/* Just wait 10ms */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   polar_flashback_log_insert_list_delay /* ms */, WAIT_EVENT_FLOG_INSERT_BG_MAIN);

		/*
		 * Emergency bailout if postmaster has died. This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			exit(1);
	}
}
