/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_repair_page.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_log_repair_page.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/polar_logindex_redo.h"
#include "miscadmin.h"
#include "polar_flashback/polar_flashback_log.h"
#include "polar_flashback/polar_flashback_log_decoder.h"
#include "polar_flashback/polar_flashback_log_index.h"
#include "polar_flashback/polar_flashback_log_repair_page.h"
#include "polar_flashback/polar_flashback_point.h"
#include "postmaster/startup.h"

bool polar_has_partial_write;
/* POLAR: Just crash will cause partial write */
static bool
may_be_partial_write(void)
{
	return AmStartupProcess() && !reachedConsistency;
}

/*
 * POLAR: Write the repaired buffer.
 *
 * NB: The buffer must be invaild, so write it without
 * any lock is safe.
 */
static void
write_repaired_buf(BufferDesc *buf)
{
	SMgrRelation reln;
	Block       buf_block;
	char       *buf_write;

	Assert(pg_atomic_read_u32(&buf->polar_redo_state) & POLAR_BUF_FLOG_DISABLE);

	buf_block = BufHdrGetBlock(buf);
	buf_write = PageEncryptCopy((Page) buf_block, buf->tag.forkNum,
								buf->tag.blockNum);
	buf_write = PageSetChecksumCopy((Page) buf_write, buf->tag.blockNum);
	reln = smgropen(buf->tag.rnode, InvalidBackendId);
	smgrwrite(reln,
			  buf->tag.forkNum,
			  buf->tag.blockNum,
			  buf_write,
			  false);
}

/*
 * POLAR: Get origin page to solve partial write problem.
 *
 * tag: The buffer tag.
 * page: The origin page.
 * replay_start_lsn: The replay start LSN.
 */
static bool
get_origin_page_for_partial_write(flog_ctl_t instance, Buffer *buf, BufferTag *tag)
{
	polar_flog_rec_ptr start_ptr;
	polar_flog_rec_ptr end_ptr;
	XLogRecPtr start_lsn;
	XLogRecPtr end_lsn;
	flog_buf_ctl_t buf_ctl = instance->buf_ctl;

	start_ptr = polar_get_fbpoint_start_ptr(buf_ctl);
	end_ptr = polar_get_flog_write_result(buf_ctl);
	Assert(end_ptr >= start_ptr);

	/* There is no flashback log */
	if (FLOG_REC_PTR_IS_INVAILD(end_ptr))
		return false;

	start_lsn = polar_get_curr_fbpoint_lsn(buf_ctl);
	end_lsn = GetRedoRecPtr();

	return polar_flashback_buffer(instance, buf, tag, start_ptr, end_ptr, start_lsn, end_lsn, ERROR, true);
}

/*
 * POLAR: The flashback log can repair the PERMANENT buffer
 * when it meet a partial write.
 */
bool
polar_can_flog_repair(flog_ctl_t instance, BufferDesc *buf_hdr, bool has_redo_action)
{
	uint32  buf_state;

	if (!polar_is_flog_enabled(instance))
		return false;

	if (may_be_partial_write() || has_redo_action)
	{
		buf_state = pg_atomic_read_u32(&buf_hdr->state);
		return buf_state & BM_PERMANENT;
	}

	return false;
}

/*
 * To repair the partial write problem.
 * Partial write problem will occur in three scenarios:
 * 1. RW crash recovery.
 * 2. Standby crash recovery.
 * 3. RO to RW online promote.
 */
void
polar_repair_partial_write(flog_ctl_t instance, BufferDesc *bufHdr)
{
	BufferTag *tag = &bufHdr->tag;
	Buffer buf;

	Assert((pg_atomic_read_u32(&bufHdr->state) & BM_VALID) == 0);
	buf = bufHdr->buf_id + 1;

	/* Wait for the flashback logindex ready */
	while (!polar_is_flog_ready(instance))
	{
		/* Handle interrupt signals of startup process to avoid hang */
		if (AmStartupProcess())
			HandleStartupProcInterrupts();
		else
			CHECK_FOR_INTERRUPTS();

		pg_usleep(1000L);
	}

	if (!get_origin_page_for_partial_write(instance, &buf, tag))
	{
		elog(ERROR, "Can't find a valid origin page for ([%u, %u, %u]), %u, %u from flashback log",
			 tag->rnode.spcNode,
			 tag->rnode.dbNode,
			 tag->rnode.relNode,
			 tag->forkNum,
			 tag->blockNum);
	}
	else
	{
		/* Flush the buffer to protect the next first modify after checkpoint. */
		write_repaired_buf(bufHdr);

		elog(LOG, "The page ([%u, %u, %u]), %u, %u has been repaired by flashback log",
			 tag->rnode.spcNode,
			 tag->rnode.dbNode,
			 tag->rnode.relNode,
			 tag->forkNum,
			 tag->blockNum);
	}
}
