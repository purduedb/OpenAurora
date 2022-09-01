/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_list.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_log_list.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/polar_logindex_redo.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "polar_flashback/polar_flashback_log_insert.h"
#include "polar_flashback/polar_flashback_log_list.h"
#include "polar_flashback/polar_flashback_point.h"
#include "port/atomics.h"
#include "postmaster/startup.h"
#include "storage/buf_internals.h"
#include "utils/timestamp.h"

/* GUCs */
int polar_flashback_log_sync_buf_timeout;

/*
 * POLAR: Clean the origin buffer bit.
 *
 * Please make sure that the origin buffer index is valid.
 */
void
polar_clean_origin_buf_bit(flog_list_ctl_t ctl, int buf_id, int8 origin_buf_index)
{
	int array_id = get_origin_buf_array_id(origin_buf_index);

	/* Clean the buffer and release */
	MemSet(ctl->origin_buf + origin_buf_index * BLCKSZ, 0, BLCKSZ);
	CLEAR_BUFFERTAG(ctl->buf_tag[origin_buf_index]);
	Assert(((get_origin_buf_bit(ctl, origin_buf_index)) & 1) == 1);
	pg_atomic_fetch_and_u32(&ctl->origin_buf_bitmap[array_id], ~((uint32) 1 << origin_buf_index));
	Assert(((get_origin_buf_bit(ctl, origin_buf_index)) & 1) == 0);

	Assert(ctl->flashback_list[buf_id].origin_buf_index != INVAILD_ORIGIN_BUF_INDEX);
	ctl->flashback_list[buf_id].origin_buf_index = INVAILD_ORIGIN_BUF_INDEX;
}

/*
 * Add one buffer to flashback log list.
 *
 * NB: The caller must guarantee only one can insert the same buffer once.
 */
static void
add_buf_to_list(int buf_id, uint8 info, flog_list_ctl_t list_ctl, flog_buf_ctl_t buf_ctl)
{
	Buffer prev;
	XLogRecPtr lsn;
	int8 origin_buf_index;

	Assert(buf_id < NBuffers && buf_id >= 0);
	Assert(list_ctl->flashback_list[buf_id].info == FLOG_LIST_SLOT_EMPTY);

	lsn = polar_get_prior_fbpoint_lsn(buf_ctl);
	Assert(!XLogRecPtrIsInvalid(lsn));
	list_ctl->flashback_list[buf_id].redo_lsn = lsn;
	list_ctl->flashback_list[buf_id].fbpoint_lsn = polar_get_local_fbpoint_lsn(buf_ctl, InvalidXLogRecPtr, InvalidXLogRecPtr);
	list_ctl->flashback_list[buf_id].info |= info;

	origin_buf_index = list_ctl->flashback_list[buf_id].origin_buf_index;

	if (origin_buf_index != INVAILD_ORIGIN_BUF_INDEX)
	{
		BufferDesc *buf_hdr = GetBufferDescriptor(buf_id);

		if (!BUFFERTAGS_EQUAL(buf_hdr->tag, list_ctl->buf_tag[origin_buf_index]))
		{
			polar_clean_origin_buf_bit(list_ctl, buf_id, origin_buf_index);
		}
	}

	SpinLockAcquire(&list_ctl->info_lck);
	list_ctl->flashback_list[buf_id].prev_buf = prev = list_ctl->tail;
	list_ctl->flashback_list[buf_id].next_buf = NOT_IN_FLOG_LIST;

	if (prev == NOT_IN_FLOG_LIST)
		list_ctl->head = buf_id;
	else
		list_ctl->flashback_list[prev].next_buf = buf_id;

	list_ctl->tail = buf_id;
	SpinLockRelease(&list_ctl->info_lck);

	pg_atomic_fetch_add_u64(&list_ctl->insert_total_num, 1);

	if (unlikely(polar_flashback_log_debug))
	{
		BufferDesc *buf_hdr = GetBufferDescriptor(buf_id);

		elog(LOG, "Insert the page [%u, %u, %u], %u, %u in buffer %d into flashback log async list, "
			 "its prev is %d, its redo lsn is %ld",
			 buf_hdr->tag.rnode.spcNode,
			 buf_hdr->tag.rnode.dbNode,
			 buf_hdr->tag.rnode.relNode,
			 buf_hdr->tag.forkNum,
			 buf_hdr->tag.blockNum,
			 buf_id, prev, lsn);
	}
}

static void
remove_buf_from_list(int buf_id, flog_list_ctl_t ctl)
{
	int prev;
	int next;

	Assert(buf_id < NBuffers && buf_id >= 0);

	ctl->flashback_list[buf_id].info = FLOG_LIST_SLOT_EMPTY;
	ctl->flashback_list[buf_id].redo_lsn = InvalidXLogRecPtr;

	SpinLockAcquire(&ctl->info_lck);
	prev = ctl->flashback_list[buf_id].prev_buf;
	next = ctl->flashback_list[buf_id].next_buf;

	if (ctl->head == buf_id)
		ctl->head = next;
	else
	{
		Assert(prev != NOT_IN_FLOG_LIST);
		ctl->flashback_list[prev].next_buf = next;
	}

	if (ctl->tail == buf_id)
		ctl->tail = prev;
	else
	{
		Assert(next != NOT_IN_FLOG_LIST);
		ctl->flashback_list[next].prev_buf = prev;
	}

	SpinLockRelease(&ctl->info_lck);

	ctl->flashback_list[buf_id].prev_buf = NOT_IN_FLOG_LIST;
	ctl->flashback_list[buf_id].next_buf = NOT_IN_FLOG_LIST;

	/* Make sure the insert of the slot becomes visible to others. */
	pg_write_barrier();
	pg_atomic_fetch_add_u64(&ctl->remove_total_num, 1);

	if (unlikely(polar_flashback_log_debug))
		elog(LOG, "Remove the buffer(%d) from flashback log list, "
			 "its prev is %d, its next is %d. "
			 "Now the head of the list is %d, the tail of the list is %d ",
			 buf_id, prev, next,
			 ctl->head, ctl->tail);
}

/*
 * POLAR: Set the buffer in the list while the buffer
 * is not in the list.
 */
static bool
set_buf_in_list(BufferDesc *buf_hdr)
{
	uint32 state;
	bool   result = true;

	state = polar_lock_redo_state(buf_hdr);

	if (state & POLAR_BUF_IN_FLOG_LIST)
		result = false;
	else
		state |= POLAR_BUF_IN_FLOG_LIST;

	polar_unlock_redo_state(buf_hdr, state);
	return result;
}

static bool
set_buf_inserting_log(BufferDesc *buf_hdr, bool *inserting_already)
{
	uint32 state;
	bool   result = false;

	state = polar_lock_redo_state(buf_hdr);

	if (state & POLAR_BUF_INSERTING_FLOG)
		*inserting_already = true;
	else if (state & POLAR_BUF_IN_FLOG_LIST)
	{
		state |= POLAR_BUF_INSERTING_FLOG;
		result = true;
	}

	polar_unlock_redo_state(buf_hdr, state);
	return result;
}

static void
process_flog_insert_interrupts(Buffer buf)
{
	if (AmStartupProcess())
		polar_startup_interrupt_with_pinned_buf(buf);
	else
	{
		if (unlikely(InterruptPending && ProcDiePending))
			polar_unpin_buffer_proc_exit(buf);

		CHECK_FOR_INTERRUPTS();
	}
}

static bool
polar_wait_buf_flog_rec_insert(BufferDesc *buf_hdr)
{
	TimestampTz start;

	start = GetCurrentTimestamp();

	/* Wait for the flashback log inserting by background */
	do
	{
		/* Handle interrupt signals of startup process to avoid hang */
		process_flog_insert_interrupts(BufferDescriptorGetBuffer(buf_hdr));

		/* NB: A IO will take 100us? So we just wait 1000us */
		pg_usleep(1000);

		if (polar_flashback_log_sync_buf_timeout && TimestampDifferenceExceeds(start,
																			   GetCurrentTimestamp(), polar_flashback_log_sync_buf_timeout))
			return false;
	}
	while (is_buf_in_flog_list(buf_hdr));

	return true;
}

static int
get_flog_list_head_with_lock(flog_list_ctl_t ctl)
{
	int buf_id;

	SpinLockAcquire(&ctl->info_lck);
	buf_id = ctl->head;
	SpinLockRelease(&ctl->info_lck);
	return buf_id;
}

/*
 * Read origin page from file.
 */
void
read_origin_page_from_file(BufferTag *tag, char *origin_page)
{
	SMgrRelation smgr = NULL;
	RelFileNode rnode;
	ForkNumber  fork_num;
	BlockNumber blkno;
	BlockNumber nblocks;

	rnode = tag->rnode;
	fork_num = tag->forkNum;
	blkno = tag->blockNum;
	MemSet(origin_page, 0, BLCKSZ);

	/* Find smgr relation for buffer */
	smgr = smgropen(rnode, InvalidBackendId);
	nblocks = smgrnblocks(smgr, fork_num);

	if (blkno >= nblocks)
	{
		if (unlikely(polar_flashback_log_debug))
		{
			elog(LOG, "The origin page of [%u, %u, %u], %u, %u is a empty page",
					rnode.spcNode,
					rnode.dbNode,
					rnode.relNode,
					fork_num,
					blkno);
		}

		return;
	}

	smgrread(smgr, fork_num, blkno, origin_page);

	/*
	 * Not check the checksum here but check it in decode the flashback log
	 * record.
	 */
	if (unlikely(polar_flashback_log_debug))
		elog(LOG, "Read the origin page [%u, %u, %u], %u, %u from file",
			 rnode.spcNode,
			 rnode.dbNode,
			 rnode.relNode,
			 fork_num,
			 blkno);
}

/*
 * Insert a flashback log record of buffer from list.
 *
 * NB: Just one process to insert record from one flashback async list.
 * Maybe the flashback async list can be many.
 *
 * The background worker will insert the flashback log record of async list
 * head buffer, but the buffer can be removed by backend in very small cases.
 * And in very small cases, the normal backend check the buffer is
 * in the list by is_buf_in_flog_list, but it may remove by the background.
 *
 * The background worker will report the error when the result is false.
 */
static bool
insert_buf_flog_rec_from_list(flog_list_ctl_t list_ctl, flog_buf_ctl_t buf_ctl,
							  flog_index_queue_ctl_t queue_ctl, BufferDesc *buf_hdr,
							  bool is_background, bool is_validate)
{
	XLogRecPtr redo_lsn;
	XLogRecPtr fbpoint_lsn;
	bool inserting_already = false;
	uint8 info;
	int buf_id;
	polar_flog_rec_ptr ptr;
	bool result = false;
	int8 origin_buf_index;
	PGAlignedBlock  origin_page;

	buf_id = buf_hdr->buf_id;
	/* Wait the buffer inserting flashback log finished */
	pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_LOG_INSERT);

	/* Only one can insert the flashback log */
	if (set_buf_inserting_log(buf_hdr, &inserting_already))
	{
		redo_lsn = list_ctl->flashback_list[buf_id].redo_lsn;
		fbpoint_lsn = list_ctl->flashback_list[buf_id].fbpoint_lsn;
		info = list_ctl->flashback_list[buf_id].info;
		origin_buf_index = list_ctl->flashback_list[buf_id].origin_buf_index;
		Assert(info & FLOG_LIST_SLOT_READY);

		/* Remove the buffer to let others go */
		remove_buf_from_list(buf_id, list_ctl);

		/* When the buffer is validate, just clean everything */
		if (!is_validate)
		{
			bool from_origin_buf = false;

			/* Read the origin page. */
			if (origin_buf_index != INVAILD_ORIGIN_BUF_INDEX)
			{
				Assert(BUFFERTAGS_EQUAL(buf_hdr->tag, list_ctl->buf_tag[origin_buf_index]));
				memcpy(origin_page.data, list_ctl->origin_buf + origin_buf_index * BLCKSZ, BLCKSZ);
				from_origin_buf = true;
			}
			else
				read_origin_page_from_file(&buf_hdr->tag, origin_page.data);

			/* Insert the flashback log for the buffer */
			ptr = polar_insert_buf_flog_rec(buf_ctl, queue_ctl, &buf_hdr->tag, redo_lsn,
					fbpoint_lsn, info, origin_page.data, from_origin_buf);
			list_ctl->flashback_list[buf_id].flashback_ptr = ptr;
		}

		if (origin_buf_index != INVAILD_ORIGIN_BUF_INDEX)
			polar_clean_origin_buf_bit(list_ctl, buf_id, origin_buf_index);

		clean_buf_flog_state(buf_hdr,
				(POLAR_BUF_IN_FLOG_LIST | POLAR_BUF_INSERTING_FLOG));

		if (is_background)
			list_ctl->bg_remove_num++;

		if (unlikely(polar_flashback_log_debug))
			elog(LOG, "Insert flashback log record from async list: "
				 "page [%u, %u, %u], %u, %u buffer(%d) by %s.",
				 buf_hdr->tag.rnode.spcNode,
				 buf_hdr->tag.rnode.dbNode,
				 buf_hdr->tag.rnode.relNode,
				 buf_hdr->tag.forkNum,
				 buf_hdr->tag.blockNum, buf_id, is_background ? "background worker" : "backend");

		result = true;
	}
	else if (inserting_already)
	{
		if (unlikely(polar_flashback_log_debug))
			elog(LOG, "Wait to insert the page [%u, %u, %u], %u, %u flashback log record "
				 "by %s.",
				 buf_hdr->tag.rnode.spcNode,
				 buf_hdr->tag.rnode.dbNode,
				 buf_hdr->tag.rnode.relNode,
				 buf_hdr->tag.forkNum,
				 buf_hdr->tag.blockNum, is_background ? "backend" : "background worker");

		/* The background worker just return */
		if (!is_background && !polar_wait_buf_flog_rec_insert(buf_hdr))
			elog(ERROR, "Cancel the process due to wait the flashback log of the page "
				 "([%u, %u, %u]), %u, %u inserting timeout. Please enlarge the "
				 "guc polar_flashback_log_sync_buf_timeout",
				 buf_hdr->tag.rnode.spcNode, buf_hdr->tag.rnode.dbNode,
				 buf_hdr->tag.rnode.relNode, buf_hdr->tag.forkNum,
				 buf_hdr->tag.blockNum);

		result = true;
	}

	pgstat_report_wait_end();

	return result;
}

static int8
get_a_free_origin_buf(flog_list_ctl_t ctl)
{
	int i;
	static int array_id = INVAILD_ORIGIN_BUF_ARRAY;

	if (array_id == INVAILD_ORIGIN_BUF_ARRAY)
		array_id = MyProc->pgprocno % POLAR_ORIGIN_PAGE_BUF_ARRAY_NUM;

	for (i = 0; i < POLAR_ORIGIN_PAGE_BUF_ARRAY_NUM; i++)
	{
	    int8 n = 0;
	    uint32 buf_bitmap_curr = 0;
	    uint32 buf_bitmap = 0;

	    buf_bitmap_curr = buf_bitmap = pg_atomic_read_u32(&ctl->origin_buf_bitmap[array_id]);

	    if (buf_bitmap_curr == 0xFFFFFFFF)
	    {
	    	array_id = ((array_id == POLAR_ORIGIN_PAGE_BUF_ARRAY_NUM - 1)? 0 : array_id + 1);
	    	continue;
	    }

	    do
	    {
	        if (!(buf_bitmap & 1) && pg_atomic_compare_exchange_u32(&ctl->origin_buf_bitmap[array_id],
	        		&buf_bitmap_curr, buf_bitmap_curr | ((uint32) 1 << n)))
	        {
	        	return array_id * POLAR_ORIGIN_PAGE_BUF_NUM_PER_ARRAY + n;
	        }

	        n++;
	        buf_bitmap_curr = pg_atomic_read_u32(&ctl->origin_buf_bitmap[array_id]);
	        buf_bitmap = buf_bitmap_curr >> n;
	    } while (n < POLAR_ORIGIN_PAGE_BUF_NUM_PER_ARRAY);

	    array_id = ((array_id == POLAR_ORIGIN_PAGE_BUF_ARRAY_NUM - 1)? 0 : array_id + 1);
	}

	return INVAILD_ORIGIN_BUF_INDEX;
}

/*
 * Add one buffer to flashback log list.
 *
 * buf: 		 The buffer.
 * is_candidate: Is a candidate.
 *
 * NB: The caller must guarantee only one can insert the same buffer once.
 */
void
polar_push_buf_to_flog_list(flog_list_ctl_t ctl, flog_buf_ctl_t buf_ctl, Buffer buf, bool is_candidate)
{
	BufferDesc *buf_hdr;
	uint8 info = 0;
	int buf_id;

	Assert(buf <= NBuffers && buf > 0);
	buf_id = buf - 1;
	buf_hdr = GetBufferDescriptor(buf_id);

	if (set_buf_in_list(buf_hdr))
	{
		/*
		 * We are here means that the replay of the buffer has been done,
		 * clean the POLAR_BUF_FLOG_DISABLE state and go on.
		 */
		if (polar_check_buf_flog_state(buf_hdr, POLAR_BUF_FLOG_DISABLE))
			clean_buf_flog_state(buf_hdr, POLAR_BUF_FLOG_DISABLE);

		info = FLOG_LIST_SLOT_READY;

		/* It is a candidate */
		if (is_candidate ||
				BufferGetLSN(buf_hdr) == InvalidXLogRecPtr)
			info |= FLOG_LIST_SLOT_CANDIDATE;

		add_buf_to_list(buf_id, info, ctl, buf_ctl);
	}
	else if (unlikely(polar_flashback_log_debug))
		elog(LOG, "The page [%u, %u, %u], %u, %u buffer %d "
			 "is in flashback log list already.",
			 buf_hdr->tag.rnode.spcNode,
			 buf_hdr->tag.rnode.dbNode,
			 buf_hdr->tag.rnode.relNode,
			 buf_hdr->tag.forkNum,
			 buf_hdr->tag.blockNum,
			 buf_id);
}

/*
 * Insert a flashback log record from list head.
 *
 * NB: Just one process to insert record from one flashback ring.
 * Maybe the flashback ring can be many.
 */
void
polar_insert_flog_rec_from_list_bg(flog_list_ctl_t list_ctl, flog_buf_ctl_t buf_ctl,
								   flog_index_queue_ctl_t queue_ctl)
{
	BufferDesc *buf_hdr;
	int buf;

	/* Get the head with spin lock */
	buf = get_flog_list_head_with_lock(list_ctl);
	Assert(buf < NBuffers);

	if (buf == NOT_IN_FLOG_LIST)
		return;

	buf_hdr = GetBufferDescriptor(buf);

	/*no cover begin*/
	/*
	 * Insert the flashback log record of the head buffer.
	 * If it is failed, we check whether someone else has inserted it.
	 * If not, just report a PANIC.
	 */
	if (!insert_buf_flog_rec_from_list(list_ctl, buf_ctl, queue_ctl,
			buf_hdr, true, false) && buf == get_flog_list_head_with_lock(list_ctl))
	{
		/* Check someone remove it or a memory PANIC */
		elog(PANIC, "The page [%u, %u, %u], %u, %u buffer(%d) is list head but not in list",
			 buf_hdr->tag.rnode.spcNode,
			 buf_hdr->tag.rnode.dbNode,
			 buf_hdr->tag.rnode.relNode,
			 buf_hdr->tag.forkNum,
			 buf_hdr->tag.blockNum, buf);
	}
	/*no cover end*/
}

/*
 * POLAR: Insert the flashback log record of the buffer by myself or
 * wait the background to insert it.
 */
void
polar_insert_buf_flog_rec_sync(flog_list_ctl_t list_ctl, flog_buf_ctl_t buf_ctl,
		flog_index_queue_ctl_t queue_ctl, BufferDesc *buf_hdr, bool is_validate)
{
	if (is_buf_in_flog_list(buf_hdr))
		insert_buf_flog_rec_from_list(list_ctl, buf_ctl, queue_ctl, buf_hdr, false, is_validate);
}

Size
polar_flog_async_list_shmem_size(void)
{
	Size        size = 0;

	/* flashback log control */
	size = sizeof(flog_list_ctl_data_t);

	/* flashback log list */
	size = add_size(size, mul_size(sizeof(flog_list_slot), NBuffers));
	/* add the origin page buffer */
	size = add_size(size, BLCKSZ);
	size = add_size(size, mul_size(BLCKSZ, POLAR_ORIGIN_PAGE_BUF_NUM));
	size = add_size(size, mul_size(sizeof(BufferTag), POLAR_ORIGIN_PAGE_BUF_NUM));
	size = add_size(size, mul_size(sizeof(pg_atomic_uint32), POLAR_ORIGIN_PAGE_BUF_ARRAY_NUM));
	return size;
}

/*
 * Init the flashback log async list data.
 */
void
polar_flog_list_init_data(flog_list_ctl_t ctl)
{
	char *allocptr;
	int i;

	MemSet(ctl, 0, sizeof(flog_list_ctl_data_t));
	allocptr = (char *)ctl;
	ctl->tail = ctl->head = NOT_IN_FLOG_LIST;
	SpinLockInit(&ctl->info_lck);
	pg_atomic_init_u64(&ctl->insert_total_num, 0);
	pg_atomic_init_u64(&ctl->remove_total_num, 0);
	ctl->bg_remove_num = 0;
	allocptr += sizeof(flog_list_ctl_data_t);
	ctl->flashback_list = (flog_list_slot *)allocptr;
	MemSet(allocptr, 0, mul_size(sizeof(flog_list_slot), NBuffers));
	allocptr += mul_size(sizeof(flog_list_slot), NBuffers);

	for (i = 0; i < NBuffers; i++)
	{
		ctl->flashback_list[i].next_buf = NOT_IN_FLOG_LIST;
		ctl->flashback_list[i].prev_buf = NOT_IN_FLOG_LIST;
		ctl->flashback_list[i].origin_buf_index = INVAILD_ORIGIN_BUF_INDEX;
	}

	allocptr = (char *) TYPEALIGN(BLCKSZ, allocptr);
	ctl->origin_buf = allocptr;
	MemSet(ctl->origin_buf, 0,
		   (Size) BLCKSZ * POLAR_ORIGIN_PAGE_BUF_NUM);
	allocptr += mul_size(BLCKSZ, POLAR_ORIGIN_PAGE_BUF_NUM);
	ctl->buf_tag = (BufferTag *)allocptr;
	MemSet(ctl->buf_tag, 0,
			   sizeof(BufferTag) * POLAR_ORIGIN_PAGE_BUF_NUM);
	allocptr += mul_size(sizeof(BufferTag), POLAR_ORIGIN_PAGE_BUF_NUM);

	ctl->origin_buf_bitmap = (pg_atomic_uint32 *)allocptr;

	for (i = 0; i < POLAR_ORIGIN_PAGE_BUF_ARRAY_NUM; i++)
	{
		pg_atomic_init_u32(&ctl->origin_buf_bitmap[i], 0);
	}
}

/*
 * Initialize the flashback log list
 */
flog_list_ctl_t
polar_flog_async_list_init(const char *name)
{
#define LIST_CTL_SUFFIX " List Control"

	flog_list_ctl_t list_ctl;
	bool found;
	char flog_list_name[FL_OBJ_MAX_NAME_LEN];

	/* Get or create the shared memory for flashback log list control block */
	FLOG_GET_OBJ_NAME(flog_list_name, name, LIST_CTL_SUFFIX);
	list_ctl = (flog_list_ctl_t)
			   ShmemInitStruct(flog_list_name,
							   polar_flog_async_list_shmem_size(), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		polar_flog_list_init_data(list_ctl);
	}
	else
		Assert(found);

	return list_ctl;
}

/* Get the list head and tail without lock */
void
polar_flog_get_async_list_info(flog_list_ctl_t ctl, int *head, int *tail)
{
	*head = ctl->head;
	*tail = ctl->tail;
}

/* POLAR: flush the flashback log record of the buffer */
void
polar_flush_buf_flog(flog_list_ctl_t list_ctl, flog_buf_ctl_t buf_ctl, BufferDesc *buf, bool is_invalidate)
{
	polar_flog_rec_ptr ptr;

	ptr = list_ctl->flashback_list[buf->buf_id].flashback_ptr;

	if (!FLOG_REC_PTR_IS_INVAILD(ptr))
	{
		/* When the buffer is invalidate, ignore to flush the flashback log record */
		if (!is_invalidate)
			polar_flog_flush(buf_ctl, ptr);

		list_ctl->flashback_list[buf->buf_id].flashback_ptr = POLAR_INVALID_FLOG_REC_PTR;
	}
}

/*no cover begin*/
void
polar_get_flog_list_stat(flog_list_ctl_t ctl, uint64 *insert_total_num,
						 uint64 *remove_total_num, uint64 *bg_remove_num)
{
	*insert_total_num = pg_atomic_read_u64(&ctl->insert_total_num);
	*remove_total_num = pg_atomic_read_u64(&ctl->remove_total_num);
	*bg_remove_num = ctl->bg_remove_num;
}
/*no cover end*/

int8
polar_add_origin_buf(flog_list_ctl_t ctl, BufferDesc *buf_desc)
{
	char *page;
	int8 buf_index;

	page = BufHdrGetBlock(buf_desc);
	buf_index = get_a_free_origin_buf(ctl);

	if (buf_index != INVAILD_ORIGIN_BUF_INDEX)
	{
		/*
		 * In some cases, like the buffer not flushed, keep the old one.
		 */
		if (is_buf_in_flog_list(buf_desc))
			/*no cover line*/
			return INVAILD_ORIGIN_BUF_INDEX;

		memcpy(ctl->origin_buf + buf_index * BLCKSZ, page, BLCKSZ);
		INIT_BUFFERTAG(ctl->buf_tag[buf_index], buf_desc->tag.rnode,
				buf_desc->tag.forkNum, buf_desc->tag.blockNum);
		ctl->flashback_list[buf_desc->buf_id].origin_buf_index = buf_index;

		if (unlikely(polar_flashback_log_debug))
		{
			elog(LOG, "Add page [%u, %u, %u], %u, %u buffer(%d) in the origin buffer %d",
					buf_desc->tag.rnode.spcNode, buf_desc->tag.rnode.dbNode,
					buf_desc->tag.rnode.relNode, buf_desc->tag.forkNum,
					buf_desc->tag.blockNum, buf_desc->buf_id, buf_index);
		}
	}

	return buf_index;
}
