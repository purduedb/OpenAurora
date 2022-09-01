/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_index_queue.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_log_index_queue.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/polar_queue_manager.h"
#include "miscadmin.h"
#include "polar_flashback/polar_flashback_log.h"
#include "polar_flashback/polar_flashback_log_file.h"
#include "polar_flashback/polar_flashback_log_index_queue.h"

/*
 * The packet content type.
 * The invaild packet content type is POLAR_RINGBUF_PKT_INVALID_TYPE.
 * The packet content type mask is POLAR_RINGBUF_PKT_TYPE_MASK.
 */
#define PKT_TYPE_BUFFER_TAG (0x10) /* The packet content is a buffertag */

/*
 * Pop the buffer tag with the flashback logindex reference.
 * Return the length of the packet length.
 */
bool
polar_flog_index_queue_ref_pop(polar_ringbuf_ref_t *ref, flog_index_queue_lsn_info *lsn_info, polar_flog_rec_ptr max_ptr)
{
	uint32 pktlen = 0;
	uint8  pkt_type = POLAR_RINGBUF_PKT_INVALID_TYPE;
	ssize_t offset = 0;
	uint32 data_len;

	if (polar_ringbuf_avail(ref) > 0)
		pkt_type = polar_ringbuf_next_ready_pkt(ref, &pktlen);
	else
		return false;

	/* Check the next ready packet type, may be the buffer is not ready. */
	if (pkt_type == POLAR_RINGBUF_PKT_INVALID_TYPE)
		return false;

	if (pkt_type != PKT_TYPE_BUFFER_TAG)
	{
		/*no cover line*/
		elog(PANIC, "Invalid flashback log queue pkt type %d from ref %s at %lu",
			 pkt_type, ref->ref_name, ref->rbuf->slot[ref->slot].pread);
	}

	POLAR_COPY_QUEUE_CONTENT(ref, offset, &(lsn_info->ptr), sizeof(polar_flog_rec_ptr));

	/* We get a invalid flashback log point from logindex queue */
	if (FLOG_REC_PTR_IS_INVAILD(lsn_info->ptr))
	{
		/*no cover line*/
		elog(PANIC, "We get a invalid flashback log record point from ref %s at %lu",
			 ref->ref_name, ref->rbuf->slot[ref->slot].pread);
	}

	POLAR_COPY_QUEUE_CONTENT(ref, offset, &(lsn_info->log_len), sizeof(uint32));

	if (lsn_info->log_len == 0)
	{
		/*no cover line*/
		elog(PANIC, "We get a invalid flashback log record length from ref %s at %lu",
			 ref->ref_name, ref->rbuf->slot[ref->slot].pread);
	}
	else if (polar_get_next_flog_ptr(lsn_info->ptr, lsn_info->log_len) > max_ptr)
		return false;

	data_len = pktlen - FLOG_INDEX_QUEUE_HEAD_SIZE;
	POLAR_COPY_QUEUE_CONTENT(ref, offset, &(lsn_info->tag), data_len);
	polar_ringbuf_update_ref(ref);

	if (unlikely(polar_flashback_log_debug))
		elog(LOG, "%s ptr: %x/%x, total length: %u, the tag: '[%u, %u, %u], %d, %u'", __func__,
			 (uint32)(lsn_info->ptr >> 32), (uint32)lsn_info->ptr, lsn_info->log_len,
			 lsn_info->tag.rnode.spcNode, lsn_info->tag.rnode.dbNode, lsn_info->tag.rnode.relNode,
			 lsn_info->tag.forkNum, lsn_info->tag.blockNum);

	return true;
}

/*
 * Keep data to update the ring buffer pread when the queue
 * is full.
 *
 * NB: It is a callback, so we don't use the polar_ringbuf_t as a parameter.
 */
static void
flog_index_queue_keep_data(void)
{
	polar_ringbuf_ref_t ref = { .slot = -1 };
	polar_ringbuf_t queue = flog_instance->queue_ctl->queue;

	if (!polar_ringbuf_new_ref(queue, true, &ref,
							   "flashback_logindex_queue_data_keep"))
	{
		/*no cover line*/
		elog(PANIC, "Failed to create flashback queue reference for flashback_logindex_queue_data_keep");
	}

	Assert(ref.rbuf == queue);
	polar_ringbuf_ref_keep_data(&ref, FLOG_INDEX_QUEUE_KEEP_RATIO);
	polar_ringbuf_release_ref(&ref);
}

Size
polar_flog_index_queue_shmem_size(int queue_buffers_MB)
{
	Size shmem_size = 0;

	if (queue_buffers_MB <= 0)
		return shmem_size;

	shmem_size = add_size(shmem_size, sizeof(flog_index_queue_ctl_data_t));
	shmem_size = add_size(shmem_size, queue_buffers_MB * 1024L * 1024L);
	shmem_size = add_size(shmem_size, sizeof(flog_index_queue_stat));
	return shmem_size;
}

void
polar_flog_index_queue_init_data(flog_index_queue_ctl_t ctl, const char *name, int queue_buffers_MB)
{
#define LOGINDEX_QUEUE_SUFFIX "_index_queue"

	char queue_name[FL_OBJ_MAX_NAME_LEN];
	uint8 *queue_data;
	Size queue_size;
	Size total_size;

	total_size = polar_flog_index_queue_shmem_size(queue_buffers_MB);
	MemSet(ctl, 0, total_size);
	queue_data = (uint8 *)ctl + sizeof(flog_index_queue_ctl_data_t);
	queue_size = queue_buffers_MB * 1024L * 1024L;
	FLOG_GET_OBJ_NAME(queue_name, name, LOGINDEX_QUEUE_SUFFIX);
	ctl->queue = polar_ringbuf_init(queue_data, queue_size, LWTRANCHE_POLAR_FLASHBACK_LOG_QUEUE, queue_name);
	ctl->queue_stat = (flog_index_queue_stat *)(queue_data + queue_size);
}

flog_index_queue_ctl_t
polar_flog_index_queue_shmem_init(const char *name, int queue_buffers_MB)
{
#define LOGINDEX_QUEUE_CTL_SUFFIX " index queue ctl"

	flog_index_queue_ctl_t ctl = NULL;
	bool found;
	char queue_ctl_name[FL_OBJ_MAX_NAME_LEN];


	if (queue_buffers_MB <= 0)
		/*no cover line*/
		return ctl;


	FLOG_GET_OBJ_NAME(queue_ctl_name, name, LOGINDEX_QUEUE_CTL_SUFFIX);
	ctl = ShmemInitStruct(queue_ctl_name, polar_flog_index_queue_shmem_size(queue_buffers_MB), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		polar_flog_index_queue_init_data(ctl, name, queue_buffers_MB);
	}
	else
		Assert(found);

	return ctl;
}

/*
 * Free the flashback logidnex queue to enlarge
 * The len is the data length in the queue not contain
 * flag and length and head info.
 *
 * NB: There is only one process to reference the ring buffer weakly,
 * free up itself when it is full.
 */
void
polar_flog_index_queue_free_up(flog_index_queue_ctl_t ctl, size_t len)
{
	polar_ringbuf_free_up(ctl->queue,
						  FLOG_INDEX_QUEUE_PKT_SIZE(len),
						  flog_index_queue_keep_data);
	pg_atomic_fetch_add_u64(&(ctl->queue_stat->free_up_total_times), 1);
}

bool
polar_flog_index_queue_free(flog_index_queue_ctl_t ctl, ssize_t len)
{
	return polar_ringbuf_free_size(ctl->queue) >= FLOG_INDEX_QUEUE_PKT_SIZE(len);
}

/*
 * Reserve the space for flashback logindex queue.
 * The size is the data size in the queue, the packet real
 * total size is 1 byte flag and 4 bytes packet length and
 * 8 bytes flashback log point and 4 bytes flashback log
 * length.
 *
 * NB: That function caller must be hold a exclusive lock.
 */
size_t
polar_flog_index_queue_reserve(flog_index_queue_ctl_t ctl, size_t size)
{
	return polar_ringbuf_pkt_reserve(ctl->queue,
									 FLOG_INDEX_QUEUE_PKT_SIZE(size));
}

void
polar_flog_index_queue_set_pkt_len(flog_index_queue_ctl_t ctl, size_t idx, uint32 len)
{
	polar_ringbuf_set_pkt_length(ctl->queue, idx, len + FLOG_INDEX_QUEUE_HEAD_SIZE);
}

bool
polar_flog_index_queue_push(flog_index_queue_ctl_t ctl, size_t rbuf_pos, flog_record *record,
							int copy_len, polar_flog_rec_ptr start_lsn, uint32 log_len)
{
	int offset = 0;
	ssize_t copy_size;
	polar_ringbuf_t ringbuf = ctl->queue;

	if (rbuf_pos >= ringbuf->size)
		/*no cover line*/
		ereport(PANIC, (errmsg("rbuf_pos=%ld is incorrect for flashback logindex queue, "
							   "queue_size=%ld", rbuf_pos, ringbuf->size)));

	offset += polar_ringbuf_pkt_write(ringbuf, rbuf_pos,
									  offset, (uint8 *)&start_lsn, sizeof(start_lsn));
	offset += polar_ringbuf_pkt_write(ringbuf, rbuf_pos,
									  offset, (uint8 *)&log_len, sizeof(log_len));

	copy_size = polar_ringbuf_pkt_write(ringbuf, rbuf_pos, offset,
										(uint8 *)(&(FL_GET_ORIGIN_PAGE_REC_DATA(record)->tag)), copy_len);

	if (copy_size != copy_len)
		/*no cover line*/
		elog(PANIC, "Failed to write packet to ringbuf, rbuf_pos=%lu, offset=%d, copy_size=%ld",
			 rbuf_pos, offset, copy_size);

	polar_ringbuf_set_pkt_flag(ringbuf, rbuf_pos,
							   PKT_TYPE_BUFFER_TAG | POLAR_RINGBUF_PKT_READY);

	if (unlikely(polar_flashback_log_debug))
		elog(LOG, "Put flashback log %x/%x lsn info (log total length is %u) "
			 "into flashback logindex queue",
			 (uint32)(start_lsn >> 32), (uint32)(start_lsn), log_len);

	return true;
}

/*
 * Read info from the queue with reference ref.
 * Return the log length and it is zero when
 * cann't read the expected flashback log lsn.
 * true when it is sucessful. And the lsn_info is
 * the target lsn info, the ptr_expected wiil be changed to
 * next flashback log record.
 *
 * NB: The ptr_expected must be a valid flashback log record.
 */
bool
polar_flog_read_info_from_queue(polar_ringbuf_ref_t *ref, polar_flog_rec_ptr ptr_expected,
		BufferTag *tag, uint32 *log_len, polar_flog_rec_ptr max_ptr)
{
	polar_flog_rec_ptr ptr = POLAR_INVALID_FLOG_REC_PTR;
	polar_flog_rec_ptr prev_ptr = POLAR_INVALID_FLOG_REC_PTR;
	flog_index_queue_lsn_info lsn_info;

	Assert(!FLOG_REC_PTR_IS_INVAILD(ptr_expected));

	do
	{
		memset(&lsn_info, 0, sizeof(flog_index_queue_lsn_info));

		if (polar_flog_index_queue_ref_pop(ref, &lsn_info, max_ptr))
			ptr = lsn_info.ptr;
		else
			return false;

		if (ptr > ptr_expected)
		{
			/* The expected flashback log is not in queue, return false. */
			if (prev_ptr == POLAR_INVALID_FLOG_REC_PTR)
				return false;
			else
			{
				/*no cover line*/
				elog(PANIC, "The flashback logindex queue is sequential, but we get %X/%X after "
					 "%X/%X, but expected point is %X/%X", (uint32)(ptr >> 32),
					 (uint32)ptr, (uint32)(prev_ptr >> 32), (uint32)prev_ptr,
					 (uint32)(ptr_expected >> 32), (uint32)(ptr_expected));
			}
		}

		prev_ptr = ptr;
	}
	while (ptr < ptr_expected);

	/* Next point must be point expected */
	Assert(ptr == ptr_expected);

	*log_len = lsn_info.log_len;
	INIT_BUFFERTAG(*tag, lsn_info.tag.rnode, lsn_info.tag.forkNum, lsn_info.tag.blockNum);

	if (unlikely(polar_flashback_log_debug))
		elog(LOG, "We found the flashback log record at %X/%X from logindex queue, "
			 "total length is %u, the tag is '[%u, %u, %u], %d, %u'",
			 (uint32)(ptr >> 32), (uint32)ptr, *log_len,
			 tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode,
			 tag->forkNum, tag->blockNum);

	return true;
}

void
polar_get_flog_index_queue_stat(flog_index_queue_stat *queue_stat, uint64 *free_up_times,
								uint64 *read_from_file_rec_nums, uint64 *read_from_queue_rec_nums)
{
	*free_up_times = pg_atomic_read_u64(&queue_stat->free_up_total_times);
	*read_from_file_rec_nums = queue_stat->read_from_file_rec_nums;
	*read_from_queue_rec_nums = queue_stat->read_from_queue_rec_nums;
}

void
polar_update_flog_index_queue_stat(flog_index_queue_stat *queue_stat, uint64 read_from_file_added, uint64 read_from_queue_added)
{
	queue_stat->read_from_file_rec_nums += read_from_file_added;
	queue_stat->read_from_queue_rec_nums += read_from_queue_added;
}
