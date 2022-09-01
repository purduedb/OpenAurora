/*----------------------------------------------------------------------------------------
 *
 * polar_ringbuf.c
 *   polar ring buffer interface routines.
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
 *    src/backend/access/logindex/polar_ringbuf.c
 * ---------------------------------------------------------------------------------------
 */

/*
 * Principal entry points:
 *
 * polar_ringbuf_pkt_reserve() -- Reserve space from ring buffer for future write.
 *          And this function should be protected by exclusive lock.
 *
 * polar_ringbuf_pkt_write() -- Write packet data to reserved space and it's safe for multi process
 *          to write different packet
 *
 * polar_ringbuf_new_ref() -- Create new ring buffer reference
 *
 * polar_ringbuf_read_next_pkt() -- Read data from ring buffer sequentially
 *
 */

#include "postgres.h"

#include "access/polar_ringbuf.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "utils/polar_bitpos.h"

static bool polar_ringbuf_update_pread(polar_ringbuf_t rbuf);

#define RINGBUF_REF_MATCH(ref) \
	((ref)->slot >= 0 && (ref)->slot < POLAR_RINGBUF_MAX_SLOT \
	 && (ref)->rbuf->slot[(ref)->slot].ref_num == (ref)->ref_num)

/*
 * Initialize ring buffer from allocated memory.
 */
polar_ringbuf_t
polar_ringbuf_init(uint8 *data, size_t len, int tranche_id, const char *trache_name)
{
	polar_ringbuf_t rbuf = (polar_ringbuf_t)data;

	LWLockRegisterTranche(tranche_id, trache_name);
	LWLockInitialize(&rbuf->lock, tranche_id);

	pg_atomic_init_u64(&rbuf->pread, 0);
	pg_atomic_init_u64(&rbuf->pwrite, 0);

	MemSet(rbuf->slot, 0, sizeof(polar_ringbuf_slot_t)*POLAR_RINGBUF_MAX_SLOT);

	rbuf->size = len - offsetof(polar_ringbuf_data_t, data);
	rbuf->occupied = 0;
	return rbuf;
}

/*
 * Create new reference for ring buffer and return true on success
 */
bool
polar_ringbuf_new_ref(polar_ringbuf_t rbuf, bool strong,
					  polar_ringbuf_ref_t *ref, char *ref_name)
{
	int i;
	uint64 unoccupied;
	bool succeed = false;

	elog(LOG, "create reference for %s", ref_name);

	LWLockAcquire(&rbuf->lock, LW_EXCLUSIVE);
	unoccupied = ~(rbuf->occupied);
	POLAR_BIT_LEAST_POS(unoccupied, i);

	/* The position start from 1 */
	if (i > 0 && i <= POLAR_RINGBUF_MAX_SLOT)
	{
		POLAR_BIT_OCCUPY(rbuf->occupied, i);
		i--;
		rbuf->slot[i].strong = strong;
		rbuf->slot[i].pread = pg_atomic_read_u64(&rbuf->pread);
		rbuf->slot[i].visit = rbuf->min_visit;
		rbuf->slot[i].ref_num = ++rbuf->ref_num;
		ref->rbuf = rbuf;
		ref->ref_num = rbuf->slot[i].ref_num;
		ref->slot = i;
		ref->strong = strong;

		if (ref_name != NULL)
		{
			strncpy(rbuf->slot[i].ref_name, ref_name, POLAR_RINGBUF_MAX_REF_NAME);
			rbuf->slot[i].ref_name[POLAR_RINGBUF_MAX_REF_NAME] = '\0';
		}
		else
			rbuf->slot[i].ref_name[0] = '\0';

		strcpy(ref->ref_name, rbuf->slot[i].ref_name);

		succeed = true;
	}

	LWLockRelease(&rbuf->lock);

	return succeed;
}

/*
 * release ring buffer reference
 */
void
polar_ringbuf_release_ref(polar_ringbuf_ref_t *ref)
{
	polar_ringbuf_t rbuf = ref->rbuf;

	if (rbuf == NULL)
		return ;

	LWLockAcquire(&rbuf->lock, LW_EXCLUSIVE);

	if (RINGBUF_REF_MATCH(ref))
	{
		POLAR_BIT_RELEASE_OCCUPIED(rbuf->occupied, ref->slot + 1);
		MemSet(&rbuf->slot[ref->slot], 0, sizeof(polar_ringbuf_slot_t));
		polar_ringbuf_update_pread(rbuf);
	}

	LWLockRelease(&rbuf->lock);

	ref->rbuf = NULL;
}

/*
 * The callback function to do auto release reference
 */
static void
polar_ringbuf_auto_release(int code, Datum arg)
{
	polar_ringbuf_ref_t *ref = (polar_ringbuf_ref_t *)arg;

	polar_ringbuf_release_ref(ref);
}

/*
 * Register auto release for the reference
 * It will be released automatically when proc_exit
 */
void
polar_ringbuf_auto_release_ref(polar_ringbuf_ref_t *ref)
{
	/* check whether it is able to register a before_shmem_exit callback */
	if (polar_check_before_shmem_exit(polar_ringbuf_auto_release, (Datum)ref, 0))
		before_shmem_exit(polar_ringbuf_auto_release, (Datum)ref);
}

/*
 * Get strong reference from weak reference
 */
bool
polar_ringbuf_get_ref(polar_ringbuf_ref_t *ref)
{
	bool got = false;
	polar_ringbuf_t rbuf = ref->rbuf;

	/*
	 * The strong reference property is changed in the same process,
	 * and it's safe to use shared lock
	 */
	LWLockAcquire(&rbuf->lock, LW_SHARED);

	if (RINGBUF_REF_MATCH(ref))
	{
		rbuf->slot[ref->slot].strong = true;
		got = true;
	}

	LWLockRelease(&rbuf->lock);

	return got;
}

/*
 * Check whether it's valid reference
 */
bool
polar_ringbuf_valid_ref(polar_ringbuf_ref_t *ref)
{
	bool ret = false;

	if (!ref || !ref->rbuf)
		return ret;

	LWLockAcquire(&(ref->rbuf->lock), LW_SHARED);

	ret = RINGBUF_REF_MATCH(ref);

	LWLockRelease(&(ref->rbuf->lock));

	return ret;
}

/*
 * Change strong reference to weak reference
 */
bool
polar_ringbuf_clear_ref(polar_ringbuf_ref_t *ref)
{
	bool cleared = false;
	polar_ringbuf_t rbuf = ref->rbuf;

	/*
	 * The strong reference property is changed in the same process,
	 * and it's safe to use shared lock
	 */
	LWLockAcquire(&rbuf->lock, LW_SHARED);

	if (RINGBUF_REF_MATCH(ref))
	{
		rbuf->slot[ref->slot].strong = false;
		cleared = true;
	}

	LWLockRelease(&rbuf->lock);

	return cleared;
}

/*
 * The reference read packet sequentially.
 * Support read from the offset of packet
 */
ssize_t
polar_ringbuf_read_next_pkt(polar_ringbuf_ref_t *ref,
							int offset, uint8 *buf, size_t len)
{
	size_t todo;
	size_t split;
	size_t pktlen;
	polar_ringbuf_t rbuf = ref->rbuf;
	size_t idx = rbuf->slot[ref->slot].pread;

	pktlen = polar_ringbuf_pkt_len(rbuf, idx);

	if (unlikely(offset >= pktlen))
		return -1;

	if ((offset + len) > pktlen)
		len = pktlen - offset;

	idx = (idx + POLAR_RINGBUF_PKTHDRSIZE + offset) % rbuf->size;
	todo = len;
	split = ((idx + len) > rbuf->size) ? rbuf->size - idx : 0;

	if (unlikely(split > 0))
	{
		memcpy(buf, rbuf->data + idx, split);
		buf += split;
		todo -= split;
		idx = 0;
	}

	memcpy(buf, rbuf->data + idx, todo);

	return len;
}

/*
 * Write packet data and the idx is the start position of the packet.
 * Support write from the offset of packet.
 */
ssize_t
polar_ringbuf_pkt_write(polar_ringbuf_t rbuf, size_t idx, int offset, uint8 *buf, size_t len)
{
	size_t todo;
	size_t split;
	size_t pktlen = polar_ringbuf_pkt_len(rbuf, idx);

	if (offset >= pktlen || (offset + len) > pktlen)
		return -1;

	idx = (idx + POLAR_RINGBUF_PKTHDRSIZE + offset) % rbuf->size;
	split = (idx + len > rbuf->size) ? rbuf->size - idx : 0;
	todo = len;

	if (split > 0)
	{
		memcpy(rbuf->data + idx, buf, split);
		buf += split;
		todo -= split;
		idx = 0;
	}

	memcpy(rbuf->data + idx, buf, todo);

	return len;
}

/*
 * Update the ring buffer least read position and to get more free space
 */
bool
polar_ringbuf_update_pread(polar_ringbuf_t rbuf)
{
	int i;
	uint64 min_slot = POLAR_RINGBUF_MAX_SLOT;
	uint64 min_visit = UINT64_MAX;
	bool   updated = false;
	uint64 occupied = rbuf->occupied;

	while (occupied)
	{
		POLAR_BIT_LEAST_POS(occupied, i);
		i--;

		if (min_visit > rbuf->slot[i].visit)
		{
			min_visit = rbuf->slot[i].visit;
			min_slot = i;
		}

		POLAR_BIT_CLEAR_LEAST(occupied);
	}

	if (min_slot != POLAR_RINGBUF_MAX_SLOT && rbuf->min_visit != min_visit)
	{
		rbuf->min_visit = min_visit;
		pg_atomic_write_u64(&rbuf->pread, rbuf->slot[min_slot].pread);
		updated = true;
	}

	return updated;
}

/*
 * Update the reference's read position
 */
void
polar_ringbuf_update_ref(polar_ringbuf_ref_t *ref)
{
	polar_ringbuf_t rbuf = ref->rbuf;
	size_t idx = rbuf->slot[ref->slot].pread;
	size_t pktlen = polar_ringbuf_pkt_len(rbuf, idx);

	LWLockAcquire(&rbuf->lock, LW_SHARED);
	rbuf->slot[ref->slot].pread = (idx + POLAR_RINGBUF_PKTHDRSIZE + pktlen) % rbuf->size;
	rbuf->slot[ref->slot].visit++;
	LWLockRelease(&rbuf->lock);
}

/*
 * No enouth space to write and eliminate one weak reference with least read position
 */
static bool
polar_ringbuf_evict_ref(polar_ringbuf_t rbuf)
{
	int i;
	bool evicted = false;
	uint8 min_slot = POLAR_RINGBUF_MAX_SLOT;
	uint64 min_visit = UINT64_MAX;
	uint64 occupied = rbuf->occupied;

	while (occupied)
	{
		POLAR_BIT_LEAST_POS(occupied, i);
		i--;

		if (!rbuf->slot[i].strong
				&& min_visit > rbuf->slot[i].visit)
		{
			min_visit = rbuf->slot[i].visit;
			min_slot = i;
		}

		POLAR_BIT_CLEAR_LEAST(occupied);
	}

	if (min_slot != POLAR_RINGBUF_MAX_SLOT
			&& pg_atomic_read_u64(&rbuf->pread) == rbuf->slot[min_slot].pread)
	{
		elog(LOG, "Polar evict slot=%d,name=%s which is weak reference", min_slot, rbuf->slot[min_slot].ref_name);
		POLAR_BIT_RELEASE_OCCUPIED(rbuf->occupied, min_slot + 1);
		MemSet(&rbuf->slot[min_slot], 0, sizeof(polar_ringbuf_slot_t));
		evicted = true;
	}

	return evicted;
}

/*
 * Update the least read position of the ring buffer to get more space for write
 */
void
polar_ringbuf_update_keep_data(polar_ringbuf_t rbuf)
{
	LWLockAcquire(&rbuf->lock, LW_EXCLUSIVE);
	polar_ringbuf_update_pread(rbuf);
	LWLockRelease(&rbuf->lock);
}

/*
 * Free up space until free size larger than aimed length
 */
void
polar_ringbuf_free_up(polar_ringbuf_t rbuf, size_t len, polar_interrupt_callback callback)
{
	bool free_up = false;

	pgstat_report_wait_start(WAIT_EVENT_LOGINDEX_QUEUE_SPACE);

	do
	{
		LWLockAcquire(&rbuf->lock, LW_EXCLUSIVE);
		free_up = (polar_ringbuf_free_size(rbuf) > len);

		if (!free_up)
		{
			if (!polar_ringbuf_update_pread(rbuf)
					&& !polar_ringbuf_evict_ref(rbuf))
			{
				/*
				 * 1. Try to update reference's pread to get more space
				 * 2. Try to evict weak reference
				 * 3. Otherwise wait for reference to comsume data
				 */
				LWLockRelease(&rbuf->lock);
				CHECK_FOR_INTERRUPTS();

				if (callback != NULL)
					callback();

				pg_usleep(10);
				continue;
			}
		}

		LWLockRelease(&rbuf->lock);
	}
	while (!free_up);

	pgstat_report_wait_end();
}

void
polar_ringbuf_ref_keep_data(polar_ringbuf_ref_t *ref, float ratio)
{
	polar_ringbuf_t rbuf = ref->rbuf;
	ssize_t keep_size = rbuf->size * ratio;
	bool updated = false;

	while (polar_ringbuf_avail(ref) > keep_size)
	{
		polar_ringbuf_update_ref(ref);
		updated = true;
	}

	if (updated)
		polar_ringbuf_update_keep_data(rbuf);
}

void
polar_ringbuf_reset(polar_ringbuf_t rbuf)
{
	for (;;)
	{
		LWLockAcquire(&rbuf->lock, LW_EXCLUSIVE);

		if (rbuf->occupied != 0)
		{
			LWLockRelease(&rbuf->lock);
			continue;
		}

		pg_atomic_write_u64(&rbuf->pread, 0);
		pg_atomic_write_u64(&rbuf->pwrite, 0);
		LWLockRelease(&rbuf->lock);
		break;
	}
}
