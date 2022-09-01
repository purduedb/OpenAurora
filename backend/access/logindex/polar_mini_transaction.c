/*-------------------------------------------------------------------------
 *
 * polar_mini_transaction.c
 *      Polar mini transaction manager
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
 *     src/backend/access/logindexm/polar_mini_transaction.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * In postgres a XLOG record may include multiple pages. We use mini transaction to keep data structure change in atomic way.
 * Without mini transaction maybe there exists different verions of data page in a structure.
 * The max number of pages in a XLOG record is XLR_MAX_BLOCK_ID and
 * we use coalesced hash to keep page lock during the mini transaction.
 * 1. When we add LSN to logindex  it's mini transaction page lock must be acquired firstly.
 * And the sequence of pages lcok acquisition is the same as redo XLOG for the specific data structure.
 * 2. When search logindex for a page to apply  its mini transaction lock must be acquired if it existsn
 */
#include "postgres.h"
#include "access/polar_mini_transaction.h"
#include "access/xlogdefs.h"
#include "access/xlogrecord.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "utils/polar_bitpos.h"

typedef struct proc_occupied_mini_trans_t
{
	mini_trans_t trans;
	bool acquired_lock[MINI_TRANSACTION_TABLE_SIZE];
} proc_occupied_mini_trans_t;

static proc_occupied_mini_trans_t proc_occupied_mini_trans = {0};

#define MINI_TRANS_IS_OCCUPIED(trans, key)             POLAR_BIT_IS_OCCUPIED((trans)->occupied, (key))
#define MINI_TRANS_OCCUPY(trans, key)                  (POLAR_BIT_OCCUPY((trans)->occupied, (key)))
#define MINI_TRANSACTION_LOCK(trans)                   (&(trans)->lock[0].lock)
#define MINI_TRANSACTION_TABLE_LOCK(trans, k)      (&(trans)->lock[1+(k)].lock)

static void
mini_trans_init(mini_trans_t trans)
{
	int i;

	trans->started = false;
	trans->lsn = InvalidXLogRecPtr;
	trans->occupied = 0;
	MemSet(trans->info, 0, sizeof(mini_trans_info_t)*MINI_TRANSACTION_TABLE_SIZE);

	LWLockRegisterTranche(LWTRANCHE_LOGINDEX_MINI_TRANSACTION, "logindex_mini_transaction");
	LWLockInitialize(MINI_TRANSACTION_LOCK(trans), LWTRANCHE_LOGINDEX_MINI_TRANSACTION);

	LWLockRegisterTranche(LWTRANCHE_LOGINDEX_MINI_TRANSACTION_TBL, "logindex_mini_transaction_tbl");

	for (i = 0; i < MINI_TRANSACTION_TABLE_SIZE; i++)
		LWLockInitialize(MINI_TRANSACTION_TABLE_LOCK(trans, i), LWTRANCHE_LOGINDEX_MINI_TRANSACTION_TBL);
}

Size
polar_logindex_mini_trans_shmem_size(void)
{
	return CACHELINEALIGN(sizeof(mini_trans_data_t));
}

mini_trans_t
polar_logindex_mini_trans_shmem_init(const char *name)
{
	bool found;
	mini_trans_t mini_trans;

	StaticAssertStmt(MINI_TRANSACTION_TABLE_SIZE <= sizeof(uint64_t) * CHAR_BIT,
					 "MINI_TRANSACTION_TABLE_SIZE is larger than 64bit");
	StaticAssertStmt(MINI_TRANSACTION_TABLE_SIZE > XLR_MAX_BLOCK_ID,
					 "MINI_TRANSACTION_TABLE_SIZE is smaller than XLR_MAX_BLOCK_ID");

	mini_trans = (mini_trans_t)ShmemInitStruct(name, polar_logindex_mini_trans_shmem_size(), &found);

	if (!IsUnderPostmaster)
		mini_trans_init(mini_trans);
	else
		Assert(found);

	return mini_trans;
}

void
polar_logindex_abort_mini_transaction(mini_trans_t trans)
{
	polar_page_lock_t l;

	if (proc_occupied_mini_trans.trans == NULL)
		return;

	Assert(proc_occupied_mini_trans.trans == trans);

	for (l = 1; l <= MINI_TRANSACTION_TABLE_SIZE; l++)
	{
		if (proc_occupied_mini_trans.acquired_lock[l - 1])
			polar_logindex_mini_trans_unlock(trans, l);
	}

	proc_occupied_mini_trans.trans = NULL;
}

int
polar_logindex_mini_trans_start(mini_trans_t trans, XLogRecPtr lsn)
{
	LWLockAcquire(MINI_TRANSACTION_LOCK(trans), LW_EXCLUSIVE);

	if (unlikely(trans->lsn != InvalidXLogRecPtr || trans->started))
	{
		LWLockRelease(MINI_TRANSACTION_LOCK(trans));
		ereport(PANIC, (errmsg("The previous lsn %ld is not end, started=%d", trans->lsn, trans->started)));
	}

	trans->started = true;
	trans->lsn = lsn;
	LWLockRelease(MINI_TRANSACTION_LOCK(trans));

	return 0;
}

static inline void
mini_trans_set_info(mini_trans_info_t *info, BufferTag *tag)
{
	INIT_BUFFERTAG(info->tag, tag->rnode, tag->forkNum, tag->blockNum);
	pg_atomic_write_u32(&info->refcount, 1);
	info->next = 0;
}

static polar_page_lock_t
mini_trans_insert_tag(mini_trans_t trans, BufferTag *tag, uint32 key)
{
	mini_trans_info_t *info = trans->info;

	if (trans->lsn == InvalidXLogRecPtr || trans->started == false)
		ereport(PANIC, (errmsg("The mini transaction lsn is not set")));

	if (key >= MINI_TRANSACTION_TABLE_SIZE)
		ereport(PANIC, (errmsg("The mini transaction key is incorrect %d", key)));

	if (!MINI_TRANS_IS_OCCUPIED(trans, key + 1))
	{
		MINI_TRANS_OCCUPY(trans, key + 1);
		mini_trans_set_info(&info[key], tag);
		return key + 1;
	}
	else
	{
		uint32 i = MINI_TRANSACTION_TABLE_SIZE;
		mini_trans_info_t *it;

		/* Find the first empty bucket */
		while (i > 0)
		{
			if (!MINI_TRANS_IS_OCCUPIED(trans, i))
				break;

			i--;
		}

		/* The table is full, terminate unsuccessfully */
		if (i == 0)
			return i;

		MINI_TRANS_OCCUPY(trans, i);
		mini_trans_set_info(&info[i - 1], tag);

		/* Find the last node in the chain and point to it */
		it = &info[key];

		while (it->next != 0)
			it = &info[it->next - 1];

		it->next = i;

		return i;
	}

}

static polar_page_lock_t
mini_trans_find(mini_trans_t trans, BufferTag *tag, uint32 key)
{
	mini_trans_info_t *info = trans->info;

	if (trans->lsn == InvalidXLogRecPtr || trans->started == false)
		ereport(PANIC, (errmsg("The mini transaction lsn is not set")));

	if (MINI_TRANS_IS_OCCUPIED(trans, key + 1))
	{
		uint32 i = key + 1;

		while (i != 0)
		{
			mini_trans_info_t *it = &info[i - 1];
			Assert(MINI_TRANS_IS_OCCUPIED(trans, i));

			if (BUFFERTAGS_EQUAL(*tag, it->tag))
				return i;

			i = it->next;
		}
	}

	return POLAR_INVALID_PAGE_LOCK;
}

static polar_page_lock_t
mini_trans_increase_ref(mini_trans_t trans, BufferTag *tag, uint32 key)
{
	uint32 i;
	mini_trans_info_t *info = trans->info;

	i = mini_trans_find(trans, tag, key);

	if (i != POLAR_INVALID_PAGE_LOCK)
	{
		mini_trans_info_t *it;
		it = &info[i - 1];
		pg_atomic_add_fetch_u32(&(it->refcount), 1);
	}

	return i;
}

polar_page_lock_t
polar_logindex_mini_trans_cond_key_lock(mini_trans_t trans, BufferTag *tag, uint32 key,
										LWLockMode mode, XLogRecPtr *lsn)
{
	polar_page_lock_t l = POLAR_INVALID_PAGE_LOCK;
	LWLock *lock = NULL;

	LWLockAcquire(MINI_TRANSACTION_LOCK(trans), LW_SHARED);

	if (trans->started == true && trans->lsn != InvalidXLogRecPtr)
	{
		l = mini_trans_increase_ref(trans, tag, key);

		if (l != POLAR_INVALID_PAGE_LOCK)
		{
			lock = MINI_TRANSACTION_TABLE_LOCK(trans, l - 1);

			if (lsn != NULL)
				*lsn = trans->lsn;
		}
	}

	LWLockRelease(MINI_TRANSACTION_LOCK(trans));

	if (lock)
	{
		LWLockAcquire(lock, mode);

		if (proc_occupied_mini_trans.trans == NULL)
			proc_occupied_mini_trans.trans = trans;

		Assert(proc_occupied_mini_trans.trans == trans);

		proc_occupied_mini_trans.acquired_lock[l - 1] = true;
	}

	return l;
}

polar_page_lock_t
polar_logindex_mini_trans_cond_lock(mini_trans_t trans, BufferTag *tag,
									LWLockMode mode, XLogRecPtr *lsn)
{
	uint32 key = MINI_TRANSACTION_HASH_PAGE(tag);
	return polar_logindex_mini_trans_cond_key_lock(trans, tag, key, mode, lsn);
}

XLogRecPtr
polar_logindex_mini_trans_key_find(mini_trans_t trans, BufferTag *tag, uint32 key)
{
	XLogRecPtr lsn = InvalidXLogRecPtr;

	LWLockAcquire(MINI_TRANSACTION_LOCK(trans), LW_SHARED);

	if (trans->started == true && trans->lsn != InvalidXLogRecPtr)
	{
		if (mini_trans_find(trans, tag, key) != POLAR_INVALID_PAGE_LOCK)
			lsn = trans->lsn;
	}

	LWLockRelease(MINI_TRANSACTION_LOCK(trans));

	return lsn;
}

XLogRecPtr
polar_logindex_mini_trans_find(mini_trans_t trans, BufferTag *tag)
{
	uint32 key = MINI_TRANSACTION_HASH_PAGE(tag);

	return polar_logindex_mini_trans_key_find(trans, tag, key);
}

polar_page_lock_t
polar_logindex_mini_trans_key_lock(mini_trans_t trans, BufferTag *tag, uint32 key,
								   LWLockMode mode, XLogRecPtr *lsn)
{
	polar_page_lock_t l;
	LWLock *lock = NULL;

	LWLockAcquire(MINI_TRANSACTION_LOCK(trans), LW_EXCLUSIVE);
	l = mini_trans_increase_ref(trans, tag, key);

	if (l != POLAR_INVALID_PAGE_LOCK)
	{
		lock = MINI_TRANSACTION_TABLE_LOCK(trans, l - 1);

		if (lsn != NULL)
			*lsn = trans->lsn;
	}
	else
	{
		l = mini_trans_insert_tag(trans, tag, key);

		if (l != POLAR_INVALID_PAGE_LOCK)
		{
			lock = MINI_TRANSACTION_TABLE_LOCK(trans, l - 1);

			if (lsn != NULL)
				*lsn = trans->lsn;
		}
	}

	LWLockRelease(MINI_TRANSACTION_LOCK(trans));

	if (lock)
	{
		LWLockAcquire(lock, mode);

		if (proc_occupied_mini_trans.trans == NULL)
			proc_occupied_mini_trans.trans = trans;

		Assert(trans == proc_occupied_mini_trans.trans);

		proc_occupied_mini_trans.acquired_lock[l - 1] = true;
	}

	return l;
}

polar_page_lock_t
polar_logindex_mini_trans_lock(mini_trans_t trans, BufferTag *tag, LWLockMode mode, XLogRecPtr *lsn)
{
	uint32 key = MINI_TRANSACTION_HASH_PAGE(tag);
	uint32 l = polar_logindex_mini_trans_key_lock(trans, tag, key, mode, lsn);

	if (l == POLAR_INVALID_PAGE_LOCK)
		ereport(PANIC, (errmsg("The mini transaction hash table is full")));

	return l;
}

void
polar_logindex_mini_trans_unlock(mini_trans_t trans, polar_page_lock_t l)
{
	mini_trans_info_t *info = trans->info;
	uint32 i ;

	if (l == POLAR_INVALID_PAGE_LOCK || l > MINI_TRANSACTION_TABLE_SIZE)
		ereport(PANIC, (errmsg("The mini transaction hash slot value is incorrect")));

	i = l - 1;

	if (trans != proc_occupied_mini_trans.trans || !proc_occupied_mini_trans.acquired_lock[i])
		elog(PANIC, "Unlock mini transaction lock %d, but it's not acquired", l);

	LWLockRelease(MINI_TRANSACTION_TABLE_LOCK(trans, i));
	LWLockAcquire(MINI_TRANSACTION_LOCK(trans), LW_SHARED);

	if (!MINI_TRANS_IS_OCCUPIED(trans, l)
			|| pg_atomic_read_u32(&info[i].refcount) == 0)
	{
		LWLockRelease(MINI_TRANSACTION_LOCK(trans));
		ereport(PANIC, (errmsg("The mini transaction hash slot state is incorrect, occupied=%ld, refcount is 0",
							   MINI_TRANS_IS_OCCUPIED(trans, l))));
	}

	pg_atomic_sub_fetch_u32(&info[i].refcount, 1);
	LWLockRelease(MINI_TRANSACTION_LOCK(trans));

	proc_occupied_mini_trans.acquired_lock[i] = false;
}

int
polar_logindex_mini_trans_end(mini_trans_t trans, XLogRecPtr lsn)
{
	mini_trans_info_t *info = trans->info;
	uint64_t occupied;
	bool unlock_all;
	int pos;

	LWLockAcquire(MINI_TRANSACTION_LOCK(trans), LW_EXCLUSIVE);

	if (trans->lsn != lsn)
	{
		LWLockRelease(MINI_TRANSACTION_LOCK(trans));
		ereport(PANIC, (errmsg("The previous lsn %ld is not finished", trans->lsn)));
	}

	trans->started = false;

	LWLockRelease(MINI_TRANSACTION_LOCK(trans));

	do
	{
		unlock_all = true;

		LWLockAcquire(MINI_TRANSACTION_LOCK(trans), LW_SHARED);
		occupied = trans->occupied;

		while (occupied)
		{
			/* Get position of the lowest bit */
			POLAR_BIT_LEAST_POS(occupied, pos);

			if (pg_atomic_read_u32(&info[pos - 1].refcount) > 0)
			{
				unlock_all = false;
				break;
			}

			/* Clear the lowest bit */
			POLAR_BIT_CLEAR_LEAST(occupied);
		}

		/* Release mini transaction lock and wait page lock to be released */
		LWLockRelease(MINI_TRANSACTION_LOCK(trans));
	}
	while (!unlock_all);

	/* Clear all occupied lock */
	while (trans->occupied)
	{
		POLAR_BIT_LEAST_POS(trans->occupied, pos);
		MemSet(&info[pos - 1], 0, sizeof(mini_trans_info_t));
		POLAR_BIT_CLEAR_LEAST(trans->occupied);
	}

	trans->lsn = InvalidXLogRecPtr;

	return 0;
}

void
polar_logindex_mini_trans_set_page_added(mini_trans_t trans, polar_page_lock_t lock)
{
	uint32 i;

	if (lock == POLAR_INVALID_PAGE_LOCK || lock > MINI_TRANSACTION_TABLE_SIZE
			|| !MINI_TRANS_IS_OCCUPIED(trans, lock))
		ereport(PANIC, (errmsg("The mini transaction hash slot value is incorrect")));


	i = lock - 1;

	Assert(trans->info[i].added == false);
	trans->info[i].added = true;
}

bool
polar_logindex_mini_trans_get_page_added(mini_trans_t trans, polar_page_lock_t lock)
{
	uint32 i;

	if (lock == POLAR_INVALID_PAGE_LOCK || lock > MINI_TRANSACTION_TABLE_SIZE
			|| !MINI_TRANS_IS_OCCUPIED(trans, lock))
		ereport(PANIC, (errmsg("The mini transaction hash slot value is incorrect")));


	i = lock - 1;

	return trans->info[i].added;
}
