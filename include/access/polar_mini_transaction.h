/*-------------------------------------------------------------------------
 *
 * polar_mini_transaction.h
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
 *   src/include/access/polar_mini_transaction.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_MINI_TRANSACTION_H
#define POLAR_MINI_TRANSACTION_H

#include "port/atomics.h"
#include "storage/buf_internals.h"

typedef uint32  polar_page_lock_t;
#define POLAR_INVALID_PAGE_LOCK 0

/*
 * Use coalesced hashing to save mini transaction info
 * See https://en.wikipedia.org/wiki/Coalesced_hashing to get more detail about colesced hashing
 */
/*
 * Use prime number to minimize hash collisions.
 * The MINI_TRANSACTION_TABLE_SIZE must be larger than XLR_MAX_BLOCK_ID.
 */
#define MINI_TRANSACTION_HASH_SIZE   (31)
#define MINI_TRANSACTION_TABLE_SIZE  (MINI_TRANSACTION_HASH_SIZE + MINI_TRANSACTION_HASH_SIZE/5)
#define MINI_TRANSACTION_HASH_PAGE(tag) \
	((uint32)tag_hash(tag, sizeof(BufferTag)) % MINI_TRANSACTION_HASH_SIZE)

typedef struct mini_trans_info_t
{
	BufferTag               tag;
	pg_atomic_uint32        refcount;
	uint32                  next;
	bool                    added;
} mini_trans_info_t;

typedef struct mini_trans_data_t
{
	LWLockMinimallyPadded       lock[1 + MINI_TRANSACTION_TABLE_SIZE]; /* One lwlock for mini_trans_t which control to get/set mini_trans_info_t. The mini transaction table lock size is MINI_TRANSACTION_TABLE_SIZE*/
	bool                        started;
	XLogRecPtr                  lsn;
	/* Map to info array, if item of info array is in use then corresponding bit is set in occupied */
	uint64                      occupied;
	mini_trans_info_t           info[MINI_TRANSACTION_TABLE_SIZE];
} mini_trans_data_t;

typedef mini_trans_data_t *mini_trans_t;

extern Size polar_logindex_mini_trans_shmem_size(void);
extern mini_trans_t polar_logindex_mini_trans_shmem_init(const char *name);

extern polar_page_lock_t polar_logindex_mini_trans_lock(mini_trans_t trans, BufferTag *tag, LWLockMode mode, XLogRecPtr *lsn);
extern polar_page_lock_t polar_logindex_mini_trans_cond_lock(mini_trans_t trans, BufferTag *tag, LWLockMode mode, XLogRecPtr *lsn);
extern void polar_logindex_mini_trans_unlock(mini_trans_t trans, polar_page_lock_t lock);
extern XLogRecPtr polar_logindex_mini_trans_find(mini_trans_t trans, BufferTag *tag);
extern polar_page_lock_t polar_logindex_mini_trans_key_lock(mini_trans_t trans, BufferTag *tag, uint32 key,
															LWLockMode mode, XLogRecPtr *lsn);
extern XLogRecPtr polar_logindex_mini_trans_key_find(mini_trans_t trans, BufferTag *tag, uint32 key);
extern polar_page_lock_t polar_logindex_mini_trans_cond_key_lock(mini_trans_t trans, BufferTag *tag, uint32 key,
																 LWLockMode mode, XLogRecPtr *lsn);

extern int polar_logindex_mini_trans_start(mini_trans_t trans, XLogRecPtr lsn);
extern void polar_logindex_abort_mini_transaction(mini_trans_t trans);
extern int polar_logindex_mini_trans_end(mini_trans_t trans, XLogRecPtr lsn);
extern void polar_logindex_mini_trans_set_page_added(mini_trans_t trans, polar_page_lock_t lock);
extern bool polar_logindex_mini_trans_get_page_added(mini_trans_t trans, polar_page_lock_t lock);

#endif
