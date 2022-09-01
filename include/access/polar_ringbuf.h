/*----------------------------------------------------------------------------------------
 *
 * polar_ringbuf.h
 *   polar ring buffer definitions.
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
 *    src/include/access/polar_ringbuf.h
 * ---------------------------------------------------------------------------------------
 */

#ifndef POLAR_LOGINDEX_RINGBUF_H
#define POLAR_LOGINDEX_RINGBUF_H

#include "port/atomics.h"
#include "storage/lwlock.h"

typedef void (*polar_interrupt_callback)(void);

/*
 * Each ring buffer reference occupy one slot.
 * Define the upper limit for ring buffer reference
 */
#define POLAR_RINGBUF_MAX_SLOT              32
#define POLAR_RINGBUF_MAX_REF_NAME          63
typedef struct polar_ringbuf_data_t         *polar_ringbuf_t;

typedef struct polar_ringbuf_slot_t
{
	/* Is this a strong reference? */
	bool                            strong;
	/* The read position of the ring buffer */
	uint64                          pread;
	/*
	 * The times of read operation
	 * And compare visit times to get least read position
	 */
	uint64                          visit;
	/* Each reference has one identity number */
	uint64                          ref_num;
	char                            ref_name[POLAR_RINGBUF_MAX_REF_NAME + 1];
} polar_ringbuf_slot_t;

/* The ring buffer reference struct */
typedef struct polar_ringbuf_ref_t
{
	/* referenced ring buffer */
	polar_ringbuf_t  rbuf;
	/* The identity number for this reference */
	uint64           ref_num;
	/* The slot number this reference occupied */
	int              slot;
	/* Is this a strong reference? */
	bool             strong;
	char             ref_name[POLAR_RINGBUF_MAX_REF_NAME + 1];
} polar_ringbuf_ref_t;

typedef struct polar_ringbuf_data_t
{
	/* This lock is used to manage slot */
	LWLock                  lock;
	/* The least read position of this ring buffer */
	pg_atomic_uint64        pread;
	/* The write position of this ring buffer */
	pg_atomic_uint64        pwrite;
	/* Increase this counter for each new reference */
	uint64                  ref_num;
	/* Map to slot array, if slot is in use then corresponding bit is set in occupied */
	uint64                  occupied;
	/* Used to manage reference */
	polar_ringbuf_slot_t    slot[POLAR_RINGBUF_MAX_SLOT];
	/*
	 * For ring buffer we cant compare read position to get least read position
	 * And we record each reference's read times.
	 * The read position with least read times is the least read position
	 */
	uint64                  min_visit;
	/* ring buffer data size */
	size_t                  size;
	uint8                   data[FLEXIBLE_ARRAY_MEMBER];
} polar_ringbuf_data_t;

/*
 * The data record in ring buffer as packet.
 * The packet head include 1 byte as flag and 4 byte record the data length
 */
#define POLAR_RINGBUF_PKTHDRSIZE 5
/* The whole packet size include packet head size and data size */
#define POLAR_RINGBUF_PKT_SIZE(len) ((len) + POLAR_RINGBUF_PKTHDRSIZE)

/* Each packet has one bit state flag */
#define POLAR_RINGBUF_PKT_FREE                  (0x00)          /* The packet data is not ready for read */
#define POLAR_RINGBUF_PKT_READY                 (0x01)          /* The packet data is ready for read */
#define POLAR_RINGBUF_PKT_STATE_MASK            (0x01)      /* The packet state mask */
/*
 * Define packet type about WAL.
 * Anyone can define it in your file, but the
 * POLAR_RINGBUF_PKT_INVALID_TYPE and POLAR_RINGBUF_PKT_TYPE_MASK
 * are stable.
 */
#define POLAR_RINGBUF_PKT_INVALID_TYPE          (0x00)      /* The packet type is invalid */
#define POLAR_RINGBUF_PKT_WAL_META              (0x10)      /* The packet content is xlog meta or clog */
#define POLAR_RINGBUF_PKT_WAL_STORAGE_BEGIN     (0x20)      /* Indicate we need to read from storage directly from this position */
#define POLAR_RINGBUF_PKT_WAL_STORAGE_END       (0x30)          /* Indicate we will read from queue after this position */
#define POLAR_RINGBUF_PKT_TYPE_MASK             (0xF0)      /* The packet type mask */

/* Get the packet data size and idx is the start position of the packet */
static inline uint32
polar_ringbuf_pkt_len(polar_ringbuf_t rbuf, size_t idx)
{
	uint32 len;
	uint8  *buf = (uint8 *)&len;
	size_t split, todo = sizeof(len);

	/* packet len is saved from idx+1 */
	idx = (idx + 1) % rbuf->size;
	split = ((idx + todo) > rbuf->size) ? rbuf->size - idx : 0;

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

extern polar_ringbuf_t polar_ringbuf_init(uint8 *data, size_t len, int tranche_id, const char *name);
extern bool polar_ringbuf_new_ref(polar_ringbuf_t rbuf, bool strong, polar_ringbuf_ref_t *ref, char *ref_name);
extern void polar_ringbuf_release_ref(polar_ringbuf_ref_t *ref);
extern bool polar_ringbuf_get_ref(polar_ringbuf_ref_t *ref);
extern bool polar_ringbuf_clear_ref(polar_ringbuf_ref_t *ref);
extern void polar_ringbuf_update_ref(polar_ringbuf_ref_t *ref);

extern ssize_t polar_ringbuf_pkt_write(polar_ringbuf_t rbuf, size_t idx, int offset, uint8 *buf, size_t len);
extern ssize_t polar_ringbuf_read_next_pkt(polar_ringbuf_ref_t *ref,
										   int offset, uint8 *buf, size_t len);
extern void polar_ringbuf_update_keep_data(polar_ringbuf_t rbuf);
extern void polar_ringbuf_free_up(polar_ringbuf_t rbuf, size_t len, polar_interrupt_callback callback);
extern void polar_ringbuf_auto_release_ref(polar_ringbuf_ref_t *ref);
extern bool polar_ringbuf_valid_ref(polar_ringbuf_ref_t *ref);

extern void polar_ringbuf_ref_keep_data(polar_ringbuf_ref_t *ref, float ratio);
extern void polar_ringbuf_reset(polar_ringbuf_t rbuf);

/*
 * get the free size of the ring buffer
 */
static inline ssize_t
polar_ringbuf_free_size(polar_ringbuf_t rbuf)
{
	ssize_t free_bytes;

	free_bytes = pg_atomic_read_u64(&rbuf->pread);
	free_bytes -= pg_atomic_read_u64(&rbuf->pwrite);

	if (free_bytes <= 0)
		free_bytes += rbuf->size;

	return free_bytes - 1;
}

/*
 * The data that is available or reserved for read
 */
static inline ssize_t
polar_ringbuf_avail(polar_ringbuf_ref_t *ref)
{
	ssize_t avail;
	polar_ringbuf_t rbuf = ref->rbuf;

	avail = pg_atomic_read_u64(&rbuf->pwrite);
	avail -= rbuf->slot[ref->slot].pread;

	if (avail < 0)
		avail += rbuf->size;

	return avail;
}

/*
 * Reserve space from ring buffer for future write
 * This function should be protected by exclusive lock
 */
static inline size_t
polar_ringbuf_pkt_reserve(polar_ringbuf_t rbuf, size_t len)
{
	size_t idx = pg_atomic_read_u64(&rbuf->pwrite);
	rbuf->data[idx] = POLAR_RINGBUF_PKT_FREE;

	/* ensure set packet flag before update pwrite */
	pg_write_barrier();
	pg_atomic_write_u64(&rbuf->pwrite,
						(idx + len) % rbuf->size);
	return idx;
}

/*
 * Check whether the next packet for this reference is ready
 * And return packet length when data is ready for read
 */
static inline uint8
polar_ringbuf_next_ready_pkt(polar_ringbuf_ref_t *ref, uint32 *pktlen)
{
	polar_ringbuf_t rbuf = ref->rbuf;
	size_t idx = rbuf->slot[ref->slot].pread;

	*pktlen = 0;

	if ((rbuf->data[idx] & POLAR_RINGBUF_PKT_STATE_MASK) != POLAR_RINGBUF_PKT_READY)
		return POLAR_RINGBUF_PKT_INVALID_TYPE;

	/* Make sure we don't see the packet flag value before get packet length */
	pg_read_barrier();

	*pktlen = polar_ringbuf_pkt_len(rbuf, idx);

	return rbuf->data[idx] & POLAR_RINGBUF_PKT_TYPE_MASK;
}

/*
 * Set packet data length.
 * The param idx is the start point of this packet
 */
static inline void
polar_ringbuf_set_pkt_length(polar_ringbuf_t rbuf, size_t idx, uint32 len)
{
	size_t split, todo = sizeof(len);
	uint8 *buf = (uint8 *)&len;

	/* The first byte is flag and packet length is saved in next 4 bytes */
	idx = (idx + 1) % rbuf->size;

	split = ((idx + todo) > rbuf->size) ? rbuf->size - idx : 0;

	if (split > 0)
	{
		memcpy(rbuf->data + idx, buf, split);
		buf += split;
		todo -= split;
		idx = 0;
	}

	memcpy(rbuf->data + idx, buf, todo);
}

/*
 * Set packet flag which it's saved in idx position
 */
static inline void
polar_ringbuf_set_pkt_flag(polar_ringbuf_t rbuf, size_t idx, uint8 flag)
{
	/* ensure all previous writes are visible before follower continues. */
	pg_write_barrier();
	rbuf->data[idx] = flag;
}

#endif
