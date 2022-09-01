/*--------------------------------------------------------------------------
 *
 * px_hash.h
 *	  Definitions and API functions for pxhash.c
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * IDENTIFICATION
 *	    src/include/px/px_hash.h
 *
 *--------------------------------------------------------------------------
 */
#ifndef PXHASH_H
#define PXHASH_H

#include "utils/rel.h"

/* GUC */
extern bool px_use_legacy_hashops;

/*
 * reduction methods.
 */
typedef enum
{
	REDUCE_LAZYMOD = 1,
	REDUCE_BITMASK,
	REDUCE_JUMP_HASH
} PxHashReduce;

/*
 * Structure that holds Greenplum Database hashing information.
 */
typedef struct PxHash
{
	uint32		hash;			/* The result hash value							*/
	int			numsegs;		/* number of segments in Greenplum Database
								 * used for partitioning  */
	PxHashReduce reducealg;	/* the algorithm used for reducing to buckets		*/
	bool		is_legacy_hash;

	int			natts;
	FmgrInfo   *hashfuncs;
} PxHash;

/*
 * Create and initialize a PxHash in the current memory context.
 */
extern PxHash *makePxHash(int numsegs, int natts, Oid *typeoids);

/*
 * Initialize PxHash for hashing the next tuple values.
 */
extern void pxhashinit(PxHash *h);

/*
 * Add an attribute to the hash calculation.
 */
extern void pxhash(PxHash *h, int attno, Datum datum, bool isnull);

/*
 * Reduce the hash to a segment number.
 */
extern unsigned int pxhashreduce(PxHash *h);

/*
 * Return a random segment number, for a randomly distributed policy.
 */
extern unsigned int pxhashrandomseg(int numsegs);

extern unsigned int pxhashsegForUpdate(unsigned long long ctidHash,int numsegs);

extern Oid	px_default_distribution_opfamily_for_type(Oid typid);
extern Oid	px_default_distribution_opclass_for_type(Oid typid);
extern Oid	px_hashproc_in_opfamily(Oid opfamily, Oid typeoid);

/* prototypes and other things, from pxlegacyhash.c */

/* 32 bit FNV-1  non-zero initial basis */
#define FNV1_32_INIT ((uint32)0x811c9dc5)

extern uint32 magic_hash_stash;

#endif							/* PXHASH_H */
