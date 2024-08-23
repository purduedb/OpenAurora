/*-------------------------------------------------------------------------
 *
 * dynahash.c
 *	  dynamic hash tables
 *
 * dynahash.c supports both local-to-a-backend hash tables and hash tables in
 * shared memory.  For shared hash tables, it is the caller's responsibility
 * to provide appropriate access interlocking.  The simplest convention is
 * that a single LWLock protects the whole hash table.  Searches (HASH_FIND or
 * hash_seq_search) need only shared lock, but any update requires exclusive
 * lock.  For heavily-used shared tables, the single-lock approach creates a
 * concurrency bottleneck, so we also support "partitioned" locking wherein
 * there are multiple LWLocks guarding distinct subsets of the table.  To use
 * a hash table in partitioned mode, the HASH_PARTITION flag must be given
 * to hash_create_vm.  This prevents any attempt to split buckets on-the-fly.
 * Therefore, each hash bucket chain operates independently, and no fields
 * of the hash header change after init except nentries and freeList.
 * (A partitioned table uses multiple copies of those fields, guarded by
 * spinlocks, for additional concurrency.)
 * This lets any subset of the hash buckets be treated as a separately
 * lockable partition.  We expect callers to use the low-order bits of a
 * lookup key's hash value as a partition number --- this will work because
 * of the way calc_bucket() maps hash values to bucket numbers.
 *
 * For hash tables in shared memory, the memory allocator function should
 * match malloc's semantics of returning NULL on failure.  For hash tables
 * in local memory, we typically use palloc() which will throw error on
 * failure.  The code in this file has to cope with both cases.
 *
 * dynahash.c provides support for these types of lookup keys:
 *
 * 1. Null-terminated C strings (truncated if necessary to fit in keysize),
 * compared as though by strcmp().  This is the default behavior.
 *
 * 2. Arbitrary binary data of size keysize, compared as though by memcmp().
 * (Caller must ensure there are no undefined padding bits in the keys!)
 * This is selected by specifying HASH_BLOBS flag to hash_create_vm.
 *
 * 3. More complex key behavior can be selected by specifying user-supplied
 * hashing, comparison, and/or key-copying functions.  At least a hashing
 * function must be supplied; comparison defaults to memcmp() and key copying
 * to memcpy() when a user-defined hashing function is selected.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/hash/dynahash.c
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original comments:
 *
 * Dynamic hashing, after CACM April 1988 pp 446-457, by Per-Ake Larson.
 * Coded into C, with minor code improvements, and with hsearch(3) interface,
 * by ejp@ausmelb.oz, Jul 26, 1988: 13:16;
 * also, hcreate/hdestroy routines added to simulate hsearch(3).
 *
 * These routines simulate hsearch(3) and family, with the important
 * difference that the hash table is dynamic - can grow indefinitely
 * beyond its original size (as supplied to hcreate()).
 *
 * Performance appears to be comparable to that of hsearch(3).
 * The 'source-code' options referred to in hsearch(3)'s 'man' page
 * are not implemented; otherwise functionality is identical.
 *
 * Compilation controls:
 * HASH_DEBUG controls some informative traces, mainly for debugging.
 * HASH_STATISTICS causes HashAccesses and HashCollisions to be maintained;
 * when combined with HASH_DEBUG, these are displayed by hdestroy().
 *
 * Problems & fixes to ejp@ausmelb.oz. WARNING: relies on pre-processor
 * concatenation property, in probably unnecessary code 'optimization'.
 *
 * Modified margo@postgres.berkeley.edu February 1990
 *		added multiple table interface
 * Modified by sullivan@postgres.berkeley.edu April 1990
 *		changed ctl structure for shared memory
 */

#include "postgres.h"

#include <limits.h>

#include "access/xact.h"
#include "common/hashfn.h"
#include "port/pg_bitutils.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/dynahash.h"
#include "utils/memutils.h"
#include "utils/version_map.h"


/*
 * Constants
 *
 * A hash table has a top-level "directory", each of whose entries points
 * to a "segment" of ssize bucket headers.  The maximum number of hash
 * buckets is thus dsize * ssize (but dsize may be expansible).  Of course,
 * the number of records in the table can be larger, but we don't want a
 * whole lot of records per bucket or performance goes down.
 *
 * In a hash table allocated in shared memory, the directory cannot be
 * expanded because it must stay at a fixed address.  The directory size
 * should be selected using hash_select_dirsize (and you'd better have
 * a good idea of the maximum number of entries!).  For non-shared hash
 * tables, the initial directory size can be left at the default.
 */
#define DEF_SEGSIZE			   256
#define DEF_SEGSIZE_SHIFT	   8	/* must be log2(DEF_SEGSIZE) */
#define DEF_DIRSIZE			   256
#define DEF_FFACTOR			   1	/* default fill factor */

/* Number of freelists to be used for a partitioned hash table. */
#define NUM_FREELISTS			32

/* A hash bucket is a linked list of HASHELEMENTs */
typedef HASHELEMENT_VM *HASHBUCKET_VM;

/*
 * Per-freelist data.
 *
 * In a partitioned hash table, each freelist is associated with a specific
 * set of hashcodes, as determined by the FREELIST_IDX() macro below.
 * nentries tracks the number of live hashtable entries having those hashcodes
 * (NOT the number of entries in the freelist, as you might expect).
 *
 * The coverage of a freelist might be more or less than one partition, so it
 * needs its own lock rather than relying on caller locking.  Relying on that
 * wouldn't work even if the coverage was the same, because of the occasional
 * need to "borrow" entries from another freelist; see get_hash_entry().
 *
 * Using an array of FreeListData instead of separate arrays of mutexes,
 * nentries and freeLists helps to reduce sharing of cache lines between
 * different mutexes.
 */
typedef struct
{
	slock_t		mutex;			/* spinlock for this freelist */
	long		nentries;		/* number of entries in associated buckets */
	HASHELEMENT_VM *freeList;		/* chain of free elements */
} FreeListData_VM;

/*
 * Header structure for a hash table --- contains all changeable info
 *
 * In a shared-memory hash table, the HASHHDR_VM is in shared memory, while
 * each backend has a local HTAB_VM struct.  For a non-shared table, there isn't
 * any functional difference between HASHHDR_VM and HTAB_VM, but we separate them
 * anyway to share code between shared and non-shared tables.
 */
struct HASHHDR_VM
{
	/*
	 * The freelist can become a point of contention in high-concurrency hash
	 * tables, so we use an array of freelists, each with its own mutex and
	 * nentries count, instead of just a single one.  Although the freelists
	 * normally operate independently, we will scavenge entries from freelists
	 * other than a hashcode's default freelist when necessary.
	 *
	 * If the hash table is not partitioned, only freeList[0] is used and its
	 * spinlock is not used at all; callers' locking is assumed sufficient.
	 */
	FreeListData_VM freeList[NUM_FREELISTS];

	INDEX_ORDER_ITEM_VM* index_order;
	Size ord_begin, ord_end;
	HASHBUCKET_VM* hash_table;

	long segment_cnt;
	long hashtable_cnt;

	/* These fields can change, but not in a partitioned table */
	/* Also, dsize can't change in a shared table, even if unpartitioned */
	uint32		max_bucket;		/* ID of maximum bucket in use */
	uint32		high_mask;		/* mask to modulo into entire table */
	uint32		low_mask;		/* mask to modulo into lower half of table */

	/* These fields are fixed at hashtable creation */
	Size		keysize;		/* hash key length in bytes */
	Size		entrysize;		/* total user element size in bytes */
	long		num_partitions; /* # partitions (must be power of 2), or 0 */
	long		ffactor;		/* target fill factor */
	int			nelem_alloc;	/* number of entries to allocate at once */

};

#define IS_PARTITIONED(hctl)  ((hctl)->num_partitions != 0)

#define FREELIST_IDX(hctl, hashcode) \
	(IS_PARTITIONED(hctl) ? (hashcode) % NUM_FREELISTS : 0)

/*
 * Top control structure for a hashtable --- in a shared table, each backend
 * has its own copy (OK since no fields change at runtime)
 */
struct HTAB_VM
{
	HASHHDR_VM *hctl;			/* => shared control information */
	INDEX_ORDER_ITEM_VM* index_order;
	HASHBUCKET_VM* hash_table;
	HashValueFunc hash;			/* hash function */
	HashCompareFunc match;		/* key comparison function */
	HashCopyFunc keycopy;		/* key copying function */
	HashAllocFunc alloc;		/* memory allocator */
	MemoryContext hcxt;			/* memory context if default allocator used */
	char	   *tabname;		/* table name (for error messages) */
	bool		isshared;		/* true if table is in shared memory */
	bool		isfixed;		/* if true, don't enlarge */

	/* freezing a shared table isn't allowed, so we can keep state here */
	bool		frozen;			/* true = no more inserts allowed */

	/* We keep local copies of these fixed values to reduce contention */
	Size		keysize;		/* hash key length in bytes */
};

/*
 * Key (also entry) part of a HASHELEMENT_VM
 */
#define ELEMENTKEY(helem)  (((char *)(helem)) + MAXALIGN(sizeof(HASHELEMENT_VM)))

/*
 * Obtain element pointer given pointer to key
 */
#define ELEMENT_FROM_KEY(key)  \
	((HASHELEMENT_VM *) (((char *) (key)) - MAXALIGN(sizeof(HASHELEMENT_VM))))

/*
 * Fast MOD arithmetic, assuming that y is a power of 2 !
 */
#define MOD(x,y)			   ((x) & ((y)-1))

/*
 * Private function prototypes
 */
static void *DynaHashAlloc(Size size);
static void *seg_alloc(HTAB_VM *hashp);
static bool element_alloc(HTAB_VM *hashp, int nelem, int freelist_idx);
static bool expand_table(HTAB_VM *hashp);
static HASHBUCKET_VM get_hash_entry(HTAB_VM *hashp, int freelist_idx);
static void hdefault(HTAB_VM *hashp);
static int	choose_nelem_alloc(Size entrysize);
static bool init_htab(HTAB_VM *hashp, long nelem);
static void hash_corrupted(HTAB_VM *hashp);
static long next_pow2_long(long num);
static int	next_pow2_int(long num);
static void register_seq_scan(HTAB_VM *hashp);
static void deregister_seq_scan(HTAB_VM *hashp);
static bool has_seq_scans(HTAB_VM *hashp);


/*
 * memory allocation support
 */
static MemoryContext CurrentDynaHashCxt = NULL;

static void *
DynaHashAlloc(Size size)
{
	Assert(MemoryContextIsValid(CurrentDynaHashCxt));
	return MemoryContextAlloc(CurrentDynaHashCxt, size);
}


/*
 * HashCompareFunc for string keys
 *
 * Because we copy keys with strlcpy(), they will be truncated at keysize-1
 * bytes, so we can only compare that many ... hence strncmp is almost but
 * not quite the right thing.
 */
static int
string_compare(const char *key1, const char *key2, Size keysize)
{
	return strncmp(key1, key2, keysize - 1);
}


/************************** CREATE ROUTINES **********************/

/*
 * hash_create_vm -- create a new dynamic hash table
 *
 *	tabname: a name for the table (for debugging purposes)
 *	nelem: maximum number of elements expected
 *	*info: additional table parameters, as indicated by flags
 *	flags: bitmask indicating which parameters to take from *info
 *
 * Note: for a shared-memory hashtable, nelem needs to be a pretty good
 * estimate, since we can't expand the table on the fly.  But an unshared
 * hashtable can be expanded on-the-fly, so it's better for nelem to be
 * on the small side and let the table grow if it's exceeded.  An overly
 * large nelem will penalize hash_seq_search speed without buying much.
 */
HTAB_VM *
hash_create_vm(const char *tabname, long nelem, HASHCTL_VM *info, int flags)
{
	HTAB_VM	   *hashp;
	HASHHDR_VM    *hctl;

	/*
	 * For shared hash tables, we have a local hash header (HTAB_VM struct) that
	 * we allocate in TopMemoryContext; all else is in shared memory.
	 *
	 * For non-shared hash tables, everything including the hash header is in
	 * a memory context created specially for the hash table --- this makes
	 * hash_destroy_vm very simple.  The memory context is made a child of either
	 * a context specified by the caller, or TopMemoryContext if nothing is
	 * specified.
	 */
	if (flags & HASH_SHARED_MEM)
	{
		/* Set up to allocate the hash header */
		CurrentDynaHashCxt = TopMemoryContext;
	}
	else
	{
		/* Create the hash table's private memory context */
		if (flags & HASH_CONTEXT)
			CurrentDynaHashCxt = info->hcxt;
		else
			CurrentDynaHashCxt = TopMemoryContext;
		CurrentDynaHashCxt = AllocSetContextCreate(CurrentDynaHashCxt,
												   "versionmap",
												   ALLOCSET_DEFAULT_SIZES);
	}

	/* Initialize the hash header, plus a copy of the table name */
	hashp = (HTAB_VM *) DynaHashAlloc(sizeof(HTAB_VM) + strlen(tabname) + 1);
	MemSet(hashp, 0, sizeof(HTAB_VM));

	hashp->tabname = (char *) (hashp + 1);
	strcpy(hashp->tabname, tabname);

	/* If we have a private context, label it with hashtable's name */
	if (!(flags & HASH_SHARED_MEM))
		MemoryContextSetIdentifier(CurrentDynaHashCxt, hashp->tabname);

	/*
	 * Select the appropriate hash function (see comments at head of file).
	 */
	if (flags & HASH_FUNCTION)
		hashp->hash = info->hash;
	else if (flags & HASH_BLOBS)
	{
		/* We can optimize hashing for common key sizes */
		Assert(flags & HASH_ELEM);
		if (info->keysize == sizeof(uint32))
			hashp->hash = uint32_hash;
		else
			hashp->hash = tag_hash;
	}
	else
		hashp->hash = string_hash;	/* default hash function */

	/*
	 * If you don't specify a match function, it defaults to string_compare if
	 * you used string_hash (either explicitly or by default) and to memcmp
	 * otherwise.
	 *
	 * Note: explicitly specifying string_hash is deprecated, because this
	 * might not work for callers in loadable modules on some platforms due to
	 * referencing a trampoline instead of the string_hash function proper.
	 * Just let it default, eh?
	 */
	if (flags & HASH_COMPARE)
		hashp->match = info->match;
	else if (hashp->hash == string_hash)
		hashp->match = (HashCompareFunc) string_compare;
	else
		hashp->match = memcmp;

	/*
	 * Similarly, the key-copying function defaults to strlcpy or memcpy.
	 */
	if (flags & HASH_KEYCOPY)
		hashp->keycopy = info->keycopy;
	else if (hashp->hash == string_hash)
		hashp->keycopy = (HashCopyFunc) strlcpy;
	else
		hashp->keycopy = memcpy;

	/* And select the entry allocation function, too. */
	if (flags & HASH_ALLOC)
		hashp->alloc = info->alloc;
	else
		hashp->alloc = DynaHashAlloc;

	if (flags & HASH_SHARED_MEM)
	{
		hashp->hctl = info->hctl;
		hashp->index_order = (INDEX_ORDER_ITEM_VM *) (((char *) info->hctl) + MAXALIGN(sizeof(HASHHDR_VM)));
		hashp->hash_table = (HASHBUCKET_VM*)(hashp->index_order + info->segment_cnt * SLOT_CNT_VM);
		hashp->hcxt = NULL;
		hashp->isshared = true;

		/* hash table already exists, we're just attaching to it */
		if (flags & HASH_ATTACH)
		{
			/* make local copies of some heavily-used values */
			hctl = hashp->hctl;
			hashp->keysize = hctl->keysize;

			return hashp;
		}
	}
	else
	{
		/* setup hash table defaults */
		hashp->hctl = NULL;
		hashp->index_order = NULL;
		hashp->hash_table = NULL;
		hashp->hcxt = CurrentDynaHashCxt;
		hashp->isshared = false;
	}

	if (!hashp->hctl)
	{
		hashp->hctl = (HASHHDR_VM *) hashp->alloc(sizeof(HASHHDR_VM));
		if (!hashp->hctl)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
	}

	hashp->frozen = false;

	hdefault(hashp);

	hctl = hashp->hctl;
	hctl->hashtable_cnt = info->hashtable_cnt;
	hctl->segment_cnt = info->segment_cnt;

	if (flags & HASH_PARTITION)
	{
		/* Doesn't make sense to partition a local hash table */
		Assert(flags & HASH_SHARED_MEM);

		/*
		 * The number of partitions had better be a power of 2. Also, it must
		 * be less than INT_MAX (see init_htab()), so call the int version of
		 * next_pow2.
		 */
		Assert(info->num_partitions == next_pow2_int(info->num_partitions));

		hctl->num_partitions = info->num_partitions;
	}

	if (flags & HASH_FFACTOR)
		hctl->ffactor = info->ffactor;

	/*
	 * hash table now allocates space for key and data but you have to say how
	 * much space to allocate
	 */
	if (flags & HASH_ELEM)
	{
		Assert(info->entrysize >= info->keysize);
		hctl->keysize = info->keysize;
		hctl->entrysize = info->entrysize;
	}

	/* make local copies of heavily-used constant fields */
	hashp->keysize = hctl->keysize;

	/* Build the hash directory structure */
	if (!init_htab(hashp, nelem))
		elog(ERROR, "failed to initialize hash table \"%s\"", hashp->tabname);

	/*
	 * For a shared hash table, preallocate the requested number of elements.
	 * This reduces problems with run-time out-of-shared-memory conditions.
	 *
	 * For a non-shared hash table, preallocate the requested number of
	 * elements if it's less than our chosen nelem_alloc.  This avoids wasting
	 * space if the caller correctly estimates a small table size.
	 */
	if ((flags & HASH_SHARED_MEM) ||
		nelem < hctl->nelem_alloc)
	{
		int			i,
					freelist_partitions,
					nelem_alloc,
					nelem_alloc_first;

		/*
		 * If hash table is partitioned, give each freelist an equal share of
		 * the initial allocation.  Otherwise only freeList[0] is used.
		 */
		if (IS_PARTITIONED(hashp->hctl))
			freelist_partitions = NUM_FREELISTS;
		else
			freelist_partitions = 1;

		nelem_alloc = nelem / freelist_partitions;
		if (nelem_alloc <= 0)
			nelem_alloc = 1;

		/*
		 * Make sure we'll allocate all the requested elements; freeList[0]
		 * gets the excess if the request isn't divisible by NUM_FREELISTS.
		 */
		if (nelem_alloc * freelist_partitions < nelem)
			nelem_alloc_first =
				nelem - nelem_alloc * (freelist_partitions - 1);
		else
			nelem_alloc_first = nelem_alloc;

		for (i = 0; i < freelist_partitions; i++)
		{
			int			temp = (i == 0) ? nelem_alloc_first : nelem_alloc;

			if (!element_alloc(hashp, temp, i))
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
		}
	}

	if (flags & HASH_FIXED_SIZE)
		hashp->isfixed = true;
	return hashp;
}

/*
 * Set default HASHHDR_VM parameters.
 */
static void
hdefault(HTAB_VM *hashp)
{
	HASHHDR_VM    *hctl = hashp->hctl;

	MemSet(hctl, 0, sizeof(HASHHDR_VM));

	hctl->hashtable_cnt = 1ull << 13;
	hctl->segment_cnt = 1ull << 15;

	/* rather pointless defaults for key & entry size */
	hctl->keysize = sizeof(KeyType);
	hctl->entrysize = sizeof(SEGMENT_ITEM_VM);

	hctl->num_partitions = 0;	/* not partitioned */

	hctl->ffactor = DEF_FFACTOR;

}

/*
 * Given the user-specified entry size, choose nelem_alloc, ie, how many
 * elements to add to the hash table when we need more.
 */
static int
choose_nelem_alloc(Size entrysize)
{
	int			nelem_alloc;
	Size		elementSize;
	Size		allocSize;

	/* Each element has a HASHELEMENT_VM header plus user data. */
	/* NB: this had better match element_alloc() */
	elementSize = MAXALIGN(sizeof(HASHELEMENT_VM)) + MAXALIGN(entrysize);

	/*
	 * The idea here is to choose nelem_alloc at least 32, but round up so
	 * that the allocation request will be a power of 2 or just less. This
	 * makes little difference for hash tables in shared memory, but for hash
	 * tables managed by palloc, the allocation request will be rounded up to
	 * a power of 2 anyway.  If we fail to take this into account, we'll waste
	 * as much as half the allocated space.
	 */
	allocSize = 32 * 4;			/* assume elementSize at least 8 */
	do
	{
		allocSize <<= 1;
		nelem_alloc = allocSize / elementSize;
	} while (nelem_alloc < 32);

	return nelem_alloc;
}

/*
 * Compute derived fields of hctl and build the initial directory/segment
 * arrays
 */
static bool
init_htab(HTAB_VM *hashp, long nelem)
{
	HASHHDR_VM    *hctl = hashp->hctl;
	int			i;

	/*
	 * initialize mutexes if it's a partitioned table
	 */
	if (IS_PARTITIONED(hctl))
		for (i = 0; i < NUM_FREELISTS; i++)
			SpinLockInit(&(hctl->freeList[i].mutex));

	/*
	 * In a partitioned table, nbuckets must be at least equal to
	 * num_partitions; were it less, keys with apparently different partition
	 * numbers would map to the same bucket, breaking partition independence.
	 * (Normally nbuckets will be much bigger; this is just a safety check.)
	 */
#define nbuckets (hctl->hashtable_cnt)
	while (nbuckets < hctl->num_partitions)
		nbuckets <<= 1;

	hctl->max_bucket = hctl->low_mask = nbuckets - 1;
	hctl->high_mask = (nbuckets << 1) - 1;

	if(!(hashp->index_order)){
		CurrentDynaHashCxt = hashp->hcxt;
		hashp->index_order = (INDEX_ORDER_ITEM_VM *)
			hashp->alloc(hctl->segment_cnt * SLOT_CNT_VM * sizeof(INDEX_ORDER_ITEM_VM));
		hctl->ord_begin = hctl->ord_end = 0;
		if (!hashp->index_order)
			return false;
	}
	if (!(hashp->hash_table))
	{
		CurrentDynaHashCxt = hashp->hcxt;
		hashp->hash_table = (HASHBUCKET_VM *)
			hashp->alloc(nbuckets * sizeof(HASHBUCKET_VM));
		if (!hashp->hash_table)
			return false;
	}

	/* Choose number of entries to allocate at a time */
	hctl->nelem_alloc = choose_nelem_alloc(hctl->entrysize);
#undef nbuckets
#ifdef HASH_DEBUG
	fprintf(stderr, "init_htab:\n%s%p\n%s%ld\n%s%ld\n%s%d\n%s%ld\n%s%u\n%s%x\n%s%x\n%s%ld\n",
			"TABLE POINTER   ", hashp,
			"FILL FACTOR     ", hctl->ffactor,
			"MAX BUCKET      ", hctl->max_bucket,
			"HIGH MASK       ", hctl->high_mask,
			"LOW  MASK       ", hctl->low_mask);
#endif
	return true;
}

/*
 * Estimate the space needed for a hashtable containing the given number
 * of entries of given size.
 * NOTE: this is used to estimate the footprint of hashtables in shared
 * memory; therefore it does not count HTAB_VM which is in local memory.
 * NB: assumes that all hash structure parameters have default values!
 */
Size
hash_estimate_size_vm(long hashtable_cnt, long segment_cnt)
{
	Size		size;
	long		nBuckets,
				nElementAllocs,
				elementSize,
				elementAllocCnt;
	Size entrysize = sizeof(SEGMENT_ITEM_VM);

	/* estimate number of buckets wanted */
	nBuckets = next_pow2_long((hashtable_cnt - 1) / DEF_FFACTOR + 1);

	/* fixed control info */
	size = MAXALIGN(sizeof(HASHHDR_VM));	/* but not HTAB_VM, per above */
	size = add_size(size, mul_size(mul_size(segment_cnt, SLOT_CNT_VM), MAXALIGN(sizeof(INDEX_ORDER_ITEM_VM))));
	size = add_size(size, mul_size(hashtable_cnt, MAXALIGN(sizeof(HASHBUCKET_VM))));
	/* elements --- allocated in groups of choose_nelem_alloc() entries */
	elementAllocCnt = choose_nelem_alloc(entrysize);
	nElementAllocs = (segment_cnt - 1) / elementAllocCnt + 1;
	elementSize = MAXALIGN(sizeof(HASHELEMENT_VM)) + MAXALIGN(entrysize);
	size = add_size(size, mul_size(nElementAllocs, CACHELINEALIGN(mul_size(elementAllocCnt, elementSize))));
	return size;
}

/*
 * Compute the required initial memory allocation for a shared-memory
 * hashtable with the given parameters.  We need space for the HASHHDR_VM
 * and for the (non expansible) directory.
 */
Size
hash_get_shared_size_vm(HASHCTL_VM *info, int flags)
{
	return MAXALIGN(sizeof(HASHHDR_VM))
		+ info->segment_cnt * SLOT_CNT_VM * MAXALIGN(sizeof(INDEX_ORDER_ITEM_VM))
		+ info->hashtable_cnt * MAXALIGN(sizeof(HASHBUCKET_VM));
}


/********************** DESTROY ROUTINES ************************/

void
hash_destroy_vm(HTAB_VM *hashp)
{
	if (hashp != NULL)
	{
		/* allocation method must be one we know how to free, too */
		Assert(hashp->alloc == DynaHashAlloc);
		/* so this hashtable must have its own context */
		Assert(hashp->hcxt != NULL);

		/*
		 * Free everything by destroying the hash table's memory context.
		 */
		MemoryContextDelete(hashp->hcxt);
	}
}

/*******************************SEARCH ROUTINES *****************************/


/*
 * get_hash_value_vm -- exported routine to calculate a key's hash value
 *
 * We export this because for partitioned tables, callers need to compute
 * the partition number (from the low-order bits of the hash value) before
 * searching.
 */
uint32
get_hash_value_vm(HTAB_VM *hashp, const void *keyPtr)
{
	return hashp->hash(keyPtr, hashp->keysize);
}

/* Convert a hash value to a bucket number */
static inline uint32
calc_bucket(HASHHDR_VM *hctl, uint32 hash_val)
{
	uint32		bucket;

	bucket = hash_val & hctl->high_mask;
	if (bucket > hctl->max_bucket)
		bucket = bucket & hctl->low_mask;

	return bucket;
}

/*
 * hash_search_vm -- look up key in table and perform action
 * hash_search_with_hash_value_vm -- same, with key's hash value already computed
 *
 * action is one of:
 *		HASH_FIND: look up key in table
 *		HASH_ENTER: look up key in table, creating entry if not present
 *		HASH_ENTER_NULL: same, but return NULL if out of memory
 *		HASH_REMOVE: look up key in table, remove entry if present
 *
 * Return value is a pointer to the element found/entered/removed if any,
 * or NULL if no match was found.  (NB: in the case of the REMOVE action,
 * the result is a dangling pointer that shouldn't be dereferenced!)
 *
 * HASH_ENTER will normally ereport a generic "out of memory" error if
 * it is unable to create a new entry.  The HASH_ENTER_NULL operation is
 * the same except it will return NULL if out of memory.  Note that
 * HASH_ENTER_NULL cannot be used with the default palloc-based allocator,
 * since palloc internally ereports on out-of-memory.
 *
 * If foundPtr isn't NULL, then *foundPtr is set true if we found an
 * existing entry in the table, false otherwise.  This is needed in the
 * HASH_ENTER case, but is redundant with the return value otherwise.
 *
 * For hash_search_with_hash_value_vm, the hashvalue parameter must have been
 * calculated with get_hash_value_vm().
 */
void *
hash_search_vm(HTAB_VM *hashp,
			const void *keyPtr,
			HASHACTION action,
			bool *foundPtr, bool *head)
{
	return hash_search_with_hash_value_vm(hashp,
									   keyPtr,
									   hashp->hash(keyPtr, hashp->keysize),
									   action,
									   foundPtr, head);
}

void *
hash_search_with_hash_value_vm(HTAB_VM *hashp,
							const void *keyPtr,
							uint32 hashvalue,
							HASHACTION action,
							bool *foundPtr, bool *head)
{
	HASHHDR_VM    *hctl = hashp->hctl;
	int			freelist_idx = FREELIST_IDX(hctl, hashvalue);
	Size		keysize;
	uint32		bucket;
	long		segment_num;
	long		segment_ndx;
	HASHBUCKET_VM currBucket;
	HASHBUCKET_VM *prevBucketPtr;
	HASHBUCKET_VM currSeg;
	HASHBUCKET_VM *prevSegPtr;
	HashCompareFunc match;

	/*
	 * Do the initial lookup
	 */
	bucket = calc_bucket(hctl, hashvalue);

	prevBucketPtr = &hashp->hash_table[bucket];
	currBucket = *prevBucketPtr;

	/*
	 * Follow collision chain looking for matching key
	 */
	match = hashp->match;		/* save one fetch in inner loop */
	keysize = hashp->keysize;	/* ditto */

	while(currBucket != NULL)
	{
		if (currBucket->hashvalue == hashvalue &&
			match(&((ITEMHEAD_VM*)ELEMENTKEY(currBucket))->PageID, keyPtr, keysize) == 0)
			break;
		prevBucketPtr = &(((ITEMHEAD_VM*)ELEMENTKEY(currBucket))->next_item);
		currBucket = *prevBucketPtr;
	}

	if (foundPtr)
		*foundPtr = (bool) (currBucket != NULL);

	/*
	 * OK, now what?
	 */
	switch (action)
	{
		case HASH_FIND:
			if (currBucket != NULL){
				*head = true;
				return (void *) ELEMENTKEY(currBucket);
			}
			return NULL;

		// case HASH_REMOVE:
		// 	if (currBucket != NULL)
		// 	{
		// 		/* if partitioned, must lock to touch nentries and freeList */
		// 		if (IS_PARTITIONED(hctl))
		// 			SpinLockAcquire(&(hctl->freeList[freelist_idx].mutex));

		// 		/* delete the record from the appropriate nentries counter. */
		// 		Assert(hctl->freeList[freelist_idx].nentries > 0);
		// 		hctl->freeList[freelist_idx].nentries--;

		// 		/* remove record from hash bucket's chain. */
		// 		*prevBucketPtr = currBucket->link;

		// 		/* add the record to the appropriate freelist. */
		// 		currBucket->link = hctl->freeList[freelist_idx].freeList;
		// 		hctl->freeList[freelist_idx].freeList = currBucket;

		// 		if (IS_PARTITIONED(hctl))
		// 			SpinLockRelease(&hctl->freeList[freelist_idx].mutex);

		// 		/*
		// 		 * better hope the caller is synchronizing access to this
		// 		 * element, because someone else is going to reuse it the next
		// 		 * time something is added to the table
		// 		 */
		// 		return (void *) ELEMENTKEY(currBucket);
		// 	}
		// 	return NULL;

		case HASH_ENTER_NULL:
			/* ENTER_NULL does not work with palloc-based allocator */
			Assert(hashp->alloc != DynaHashAlloc);
			/* FALL THRU */

		case HASH_ENTER:
			/* Return existing element if found, else create one */
			*head = true;
			prevSegPtr = prevBucketPtr;
			currSeg = currBucket;
			while(currSeg != NULL && (*head
				? (((ITEMHEAD_VM*)ELEMENTKEY(currSeg))->lsn[ITEMHEAD_SLOT_CNT_VM - 1] != InvalidXLogRecPtr)
				: (((ITEMSEG_VM*)ELEMENTKEY(currSeg))->lsn[ITEMSEG_SLOT_CNT_VM - 1] != InvalidXLogRecPtr))){
				prevSegPtr = *head
					? (&((ITEMHEAD_VM*)ELEMENTKEY(currSeg))->next_seg)
					: (&((ITEMSEG_VM*)ELEMENTKEY(currSeg))->next_seg);
				currSeg = *prevSegPtr;
				*head = false;
			}
			if (currSeg != NULL)
				return (void *) ELEMENTKEY(currSeg);

			/* disallow inserts if frozen */
			if (hashp->frozen)
				elog(ERROR, "cannot insert into frozen hashtable \"%s\"",
					 hashp->tabname);

			currSeg = get_hash_entry(hashp, freelist_idx);
			if (currSeg == NULL)
			{
				/* out of memory */
				if (action == HASH_ENTER_NULL)
					return NULL;
				/* report a generic message */
				if (hashp->isshared)
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							 errmsg("out of shared memory")));
				else
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							 errmsg("out of memory")));
			}

			/* link into hashbucket chain */
			*prevSegPtr = currSeg;
			currSeg->link = NULL;

			/* copy key into record */
			currSeg->hashvalue = hashvalue;
			if(*head){
				hashp->keycopy(&((ITEMHEAD_VM*)ELEMENTKEY(currSeg))->PageID, keyPtr, keysize);
				((ITEMHEAD_VM*)ELEMENTKEY(currSeg))->next_item = NULL;
				((ITEMHEAD_VM*)ELEMENTKEY(currSeg))->next_seg = NULL;
				((ITEMHEAD_VM*)ELEMENTKEY(currSeg))->tail_seg = currSeg;
				for(int i = 0; i < ITEMHEAD_SLOT_CNT_VM; i++)
					((ITEMHEAD_VM*)ELEMENTKEY(currSeg))->lsn[i] = InvalidXLogRecPtr;
			}
			else{
				((ITEMSEG_VM*)ELEMENTKEY(currSeg))->next_seg = NULL;
				for(int i = 0; i < ITEMSEG_SLOT_CNT_VM; i++)
					((ITEMSEG_VM*)ELEMENTKEY(currSeg))->lsn[i] = InvalidXLogRecPtr;
				((ITEMHEAD_VM*)ELEMENTKEY(currBucket))->tail_seg = currSeg;
			}

			/*
			 * Caller is expected to fill the data field on return.  DO NOT
			 * insert any code that could possibly throw error here, as doing
			 * so would leave the table entry incomplete and hence corrupt the
			 * caller's data structure.
			 */

			return (void *) ELEMENTKEY(currSeg);
	}

	elog(ERROR, "unrecognized hash action code: %d", (int) action);

	return NULL;				/* keep compiler quiet */
}

/*
 * hash_update_hash_key_vm -- change the hash key of an existing table entry
 *
 * This is equivalent to removing the entry, making a new entry, and copying
 * over its data, except that the entry never goes to the table's freelist.
 * Therefore this cannot suffer an out-of-memory failure, even if there are
 * other processes operating in other partitions of the hashtable.
 *
 * Returns true if successful, false if the requested new hash key is already
 * present.  Throws error if the specified entry pointer isn't actually a
 * table member.
 *
 * NB: currently, there is no special case for old and new hash keys being
 * identical, which means we'll report false for that situation.  This is
 * preferable for existing uses.
 *
 * NB: for a partitioned hashtable, caller must hold lock on both relevant
 * partitions, if the new hash key would belong to a different partition.
 */
bool
hash_update_hash_key_vm(HTAB_VM *hashp,
					 void *existingEntry,
					 const void *newKeyPtr)
{
	Assert(false);
}

void* hash_next_segment_vm(void* item, bool head){
	HASHELEMENT_VM* next_seg = head ? ((ITEMHEAD_VM*)item)->next_seg : ((ITEMSEG_VM*)item)->next_seg;
	return next_seg != NULL ? (char *)next_seg + MAXALIGN(sizeof(HASHELEMENT_VM)) : NULL;
}

/*
 * Allocate a new hashtable entry if possible; return NULL if out of memory.
 * (Or, if the underlying space allocator throws error for out-of-memory,
 * we won't return at all.)
 */
static HASHBUCKET_VM
get_hash_entry(HTAB_VM *hashp, int freelist_idx)
{
	HASHHDR_VM    *hctl = hashp->hctl;
	HASHBUCKET_VM	newElement;

	for (;;)
	{
		/* if partitioned, must lock to touch nentries and freeList */
		if (IS_PARTITIONED(hctl))
			SpinLockAcquire(&hctl->freeList[freelist_idx].mutex);

		/* try to get an entry from the freelist */
		newElement = hctl->freeList[freelist_idx].freeList;

		if (newElement != NULL)
			break;

		if (IS_PARTITIONED(hctl))
			SpinLockRelease(&hctl->freeList[freelist_idx].mutex);

		/*
		 * No free elements in this freelist.  In a partitioned table, there
		 * might be entries in other freelists, but to reduce contention we
		 * prefer to first try to get another chunk of buckets from the main
		 * shmem allocator.  If that fails, though, we *MUST* root through all
		 * the other freelists before giving up.  There are multiple callers
		 * that assume that they can allocate every element in the initially
		 * requested table size, or that deleting an element guarantees they
		 * can insert a new element, even if shared memory is entirely full.
		 * Failing because the needed element is in a different freelist is
		 * not acceptable.
		 */
		if (!element_alloc(hashp, hctl->nelem_alloc, freelist_idx))
		{
			int			borrow_from_idx;

			if (!IS_PARTITIONED(hctl))
				return NULL;	/* out of memory */

			/* try to borrow element from another freelist */
			borrow_from_idx = freelist_idx;
			for (;;)
			{
				borrow_from_idx = (borrow_from_idx + 1) % NUM_FREELISTS;
				if (borrow_from_idx == freelist_idx)
					break;		/* examined all freelists, fail */

				SpinLockAcquire(&(hctl->freeList[borrow_from_idx].mutex));
				newElement = hctl->freeList[borrow_from_idx].freeList;

				if (newElement != NULL)
				{
					hctl->freeList[borrow_from_idx].freeList = newElement->link;
					SpinLockRelease(&(hctl->freeList[borrow_from_idx].mutex));

					/* careful: count the new element in its proper freelist */
					SpinLockAcquire(&hctl->freeList[freelist_idx].mutex);
					hctl->freeList[freelist_idx].nentries++;
					SpinLockRelease(&hctl->freeList[freelist_idx].mutex);

					return newElement;
				}

				SpinLockRelease(&(hctl->freeList[borrow_from_idx].mutex));
			}

			/* no elements available to borrow either, so out of memory */
			return NULL;
		}
	}

	/* remove entry from freelist, bump nentries */
	hctl->freeList[freelist_idx].freeList = newElement->link;
	hctl->freeList[freelist_idx].nentries++;

	if (IS_PARTITIONED(hctl))
		SpinLockRelease(&hctl->freeList[freelist_idx].mutex);

	return newElement;
}

/*
 * hash_get_num_entries_vm -- get the number of entries in a hashtable
 */
long
hash_get_num_entries_vm(HTAB_VM *hashp)
{
	int			i;
	long		sum = hashp->hctl->freeList[0].nentries;

	/*
	 * We currently don't bother with acquiring the mutexes; it's only
	 * sensible to call this function if you've got lock on all partitions of
	 * the table.
	 */
	if (IS_PARTITIONED(hashp->hctl))
	{
		for (i = 1; i < NUM_FREELISTS; i++)
			sum += hashp->hctl->freeList[i].nentries;
	}

	return sum;
}

/*
 * hash_freeze_vm
 *			Freeze a hashtable against future insertions (deletions are
 *			still allowed)
 *
 * The reason for doing this is that by preventing any more bucket splits,
 * we no longer need to worry about registering hash_seq_search scans,
 * and thus caller need not be careful about ensuring hash_seq_term gets
 * called at the right times.
 *
 * Multiple calls to hash_freeze_vm() are allowed, but you can't freeze a table
 * with active scans (since hash_seq_term would then do the wrong thing).
 */
void
hash_freeze_vm(HTAB_VM *hashp)
{
	Assert(false);
}


/********************************* UTILITIES ************************/

/*
 * Expand the table by adding one more hash bucket.
 */
static bool
expand_table(HTAB_VM *hashp)
{
	Assert(false);
}

static bool
dir_realloc(HTAB_VM *hashp)
{
	Assert(false);
}

static void*
seg_alloc(HTAB_VM *hashp)
{
	Assert(false);
}

/*
 * allocate some new elements and link them into the indicated free list
 */
static bool
element_alloc(HTAB_VM *hashp, int nelem, int freelist_idx)
{
	HASHHDR_VM    *hctl = hashp->hctl;
	Size		elementSize;
	HASHELEMENT_VM *firstElement;
	HASHELEMENT_VM *tmpElement;
	HASHELEMENT_VM *prevElement;
	int			i;

	if (hashp->isfixed)
		return false;

	/* Each element has a HASHELEMENT_VM header plus user data. */
	elementSize = MAXALIGN(sizeof(HASHELEMENT_VM)) + MAXALIGN(hctl->entrysize);

	CurrentDynaHashCxt = hashp->hcxt;
	firstElement = (HASHELEMENT_VM *) hashp->alloc(nelem * elementSize);

	if (!firstElement)
		return false;

	/* prepare to link all the new entries into the freelist */
	prevElement = NULL;
	tmpElement = firstElement;
	for (i = 0; i < nelem; i++)
	{
		tmpElement->link = prevElement;
		prevElement = tmpElement;
		tmpElement = (HASHELEMENT_VM *) (((char *) tmpElement) + elementSize);
	}

	/* if partitioned, must lock to touch freeList */
	if (IS_PARTITIONED(hctl))
		SpinLockAcquire(&hctl->freeList[freelist_idx].mutex);

	/* freelist could be nonempty if two backends did this concurrently */
	firstElement->link = hctl->freeList[freelist_idx].freeList;
	hctl->freeList[freelist_idx].freeList = prevElement;

	if (IS_PARTITIONED(hctl))
		SpinLockRelease(&hctl->freeList[freelist_idx].mutex);

	return true;
}

/* complain when we have detected a corrupted hashtable */
static void
hash_corrupted(HTAB_VM *hashp)
{
	/*
	 * If the corruption is in a shared hashtable, we'd better force a
	 * systemwide restart.  Otherwise, just shut down this one backend.
	 */
	if (hashp->isshared)
		elog(PANIC, "hash table \"%s\" corrupted", hashp->tabname);
	else
		elog(FATAL, "hash table \"%s\" corrupted", hashp->tabname);
}

/* calculate ceil(log base 2) of num */
int
my_log2_vm(long num)
{
	/*
	 * guard against too-large input, which would be invalid for
	 * pg_ceil_log2_*()
	 */
	if (num > LONG_MAX / 2)
		num = LONG_MAX / 2;

#if SIZEOF_LONG < 8
	return pg_ceil_log2_32(num);
#else
	return pg_ceil_log2_64(num);
#endif
}

/* calculate first power of 2 >= num, bounded to what will fit in a long */
static long
next_pow2_long(long num)
{
	/* my_log2_vm's internal range check is sufficient */
	return 1L << my_log2_vm(num);
}

/* calculate first power of 2 >= num, bounded to what will fit in an int */
static int
next_pow2_int(long num)
{
	if (num > INT_MAX / 2)
		num = INT_MAX / 2;
	return 1 << my_log2_vm(num);
}


/************************* SEQ SCAN TRACKING ************************/

/*
 * We track active hash_seq_search scans here.  The need for this mechanism
 * comes from the fact that a scan will get confused if a bucket split occurs
 * while it's in progress: it might visit entries twice, or even miss some
 * entirely (if it's partway through the same bucket that splits).  Hence
 * we want to inhibit bucket splits if there are any active scans on the
 * table being inserted into.  This is a fairly rare case in current usage,
 * so just postponing the split until the next insertion seems sufficient.
 *
 * Given present usages of the function, only a few scans are likely to be
 * open concurrently; so a finite-size stack of open scans seems sufficient,
 * and we don't worry that linear search is too slow.  Note that we do
 * allow multiple scans of the same hashtable to be open concurrently.
 *
 * This mechanism can support concurrent scan and insertion in a shared
 * hashtable if it's the same backend doing both.  It would fail otherwise,
 * but locking reasons seem to preclude any such scenario anyway, so we don't
 * worry.
 *
 * This arrangement is reasonably robust if a transient hashtable is deleted
 * without notifying us.  The absolute worst case is we might inhibit splits
 * in another table created later at exactly the same address.  We will give
 * a warning at transaction end for reference leaks, so any bugs leading to
 * lack of notification should be easy to catch.
 */

#define MAX_SEQ_SCANS 100

static HTAB_VM *seq_scan_tables[MAX_SEQ_SCANS];	/* tables being scanned */
static int	seq_scan_level[MAX_SEQ_SCANS];	/* subtransaction nest level */
static int	num_seq_scans = 0;


/* Register a table as having an active hash_seq_search scan */
static void
register_seq_scan(HTAB_VM *hashp)
{
	Assert(false);
}

/* Deregister an active scan */
static void
deregister_seq_scan(HTAB_VM *hashp)
{
	Assert(false);
}

/* Check if a table has any active scan */
static bool
has_seq_scans(HTAB_VM *hashp)
{
	Assert(false);
}
