/*-------------------------------------------------------------------------
 *
 * px_util.h
 *	  Header file for routines in pxutil.c and results returned by
 *	  those routines.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * IDENTIFICATION
 *	    src/include/px/px_util.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PXUTIL_H
#define PXUTIL_H

#include "nodes/pg_list.h"
#include "utils/rel.h"


struct PxWorkerDescriptor;

typedef struct PxNodeInfo PxNodeInfo;
typedef struct PxNodes PxNodes;

/* --------------------------------------------------------------------------------------------------
 * Structure for PX 2.0 database information
 *
 * The information contained in this structure represents logically a row from
 * px_node_configuration.  It is used to describe either an entry
 * database or a segment database.
 *
 * Storage for instances of this structure are palloc'd.  Storage for the values
 * pointed to by non-NULL char*'s are also palloc'd.
 *
 */
#define COMPONENT_DBS_MAX_ADDRS (8)

/* POLAR px: For Multi Insert */
#define RW_SEGMENT				(-10002)
#define RW_COUNTER_START		(100000)
/* POLAR end */

typedef struct PxNodeConfigEntry
{
	/* copy of entry in px_node_configuration */
	int16		dbid;			/* the dbid of this database */
	int16		node_idx;		/* content indicator: -1 for entry database,
								 * 0, ..., n-1 for segment database */

	char		role;			/* primary, master, mirror, master-standby */
	char		preferred_role; /* what role would we "like" to have this
								 * segment in ? */
	char		mode;
	char		status;
	int32		dop;
	int32		port;			/* port that instance is listening on */
	char	   *datadir;		/* absolute path to data directory on the
								 * host. */

	/* additional cached info */
	char	   *name;
	char	   *hostip;			/* cached lookup of name */
	char	   *hostaddrs[COMPONENT_DBS_MAX_ADDRS]; /* cached lookup of names */
} PxNodeConfigEntry;

struct PxNodeInfo
{
	struct PxNodeConfigEntry *config;

	PxNodes 	*px_nodes;	/* point to owners */

	int16		hostSegs;		/* number of primary segments on the same hosts */
	List	   *freelist;		/* list of idle segment dbs */
	int			numIdlePXs;
	int			numActivePXs;
	int			cm_node_idx;    /* position of cluster_map config, for adaptive scan */
	int			cm_node_size;   /* node size of cluster_map */
};


/* --------------------------------------------------------------------------------------------------
 * Structure for return value from getPxWorkerDatabases()
 *
 * The storage for instances of this structure returned by getPxWorkerInstances() is palloc'd.
 * freePxWorkerDatabases() can be used to release the structure.
 */
struct PxNodes
{
	PxNodeInfo *qcInfo;			/* array of PxNodeInfo's for QcWorker*/
	int			totalQcNodes;	/* count of the array  */
	PxNodeInfo *pxInfo;			/* array of PxNodeInfo's for PxWorker*/
	int         totalPxNodes;   /* count of the array  */

	uint8		fts_version;		/* the version of fts */
	int			expand_version;
	int			numActivePXs;
	int			numIdlePXs;
	int			pxCounter;
	/* POLAR px */
	int			rwCounter;
	/* POLAR end */
	List	   *freeCounterList;
};

/*  */
typedef enum SegmentType
{
	SEGMENTTYPE_EXPLICT_WRITER = 1,
	SEGMENTTYPE_EXPLICT_READER,
	SEGMENTTYPE_ANY
} SegmentType;

/*
 * performs all necessary setup required for initializing Greenplum Database components.
 *
 * This includes pxlink_setup() and initializing the Motion Layer.
 *
 */
extern void px_setup(void);


/*
 * performs all necessary cleanup required when cleaning up Greenplum Database components
 * when disabling Greenplum Database functionality.
 *
 */
extern void px_cleanup(int code, Datum arg pg_attribute_unused());


/*
 * pxnode_getPxNodes() returns a pointer to a PxNodes
 * structure. Both the qcInfo array and the px_info_array are ordered by worker_idx,
 * isprimary desc.
 *
 * The PxNodes structure, and the qcInfo array and the px_info_array
 * are contained in palloc'd storage allocated from the current storage context.
 * The same is true for pointer-based values in PxNodeInfo.  The caller is responsible
 * for setting the current storage context and releasing the storage occupied the returned values.
 */
PxNodes *pxnode_getPxNodes(void);
void pxnode_destroyPxNodes(void);

PxNodeInfo *pxnode_getPxNodeInfo(int contentId);

struct PxWorkerDescriptor *pxnode_allocateIdlePX(int logicalWorkerIdx, int logicalTotalWorkers, SegmentType segmentType);

void		pxnode_recycleIdlePX(struct PxWorkerDescriptor *pxWorkerDesc, bool forceDestroy);

/*
 * Given total number of primary segment databases and a number of segments
 * to "skip" - this routine creates a boolean map (array) the size of total
 * number of segments and randomly selects several entries (total number of
 * total_to_skip) to be marked as "skipped". This is used for external tables
 * with the 'gpfdist' protocol where we want to get a number of *random* segdbs
 * to connect to a gpfdist client.
 */
extern bool *makeRandomSegMap(int total_primaries, int total_to_skip);

/*
 * Returns the number of segments
 */
extern int	getPxWorkerCount(void);

#define ELOG_DISPATCHER_DEBUG(...) do { \
       if (px_log_gang >= PXVARS_VERBOSITY_DEBUG) elog(LOG, __VA_ARGS__); \
    } while(false);

extern MemoryContext SwitchToPXWorkerContext(void);
extern char *GeneratePxWorkerNames(void);

#endif							/* PXUTIL_H */
