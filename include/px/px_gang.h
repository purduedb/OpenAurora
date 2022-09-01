/*-------------------------------------------------------------------------
 *
 * px_gang.h
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * IDENTIFICATION
 *	    src/include/px/px_gang.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PXGANG_H_
#define _PXGANG_H_

#include "executor/execdesc.h"
#include "access/relscan.h"
#include "utils/portal.h"

#include "px/px_util.h"

struct Port;
struct QueryDesc;
struct DirectDispatchInfo;
struct EState;
struct PQExpBufferData;
struct PxDispatcherState;

/*
 * A gang represents a single group of workers on each connected segDB
 */
typedef struct Gang
{
	GangType	type;

	int			size;

	/*
	 * Array of PXs/segDBs that make up this gang. Sorted by segment index.
	 */
	struct PxWorkerDescriptor **db_descriptors;

	/* For debugging purposes only. These do not add any actual functionality. */
	bool		allocated;
} Gang;

extern int	px_identifier;

extern int	ic_htab_size;
extern int	px_logical_worker_idx;
extern int	px_logical_total_workers;
extern MemoryContext GangContext;
extern Gang *CurrentGangCreating;

/* POLAR */
extern uint64 px_sql_wal_lsn;
extern uint64 px_auth_systemid;

/*
 * pxgang_createGang:
 *
 * Creates a new gang by logging on a session to each segDB involved.
 *
 * call this function in GangContext memory context.
 * elog ERROR or return a non-NULL gang.
 */
extern Gang *pxgang_createGang(List *segments, SegmentType segmentType);

extern const char *gangTypeToString(GangType type);

extern void setupPxProcessList(ExecSlice * slice);

extern List *getPxProcessesForQC(int isPrimary);

extern Gang *AllocateGang(struct PxDispatcherState *ds, enum GangType type, List *segments);
extern void RecycleGang(Gang *gp, bool forceDestroy);

Gang	   *buildGangDefinition(List *segments, SegmentType segmentType);
bool build_pxid_param(char *buf, int bufsz, int identifier, int icHtabSize);

extern void get_worker_info_by_identifier(ExecSlice *slice, int *idx, int *total_count);

char	   *makeOptions(void);
extern bool segment_failure_due_to_recovery(const char *error_message);

/*
 * pxgang_parse_pxid_params
 *
 * Called very early in backend initialization, to interpret the "pxid"
 * parameter value that a qExec receives from its qDisp.
 *
 * At this point, client authentication has not been done; the backend
 * command line options have not been processed; GUCs have the settings
 * inherited from the postmaster; etc; so don't try to do too much in here.
 */
extern void pxgang_parse_pxid_params(struct Port *port, const char *pxid_value);

/*
 * PX Worker Process information
 *
 * This structure represents the global information about a worker process.
 * It is constructed on the entry process (QC) and transmitted as part of
 * the global slice table to the involved PXs. Note that this is an
 * immutable, fixed-size structure so it can be held in a contiguous
 * array. In the ExecSlice node, however, it is held in a List.
 */
typedef struct PxProcess
{
	NodeTag		type;

	/*
	 * These fields are established at connection (libpq) time and are
	 * available to the QC in PGconn structure associated with connected PX.
	 * It needs to be explicitly transmitted to PX's
	 */
	char	   *listenerAddr;	/* Interconnect listener IPv4 address, a
								 * C-string */

	int			listenerPort;	/* Interconnect listener port */
	int			remotePort;		/* Interconnect remote port */
	int			pid;			/* Backend PID of the process. */

	int			contentid;
	int			contentCount;
	int			identifier;		/* unique identifier in the pxworker
								 * segment pool */
} PxProcess;

typedef Gang *(*CreateGangFunc) (List *segments, SegmentType segmentType);

PXScanDesc	CreatePXScanDesc(void);

#endif							/* _PXGANG_H_ */
