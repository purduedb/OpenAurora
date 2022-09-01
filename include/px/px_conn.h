/*-------------------------------------------------------------------------
 *
 * px_conn.h
 *
 * Functions returning results from a remote database
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/px/px_conn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PXCONN_H
#define PXCONN_H


typedef struct SnapshotData SnapshotData;

/* --------------------------------------------------------------------------------------------------
 * Structure for segment database definition and working values
 */
typedef struct LogicalWorkerInfo
{
	int			idx;
	int			total_count;
} LogicalWorkerInfo;

typedef struct PxWorkerDescriptor
{
	/*
	 * Points to the PxNodeInfo structure describing the parameters
	 * for this segment database.  Information in this structure is obtained
	 * from the Greenplum administrative schema tables.
	 */
	struct PxNodeInfo *pxNodeInfo;

	/*
	 * A non-NULL value points to the PGconn block of a successfully
	 * established connection to the segment database.
	 */
	PGconn	   *conn;

	LogicalWorkerInfo logicalWorkerInfo;

	/*
	 * Connection info saved at most recent PQconnectdb.
	 *
	 * NB: Use malloc/free, not palloc/pfree, for the items below.
	 */
	uint32		motionListener; /* interconnect listener port */
	int32		backendPid;
	char	   *whoami;			/* PX identifier for msgs */
	bool		isWriter;
	int			identifier;		/* unique identifier in the pxworker
								 * segment pool */
	char	   *serialized_snap; /* serialize snapshot */
} PxWorkerDescriptor;

PxWorkerDescriptor *pxconn_createWorkerDescriptor(struct PxNodeInfo *pxinfo, int identifier,
								int logicalWorkerIdx, int logicalTotalWorkers);

/* Free all memory owned by a segment descriptor. */
void pxconn_termWorkerDescriptor(PxWorkerDescriptor *pxWorkerDesc);


/* Connect to a PX as a client via libpq. */
void pxconn_doConnectStart(PxWorkerDescriptor *pxWorkerDesc,
					   const char *pxid,
					   const char *options);
void pxconn_doConnectComplete(PxWorkerDescriptor *pxWorkerDesc);

/*
 * Read result from connection and discard it.
 *
 * Retry at most N times.
 *
 * Return false if there'er still leftovers.
 */
bool pxconn_discardResults(PxWorkerDescriptor *pxWorkerDesc,
					   int retryCount);

/* Return if it's a bad connection */
bool		pxconn_isBadConnection(PxWorkerDescriptor *pxWorkerDesc);

/* Set the slice index for error messages related to this PX. */
void		pxconn_setPXIdentifier(PxWorkerDescriptor *pxWorkerDesc, int sliceIndex);

/*
 * Send cancel/finish signal to still-running PX through libpq.
 *
 * errbuf is used to return error message(recommended size is 256 bytes).
 *
 * Returns true if we successfully sent a signal
 * (not necessarily received by the target process).
 */
bool		pxconn_signalPX(PxWorkerDescriptor *pxWorkerDesc, char *errbuf, bool isCancel);

#endif							/* PXCONN_H */
