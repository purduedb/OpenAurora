/*-------------------------------------------------------------------------
 *
 * freespace.h
 *	  POSTGRES free space map for quickly finding free space in relations
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/freespace.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FREESPACE_H_
#define FREESPACE_H_

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"

#include "access/xlogdefs.h"
#include "storage/smgr.h"
/* prototypes for public functions in freespace.c */
extern Size GetRecordedFreeSpace(Relation rel, BlockNumber heapBlk);
extern BlockNumber GetPageWithFreeSpace(Relation rel, Size spaceNeeded);
extern BlockNumber RecordAndGetPageWithFreeSpace(Relation rel,
												 BlockNumber oldPage,
												 Size oldSpaceAvail,
												 Size spaceNeeded);
extern void RecordPageWithFreeSpace(Relation rel, BlockNumber heapBlk,
									Size spaceAvail);
extern void XLogRecordPageWithFreeSpace(RelFileNode rnode, BlockNumber heapBlk,
										Size spaceAvail);

extern BlockNumber FreeSpaceMapPrepareTruncateRel(Relation rel,
												  BlockNumber nblocks);
extern void FreeSpaceMapVacuum(Relation rel);
extern void FreeSpaceMapVacuumRange(Relation rel, BlockNumber start,
									BlockNumber end);
extern BlockNumber polar_calc_fsm_blocks(SMgrRelation reln, BlockNumber heap_blocks, uint16 *first_removed_slot);

#endif							/* FREESPACE_H_ */
