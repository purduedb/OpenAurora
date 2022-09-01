/*-------------------------------------------------------------------------
 *
 * polar_storage_idx.c
 *    WAL redo parse logic for storage xlog.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *           src/backend/access/logindex/polar_storage_idx.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/polar_logindex_redo.h"
#include "access/visibilitymap.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "catalog/storage_xlog.h"
#include "storage/freespace.h"
#include "utils/inval.h"

static void
polar_record_truncate_heap_info(SMgrRelation reln, polar_logindex_redo_ctl_t instance, XLogRecPtr lsn, xl_smgr_truncate *xlrec)
{
	if (instance->rel_size_cache)
		polar_record_rel_size_with_lock(instance->rel_size_cache, lsn, &xlrec->rnode, MAIN_FORKNUM, xlrec->blkno);

	/*
	 * Get rid of any buffers for the about-to-be-deleted blocks. bufmgr will
	 * just drop them without bothering to write the contents.
	 */
	ForkNumber forkNumber = MAIN_FORKNUM;
    BlockNumber blockNumber = xlrec->blkno;
	DropRelFileNodeBuffers(reln->smgr_rnode, &forkNumber, 1, &blockNumber);
}

static void
polar_record_truncate_vm_info(SMgrRelation reln, polar_logindex_redo_ctl_t instance, XLogRecPtr lsn, xl_smgr_truncate *xlrec)
{
	BlockNumber new_blocks;
	BlockNumber heap_block = xlrec->blkno;
	BlockNumber trunc_block = HEAPBLK_TO_MAPBLOCK(heap_block);
	uint32 trunc_byte = HEAPBLK_TO_MAPBYTE(heap_block);
	uint8  trunc_offset = HEAPBLK_TO_OFFSET(heap_block);

	if (trunc_byte != 0 || trunc_offset != 0)
	{
		/*
		 * We will drop the buffer for (trunc_block + 1) from buffer pool.
		 * And it will be replayed by new page log.
		 * See visibilitymap_truncate for more information
		 */
		new_blocks = trunc_block + 1;
	}
	else
		new_blocks = trunc_block;

	if (instance->rel_size_cache)
		polar_record_rel_size_with_lock(instance->rel_size_cache, lsn, &xlrec->rnode, VISIBILITYMAP_FORKNUM, new_blocks);

	/*
	 * Get rid of any buffers for the about-to-be-deleted blocks. bufmgr will
	 * just drop them without bothering to write the contents.
	 */
	ForkNumber forkNumber = VISIBILITYMAP_FORKNUM;
	BlockNumber blockNumber = trunc_block;
	DropRelFileNodeBuffers(reln->smgr_rnode, &forkNumber, 1, &blockNumber);
}

static void
polar_record_truncate_fsm_info(SMgrRelation reln, polar_logindex_redo_ctl_t instance, XLogRecPtr lsn, xl_smgr_truncate *xlrec)
{
	uint16 first_removed_slot;
	BlockNumber new_blocks;
	BlockNumber trunc_blocks = polar_calc_fsm_blocks(reln, xlrec->blkno, &first_removed_slot);

	if (first_removed_slot > 0)
	{
		/*
		 * We will drop buffer for (new_blocks + 1) from buffer pool.
		 * And it will be replayed by new page log.
		 * See FreeSpaceMapTruncateRel for more information
		 */
		new_blocks = trunc_blocks + 1;
	}
	else
		new_blocks = trunc_blocks;

	if (instance->rel_size_cache)
		polar_record_rel_size_with_lock(instance->rel_size_cache, lsn, &xlrec->rnode, FSM_FORKNUM, new_blocks);

	/*
	 * Get rid of any buffers for the about-to-be-deleted blocks. bufmgr will
	 * just drop them without bothering to write the contents.
	 */
	ForkNumber forkNumber = FSM_FORKNUM;
	BlockNumber blockNumber = trunc_blocks;
	DropRelFileNodeBuffers(reln->smgr_rnode, &forkNumber, 1, &blockNumber);

	/*
	 * We might as well update the local smgr_fsm_nblocks setting.
	 * smgrtruncate sent an smgr cache inval message, which will cause other
	 * backends to invalidate their copy of smgr_fsm_nblocks, and this one too
	 * at the next command boundary.  But this ensures it isn't outright wrong
	 * until then.
	 */
	reln->smgr_fsm_nblocks = new_blocks;
}

static void
polar_record_truncate_info(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(record);
	SMgrRelation reln;

	reln = smgropen(xlrec->rnode, InvalidBackendId);

	if ((xlrec->flags & SMGR_TRUNCATE_HEAP) != 0)
		polar_record_truncate_heap_info(reln, instance, lsn, xlrec);

	if ((xlrec->flags & SMGR_TRUNCATE_FSM) != 0)
		polar_record_truncate_fsm_info(reln, instance, lsn, xlrec);

	if ((xlrec->flags & SMGR_TRUNCATE_VM) != 0)
		polar_record_truncate_vm_info(reln, instance, lsn, xlrec);

	/*
	 * Send a shared-inval message to force other backends to close any smgr
	 * references they may have for this rel.  This is useful because they
	 * might have open file pointers to segments that got removed, and/or
	 * smgr_targblock variables pointing past the new rel end.  (The inval
	 * message will come back to our backend, too, causing a
	 * probably-unnecessary local smgr flush.  But we don't expect that this
	 * is a performance-critical path.)  As in the unlink code, we want to be
	 * sure the message is sent before we start changing things on-disk.
	 */
	CacheInvalidateSmgr(reln->smgr_rnode);
}

bool
polar_storage_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/* Backup blocks are not used in smgr records */
	Assert(!XLogRecHasAnyBlockRefs(record));

	switch (info)
	{
		case XLOG_SMGR_CREATE:
			/* Read-Only node does not create files */
			if (polar_in_replica_mode())
				return true;
			break;

		case XLOG_SMGR_TRUNCATE:
			polar_record_truncate_info(instance, record);
			/* Read-Only node does not truncate files */
			if (polar_in_replica_mode())
				return true;
			break;

		default:
			break;
	}

	return false;
}
