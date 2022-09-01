/*-------------------------------------------------------------------------
 *
 * polar_hash_xlog_idx.c
 *   Implementation of parse hash records.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 * src/backend/access/logindex/polar_hash_xlog_idx.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/hash.h"
#include "access/hash_xlog.h"
#include "access/polar_logindex_redo.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/standby.h"

static void
polar_hash_xlog_add_ovfl_page_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	polar_logindex_save_block(instance, record, 0);
	polar_logindex_save_block(instance, record, 1);

	if (XLogRecHasBlockRef(record, 2))
		polar_logindex_save_block(instance, record, 2);

	if (XLogRecHasBlockRef(record, 3))
		polar_logindex_save_block(instance, record, 3);

	polar_logindex_save_block(instance, record, 4);
}

static void
polar_hash_xlog_add_ovfl_page_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	BufferTag ovfl_tag;
	polar_page_lock_t    ovfl_lock;
	Buffer    ovfl_buf;

	POLAR_MINI_TRANS_REDO_PARSE(instance, record, 0, ovfl_tag, ovfl_lock, ovfl_buf);

	polar_logindex_redo_parse(instance, record, 1);

	if (BufferIsValid(ovfl_buf))
		UnlockReleaseBuffer(ovfl_buf);

	polar_logindex_mini_trans_unlock(instance->mini_trans, ovfl_lock);

	if (XLogRecHasBlockRef(record, 2))
		polar_logindex_redo_parse(instance, record, 2);

	if (XLogRecHasBlockRef(record, 3))
		polar_logindex_redo_parse(instance, record, 3);

	polar_logindex_redo_parse(instance, record, 4);
}

static void
polar_hash_xlog_split_allocate_page_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	polar_logindex_save_block(instance, record, 0);
	polar_logindex_save_block(instance, record, 1);
	polar_logindex_save_block(instance, record, 2);
}

static void
polar_hash_xlog_split_allocate_page_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	BufferTag old_tag, new_tag;
	polar_page_lock_t old_page_lock, new_page_lock;
	Buffer old_buf = InvalidBuffer;
	Buffer new_buf = InvalidBuffer;


	POLAR_MINI_TRANS_CLEANUP_PARSE(instance, record, 0, old_tag, old_page_lock, old_buf);
	POLAR_MINI_TRANS_REDO_PARSE(instance, record, 1, new_tag, new_page_lock, new_buf);

	if (BufferIsValid(old_buf))
		UnlockReleaseBuffer(old_buf);

	polar_logindex_mini_trans_unlock(instance->mini_trans, old_page_lock);

	if (BufferIsValid(new_buf))
		UnlockReleaseBuffer(new_buf);

	polar_logindex_mini_trans_unlock(instance->mini_trans, new_page_lock);

	polar_logindex_redo_parse(instance, record, 2);
}

static void
polar_hash_xlog_move_page_contents_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	if (XLogRecHasBlockRef(record, 0))
	{
		polar_logindex_save_block(instance, record, 0);
		polar_logindex_save_block(instance, record, 1);
	}
	else
		polar_logindex_save_block(instance, record, 1);

	polar_logindex_save_block(instance, record, 2);
}

static void
polar_hash_xlog_move_page_contents_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	BufferTag write_tag, bucket_tag;
	polar_page_lock_t write_lock, bucket_lock = POLAR_INVALID_PAGE_LOCK;
	Buffer write_buf = InvalidBuffer,
		   bucket_buf = InvalidBuffer;

	if (XLogRecHasBlockRef(record, 0))
	{
		POLAR_MINI_TRANS_CLEANUP_PARSE(instance, record, 0, bucket_tag, bucket_lock, bucket_buf);
		POLAR_MINI_TRANS_REDO_PARSE(instance, record, 1, write_tag, write_lock, write_buf);
	}
	else
		POLAR_MINI_TRANS_CLEANUP_PARSE(instance, record, 1, write_tag, write_lock, write_buf);

	polar_logindex_redo_parse(instance, record, 2);

	if (BufferIsValid(write_buf))
		UnlockReleaseBuffer(write_buf);

	polar_logindex_mini_trans_unlock(instance->mini_trans, write_lock);

	if (XLogRecHasBlockRef(record, 0))
	{
		if (BufferIsValid(bucket_buf))
			UnlockReleaseBuffer(bucket_buf);

		if (bucket_lock != POLAR_INVALID_PAGE_LOCK)
			polar_logindex_mini_trans_unlock(instance->mini_trans, bucket_lock);
	}
}

static void
polar_hash_xlog_squeeze_page_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	if (XLogRecHasBlockRef(record, 0))
	{
		polar_logindex_save_block(instance, record, 0);
		polar_logindex_save_block(instance, record, 1);
	}
	else
		polar_logindex_save_block(instance, record, 1);

	polar_logindex_save_block(instance, record, 2);

	if (XLogRecHasBlockRef(record, 3))
		polar_logindex_save_block(instance, record, 3);

	if (XLogRecHasBlockRef(record, 4))
		polar_logindex_save_block(instance, record, 4);

	polar_logindex_save_block(instance, record, 5);

	if (XLogRecHasBlockRef(record, 6))
		polar_logindex_save_block(instance, record, 6);
}

static void
polar_hash_xlog_squeeze_page_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	BufferTag write_tag, bucket_tag;
	polar_page_lock_t write_lock, bucket_lock = POLAR_INVALID_PAGE_LOCK;
	Buffer write_buf = InvalidBuffer,
		   bucket_buf = InvalidBuffer;

	if (XLogRecHasBlockRef(record, 0))
	{
		POLAR_MINI_TRANS_CLEANUP_PARSE(instance, record, 0, bucket_tag, bucket_lock, bucket_buf);
		POLAR_MINI_TRANS_REDO_PARSE(instance, record, 1, write_tag, write_lock, write_buf);
	}
	else
		POLAR_MINI_TRANS_CLEANUP_PARSE(instance, record, 1, write_tag, write_lock, write_buf);

	polar_logindex_redo_parse(instance, record, 2);

	if (XLogRecHasBlockRef(record, 3))
		polar_logindex_redo_parse(instance, record, 3);

	if (XLogRecHasBlockRef(record, 4))
		polar_logindex_redo_parse(instance, record, 4);

	if (BufferIsValid(write_buf))
		UnlockReleaseBuffer(write_buf);

	polar_logindex_mini_trans_unlock(instance->mini_trans, write_lock);

	if (XLogRecHasBlockRef(record, 0))
	{
		if (BufferIsValid(bucket_buf))
			UnlockReleaseBuffer(bucket_buf);

		if (bucket_lock != POLAR_INVALID_PAGE_LOCK)
			polar_logindex_mini_trans_unlock(instance->mini_trans, bucket_lock);
	}

	polar_logindex_redo_parse(instance, record, 5);

	if (XLogRecHasBlockRef(record, 6))
		polar_logindex_redo_parse(instance, record, 6);
}

static void
polar_hash_xlog_vacuum_one_page_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	/*
	 * If we have any conflict processing to do, it must happen before we
	 * update the page.
	 *
	 * Hash index records that are marked as LP_DEAD and being removed during
	 * hash index tuple insertion can conflict with standby queries. You might
	 * think that vacuum records would conflict as well, but we've handled
	 * that already.  XLOG_HEAP2_CLEANUP_INFO records provide the highest xid
	 * cleaned by the vacuum of the heap and so we can resolve any conflicts
	 * just once when that arrives.  After that we know that no conflicts
	 * exist from individual hash index vacuum records on that index.
	 */
	if (reachedConsistency && InHotStandby && polar_enable_resolve_conflict)
	{
	    Assert(0);
//		TransactionId latestRemovedXid =
//			hash_xlog_vacuum_get_latestRemovedXid(record);
//		RelFileNode rnode;
//
//		XLogRecGetBlockTag(record, 0, &rnode, NULL, NULL);
//		ResolveRecoveryConflictWithSnapshot(latestRemovedXid, rnode);
	}

	polar_logindex_cleanup_parse(instance, record, 0);
	polar_logindex_redo_parse(instance, record, 1);
}

static void
polar_hash_xlog_delete_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	if (XLogRecHasBlockRef(record, 0))
	{
		polar_logindex_save_block(instance, record, 0);
		polar_logindex_save_block(instance, record, 1);
	}
	else
		polar_logindex_save_block(instance, record, 1);
}

static void
polar_hash_xlog_delete_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	BufferTag bucket_tag, del_tag;
	polar_page_lock_t bucket_lock = POLAR_INVALID_PAGE_LOCK, del_lock;
	Buffer bucket_buf = InvalidBuffer, del_buf = InvalidBuffer;

	if (XLogRecHasBlockRef(record, 0))
	{
		POLAR_MINI_TRANS_CLEANUP_PARSE(instance, record, 0, bucket_tag, bucket_lock, bucket_buf);
		POLAR_MINI_TRANS_REDO_PARSE(instance, record, 1, del_tag, del_lock, del_buf);
	}
	else
		POLAR_MINI_TRANS_CLEANUP_PARSE(instance, record, 1, del_tag, del_lock, del_buf);

	if (BufferIsValid(del_buf))
		UnlockReleaseBuffer(del_buf);

	polar_logindex_mini_trans_unlock(instance->mini_trans, del_lock);

	if (XLogRecHasBlockRef(record, 0))
	{
		if (BufferIsValid(bucket_buf))
			UnlockReleaseBuffer(bucket_buf);

		polar_logindex_mini_trans_unlock(instance->mini_trans, bucket_lock);
	}
}

/*
 * replay a hash index meta page
 */
static XLogRedoAction
polar_hash_xlog_init_meta_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_hash_init_meta_page *xlrec = (xl_hash_init_meta_page *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Page        page;
	BufferTag      meta_tag;

	POLAR_GET_LOG_TAG(record, meta_tag, 0);

	if (BUFFERTAGS_EQUAL(*tag, meta_tag))
	{
		/* create the index' metapage */
		POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);

		Assert(BufferIsValid(*buffer));
		_hash_init_metabuffer(*buffer, xlrec->num_tuples, xlrec->procid,
							  xlrec->ffactor, true);
		page = (Page) BufferGetPage(*buffer);
		PageSetLSN(page, lsn);

		/*
		 * Force the on-disk state of init forks to always be in sync with the
		 * state in shared buffers.  See XLogReadBufferForRedoExtended.  We need
		 * special handling for init forks as create index operations don't log a
		 * full page image of the metapage.
		 */
		if (meta_tag.forkNum == INIT_FORKNUM)
			FlushOneBuffer(*buffer);

		action = BLK_NEEDS_REDO;
	}

	return action;
}

/*
 * replay a hash index bitmap page
 */
static XLogRedoAction
polar_hash_xlog_init_bitmap_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_hash_init_bitmap_page *xlrec = (xl_hash_init_bitmap_page *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Page        page;
	HashMetaPage metap;
	uint32      num_buckets;
	BufferTag   bitmap_tag, meta_tag;

	POLAR_GET_LOG_TAG(record, bitmap_tag, 0);

	if (BUFFERTAGS_EQUAL(*tag, bitmap_tag))
	{
		/*
		 * Initialize bitmap page
		 */
		POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
		_hash_initbitmapbuffer(*buffer, xlrec->bmsize, true);
		PageSetLSN(BufferGetPage(*buffer), lsn);

		/*
		 * Force the on-disk state of init forks to always be in sync with the
		 * state in shared buffers.  See XLogReadBufferForRedoExtended.  We need
		 * special handling for init forks as create index operations don't log a
		 * full page image of the metapage.
		 */
		if (bitmap_tag.forkNum == INIT_FORKNUM)
			FlushOneBuffer(*buffer);

		return BLK_NEEDS_REDO;
	}

	POLAR_GET_LOG_TAG(record, meta_tag, 1);

	if (BUFFERTAGS_EQUAL(*tag, meta_tag))
	{
		action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

		/* add the new bitmap page to the metapage's list of bitmaps */
		if (action == BLK_NEEDS_REDO)
		{
			/*
			 * Note: in normal operation, we'd update the metapage while still
			 * holding lock on the bitmap page.  But during replay it's not
			 * necessary to hold that lock, since nobody can see it yet; the
			 * creating transaction hasn't yet committed.
			 */
			page = BufferGetPage(*buffer);
			metap = HashPageGetMeta(page);

			num_buckets = metap->hashm_maxbucket + 1;
			metap->hashm_mapp[metap->hashm_nmaps] = num_buckets + 1;
			metap->hashm_nmaps++;

			PageSetLSN(page, lsn);

			if (meta_tag.forkNum == INIT_FORKNUM)
				FlushOneBuffer(*buffer);
		}
	}

	return action;
}

/*
 * replay a hash index insert without split
 */
static XLogRedoAction
polar_hash_xlog_insert(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	HashMetaPage metap;
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_hash_insert *xlrec = (xl_hash_insert *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Page        page;
	BufferTag   insert_tag, meta_tag;

	POLAR_GET_LOG_TAG(record, insert_tag, 0);

	if (BUFFERTAGS_EQUAL(*tag, insert_tag))
	{
		action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			Size        datalen;
			char       *datapos = XLogRecGetBlockData(record, 0, &datalen);

			page = BufferGetPage(*buffer);

			if (PageAddItem(page, (Item) datapos, datalen, xlrec->offnum,
							false, false) == InvalidOffsetNumber)
			{
				POLAR_LOG_REDO_INFO(page, record);
				elog(PANIC, "polar_hash_xlog_insert: failed to add item");
			}

			PageSetLSN(page, lsn);
		}

		return action;
	}

	POLAR_GET_LOG_TAG(record, meta_tag, 1);

	if (BUFFERTAGS_EQUAL(*tag, meta_tag))
	{
		action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			/*
			 * Note: in normal operation, we'd update the metapage while still
			 * holding lock on the page we inserted into.  But during replay it's
			 * not necessary to hold that lock, since no other index updates can
			 * be happening concurrently.
			 */
			page = BufferGetPage(*buffer);
			metap = HashPageGetMeta(page);
			metap->hashm_ntuples += 1;

			PageSetLSN(page, lsn);
		}
	}

	return action;
}

/*
 * replay addition of overflow page for hash index
 */
static XLogRedoAction
polar_hash_xlog_add_ovfl_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_hash_add_ovfl_page *xlrec = (xl_hash_add_ovfl_page *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Page        ovflpage;
	HashPageOpaque ovflopaque;
	uint32     *num_bucket;
	char       *data;
	Size        datalen PG_USED_FOR_ASSERTS_ONLY;
	bool        new_bmpage = false;
	BlockNumber newmapblk = InvalidBlockNumber;
	BufferTag   meta_tag, left_tag, newmap_tag, ovfl_tag, map_tag;

	POLAR_GET_LOG_TAG(record, ovfl_tag, 0);
	POLAR_GET_LOG_TAG(record, left_tag, 1);

	if (BUFFERTAGS_EQUAL(*tag, ovfl_tag))
	{
		POLAR_INIT_BUFFER_FOR_REDO(record, 0, buffer);
		Assert(BufferIsValid(*buffer));

		data = XLogRecGetBlockData(record, 0, &datalen);
		num_bucket = (uint32 *) data;
		Assert(datalen == sizeof(uint32));
		_hash_initbuf(*buffer, InvalidBlockNumber, *num_bucket, LH_OVERFLOW_PAGE,
					  true);
		/* update backlink */
		ovflpage = BufferGetPage(*buffer);
		ovflopaque = (HashPageOpaque) PageGetSpecialPointer(ovflpage);
		ovflopaque->hasho_prevblkno = left_tag.blockNum;

		PageSetLSN(ovflpage, lsn);

		return BLK_NEEDS_REDO;
	}

	if (BUFFERTAGS_EQUAL(*tag, left_tag))
	{
		action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			Page        leftpage;
			HashPageOpaque leftopaque;

			leftpage = BufferGetPage(*buffer);
			leftopaque = (HashPageOpaque) PageGetSpecialPointer(leftpage);
			leftopaque->hasho_nextblkno = ovfl_tag.blockNum;

			PageSetLSN(leftpage, lsn);
		}

		return action;
	}

	/*
	 * Note: in normal operation, we'd update the bitmap and meta page while
	 * still holding lock on the overflow pages.  But during replay it's not
	 * necessary to hold those locks, since no other index updates can be
	 * happening concurrently.
	 */
	if (XLogRecHasBlockRef(record, 2))
	{
		POLAR_GET_LOG_TAG(record, map_tag, 2);

		if (BUFFERTAGS_EQUAL(*tag, map_tag))
		{
			action = POLAR_READ_BUFFER_FOR_REDO(record, 2, buffer);

			if (action == BLK_NEEDS_REDO)
			{
				Page        mappage = (Page) BufferGetPage(*buffer);
				uint32     *freep = NULL;
				uint32     *bitmap_page_bit;

				freep = HashPageGetBitmap(mappage);

				data = XLogRecGetBlockData(record, 2, &datalen);
				bitmap_page_bit = (uint32 *) data;

				SETBIT(freep, *bitmap_page_bit);

				PageSetLSN(mappage, lsn);
			}

			return action;
		}
	}

	if (XLogRecHasBlockRef(record, 3))
	{
		POLAR_GET_LOG_TAG(record, newmap_tag, 3);
		new_bmpage = true;
		newmapblk = newmap_tag.blockNum;

		if (BUFFERTAGS_EQUAL(*tag, newmap_tag))
		{
			POLAR_INIT_BUFFER_FOR_REDO(record, 3, buffer);
			_hash_initbitmapbuffer(*buffer, xlrec->bmsize, true);

			PageSetLSN(BufferGetPage(*buffer), lsn);

			return BLK_NEEDS_REDO;
		}
	}

	POLAR_GET_LOG_TAG(record, meta_tag, 4);

	if (BUFFERTAGS_EQUAL(*tag, meta_tag))
	{
		action = POLAR_READ_BUFFER_FOR_REDO(record, 4, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			HashMetaPage metap;
			Page        page;
			uint32     *firstfree_ovflpage;

			data = XLogRecGetBlockData(record, 4, &datalen);
			firstfree_ovflpage = (uint32 *) data;

			page = BufferGetPage(*buffer);
			metap = HashPageGetMeta(page);
			metap->hashm_firstfree = *firstfree_ovflpage;

			if (!xlrec->bmpage_found)
			{
				metap->hashm_spares[metap->hashm_ovflpoint]++;

				if (new_bmpage)
				{
					Assert(BlockNumberIsValid(newmapblk));

					metap->hashm_mapp[metap->hashm_nmaps] = newmapblk;
					metap->hashm_nmaps++;
					metap->hashm_spares[metap->hashm_ovflpoint]++;
				}
			}

			PageSetLSN(page, lsn);
		}
	}

	return action;
}

/*
 * replay allocation of page for split operation
 */
static XLogRedoAction
polar_hash_xlog_split_allocate_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_hash_split_allocate_page *xlrec = (xl_hash_split_allocate_page *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Size        datalen PG_USED_FOR_ASSERTS_ONLY;
	char       *data;
	BufferTag  old_tag, new_tag, meta_tag;

	/*
	 * To be consistent with normal operation, here we take cleanup locks on
	 * both the old and new buckets even though there can't be any concurrent
	 * inserts.
	 */

	POLAR_GET_LOG_TAG(record, old_tag, 0);

	if (BUFFERTAGS_EQUAL(*tag, old_tag))
	{
		/* replay the record for old bucket */
		action = XLogReadBufferForRedoExtended(record, 0, POLAR_READ_MODE(*buffer), true, buffer);

		/*
		 * Note that we still update the page even if it was restored from a full
		 * page image, because the special space is not included in the image.
		 */
		if (action == BLK_NEEDS_REDO || action == BLK_RESTORED)
		{
			Page        oldpage;
			HashPageOpaque oldopaque;

			oldpage = BufferGetPage(*buffer);
			oldopaque = (HashPageOpaque) PageGetSpecialPointer(oldpage);

			oldopaque->hasho_flag = xlrec->old_bucket_flag;
			oldopaque->hasho_prevblkno = xlrec->new_bucket;

			PageSetLSN(oldpage, lsn);
		}

		return action;
	}

	POLAR_GET_LOG_TAG(record, new_tag, 1);

	if (BUFFERTAGS_EQUAL(*tag, new_tag))
	{
		/* replay the record for new bucket */
		POLAR_INIT_BUFFER_FOR_REDO(record, 1, buffer);
		_hash_initbuf(*buffer, xlrec->new_bucket, xlrec->new_bucket,
					  xlrec->new_bucket_flag, true);

		PageSetLSN(BufferGetPage(*buffer), lsn);

		return BLK_NEEDS_REDO;
	}

	POLAR_GET_LOG_TAG(record, meta_tag, 2);

	if (BUFFERTAGS_EQUAL(*tag, meta_tag))
	{
		/*
		 * Note: in normal operation, we'd update the meta page while still
		 * holding lock on the old and new bucket pages.  But during replay it's
		 * not necessary to hold those locks, since no other bucket splits can be
		 * happening concurrently.
		 */

		/* replay the record for metapage changes */
		action = POLAR_READ_BUFFER_FOR_REDO(record, 2, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			Page        page;
			HashMetaPage metap;

			page = BufferGetPage(*buffer);
			metap = HashPageGetMeta(page);
			metap->hashm_maxbucket = xlrec->new_bucket;

			data = XLogRecGetBlockData(record, 2, &datalen);

			if (xlrec->flags & XLH_SPLIT_META_UPDATE_MASKS)
			{
				uint32      lowmask;
				uint32     *highmask;

				/* extract low and high masks. */
				memcpy(&lowmask, data, sizeof(uint32));
				highmask = (uint32 *)((char *) data + sizeof(uint32));

				/* update metapage */
				metap->hashm_lowmask = lowmask;
				metap->hashm_highmask = *highmask;

				data += sizeof(uint32) * 2;
			}

			if (xlrec->flags & XLH_SPLIT_META_UPDATE_SPLITPOINT)
			{
				uint32      ovflpoint;
				uint32     *ovflpages;

				/* extract information of overflow pages. */
				memcpy(&ovflpoint, data, sizeof(uint32));
				ovflpages = (uint32 *)((char *) data + sizeof(uint32));

				/* update metapage */
				metap->hashm_spares[ovflpoint] = *ovflpages;
				metap->hashm_ovflpoint = ovflpoint;
			}

			PageSetLSN(BufferGetPage(*buffer), lsn);
		}
	}

	return action;
}

static XLogRedoAction
polar_hash_xlog_split_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	return POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);
}

/*
 * replay completion of split operation
 */
static XLogRedoAction
polar_hash_xlog_split_complete(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_hash_split_complete *xlrec = (xl_hash_split_complete *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	BufferTag old_tag, new_tag;

	POLAR_GET_LOG_TAG(record, old_tag, 0);

	if (BUFFERTAGS_EQUAL(*tag, old_tag))
	{
		/* replay the record for old bucket */

		action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		/*
		 * Note that we still update the page even if it was restored from a full
		 * page image, because the bucket flag is not included in the image.
		 */
		if (action == BLK_NEEDS_REDO || action == BLK_RESTORED)
		{
			Page        oldpage;
			HashPageOpaque oldopaque;

			oldpage = BufferGetPage(*buffer);
			oldopaque = (HashPageOpaque) PageGetSpecialPointer(oldpage);

			oldopaque->hasho_flag = xlrec->old_bucket_flag;

			PageSetLSN(oldpage, lsn);
		}

		return action;
	}

	POLAR_GET_LOG_TAG(record, new_tag, 1);

	if (BUFFERTAGS_EQUAL(*tag, new_tag))
	{
		/* replay the record for new bucket */
		action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

		/*
		 * Note that we still update the page even if it was restored from a full
		 * page image, because the bucket flag is not included in the image.
		 */
		if (action == BLK_NEEDS_REDO || action == BLK_RESTORED)
		{
			Page        newpage;
			HashPageOpaque nopaque;

			newpage = BufferGetPage(*buffer);
			nopaque = (HashPageOpaque) PageGetSpecialPointer(newpage);

			nopaque->hasho_flag = xlrec->new_bucket_flag;

			PageSetLSN(newpage, lsn);
		}
	}

	return action;
}

/*
 * replay move of page contents for squeeze operation of hash index
 */
static XLogRedoAction
polar_hash_xlog_move_page_contents(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_hash_move_page_contents *xldata = (xl_hash_move_page_contents *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	BufferTag bucket_tag, write_tag, del_tag;

	if (!xldata->is_prim_bucket_same_wrt)
	{
		POLAR_GET_LOG_TAG(record, bucket_tag, 0);

		if (BUFFERTAGS_EQUAL(*tag, bucket_tag))
		{
			/*
			 * we don't care for return value as the purpose of reading bucketbuf
			 * is to ensure a cleanup lock on primary bucket page.
			 */
			return XLogReadBufferForRedoExtended(record, 0,
												 POLAR_READ_MODE(*buffer), true, buffer);
		}
	}

	POLAR_GET_LOG_TAG(record, write_tag, 1);

	if (BUFFERTAGS_EQUAL(*tag, write_tag))
	{
		/*
		 * Ensure we have a cleanup lock on primary bucket page before we start
		 * with the actual replay operation.  This is to ensure that neither a
		 * scan can start nor a scan can be already-in-progress during the replay
		 * of this operation.  If we allow scans during this operation, then they
		 * can miss some records or show the same record multiple times.
		 */
		if (xldata->is_prim_bucket_same_wrt)
			action = XLogReadBufferForRedoExtended(record, 1,
												   POLAR_READ_MODE(*buffer), true, buffer);
		else
			action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

		/* replay the record for adding entries in overflow buffer */
		if (action == BLK_NEEDS_REDO)
		{
			Page        writepage;
			char       *begin;
			char       *data;
			Size        datalen;
			uint16      ninserted = 0;

			data = begin = XLogRecGetBlockData(record, 1, &datalen);

			writepage = (Page) BufferGetPage(*buffer);

			if (xldata->ntups > 0)
			{
				OffsetNumber *towrite = (OffsetNumber *) data;

				data += sizeof(OffsetNumber) * xldata->ntups;

				while (data - begin < datalen)
				{
					IndexTuple  itup = (IndexTuple) data;
					Size        itemsz;
					OffsetNumber l;

					itemsz = IndexTupleSize(itup);
					itemsz = MAXALIGN(itemsz);

					data += itemsz;

					l = PageAddItem(writepage, (Item) itup, itemsz, towrite[ninserted], false, false);

					if (l == InvalidOffsetNumber)
					{
						POLAR_LOG_REDO_INFO(writepage, record);
						elog(ERROR, "polar_hash_xlog_move_page_contents: failed to add item to hash index page, size %d bytes",
							 (int) itemsz);
					}

					ninserted++;
				}
			}

			/*
			 * number of tuples inserted must be same as requested in REDO record.
			 */
			Assert(ninserted == xldata->ntups);

			PageSetLSN(writepage, lsn);
		}

		return action;
	}

	POLAR_GET_LOG_TAG(record, del_tag, 2);

	if (BUFFERTAGS_EQUAL(*tag, del_tag))
	{
		/* replay the record for deleting entries from overflow buffer */
		action = POLAR_READ_BUFFER_FOR_REDO(record, 2, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			Page        page;
			char       *ptr;
			Size        len;

			ptr = XLogRecGetBlockData(record, 2, &len);

			page = (Page) BufferGetPage(*buffer);

			if (len > 0)
			{
				OffsetNumber *unused;
				OffsetNumber *unend;

				unused = (OffsetNumber *) ptr;
				unend = (OffsetNumber *)((char *) ptr + len);

				if ((unend - unused) > 0)
					PageIndexMultiDelete(page, unused, unend - unused);
			}

			PageSetLSN(page, lsn);
		}
	}

	return action;
}

/*
 * replay squeeze page operation of hash index
 */
static XLogRedoAction
polar_hash_xlog_squeeze_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_hash_squeeze_page *xldata = (xl_hash_squeeze_page *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	BufferTag bucket_tag, write_tag, ovfl_tag, prev_tag, next_tag, map_tag, meta_tag;

	if (!xldata->is_prim_bucket_same_wrt)
	{
		POLAR_GET_LOG_TAG(record, bucket_tag, 0);

		if (BUFFERTAGS_EQUAL(*tag, bucket_tag))
		{
			/*
			 * we don't care for return value as the purpose of reading bucketbuf
			 * is to ensure a cleanup lock on primary bucket page.
			 */
			return XLogReadBufferForRedoExtended(record, 0, POLAR_READ_MODE(*buffer),
												 true, buffer);
		}
	}

	POLAR_GET_LOG_TAG(record, write_tag, 1);

	if (BUFFERTAGS_EQUAL(*tag, write_tag))
	{
		/*
		 * Ensure we have a cleanup lock on primary bucket page before we start
		 * with the actual replay operation.  This is to ensure that neither a
		 * scan can start nor a scan can be already-in-progress during the replay
		 * of this operation.  If we allow scans during this operation, then they
		 * can miss some records or show the same record multiple times.
		 */
		if (xldata->is_prim_bucket_same_wrt)
			action = XLogReadBufferForRedoExtended(record, 1,
												   POLAR_READ_MODE(*buffer), true, buffer);
		else
			action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

		/* replay the record for adding entries in overflow buffer */
		if (action == BLK_NEEDS_REDO)
		{
			Page        writepage;
			char       *begin;
			char       *data;
			Size        datalen;
			uint16      ninserted = 0;

			data = begin = XLogRecGetBlockData(record, 1, &datalen);

			writepage = (Page) BufferGetPage(*buffer);

			if (xldata->ntups > 0)
			{
				OffsetNumber *towrite = (OffsetNumber *) data;

				data += sizeof(OffsetNumber) * xldata->ntups;

				while (data - begin < datalen)
				{
					IndexTuple  itup = (IndexTuple) data;
					Size        itemsz;
					OffsetNumber l;

					itemsz = IndexTupleSize(itup);
					itemsz = MAXALIGN(itemsz);

					data += itemsz;

					l = PageAddItem(writepage, (Item) itup, itemsz, towrite[ninserted], false, false);

					if (l == InvalidOffsetNumber)
					{
						POLAR_LOG_REDO_INFO(writepage, record);
						elog(ERROR, "polar_hash_xlog_squeeze_page: failed to add item to hash index page, size %d bytes",
							 (int) itemsz);
					}

					ninserted++;
				}
			}

			/*
			 * number of tuples inserted must be same as requested in REDO record.
			 */
			Assert(ninserted == xldata->ntups);

			/*
			 * if the page on which are adding tuples is a page previous to freed
			 * overflow page, then update its nextblno.
			 */
			if (xldata->is_prev_bucket_same_wrt)
			{
				HashPageOpaque writeopaque = (HashPageOpaque) PageGetSpecialPointer(writepage);

				writeopaque->hasho_nextblkno = xldata->nextblkno;
			}

			PageSetLSN(writepage, lsn);
		}

		return action;
	}

	POLAR_GET_LOG_TAG(record, ovfl_tag, 2);

	if (BUFFERTAGS_EQUAL(*tag, ovfl_tag))
	{
		/* replay the record for initializing overflow buffer */
		action = POLAR_READ_BUFFER_FOR_REDO(record, 2, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			Page        ovflpage;
			HashPageOpaque ovflopaque;

			ovflpage = BufferGetPage(*buffer);

			_hash_pageinit(ovflpage, BufferGetPageSize(*buffer));

			ovflopaque = (HashPageOpaque) PageGetSpecialPointer(ovflpage);

			ovflopaque->hasho_prevblkno = InvalidBlockNumber;
			ovflopaque->hasho_nextblkno = InvalidBlockNumber;
			ovflopaque->hasho_bucket = -1;
			ovflopaque->hasho_flag = LH_UNUSED_PAGE;
			ovflopaque->hasho_page_id = HASHO_PAGE_ID;

			PageSetLSN(ovflpage, lsn);
		}

		return action;
	}

	if (!xldata->is_prev_bucket_same_wrt)
	{
		POLAR_GET_LOG_TAG(record, prev_tag, 3);

		if (BUFFERTAGS_EQUAL(*tag, prev_tag))
		{
			action = POLAR_READ_BUFFER_FOR_REDO(record, 3, buffer);

			/* replay the record for page previous to the freed overflow page */
			if (action == BLK_NEEDS_REDO)
			{
				Page        prevpage = BufferGetPage(*buffer);
				HashPageOpaque prevopaque = (HashPageOpaque) PageGetSpecialPointer(prevpage);

				prevopaque->hasho_nextblkno = xldata->nextblkno;

				PageSetLSN(prevpage, lsn);
			}

			return action;
		}
	}

	if (XLogRecHasBlockRef(record, 4))
	{
		POLAR_GET_LOG_TAG(record, next_tag, 4);

		if (BUFFERTAGS_EQUAL(*tag, next_tag))
			/* replay the record for page next to the freed overflow page */
		{
			action = POLAR_READ_BUFFER_FOR_REDO(record, 4, buffer);

			if (action == BLK_NEEDS_REDO)
			{
				Page        nextpage = BufferGetPage(*buffer);
				HashPageOpaque nextopaque = (HashPageOpaque) PageGetSpecialPointer(nextpage);

				nextopaque->hasho_prevblkno = xldata->prevblkno;

				PageSetLSN(nextpage, lsn);
			}

			return action;
		}
	}

	POLAR_GET_LOG_TAG(record, map_tag, 5);

	if (BUFFERTAGS_EQUAL(*tag, map_tag))
	{
		/*
		 * Note: in normal operation, we'd update the bitmap and meta page while
		 * still holding lock on the primary bucket page and overflow pages.  But
		 * during replay it's not necessary to hold those locks, since no other
		 * index updates can be happening concurrently.
		 */
		/* replay the record for bitmap page */
		action = POLAR_READ_BUFFER_FOR_REDO(record, 5, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			Page        mappage = (Page) BufferGetPage(*buffer);
			uint32     *freep = NULL;
			char       *data;
			uint32     *bitmap_page_bit;
			Size        datalen;

			freep = HashPageGetBitmap(mappage);

			data = XLogRecGetBlockData(record, 5, &datalen);
			bitmap_page_bit = (uint32 *) data;

			CLRBIT(freep, *bitmap_page_bit);

			PageSetLSN(mappage, lsn);
		}

		return action;
	}

	if (XLogRecHasBlockRef(record, 6))
	{
		/* replay the record for meta page */
		POLAR_GET_LOG_TAG(record, meta_tag, 6);

		if (BUFFERTAGS_EQUAL(*tag, meta_tag))
		{
			action = POLAR_READ_BUFFER_FOR_REDO(record, 6, buffer);

			if (action == BLK_NEEDS_REDO)
			{
				HashMetaPage metap;
				Page        page;
				char       *data;
				uint32     *firstfree_ovflpage;
				Size        datalen;

				data = XLogRecGetBlockData(record, 6, &datalen);
				firstfree_ovflpage = (uint32 *) data;

				page = BufferGetPage(*buffer);
				metap = HashPageGetMeta(page);
				metap->hashm_firstfree = *firstfree_ovflpage;

				PageSetLSN(page, lsn);
			}
		}
	}

	return action;
}

/*
 * replay delete operation of hash index
 */
static XLogRedoAction
polar_hash_xlog_delete(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_hash_delete *xldata = (xl_hash_delete *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Page        page;
	BufferTag   bucket_tag, del_tag;

	if (!xldata->is_primary_bucket_page)
	{
		POLAR_GET_LOG_TAG(record, bucket_tag, 0);

		if (BUFFERTAGS_EQUAL(*tag, bucket_tag))
		{
			/*
			 * we don't care for return value as the purpose of reading bucketbuf
			 * is to ensure a cleanup lock on primary bucket page.
			 */
			return XLogReadBufferForRedoExtended(record, 0, POLAR_READ_MODE(*buffer)
												 , true, buffer);
		}
	}

	POLAR_GET_LOG_TAG(record, del_tag, 1);

	if (BUFFERTAGS_EQUAL(*tag, del_tag))
	{
		/*
		 * Ensure we have a cleanup lock on primary bucket page before we start
		 * with the actual replay operation.  This is to ensure that neither a
		 * scan can start nor a scan can be already-in-progress during the replay
		 * of this operation.  If we allow scans during this operation, then they
		 * can miss some records or show the same record multiple times.
		 */
		if (xldata->is_primary_bucket_page)
			action = XLogReadBufferForRedoExtended(record, 1, POLAR_READ_MODE(*buffer),
												   true, buffer);
		else
			action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

		/* replay the record for deleting entries in bucket page */
		if (action == BLK_NEEDS_REDO)
		{
			char       *ptr;
			Size        len;

			ptr = XLogRecGetBlockData(record, 1, &len);

			page = (Page) BufferGetPage(*buffer);

			if (len > 0)
			{
				OffsetNumber *unused;
				OffsetNumber *unend;

				unused = (OffsetNumber *) ptr;
				unend = (OffsetNumber *)((char *) ptr + len);

				if ((unend - unused) > 0)
					PageIndexMultiDelete(page, unused, unend - unused);
			}

			/*
			 * Mark the page as not containing any LP_DEAD items only if
			 * clear_dead_marking flag is set to true. See comments in
			 * hashbucketcleanup() for details.
			 */
			if (xldata->clear_dead_marking)
			{
				HashPageOpaque pageopaque;

				pageopaque = (HashPageOpaque) PageGetSpecialPointer(page);
				pageopaque->hasho_flag &= ~LH_PAGE_HAS_DEAD_TUPLES;
			}

			PageSetLSN(page, lsn);
		}
	}

	return action;
}

/*
 * replay split cleanup flag operation for primary bucket page.
 */
static XLogRedoAction
polar_hash_xlog_split_cleanup(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	XLogRedoAction action = BLK_NOTFOUND;
	Page        page;
	BufferTag   bucket_tag;

	POLAR_GET_LOG_TAG(record, bucket_tag, 0);

	if (BUFFERTAGS_EQUAL(*tag, bucket_tag))
	{
		action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			HashPageOpaque bucket_opaque;

			page = (Page) BufferGetPage(*buffer);

			bucket_opaque = (HashPageOpaque) PageGetSpecialPointer(page);
			bucket_opaque->hasho_flag &= ~LH_BUCKET_NEEDS_SPLIT_CLEANUP;
			PageSetLSN(page, lsn);
		}
	}

	return action;
}

/*
 * replay for update meta page
 */
static XLogRedoAction
polar_hash_xlog_update_meta_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_hash_update_meta_page *xldata = (xl_hash_update_meta_page *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	HashMetaPage metap;
	Page        page;
	BufferTag   meta_tag;

	POLAR_GET_LOG_TAG(record, meta_tag, 0);

	if (BUFFERTAGS_EQUAL(*tag, meta_tag))
	{
		action = POLAR_READ_BUFFER_FOR_REDO(record, 0, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(*buffer);
			metap = HashPageGetMeta(page);

			metap->hashm_ntuples = xldata->ntuples;

			PageSetLSN(page, lsn);
		}
	}

	return action;
}

/*
 * replay delete operation in hash index to remove
 * tuples marked as DEAD during index tuple insertion.
 */
static XLogRedoAction
polar_hash_xlog_vacuum_one_page(XLogReaderState *record, BufferTag *tag, Buffer *buffer)
{
	XLogRecPtr  lsn = record->EndRecPtr;
	xl_hash_vacuum_one_page *xldata = (xl_hash_vacuum_one_page *) XLogRecGetData(record);
	XLogRedoAction action = BLK_NOTFOUND;
	Page        page;
	HashPageOpaque pageopaque;
	BufferTag del_tag, meta_tag;

	POLAR_GET_LOG_TAG(record, del_tag, 0);

	if (BUFFERTAGS_EQUAL(*tag, del_tag))
	{
		action = XLogReadBufferForRedoExtended(record, 0, POLAR_READ_MODE(*buffer), true, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			page = (Page) BufferGetPage(*buffer);

			if (XLogRecGetDataLen(record) > SizeOfHashVacuumOnePage)
			{
				OffsetNumber *unused;

				unused = (OffsetNumber *)((char *) xldata + SizeOfHashVacuumOnePage);

				PageIndexMultiDelete(page, unused, xldata->ntuples);
			}

			/*
			 * Mark the page as not containing any LP_DEAD items. See comments in
			 * _hash_vacuum_one_page() for details.
			 */
			pageopaque = (HashPageOpaque) PageGetSpecialPointer(page);
			pageopaque->hasho_flag &= ~LH_PAGE_HAS_DEAD_TUPLES;

			PageSetLSN(page, lsn);
		}

		return action;
	}

	POLAR_GET_LOG_TAG(record, meta_tag, 1);

	if (BUFFERTAGS_EQUAL(*tag, meta_tag))
	{
		action = POLAR_READ_BUFFER_FOR_REDO(record, 1, buffer);

		if (action == BLK_NEEDS_REDO)
		{
			Page        metapage;
			HashMetaPage metap;

			metapage = BufferGetPage(*buffer);
			metap = HashPageGetMeta(metapage);

			metap->hashm_ntuples -= xldata->ntuples;

			PageSetLSN(metapage, lsn);
		}
	}

	return action;
}

void
polar_hash_idx_save(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_HASH_INIT_META_PAGE:
			polar_logindex_save_block(instance, record, 0);
			break;

		case XLOG_HASH_INIT_BITMAP_PAGE:
			polar_logindex_save_block(instance, record, 0);
			polar_logindex_save_block(instance, record, 1);
			break;

		case XLOG_HASH_INSERT:
			polar_logindex_save_block(instance, record, 0);
			polar_logindex_save_block(instance, record, 1);
			break;

		case XLOG_HASH_ADD_OVFL_PAGE:
			polar_hash_xlog_add_ovfl_page_save(instance, record);
			break;

		case XLOG_HASH_SPLIT_ALLOCATE_PAGE:
			polar_hash_xlog_split_allocate_page_save(instance, record);
			break;

		case XLOG_HASH_SPLIT_PAGE:
			polar_logindex_save_block(instance, record, 0);
			break;

		case XLOG_HASH_SPLIT_COMPLETE:
			polar_logindex_save_block(instance, record, 0);
			polar_logindex_save_block(instance, record, 1);
			break;

		case XLOG_HASH_MOVE_PAGE_CONTENTS:
			polar_hash_xlog_move_page_contents_save(instance, record);
			break;

		case XLOG_HASH_SQUEEZE_PAGE:
			polar_hash_xlog_squeeze_page_save(instance, record);
			break;

		case XLOG_HASH_DELETE:
			polar_hash_xlog_delete_save(instance, record);
			break;

		case XLOG_HASH_SPLIT_CLEANUP:
		case XLOG_HASH_UPDATE_META_PAGE:
			polar_logindex_save_block(instance, record, 0);
			break;

		case XLOG_HASH_VACUUM_ONE_PAGE:
			polar_logindex_save_block(instance, record, 0);
			polar_logindex_save_block(instance, record, 1);
			break;

		default:
			elog(PANIC, "polar_hash_idx_save: unknown op code %u", info);
			break;
	}
}

bool
polar_hash_idx_parse(polar_logindex_redo_ctl_t instance, XLogReaderState *record)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_HASH_INIT_META_PAGE:
			polar_logindex_redo_parse(instance, record, 0);
			break;

		case XLOG_HASH_INIT_BITMAP_PAGE:
			polar_logindex_redo_parse(instance, record, 0);
			polar_logindex_redo_parse(instance, record, 1);
			break;

		case XLOG_HASH_INSERT:
			polar_logindex_redo_parse(instance, record, 0);
			polar_logindex_redo_parse(instance, record, 1);
			break;

		case XLOG_HASH_ADD_OVFL_PAGE:
			polar_hash_xlog_add_ovfl_page_parse(instance, record);
			break;

		case XLOG_HASH_SPLIT_ALLOCATE_PAGE:
			polar_hash_xlog_split_allocate_page_parse(instance, record);
			break;

		case XLOG_HASH_SPLIT_PAGE:
			polar_logindex_redo_parse(instance, record, 0);
			break;

		case XLOG_HASH_SPLIT_COMPLETE:
			polar_logindex_redo_parse(instance, record, 0);
			polar_logindex_redo_parse(instance, record, 1);
			break;

		case XLOG_HASH_MOVE_PAGE_CONTENTS:
			polar_hash_xlog_move_page_contents_parse(instance, record);
			break;

		case XLOG_HASH_SQUEEZE_PAGE:
			polar_hash_xlog_squeeze_page_parse(instance, record);
			break;

		case XLOG_HASH_DELETE:
			polar_hash_xlog_delete_parse(instance, record);
			break;

		case XLOG_HASH_SPLIT_CLEANUP:
		case XLOG_HASH_UPDATE_META_PAGE:
			polar_logindex_redo_parse(instance, record, 0);
			break;

		case XLOG_HASH_VACUUM_ONE_PAGE:
			polar_hash_xlog_vacuum_one_page_parse(instance, record);
			break;

		default:
			elog(PANIC, "polar_hash_idx_parse: unknown op code %u", info);
			break;
	}

	return true;
}

XLogRedoAction
polar_hash_idx_redo(polar_logindex_redo_ctl_t instance, XLogReaderState *record,  BufferTag *tag, Buffer *buffer)
{
	uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_HASH_INIT_META_PAGE:
			return polar_hash_xlog_init_meta_page(record, tag, buffer);

		case XLOG_HASH_INIT_BITMAP_PAGE:
			return polar_hash_xlog_init_bitmap_page(record, tag, buffer);

		case XLOG_HASH_INSERT:
			return polar_hash_xlog_insert(record, tag, buffer);

		case XLOG_HASH_ADD_OVFL_PAGE:
			return polar_hash_xlog_add_ovfl_page(record, tag, buffer);

		case XLOG_HASH_SPLIT_ALLOCATE_PAGE:
			return polar_hash_xlog_split_allocate_page(record, tag, buffer);

		case XLOG_HASH_SPLIT_PAGE:
			return polar_hash_xlog_split_page(record, tag, buffer);

		case XLOG_HASH_SPLIT_COMPLETE:
			return polar_hash_xlog_split_complete(record, tag, buffer);

		case XLOG_HASH_MOVE_PAGE_CONTENTS:
			return polar_hash_xlog_move_page_contents(record, tag, buffer);

		case XLOG_HASH_SQUEEZE_PAGE:
			return polar_hash_xlog_squeeze_page(record, tag, buffer);

		case XLOG_HASH_DELETE:
			return polar_hash_xlog_delete(record, tag, buffer);

		case XLOG_HASH_SPLIT_CLEANUP:
			return polar_hash_xlog_split_cleanup(record, tag, buffer);

		case XLOG_HASH_UPDATE_META_PAGE:
			return polar_hash_xlog_update_meta_page(record, tag, buffer);

		case XLOG_HASH_VACUUM_ONE_PAGE:
			return polar_hash_xlog_vacuum_one_page(record, tag, buffer);

		default:
			elog(PANIC, "polar_hash_idx_redo: unknown op code %u", info);
	}

	return BLK_NOTFOUND;
}
