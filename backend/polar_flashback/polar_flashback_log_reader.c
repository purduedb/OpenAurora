/*-------------------------------------------------------------------------
 *
 * polar_flashback_log_reader.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, Alibaba Group Holding limited
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_log_reader.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "polar_flashback/polar_flashback_log_file.h"
#include "polar_flashback/polar_flashback_log_mem.h"
#include "polar_flashback/polar_flashback_log_reader.h"
#include "utils/memutils.h"

static int  read_file = -1;
static uint64 read_segno = 0;
static uint32 read_off = 0;
static uint32 read_len = 0;

/* size of the buffer allocated for error message. */
#define MAX_ERRORMSG_LEN 1000
/* load the flashback log switch ptrs */
#define load_switch_ptrs(dir, ptrs) \
	(ptrs != NIL? ptrs : polar_read_flog_history_file(dir))

static void report_invalid_flog_record(flog_reader_state *state,
									   const char *fmt, ...) pg_attribute_printf(2, 3);

/*
 * Allocate readRecordBuf to fit a record of at least the given length.
 * Returns true if successful, false if out of memory.
 *
 * readRecordBufSize is set to the new buffer size.
 *
 * To avoid useless small increases, round its size to a multiple of
 * POLAR_FLASHBACK_LOG_BLCKSZ, and make sure it's at least 3*Max(BLCKSZ, POLAR_FLASHBACK_LOG_BLCKSZ) to start
 * with.  (That is enough for all "normal" records, but very large commit or
 * abort records might need more space.)
 */
static bool
allocate_flog_read_buf(flog_reader_state *state, uint32 rec_len)
{
	uint32      new_size = rec_len;

	new_size += POLAR_FLOG_BLCKSZ - (new_size % POLAR_FLOG_BLCKSZ);
	new_size = Max(new_size, 3 * Max(BLCKSZ, POLAR_FLOG_BLCKSZ));

	if (state->read_record_buf)
		/*no cover line*/
		pfree(state->read_record_buf);

	state->read_record_buf =
		(char *) polar_palloc_extended_in_crit(new_size, MCXT_ALLOC_NO_OOM);

	if (state->read_record_buf == NULL)
	{
		/*no cover begin*/
		state->read_record_buf_size = 0;
		return false;
		/*no cover end*/
	}

	state->read_record_buf_size = new_size;
	return true;
}

/*
 * Invalidate the flashback_log_reader's read state to force a re-read.
 */
static void
flog_inval_read_state(flog_reader_state *state)
{
	state->read_seg_no = 0;
	state->read_off = 0;
	state->read_len = 0;
}

/*
 * Construct a string in state->errormsg_buf explaining what's wrong with
 * the current record being read.
 */
static void
report_invalid_flog_record(flog_reader_state *state,
						   const char *fmt, ...)
{
	va_list     args;

	fmt = _(fmt);

	va_start(args, fmt);
	vsnprintf(state->errormsg_buf, MAX_ERRORMSG_LEN, fmt, args);
	va_end(args);
}

/* POLAR: Is the flashback log point a switch point whose prev_lsn is wrong. */
static bool
is_flog_ptr_switch(polar_flog_rec_ptr ptr, polar_flog_rec_ptr prev_ptr, flog_reader_state *reader)
{
	ListCell   *cell;

	if (ptr >= FLOG_LONG_PHD_SIZE)
		ptr -= FLOG_LONG_PHD_SIZE;

	/* Check first */
	if (ptr % POLAR_FLOG_SEG_SIZE != 0 ||
			!FLOG_REC_PTR_IS_INVAILD(prev_ptr))
		return false;

	/* Check strictly */
	reader->switch_ptrs = load_switch_ptrs(reader->flog_buf_ctl->dir, reader->switch_ptrs);

	foreach (cell, reader->switch_ptrs)
	{
		flog_history_entry *tle = (flog_history_entry *) lfirst(cell);

		if (tle->next_ptr == ptr)
			return true;
	}

	return false;
}

/*
 * Validate an flashback log record header.
 *
 * This is just a convenience subroutine to avoid duplicated code in
 * polar_read_flashback_log_record.  It's not intended for use from anywhere else.
 */
static bool
valid_flog_rec_header(flog_reader_state *state,
					  polar_flog_rec_ptr ptr, polar_flog_rec_ptr prev_ptr,
					  flog_record *record, bool rand_access)
{
	if (record->xl_tot_len < FLOG_REC_HEADER_SIZE)
	{
		/*no cover begin*/
		report_invalid_flog_record(state,
								   "invalid flashback log record length at %X/%X: wanted larger than or equal to %u, "
								   "got %u", (uint32)(ptr >> 32), (uint32) ptr,
								   (uint32) FLOG_REC_HEADER_SIZE, record->xl_tot_len);
		return false;
		/*no cover end*/
	}

	if (rand_access)
	{
		/*
		 * We can't exactly verify the prev-link, but surely it should be less
		 * than the record's own address.
		 */
		if (record->xl_prev >= ptr)
		{
			/*no cover begin*/
			report_invalid_flog_record(state,
									   "flashback log record with incorrect prev-link %X/%X at %X/%X",
									   (uint32)(record->xl_prev >> 32),
									   (uint32) record->xl_prev,
									   (uint32)(ptr >> 32), (uint32) ptr);
			return false;
			/*no cover end*/
		}
	}
	else
	{
		/*
		 * Record's prev-link should exactly match our previous location. This
		 * check guards against torn flashback log pages where a stale but valid-looking
		 * flashback log record starts on a sector boundary.
		 */
		if (record->xl_prev != prev_ptr)
		{
			if (is_flog_ptr_switch(ptr, record->xl_prev, state))
				return true;

			/*no cover begin*/
			report_invalid_flog_record(state,
									   "flashback log record with incorrect prev-link %X/%X at %X/%X",
									   (uint32)(record->xl_prev >> 32),
									   (uint32) record->xl_prev,
									   (uint32)(ptr >> 32), (uint32) ptr);
			return false;
			/*no cover end*/
		}
	}

	return true;
}

/*
 * Validate a flsahback log page header.
 *
 * Check if 'phdr' is valid as the header of the flashback log page at position
 * 'recptr'.
 */
static bool
flog_page_header_validate(flog_reader_state *state,
						  polar_flog_rec_ptr recptr, char *phdr)
{
	polar_flog_rec_ptr  recaddr;
	uint64  segno;
	int32       offset;
	flog_page_header hdr = (flog_page_header) phdr;

	Assert((recptr % POLAR_FLOG_BLCKSZ) == 0);

	segno = flog_ptr_to_seg(recptr, state->segment_size);
	offset = FLOG_SEGMENT_OFFSET(recptr, state->segment_size);

	flog_seg_offset_to_ptr(segno, offset, state->segment_size, recaddr);

	if (hdr->xlp_magic != FLOG_PAGE_MAGIC)
	{
		/*no cover begin*/
		char        fname[FLOG_MAX_FNAME_LEN];

		get_flog_fname(fname, segno, state->segment_size, FLOG_DEFAULT_TIMELINE);

		report_invalid_flog_record(state,
								   "invalid magic number %04X in flashback log segment %s, offset %u",
								   hdr->xlp_magic,
								   fname,
								   offset);
		return false;
		/*no cover end*/
	}

	if (hdr->xlp_version < FLOG_PAGE_VERSION)
	{
		/*no cover begin*/
		char        fname[FLOG_MAX_FNAME_LEN];

		get_flog_fname(fname, segno, state->segment_size, FLOG_DEFAULT_TIMELINE);
		report_invalid_flog_record(state,
								   "invalid version %04X in flashback log segment %s, offset %u",
								   hdr->xlp_version,
								   fname,
								   offset);
		return false;
		/*no cover end*/
	}

	if ((hdr->xlp_info & ~FLOG_ALL_FLAGS) != 0)
	{
		/*no cover begin*/
		char        fname[FLOG_MAX_FNAME_LEN];

		get_flog_fname(fname, segno, state->segment_size, FLOG_DEFAULT_TIMELINE);

		report_invalid_flog_record(state,
								   "invalid info bits %04X in flashback log segment %s, offset %u",
								   hdr->xlp_info,
								   fname,
								   offset);
		return false;
		/*no cover end*/
	}

	if (hdr->xlp_info & FLOG_LONG_PAGE_HEADER)
	{
		polar_long_page_header longhdr = (polar_long_page_header) hdr;

		if (state->system_identifier &&
				longhdr->xlp_sysid != state->system_identifier)
		{
			/*no cover begin*/
			char        fhdrident_str[32];
			char        sysident_str[32];

			/*
			 * Format sysids separately to keep platform-dependent format code
			 * out of the translatable message string.
			 */
			snprintf(fhdrident_str, sizeof(fhdrident_str), UINT64_FORMAT,
					 longhdr->xlp_sysid);
			snprintf(sysident_str, sizeof(sysident_str), UINT64_FORMAT,
					 state->system_identifier);
			report_invalid_flog_record(state,
									   "flashback log file is from different database system: "
									   "flashback log file database system identifier is %s, "
									   "pg_control database system identifier is %s",
									   fhdrident_str, sysident_str);
			return false;
			/*no cover end*/
		}
		else if (longhdr->xlp_seg_size != state->segment_size)
		{
			/*no cover begin*/
			report_invalid_flog_record(state,
									   "flashback log file is from different database system: "
									   "incorrect segment size in page header");
			return false;
			/*no cover end*/
		}
		else if (longhdr->xlp_blcksz != POLAR_FLOG_BLCKSZ)
		{
			/*no cover begin*/
			report_invalid_flog_record(state,
									   "flashback log file is from different database system: "
									   "incorrect POLAR_FLASHBACK_LOG_BLCKSZ in page header");
			return false;
			/*no cover end*/
		}
	}
	else if (offset == 0)
	{
		/*no cover begin*/
		char        fname[FLOG_MAX_FNAME_LEN];

		get_flog_fname(fname, segno, state->segment_size, FLOG_DEFAULT_TIMELINE);

		/* hmm, first page of file doesn't have a long header? */
		report_invalid_flog_record(state,
								   "invalid info bits %04X in flashback log segment %s, offset %u",
								   hdr->xlp_info,
								   fname,
								   offset);
		return false;
		/*no cover end*/
	}

	/*
	 * Check that the address on the page agrees with what we expected. This
	 * check typically fails when an old flashback log segment is recycled,
	 * and hasn't yet been overwritten with new data yet.
	 */
	if (hdr->xlp_pageaddr != recaddr)
	{
		/*no cover begin*/
		char        fname[FLOG_MAX_FNAME_LEN];

		get_flog_fname(fname, segno, state->segment_size, FLOG_DEFAULT_TIMELINE);

		report_invalid_flog_record(state,
								   "unexpected pageaddr %X/%X in flashback log segment %s, offset %u",
								   (uint32)(hdr->xlp_pageaddr >> 32), (uint32) hdr->xlp_pageaddr,
								   fname,
								   offset);
		return false;
		/*no cover end*/
	}

	state->latest_page_ptr = recptr;

	return true;
}

/*
 * Read a single flashback log page including at least [page_ptr, req_len] of valid data
 * via the read_page() callback.
 *
 * Returns -1 if the required page cannot be read for some reason; errormsg_buf
 * is set in that case (unless the error occurs in the read_page callback).
 *
 * We fetch the page from a reader-local cache if we know we have the required
 * data and if there hasn't been any error since caching the data.
 */
static int
read_flog_page_internal(flog_reader_state *state,
						polar_flog_rec_ptr page_ptr, int req_len)
{
	int         read_len;
	uint32      target_pageoff;
	uint64      target_segno;
	flog_page_header hdr;

	Assert((page_ptr % POLAR_FLOG_BLCKSZ) == 0);
	target_segno = flog_ptr_to_seg(page_ptr, state->segment_size);
	target_pageoff = FLOG_SEGMENT_OFFSET(page_ptr, state->segment_size);

	/* check whether we have all the requested data already */
	if (target_segno == state->read_seg_no && target_pageoff == state->read_off &&
			req_len < state->read_len)
		return state->read_len;

	/*
	 * Data is not in our buffer.
	 *
	 * Every time we actually read the page, even if we looked at parts of it
	 * before, we need to do verification as the read_page callback might now
	 * be rereading data from a different source.
	 *
	 * Whenever switching to a new segment, we read the first page of the
	 * file and validate its header, even if that's not where the target
	 * record is.  This is so that we can check the additional identification
	 * info that is present in the first page's "long" header.
	 */
	if (target_segno != state->read_seg_no && target_pageoff != 0)
	{
		polar_flog_rec_ptr  targetSegmentPtr = page_ptr - target_pageoff;

		read_len = state->read_page(state, targetSegmentPtr, POLAR_FLOG_BLCKSZ,
									state->curr_rec_ptr, state->read_buf);

		if (read_len < 0)
			/*no cover line*/
			goto err;

		if (!flog_page_header_validate(state, targetSegmentPtr,
									   state->read_buf))
			/*no cover line*/
			goto err;
	}

	/*
	 * First, read the requested data length, but at least a short page header
	 * so that we can validate it.
	 */
	read_len = state->read_page(state, page_ptr, Max(req_len, FLOG_SHORT_PHD_SIZE),
								state->curr_rec_ptr, state->read_buf);

	if (read_len < 0)
		goto err;

	Assert(read_len <= POLAR_FLOG_BLCKSZ);

	/* Do we have enough data to check the header length? */
	if (read_len <= FLOG_SHORT_PHD_SIZE)
		goto err;

	Assert(read_len >= req_len);

	hdr = (flog_page_header) state->read_buf;

	/* still not enough */
	if (read_len < FLOG_PAGE_HEADER_SIZE(hdr))
	{
		read_len = state->read_page(state, page_ptr, FLOG_PAGE_HEADER_SIZE(hdr),
									state->curr_rec_ptr, state->read_buf);

		if (read_len < 0)
			goto err;
	}

	/*
	 * Now that we know we have the full header, validate it.
	 */
	if (!flog_page_header_validate(state, page_ptr, (char *) hdr))
		goto err;

	/* update read state information */
	state->read_seg_no = target_segno;
	state->read_off = target_pageoff;
	state->read_len = read_len;

	return read_len;

err:
	flog_inval_read_state(state);
	return -1;
}

/*
 * CRC-check an flashback log record.  We do not believe the contents of an
 * flashback log record (other than to the minimal extent of computing
 * the amount of data to read in) until we've checked the CRCs.
 *
 * We assume all of the record (that is, xl_tot_len bytes) has been read
 * into memory at *record.  Also, polar_flashback_log_page_header_validate()
 * has accepted the record's header, which means in particular that
 * xl_tot_len is at least FLASHBACK_LOG_REC_SIZE.
 */
static bool
flog_record_validate(flog_reader_state *state,
					 flog_record *record, polar_flog_rec_ptr recptr)
{
	pg_crc32c   crc;

	/* Calculate the CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) record) + FLOG_REC_HEADER_SIZE,
				record->xl_tot_len - FLOG_REC_HEADER_SIZE);
	/* include the record header last */
	COMP_CRC32C(crc, (char *) record, offsetof(flog_record, xl_crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(record->xl_crc, crc))
	{
		/*no cover begin*/
		report_invalid_flog_record(state,
								   "incorrect data crc in flashback log "
								   "record at %X/%X",
								   (uint32)(recptr >> 32), (uint32) recptr);
		return false;
		/*no cover end*/
	}

	return true;
}

/*
 * Attempt to read an flashback log record.
 *
 * If RecPtr is valid, try to read a record at that position.  Otherwise
 * try to read a record just after the last one previously read.
 *
 * If the read_page callback fails to read the requested data, NULL is
 * returned.  The callback is expected to have reported the error; errormsg
 * is set to NULL.
 *
 * If the reading fails for some other reason, NULL is also returned, and
 * *errormsg is set to a string with details of the failure.
 *
 * The returned pointer (or *errormsg) points to an internal buffer that's
 * valid until the next call to polar_read_flashback_log_record.
 */
flog_record *
polar_read_flog_record(flog_reader_state *state,
					   polar_flog_rec_ptr rec_ptr, char **errormsg)
{
	flog_record *record;
	polar_flog_rec_ptr  target_page_ptr;
	uint32      len,
				total_len = 0;
	uint32      target_rec_off;
	uint32      page_header_size;
	bool        got_header;
	int         read_off;
	uint32      got_len = 0;
	bool rand_access = false;
	polar_flog_rec_ptr next_ptr;

	Assert(errormsg);
	/* reset error state */
	*errormsg = NULL;
	state->errormsg_buf[0] = '\0';

	/* reset in_switch_region */
	state->in_switch_region = false;

	if (FLOG_REC_PTR_IS_INVAILD(rec_ptr))
	{
		/* No explicit start point; read the record after the one we just read */
		rec_ptr = state->end_rec_ptr;

		if (FLOG_REC_PTR_IS_INVAILD(state->read_rec_ptr))
			rand_access = true;

		/*
		 * rec_ptr is pointing to end+1 of the previous flashback log record.  If we're
		 * at a page boundary, no more records can fit on the current page. We
		 * must skip over the page header, but we can't do that until we've
		 * read in the page, since the header size is variable.
		 */
	}
	else
	{
		/*
		 * Caller supplied a position to start at.
		 *
		 * In this case, the passed-in record pointer should already be
		 * pointing to a valid record starting position.
		 */
		Assert(FLOG_REC_PTR_IS_VAILD(rec_ptr));
		rand_access = true;
	}

	state->curr_rec_ptr = rec_ptr;

	target_page_ptr = rec_ptr - (rec_ptr % POLAR_FLOG_BLCKSZ);
	target_rec_off = rec_ptr % POLAR_FLOG_BLCKSZ;

	/*
	 * Read the page containing the record into state->readBuf. Request enough
	 * byte to cover the whole record header, or at least the part of it that
	 * fits on the same page.
	 */
	read_off = read_flog_page_internal(state,
									   target_page_ptr,
									   Min(target_rec_off + FLOG_REC_HEADER_SIZE, POLAR_FLOG_BLCKSZ));

	if (read_off < 0)
		goto err;

	got_len = read_off - target_rec_off;

	/*
	 * ReadPageInternal always returns at least the page header, so we can
	 * examine it now.
	 */
	page_header_size = FLOG_PAGE_HEADER_SIZE((flog_page_header) state->read_buf);

	if (target_rec_off == 0)
	{
		/*
		 * At page start, so skip over page header.
		 */
		rec_ptr += page_header_size;
		target_rec_off = page_header_size;
	}
	else if (target_rec_off < page_header_size)
	{
		/*no cover begin*/
		report_invalid_flog_record(state, "invalid flashback log record offset at %X/%X",
								   (uint32)(rec_ptr >> 32), (uint32) rec_ptr);
		goto err;
		/*no cover end*/
	}

	/* Can't read flashback log from contrecord ptr */
	if ((((flog_page_header) state->read_buf)->xlp_info & FLOG_FIRST_IS_CONTRECORD) &&
			target_rec_off == page_header_size)
	{
		/*no cover begin*/
		report_invalid_flog_record(state, "contrecord flashback log is requested by %X/%X",
								   (uint32)(rec_ptr >> 32), (uint32) rec_ptr);
		goto err;
		/*no cover end*/
	}

	/* ReadPageInternal has verified the page header */
	Assert(page_header_size <= read_off);

	/*
	 * Read the record length.
	 *
	 * NB: Even though we use an polar_flashback_log_record pointer here, the whole record
	 * header might not fit on this page. xl_tot_len is the first field of the
	 * struct, so it must be on this page (the records are MAXALIGNed), but we
	 * cannot access any other fields until we've verified that we got the
	 * whole header.
	 */
	record = (flog_record *)(state->read_buf + rec_ptr % POLAR_FLOG_BLCKSZ);
	total_len = record->xl_tot_len;

	/*
	 * If the whole record header is on this page, validate it immediately.
	 * Otherwise do just a basic sanity check on xl_tot_len, and validate the
	 * rest of the header after reading it from the next page.  The xl_tot_len
	 * check is necessary here to ensure that we enter the "Need to reassemble
	 * record" code path below; otherwise we might fail to apply
	 * polar_valid_flashback_log_rec_header at all.
	 */
	if (target_rec_off <= POLAR_FLOG_BLCKSZ - FLOG_REC_HEADER_SIZE)
	{
		/*no cover begin*/
		if (!valid_flog_rec_header(state, rec_ptr, state->read_rec_ptr,
								   record, rand_access))
			goto err;

		got_header = true;
		/*no cover end*/
	}
	else
	{
		/* XXX: more validation should be done here */
		if (total_len < FLOG_REC_HEADER_SIZE)
		{
			/*no cover begin*/
			report_invalid_flog_record(state,
									   "invalid flashback log record length at %X/%X: wanted %u, got %u",
									   (uint32)(rec_ptr >> 32), (uint32) rec_ptr,
									   (uint32) FLOG_REC_HEADER_SIZE, total_len);
			goto err;
			/*no cover end*/
		}

		got_header = false;
	}

	/*
	 * Enlarge readRecordBuf as needed.
	 */
	if (total_len > state->read_record_buf_size && !allocate_flog_read_buf(state, total_len))
	{
		/*no cover begin*/
		/* We treat this as a "bogus data" condition */
		report_invalid_flog_record(state, "record length %u at %X/%X too long",
								   total_len,
								   (uint32)(rec_ptr >> 32), (uint32) rec_ptr);
		goto err;
		/*no cover end*/
	}

	len = POLAR_FLOG_BLCKSZ - rec_ptr % POLAR_FLOG_BLCKSZ;

	if (total_len > len)
	{
		/* Need to reassemble record */
		char       *contdata;
		flog_page_header page_header;
		char       *buffer;

		/* Copy the first fragment of the record from the first page. */
		memcpy(state->read_record_buf,
			   state->read_buf + rec_ptr % POLAR_FLOG_BLCKSZ, len);
		buffer = state->read_record_buf + len;
		got_len = len;

		do
		{
			/* Calculate pointer to beginning of next page */
			target_page_ptr += POLAR_FLOG_BLCKSZ;

			/* Wait for the next page to become available */
			read_off = read_flog_page_internal(state, target_page_ptr,
											   Min(total_len - got_len + FLOG_SHORT_PHD_SIZE,
												   POLAR_FLOG_BLCKSZ));

			if (read_off < 0)
				goto err;

			Assert(FLOG_SHORT_PHD_SIZE <= read_off);

			/* Check that the continuation on next page looks valid */
			page_header = (flog_page_header) state->read_buf;

			if (!(page_header->xlp_info & FLOG_FIRST_IS_CONTRECORD))
			{
				/*no cover begin*/
				report_invalid_flog_record(state,
										   "there is no contrecord flag at flashback log %X/%X",
										   (uint32)(rec_ptr >> 32), (uint32) rec_ptr);
				goto err;
				/*no cover end*/
			}

			/*
			 * Cross-check that xlp_rem_len agrees with how much of the record
			 * we expect there to be left.
			 */
			if (page_header->xlp_rem_len == 0 ||
					total_len != (page_header->xlp_rem_len + got_len))
			{
				/*no cover begin*/
				report_invalid_flog_record(state,
										   "invalid contrecord length %u at flashback log %X/%X",
										   page_header->xlp_rem_len,
										   (uint32)(rec_ptr >> 32), (uint32) rec_ptr);
				goto err;
				/*no cover end*/
			}

			/* Append the continuation from this page to the buffer */
			page_header_size = FLOG_PAGE_HEADER_SIZE(page_header);

			if (read_off < page_header_size)
				read_off = read_flog_page_internal(state, target_page_ptr,
												   page_header_size);

			Assert(page_header_size <= read_off);

			contdata = (char *) state->read_buf + page_header_size;
			len = POLAR_FLOG_BLCKSZ - page_header_size;

			if (page_header->xlp_rem_len < len)
				len = page_header->xlp_rem_len;

			if (read_off < page_header_size + len)
				read_off = read_flog_page_internal(state, target_page_ptr,
												   page_header_size + len);

			memcpy(buffer, (char *) contdata, len);
			buffer += len;
			got_len += len;

			/* If we just reassembled the record header, validate it. */
			if (!got_header)
			{
				record = (flog_record *) state->read_record_buf;

				if (!valid_flog_rec_header(state, rec_ptr, state->read_rec_ptr,
										   record, rand_access))
					goto err;

				got_header = true;
			}
		}
		while (got_len < total_len);

		Assert(got_header);

		record = (flog_record *) state->read_record_buf;

		if (!flog_record_validate(state, record, rec_ptr))
			goto err;

		page_header_size = FLOG_PAGE_HEADER_SIZE((flog_page_header) state->read_buf);
		state->read_rec_ptr = rec_ptr;
		state->end_rec_ptr = target_page_ptr + page_header_size
							 + MAXALIGN(page_header->xlp_rem_len);
	}
	else
	{
		read_off = read_flog_page_internal(state, target_page_ptr,
										   Min(target_rec_off + total_len, POLAR_FLOG_BLCKSZ));

		if (read_off < 0)
			goto err;

		/* Record does not cross a page boundary */
		if (!flog_record_validate(state, record, rec_ptr))
			goto err;

		state->end_rec_ptr = rec_ptr + MAXALIGN(total_len);

		state->read_rec_ptr = rec_ptr;
		memcpy(state->read_record_buf, record, total_len);
	}

	Assert(convert_to_first_valid_ptr(state->end_rec_ptr) ==
			polar_get_next_flog_ptr(state->read_rec_ptr, record->xl_tot_len));
	return record;

err:

	/*
	 * Invalidate the read state. We might read from a different source after
	 * failure.
	 */
	flog_inval_read_state(state);

	if (state->errormsg_buf[0] != '\0')
		*errormsg = state->errormsg_buf;

	/* Is the record in the switch region */
	next_ptr = state->curr_rec_ptr;

	if (polar_is_flog_rec_ignore(&next_ptr, total_len, state))
	{
		state->in_switch_region = true;
		state->end_rec_ptr = next_ptr;
	}

	return NULL;
}

/*
 * Allocate and initialize a new flashback log reader.
 *
 * Returns NULL if the flashback log reader couldn't be allocated.
 * Change the memory context to flashback log read memory context.
 */
flog_reader_state *
polar_flog_reader_allocate(int segment_size, page_read_callback page_read_func,
						   void *private_data, flog_buf_ctl_t flog_buf_ctl)
{
	flog_reader_state *state;

	state = (flog_reader_state *)
			polar_palloc_extended_in_crit(sizeof(flog_reader_state),
										  MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);

	if (!state)
		return NULL;

	/*
	 * Permanently allocate readBuf.  We do it this way, rather than just
	 * making a static array, for two reasons: (1) no need to waste the
	 * storage in most instantiations of the backend; (2) a static char array
	 * isn't guaranteed to have any particular alignment, whereas
	 * palloc_extended() will provide MAXALIGN'd storage.
	 */
	state->read_buf = (char *) polar_palloc_extended_in_crit(POLAR_FLOG_BLCKSZ,
															 MCXT_ALLOC_NO_OOM);

	if (!state->read_buf)
	{
		/*no cover begin*/
		pfree(state);
		return NULL;
		/*no cover end*/
	}

	state->system_identifier = GetSystemIdentifier();
	state->segment_size = segment_size;
	state->read_page = page_read_func;
	/* system_identifier initialized to zeroes above */
	state->private_data = private_data;
	/* ReadRecPtr and EndRecPtr initialized to zeroes above */
	/* readSegNo, readOff, readLen, readPageTLI initialized to zeroes above */
	state->errormsg_buf = polar_palloc_extended_in_crit(MAX_ERRORMSG_LEN + 1,
														MCXT_ALLOC_NO_OOM);

	if (!state->errormsg_buf)
	{
		/*no cover begin*/
		pfree(state->read_buf);
		pfree(state);
		return NULL;
		/*no cover end*/
	}

	state->errormsg_buf[0] = '\0';
	state->switch_ptrs = NIL;
	state->in_switch_region = false;
	state->flog_buf_ctl = flog_buf_ctl;

	/*
	 * Allocate an initial readRecordBuf of minimal size, which can later be
	 * enlarged if necessary.
	 */
	if (!allocate_flog_read_buf(state, 0))
	{
		/*no cover begin*/
		pfree(state->errormsg_buf);
		pfree(state->read_buf);
		list_free_deep(state->switch_ptrs);
		pfree(state);
		return NULL;
		/*no cover end*/
	}

	return state;
}

/*
 * POLAR: Free the flashback log reader state and switch to old context.
 */
void
polar_flog_reader_free(flog_reader_state *state)
{
	pfree(state->errormsg_buf);

	if (state->read_record_buf)
		pfree(state->read_record_buf);

	pfree(state->read_buf);
	list_free_deep(state->switch_ptrs);
	pfree(state);
}

/*
 * Read the flashback log page containing targetRecPtr into readBuf
 * (if not read already). It is just called by polar_flashback_logindex_startup.
 *
 * Returns number of bytes read, if the page is read successfully,
 * or -1 in case of errors.
 *
 * When errors occur, they are ereport'ed in WARNING level.
 *
 */
int
polar_flog_page_read(flog_reader_state *state,
					 polar_flog_rec_ptr target_page_ptr, int req_len,
					 polar_flog_rec_ptr target_rec_ptr, char *cur_page)
{
	uint32      target_page_off;
	uint64      target_seg_no PG_USED_FOR_ASSERTS_ONLY;
	int         polar_read_rc = -1;
	char        file_name[FLOG_MAX_FNAME_LEN];
	polar_flog_rec_ptr read_upto;

	Assert(state->flog_buf_ctl);
	/* Check there is enough data? */
	read_upto = polar_get_flog_write_result(state->flog_buf_ctl);

	if (target_page_ptr + POLAR_FLOG_BLCKSZ <= read_upto)
	{
		/*
		 * more than one block available; read only that block, have caller
		 * come back if they need more.
		 */
		read_len = POLAR_FLOG_BLCKSZ;
	}
	else if (target_page_ptr + req_len > read_upto)
	{
		/*
		 * Not enough data there. Warn when we read log beyond read upto point.
		 * Put the error message to state for caller to check.
		 */
		if (unlikely(polar_flashback_log_debug))
			elog(WARNING, "No enough flashback log in disk while read from "
					"target page pointer %ld, request length %d and read upto %ld",
					target_page_ptr, req_len, read_upto);
		report_invalid_flog_record(state, REC_UNFLUSHED_ERROR_MSG);
		goto next_record_is_invalid;
	}
	else
	{
		/* enough bytes available to satisfy the request */
		read_len = read_upto - target_page_ptr;
	}

	target_seg_no = flog_ptr_to_seg(target_page_ptr, POLAR_FLOG_SEG_SIZE);
	target_page_off = FLOG_SEGMENT_OFFSET(target_page_ptr, POLAR_FLOG_SEG_SIZE);

	/*
	 * See if we need to switch to a new segment because the requested record
	 * is not in the currently open one.
	 */
	if (read_file >= 0 &&
			!ptr_in_flog_seg(target_page_ptr, read_segno, POLAR_FLOG_SEG_SIZE))
	{
		close(read_file);
		read_file = -1;
	}

	read_segno = flog_ptr_to_seg(target_page_ptr, POLAR_FLOG_SEG_SIZE);

	if (read_file < 0)
	{
		if (polar_is_flog_file_exist(state->flog_buf_ctl->dir, target_page_ptr, WARNING))
			read_file = polar_flog_file_open(read_segno, state->flog_buf_ctl->dir);
		else
		{
			/*no cover begin*/
			get_flog_fname(file_name, read_segno,
						   POLAR_FLOG_SEG_SIZE, FLOG_DEFAULT_TIMELINE);
			report_invalid_flog_record(state,
									   "Can't find the flashback log segno file %s", file_name);
			goto next_record_is_invalid;
			/*no cover end*/
		}
	}

	/*
	 * At this point, we have the right segment open and if
	 * the requested record is in it.
	 */
	Assert(read_file != -1);

	/* Read the requested page */
	read_off = target_page_off;

	pgstat_report_wait_start(WAIT_EVENT_FLASHBACK_LOG_READ);
	polar_read_rc = polar_pread(read_file, cur_page, POLAR_FLOG_BLCKSZ, (off_t) read_off);

	if (polar_read_rc != POLAR_FLOG_BLCKSZ)
	{

		char        fname[FLOG_MAX_FNAME_LEN];
		int         save_errno = errno;

		/*no cover begin*/
		pgstat_report_wait_end();
		get_flog_fname(fname, read_segno, POLAR_FLOG_SEG_SIZE, FLOG_DEFAULT_TIMELINE);
		errno = save_errno;
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not read from flashback log segment %s, offset %u: %m",
						fname, read_off)));
		goto next_record_is_invalid;
		/*no cover end*/
	}

	pgstat_report_wait_end();

	Assert(target_seg_no == read_segno);
	Assert(target_page_off == read_off);
	Assert(req_len <= read_len);

	/*
	 * Check the page header immediately, so that we can retry immediately if
	 * it's not valid. This may seem unnecessary, because polar_flashback_log_read_record()
	 * validates the page header anyway, and would propagate the failure up to
	 * ReadRecord(), which would retry. However, there's a corner case with
	 * continuation records, if a record is split across two pages such that
	 * we would need to read the two pages from different sources.
	 *
	 * Validating the page header is cheap enough that doing it twice
	 * shouldn't be a big deal from a performance point of view.
	 */
	if (!flog_page_header_validate(state, target_page_ptr, cur_page))
	{
		/*no cover line*/
		goto next_record_is_invalid;
	}

	return read_len;

next_record_is_invalid:

	if (read_file >= 0)
		close(read_file);

	read_file = -1;
	read_len = 0;
	return -1;
}

/*
 * POLAR: Check the flashback log record error can be ignore?
 *
 * Change the ptr to next_ptr.
 */
bool
polar_is_flog_rec_ignore(polar_flog_rec_ptr *ptr, uint32 log_len, flog_reader_state *reader)
{
	ListCell   *cell;
	polar_flog_rec_ptr end_ptr;

	if (log_len == 0)
		end_ptr = *ptr;
	else
		end_ptr = polar_get_next_flog_ptr(*ptr, log_len);

	reader->switch_ptrs = load_switch_ptrs(reader->flog_buf_ctl->dir, reader->switch_ptrs);
	foreach (cell, reader->switch_ptrs)
	{
		flog_history_entry *tle = (flog_history_entry *) lfirst(cell);

		if ((end_ptr > tle->switch_ptr && *ptr < tle->next_ptr) ||
				*ptr == tle->switch_ptr)
		{
			elog(WARNING, "The flashback log record at %X/%X will be ignore. and switch to "
				 "%X/%X", (uint32)(*ptr >> 32), (uint32)(*ptr),
				 (uint32)((tle->next_ptr + FLOG_LONG_PHD_SIZE) >> 32),
				 (uint32)(tle->next_ptr + FLOG_LONG_PHD_SIZE));
			*ptr = tle->next_ptr + FLOG_LONG_PHD_SIZE;
			return true;
		}
		/* We have check it in the before */
		else if (*ptr > tle->next_ptr)
			return false;
	}

	return false;
}
