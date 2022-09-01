/*-------------------------------------------------------------------------
 *
 * polar_flashback_point.h
 *
 *
 * Copyright (c) 2020-2120, Alibaba-inc PolarDB Group
 *
 * src/include/polar_flashback/polar_flashback_point.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_FLASHBACK_POINT_H
#define POLAR_FLASHBACK_POINT_H
#include "postgres.h"

#include "polar_flashback/polar_flashback_log.h"

extern void polar_set_fbpoint_wal_info(flog_buf_ctl_t buf_ctl, XLogRecPtr fbpoint_lsn, pg_time_t fbpoint_time, XLogRecPtr bg_replayed_lsn, bool is_restart_point);
extern XLogRecPtr polar_get_curr_fbpoint_lsn(flog_buf_ctl_t buf_ctl);
extern XLogRecPtr polar_get_prior_fbpoint_lsn(flog_buf_ctl_t buf_ctl);
extern XLogRecPtr polar_get_local_fbpoint_lsn(flog_buf_ctl_t buf_ctl, XLogRecPtr page_lsn, XLogRecPtr redo_lsn);
extern polar_flog_rec_ptr polar_get_fbpoint_start_ptr(flog_buf_ctl_t ctl);

extern bool polar_is_page_first_modified(flog_buf_ctl_t buf_ctl, XLogRecPtr page_lsn, XLogRecPtr redo_lsn);
extern bool polar_is_flashback_point(flog_ctl_t instance, XLogRecPtr checkpoint_lsn, XLogRecPtr bg_replayed_lsn, int *flags, bool is_restart_point);
#endif
