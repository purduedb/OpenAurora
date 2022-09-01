/*-------------------------------------------------------------------------
 *
 * px_vars.h
 *	  Definitions for Greenplum-specific global variables.
 *
 * Portions Copyright (c) 2003-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * IDENTIFICATION
 *	    src/include/px/px_vars.h
 *
 * NOTES
 *	  See src/backend/utils/misc/guc_px.c for variable external specification.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PXVARS_H
#define PXVARS_H

#define MASTER_CONTENT_ID (-1)

#include "access/xlogdefs.h"	/* XLogRecPtr */
#include "utils/guc.h"

/*
 * ----- Declarations of PX-specific global variables ------
 */
#define TUPLE_CHUNK_ALIGN	1

typedef enum
{
	PX_ROLE_UTILITY = 0,		/* Operating as a simple database engine */
	PX_ROLE_QC,			/* Operating as the parallel query dispatcher */
	PX_ROLE_PX,			/* Operating as a parallel query executor */
	PX_ROLE_UNDEFINED			/* Should never see this role in use */
} PxRoleValue;

extern PxRoleValue px_role;		/* GUC var - server operating mode.  */

/* Parameter px_is_writer
 *
 * This run_time parameter indicates whether session is a qExec that is a
 * "writer" process.  Only writers are allowed to write changes to the database
 * and currently there is only one writer per group of segmates.
 *
 * It defaults to false, but may be specified as true using a connect option.
 * This should only ever be set by a QC connecting to a PX, rather than
 * directly.
 */
extern bool px_is_writer;

/* Parameter px_session_id
 *
 * This run time parameter indicates a unique id to identify a particular user
 * session throughout the entire Greenplum array.
 */
extern int	px_session_id;
#define InvalidPxSessionId	(-1)

struct TraceId
{
	union
    {
      struct
      {
        uint32 ip_hash;
        uint16 port;
        uint16 seq;
      };
      uint64_t uval;	  
    };
};

extern struct TraceId	sql_trace_id;

/* The Hostname where this segment's QC is located. This variable is NULL for the QC itself */
extern char *px_qc_hostname;

/* The Postmaster listener port for the QC.  This variable is 0 for the QC itself.*/
extern int	px_qc_port;

/*How many gangs to keep around from stmt to stmt.*/
extern int	px_cached_px_workers;

/*
 * Used to set the maximum length of the current query which is displayed
 * when the user queries pg_stat_activty table.
 */
extern int	pgstat_track_activity_query_size;

/* Parameter debug_print_slice_table
 *
 * This run-time parameter, if true, causes the slice table for a plan to
 * display on the server log.  On a QC this occurs prior to sending the
 * slice table to any PX.  On a PX it occurs just prior to execution of
 * the plan slice.
 */
extern bool debug_print_slice_table;

/*
 * px_command_count
 *
 * This GUC is 0 at the beginning of a client session and
 * increments for each command received from the client.
 */
extern int	px_command_count;

extern const char *role_to_string(PxRoleValue role);

/* GUC var - timeout specifier for
 * gang creation */
extern int	px_worker_connect_timeout; 

extern int	px_gang_creation_retry_count;	/* How many retries ? */
extern int	px_gang_creation_retry_timer;	/* How long between retries */

/*
 * Parameter px_interconnect_max_packet_size
 *
 * The run-time parameter px_interconnect_max_packet_size controls the largest packet
 * size that the interconnect will transmit.  In general, the interconnect
 * tries to squeeze as many tuplechunks as possible into a packet without going
 * above this size limit.
 *
 * The largest tuple chunk size that the system will form can be derived
 * from this with:
 *		MAX_CHUNK_SIZE = px_interconnect_max_packet_size - PACKET_HEADER_SIZE - TUPLE_CHUNK_HEADER_SIZE
 *
 *
 */
extern int	px_interconnect_max_packet_size; /* GUC var */

#define DEFAULT_PACKET_SIZE 8192
#define MIN_PACKET_SIZE 512
#define MAX_PACKET_SIZE 65507	/* Max payload for IPv4/UDP (subtract 20 more
								 * for IPv6 without extensions) */

/*
 * Support for multiple "types" of interconnect
 */
typedef enum PxVars_Interconnect_Type
{
	INTERCONNECT_TYPE_TCP = 0,
	INTERCONNECT_TYPE_UDPIFC,
} PxVars_Interconnect_Type;

extern int	px_interconnect_type;

typedef enum PxVars_Interconnect_Method
{
	INTERCONNECT_FC_METHOD_CAPACITY = 0,
	INTERCONNECT_FC_METHOD_LOSS = 2,
} PxVars_Interconnect_Method;

extern int	px_interconnect_fc_method;

/*
 * Parameter px_interconnect_queue_depth
 *
 * The run-time parameter px_interconnect_queue_depth controls the
 * number of outstanding messages allowed to accumulated on a
 * 'connection' before the peer will start to backoff.
 *
 * This guc is specific to the UDP-interconnect.
 *
 */
extern int	px_interconnect_queue_depth;

/*
 * Parameter px_interconnect_snd_queue_depth
 *
 * The run-time parameter px_interconnect_snd_queue_depth controls the
 * average number of outstanding messages on the sender side
 *
 * This guc is specific to the UDP-interconnect.
 *
 */
extern int	px_interconnect_snd_queue_depth;
extern int	px_interconnect_timer_period;
extern int	px_interconnect_timer_checking_period;
extern int	px_interconnect_default_rtt;
extern int	px_interconnect_min_rto;
extern int	px_interconnect_transmit_timeout;
extern int	px_interconnect_min_retries_before_timeout;
extern int	px_interconnect_debug_retry_interval;

/* UDP recv buf size in KB.  For testing */
extern int	px_interconnect_udp_bufsize_k;

/*
 * Parameter px_interconnect_aggressive_retry
 *
 * The run-time parameter px_interconnect_aggressive_retry controls the
 * activation of the application-level retry (which acts much faster than the OS-level
 * TCP retries); In most cases this should stay enabled.
 */
extern bool px_interconnect_aggressive_retry;	/* fast-track app-level retry */

/*
 * Parameter px_interconnect_full_crc
 *
 * Perform a full CRC on UDP-packets as they depart and arrive.
 */
extern bool px_interconnect_full_crc;

/*
 * Parameter px_interconnect_log_stats
 *
 * Emit interconnect statistics at log-level, instead of debug1
 */
extern bool px_interconnect_log_stats;

extern bool px_interconnect_cache_future_packets;

#define UNDEF_SEGMENT -2

/*
 * Parameter interconnect_setup_timeout
 *
 * The run-time parameter (GUC variable) interconnect_setup_timeout is used
 * during SetupInterconnect() to timeout the operation.  This value is in
 * seconds.  Setting it to zero effectively disables the timeout.
 */
extern int	interconnect_setup_timeout;

extern int	px_interconnect_tcp_listener_backlog;

extern bool px_use_global_function;

#ifdef USE_ASSERT_CHECKING
/*
 * UDP-IC Test hooks (for fault injection).
 */
extern int	px_interconnect_udpic_dropseg;	/* specifies which segments to apply the
								 * following two gucs -- if set to
								 * UNDEF_SEGMENT, all segments apply them. */
extern int	px_interconnect_udpic_dropxmit_percent;
extern int	px_interconnect_udpic_dropacks_percent;
extern int	px_interconnect_udpic_fault_inject_percent;
extern int	px_interconnect_udpic_fault_inject_bitmap;
#endif

/*
 * Each slice table has a unique ID (certain commands like "vacuum
 * analyze" run many many slice-tables for each px_command_id). This
 * gets passed around as part of the slice-table (not used as a guc!).
 */
extern uint32 px_interconnect_id;

/* --------------------------------------------------------------------------------------------------
 * Logging
 */

typedef enum PxVars_Verbosity
{
	PXVARS_VERBOSITY_UNDEFINED = 0,
	PXVARS_VERBOSITY_OFF,
	PXVARS_VERBOSITY_TERSE,
	PXVARS_VERBOSITY_VERBOSE,
	PXVARS_VERBOSITY_DEBUG,
} PxVars_Verbosity;

/* Enable single-mirror pair dispatch. */
extern bool px_enable_direct_dispatch;

/* Name of pseudo-function to access any table as if it was randomly distributed. */
#define POLAR_GLOBAL_FUNCTION "polar_global_function"

/*
 * px_log_gang
 *
 * Should creation, reallocation and cleanup of gangs of PX processes be logged?
 * "OFF"     -> only errors are logged
 * "TERSE"   -> terse logging of routine events, e.g. creation of new qExecs
 * "VERBOSE" -> gang allocation per command is logged
 * "DEBUG"   -> additional events are logged at severity level DEBUG1 to DEBUG5
 *
 * The messages that are enabled by the TERSE and VERBOSE settings are
 * written with a severity level of LOG.
 */
extern int	px_log_gang;

/*
 * px_interconnect_log
 *
 * Should connections between internal processes be logged?  (qDisp/qExec/etc)
 * "OFF"     -> connection errors are logged
 * "TERSE"   -> terse logging of routine events, e.g. successful connections
 * "VERBOSE" -> most interconnect setup events are logged
 * "DEBUG"   -> additional events are logged at severity level DEBUG1 to DEBUG5.
 *
 * The messages that are enabled by the TERSE and VERBOSE settings are
 * written with a severity level of LOG.
 */
extern int	px_interconnect_log ;

/*
 * use this var cache guc polar_enable_px
 * so that, it would change when doing one px
 */
extern bool	px_is_executing;
extern bool	px_is_planning;
extern bool	px_adaptive_paging;
extern bool px_adaptive_scan_setup;
extern bool px_prefetch_inner_executing;
extern bool	cached_px_enable_replay_wait;
/* Parallel DML */
extern int local_px_insert_dop_num;

#define SET_PX_EXECUTION_STATUS(status)								\
	px_is_executing = status;										\
	px_is_executing ?												\
		pg_atomic_test_set_flag(&MyProc->polar_px_is_executing) :	\
		pg_atomic_clear_flag(&MyProc->polar_px_is_executing);

/* --------------------------------------------------------------------------------------------------
 * Greenplum Optimizer GUCs
 */

/*
 * "px_motion_cost_per_row"
 *
 * If >0, the planner uses this value -- instead of 2 * "cpu_tuple_cost" --
 * for Motion operator cost estimation.
 */
extern double px_motion_cost_per_row;

/*
 * Enable/disable the special optimization of MIN/MAX aggregates as
 * Index Scan with limit.
 */
extern bool px_enable_minmax_optimization;

/*
 * "px_enable_multiphase_agg"
 *
 * Unlike some other enable... vars, px_enable_multiphase_agg is not cost based.
 * When set to false, the planner will not use multi-phase aggregation.
 */
extern bool px_enable_multiphase_agg;

/*
 * Damping factor for selecticity damping
 */
extern double px_selectivity_damping_factor;


/* ----- Experimental Features ----- */

/*
 * "px_enable_agg_distinct"
 *
 * May Greenplum redistribute on the argument of a lone aggregate distinct in
 * order to use 2-phase aggregation?
 *
 * The code does uses planner estimates to decide whether to use this feature,
 * when enabled.
 */
extern bool px_enable_agg_distinct;

/*
 * "px_enable_agg_distinct_pruning"
 *
 * May Greenplum use grouping in the first phases of 3-phase aggregation to
 * prune values from DISTINCT-qualified aggregate function arguments?
 *
 * The code uses planner estimates to decide whether to use this feature,
 * when enabled.  See, however, px_eager_dqa_pruning.
 */
extern bool px_enable_dqa_pruning;

/*
 * "px_eager_agg_distinct_pruning"
 *
 * Should Greenplum bias planner estimates so as to favor the use of grouping
 * in the first phases of 3-phase aggregation to prune values from DISTINCT-
 * qualified aggregate function arguments?
 *
 * Note that this has effect only when px_enable_dqa_pruning it true.  It
 * provided to facilitate testing and is not a tuning parameter.
 */
extern bool px_eager_dqa_pruning;

/*
 * "px_eager_one_phase_agg"
 *
 * Should Greenplum bias planner estimates so as to favor the use of one
 * phase aggregation?
 *
 * It is provided to facilitate testing and is not a tuning parameter.
 */
extern bool px_eager_one_phase_agg;

/*
 * "px_eager_two_phase_agg"
 *
 * Should Greenplum bias planner estimates so as to favor the use of two
 * phase aggregation?
 *
 * It is provided to facilitate testing and is not a tuning parameter.
 */
extern bool px_eager_two_phase_agg;

/* May Greenplum apply Unique operator (and possibly a Sort) in parallel prior
 * to the collocation motion for a Unique operator?  The idea is to reduce
 * the number of rows moving over the interconnect.
 *
 * The code uses planner estimates for this.  If enabled, the tactic is used
 * only if it is estimated to be cheaper that a 1-phase approach.  However,
 * see px_eqger_preunique.
 */
extern bool px_enable_preunique;

/* If px_enable_preunique is true, then  apply the associated optimzation
 * in an "eager" fashion.  In effect, this setting overrides the cost-
 * based decision whether to use a 2-phase approach to duplicate removal.
 */
extern bool px_eager_preunique;

/* Greenplum MK Sort */
extern bool px_enable_mk_sort;

#ifdef USE_ASSERT_CHECKING
extern bool px_mk_sort_check;
#endif

extern bool trace_sort;

/* Sharing of plan fragments for common table expressions */
extern bool px_cte_sharing;

/*  Max size of dispatched plans; 0 if no limit */
extern int	px_max_plan_size;

/* If we use two stage hashagg, we can stream the bottom half */
extern bool px_hashagg_streambottom;

/* Analyze tools */
extern int	px_motion_slice_noop;

extern Oid	px_workerid_funcid;
extern char *px_workerid_funcname;

/* gpmon alert level, control log alert level used by gpperfmon */
typedef enum
{
	PXPERFMON_LOG_ALERT_LEVEL_NONE,
	PXPERFMON_LOG_ALERT_LEVEL_WARNING,
	PXPERFMON_LOG_ALERT_LEVEL_ERROR,
	PXPERFMON_LOG_ALERT_LEVEL_FATAL,
	PXPERFMON_LOG_ALERT_LEVEL_PANIC
} PxperfmonLogAlertLevel;

extern int	px_workfile_caching_loglevel;

extern bool coredump_on_memerror;

/*
 * Autostats feature, whether or not to to automatically run ANALYZE after
 * insert/delete/update/ctas or after ctas/copy/insert in case the target
 * table has no statistics
 */
typedef enum
{
	PX_AUTOSTATS_NONE = 0,		/* Autostats is switched off */
	PX_AUTOSTATS_ON_CHANGE,		/* Autostats is enabled on change
								 * (insert/delete/update/ctas) */
	PX_AUTOSTATS_ON_NO_STATS,	/* Autostats is enabled on ctas or copy or
								 * insert if no stats are present */
} PxAutoStatsModeValue;

/* --------------------------------------------------------------------------------------------------
 * Server debugging
 */

/* --------------------------------------------------------------------------------------------------
 * Non-GUC globals
 */

#define UNSET_SLICE_ID -1
extern int	currentSliceId;

extern int	px_total_plans;

extern int	px_total_slices;
extern int	px_max_slices;
extern int	px_serialize_version;

typedef struct PxId
{
	int32		dbid;			/* the dbid of this database, unused now */
	int32		workerid;		/* content indicator: -1 for entry database,
								 * 0, ..., n-1 for segment database * a
								 * primary and its mirror have the same
								 * segIndex */
} PxId;

/* --------------------------------------------------------------------------------------------------
 * Global variable declaration for the data for the single row of px_id table
 */
extern PxId PxIdentity;

#define UNINITIALIZED_PX_IDENTITY_VALUE (-1)

/* Stores the listener port that this process uses to listen for incoming
 * Interconnect connections from other Motion nodes.
 */
extern uint32 px_listener_port;

#define IS_PX_SETUP_DONE() (px_listener_port > 0)
#define IS_QUERY_DISPATCHER() (PxIdentity.workerid == MASTER_CONTENT_ID)

#define IS_PX_NEED_CANCELED() \
	(InterruptPending && (QueryCancelPending || ProcDiePending))

#define IS_PX_ADPS_SETUP_DONE() (px_adaptive_scan_setup == true)

/*
 * Thread-safe routine to write to the log
 */
extern void write_log(const char *fmt,...) pg_attribute_printf(1, 2);

extern int32 get_px_workerid(void);
extern uint32 get_px_workerid_funcid(void);
extern void update_px_workerid_funcid(void);

extern bool polar_is_stmt_enable_px(void);
extern int polar_get_stmt_px_dop(void);

#endif							/* PXVARS_H */
