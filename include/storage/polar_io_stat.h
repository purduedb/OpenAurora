/*
 * polar_io_stat.h
 *
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *      src/include/storage/polar_io_stat.h
 */
#ifndef POLAR_IO_STAT_H
#define POLAR_IO_STAT_H

#include "postgres.h"
#include "portability/instr_time.h"
/*
 * this is a struct that stat ploar processes about io opreation.
 */
typedef struct PolarProcIOStat
{
	uint64			io_number_read;  				/* Total count of read the file */
	uint64			io_number_write;				/* Total count of write the file */
	uint64 			io_throughtput_read;			/* Total Bytes of read */
	uint64 			io_throughtput_write;			/* Total Bytes of write */
	instr_time		io_latency_read;				/* Total time cost of read the file */
	instr_time		io_latency_write;				/* Total time cost of read the file */
	uint64			io_open_num;					/* Total number of open the file */
	uint64 			io_close_num;					/* Total number of close the file */
	instr_time		io_open_time;					/* Total time cost of open file, but we don't use this */
	instr_time		io_seek_time;
	uint64			io_seek_count;
	instr_time		io_creat_time;
	uint64			io_creat_count;
	instr_time		io_fsync_time;
	uint64			io_fsync_count;
	instr_time		io_falloc_time;
	uint64			io_falloc_count;

}PolarProcIOStat;

enum IOLatencyInterval{
	LATENCY_200 = 0,
	LATENCY_400 = 1,
	LATENCY_600 = 2,
	LATENCY_800 = 3,
	LATENCY_1ms = 4,
	LATENCY_10ms = 5,
	LATENCY_100ms = 6,
	LATENCY_OUT = 7,

	LATENCY_INTERVAL_LEN = 8
};
enum IOLatencyKind{
	LATENCY_read = 0,
	LATENCY_write = 1,
	LATENCY_open = 2,
	LATENCY_seek = 3,
	LATENCY_creat = 4,
	LATENCY_fsync = 5,
	LATENCY_falloc = 6,

	LATENCY_KIND_LEN = 7
};


/*
 * ploar IO type 
 * At present, we mainly count several secondary directories, 
 * and the demand is like this.
 */
enum PLOARIOType{
	POLARIO_WAL = 0, 
	POLARIO_DATA = 1,
	POLARIO_CLOG = 2,
	POLARIO_GLOBAL = 3,
	POLARIO_LOGINDEX = 4,
	POLARIO_MULTIXACT = 5,
	POLARIO_TWOPHASE = 6,
	POLARIO_REPLSOT = 7,
	POLARIO_SNAPSHOTS = 8,
	POLARIO_SUBTRANS = 9,
	POLARIO_OTHER = 10,
	/* NB: Define the size here for future maintenance */
	POLARIO_TYPE_SIZE = 11
};

/*
 * ploar IO location 
 */
enum PLOARIOLoc{
	POLARIO_LOCAL = 0,
	POLARIO_SHARED = 1,
	/* NB: Define the size here for future maintenance */
	POLARIO_LOC_SIZE = 2
};

typedef struct POLAR_PROC_IO
{
    /* 
	 * We do statistics IO for every process
	 * We do three dimensions of IO statistics 
	 * so that we can make a more detailed and specific analysis.
	 * there are follows:
	 * processes ,file type(data,wal,clog),file location(local, polar file system)
	 * NB : we initialized this struct arrarys when create global memory， 
	 *		and memset this in child processes Init (ProcessInit or AuxiliaryInit) again.
	 * 		so ,we will save these value to ProcGobal when child processes is destroyed. 
	 * 		we can make an accurate summary convenient.
	 */
    int             pid;
	PolarProcIOStat polar_proc_io_stat_dist[POLARIO_TYPE_SIZE][POLARIO_LOC_SIZE]; 
	uint64	num_latency_dist[LATENCY_KIND_LEN][LATENCY_INTERVAL_LEN];
}POLAR_PROC_IO;

extern POLAR_PROC_IO *PolarIOStatArray;

/*
 * POLAR: use this struct to collect io read throughtput using for crash recovery
 * rto optimizer.
 */
#define POLAR_PROC_GLOBAL_IO_READ_LEN 10
#define POLAR_IO_READ_STATISTICS_TIMES 1000

typedef struct POLAR_PROC_GLOBAL_IO_READ
{
	int 		count; /* How many times for statistics collecting */
	uint64 		delta_io_read_size; /* The delta io read size in a statistics period */
	instr_time	delta_io_read_time; /* The delta io read time in a statistics period */
	uint64		io_read_size_avg; /* The average of io read size in a statistics period */
	double		io_read_time_avg; /* The average of io read time in a statistics period */
								  /* The top POLAR_PROC_GLOBAL_IO_READ_LEN of io read throughtput in history */
	uint64		io_read_throughtput[POLAR_PROC_GLOBAL_IO_READ_LEN];
	uint64		max_throughtput; /* Max throughtput in io_read_throughtput */
	bool		enabled; /* flags to judge if it is enabled */
	uint64 		force_delay; /* statistics infomation for polar_monitor */
	uint64 		less_than_delay; /* statistics infomation for polar_monitor */
	uint64 		more_than_delay; /* statistics infomation for polar_monitor */
	pg_atomic_uint32 lock; /* flags to judge if it is in the statistics caculate */
} POLAR_PROC_GLOBAL_IO_READ;

typedef struct PolarCollectIoStat
{
	int64	shared_read_ps;
	int64	shared_write_ps;
	int64	shared_read_throughput;
	int64	shared_write_throughput;
	double	shared_read_latency;
	double 	shared_write_latency;
	int64	io_open_num;
	int64	io_seek_count;
	double	io_open_time;
	double	io_seek_time;
} PolarCollectIoStat;

extern POLAR_PROC_GLOBAL_IO_READ *PolarGlobalIOReadStats;

extern void polar_io_stat_shmem_startup(void);
extern int	polar_get_io_proc_index(void);
extern void polar_io_shmem_exit_cleanup(void);
extern int polario_kind_to_location(int kind);
extern int polar_collect_io_stat(PolarCollectIoStat *io_stat_info, int backendid);

#define PolarNumProcIOStatSlots (MaxBackends + NUM_AUXPROCTYPES + 1)

#endif							/* POLAR_IO_STAT_H */
