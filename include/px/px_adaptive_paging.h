/*-------------------------------------------------------------------------
 *
 * px_adaptive_paging.h
 *	  Dynamic paging between PX and QC.
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
 *	  src/include/px/px_adaptive_paging.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PX_adaptive_paging_H
#define PX_adaptive_paging_H

#include "px/px_vars.h"
#include "storage/block.h"

/******* define ********/
#define MSG_ADAPTIVE_PAGING "seqscan_paging"
#define PACKET_TYPE_PAGING 'P'

/******* struct ********/
typedef struct PxWorkerDescriptor PxWorkerDescriptor;
/* struct for dynamic paging request */
typedef struct _SeqscanPageRequest {
	Oid          table_oid;
	int          task_id;
	int			 direction;  /* 1 or -1 */
	BlockNumber	 page_count; /* page count in relation */
	BlockNumber	 scan_start; /* default : 0 */
	BlockNumber	 scan_count; /* default : -1 */
	BlockNumber	 current_page;
	int			 worker_id;
	int64_t		 scan_round; /* can scan max_int64 round */
} SeqscanPageRequest;

/* struct for dynamic paging response */
typedef struct _SeqscanPageResponse {
	int			 success;     /* 1: success, 0: failed */
	BlockNumber	 page_start;  /* page to start scan*/
	BlockNumber	 page_end;    /* page to finish scan*/
} SeqscanPageResponse;

SeqscanPageResponse
px_adps_request_scan_unit(SeqscanPageRequest* req);

bool px_adps_check_valid(void);
void px_adps_array_free(void);

void px_adps_analyze_result(int scan_id, char**);

SeqscanPageResponse
px_adps_get_response_block(SeqscanPageRequest *seqReq, int node_idx, int node_count);
/* end of various paging */
#endif
