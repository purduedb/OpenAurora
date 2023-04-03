//
// Created by rainman on 2023/3/29.
//

#ifndef DB2_PG_WAKEUP_LATCH_H
#define DB2_PG_WAKEUP_LATCH_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
extern void WaitForNewXLog(uint64_t currentLsn, uint32_t wait_usec);

extern void WakeupStartupRecovery();

extern void WakeupStartupResourceDestroy();

#ifdef __cplusplus
}
#endif

#endif //DB2_PG_WAKEUP_LATCH_H
