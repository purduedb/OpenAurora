//
// Created by pang65 on 1/15/23.
//

#ifndef DB2_PG_BACKGROUND_HASHMAP_VACUUMER_H
#define DB2_PG_BACKGROUND_HASHMAP_VACUUMER_H

#include "access/logindex_hashmap.h"

#ifdef __cplusplus
extern "C" {
#endif

extern bool BackgroundHashMapCleanRocksdb(HashMap hashMap);

#ifdef __cplusplus
}
#endif

#endif //DB2_PG_BACKGROUND_HASHMAP_VACUUMER_H
