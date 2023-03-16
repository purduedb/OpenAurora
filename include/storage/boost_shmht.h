//
// Created by rainman on 2023/3/8.
//

#ifndef DB2_PG_BOOST_SHMHT_H
#define DB2_PG_BOOST_SHMHT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "storage/rel_cache.h"

#include "storage/boost_shmht.h"



extern void UnorderedHashMapInsert(char* key, uint32_t value);

extern bool UnorderedHashMapGet(char* key, uint32_t *result);

extern bool UnorderedHashMapCreate();

extern bool UnorderedHashMapAttach();

#ifdef __cplusplus
}
#endif

#endif //DB2_PG_BOOST_SHMHT_H
