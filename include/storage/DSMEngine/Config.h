#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "storage/DSMEngine/Common.h"

class CacheConfig {
public:
  uint32_t cacheSize;

  CacheConfig(uint32_t cacheSize = 1) : cacheSize(cacheSize) {}
};

class DSMConfig {
public:
  CacheConfig cacheConfig;
  uint32_t MemoryNodeNum;
  uint32_t ComputeNodeNum;
  uint64_t dsmSize; // G

  DSMConfig(const CacheConfig &cacheConfig = CacheConfig(),
            uint32_t machineNR = 2, uint64_t dsmSize = 120)// THe rdma_mg memory size initialize here.
      : cacheConfig(cacheConfig), MemoryNodeNum(machineNR), dsmSize(dsmSize) {}
};

#endif /* __CONFIG_H__ */
