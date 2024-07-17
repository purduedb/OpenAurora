// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "utils/DSMEngine/hash.h"

#include <cstring>

#include "utils/DSMEngine/coding.h"
#include "access/logindex_hashmap.h"

// The FALLTHROUGH_INTENDED macro can be used to annotate implicit fall-through
// between switch labels. The real definition should be provided externally.
// This one is a fallback version for unsupported compilers.
#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED \
  do {                       \
  } while (0)
#endif

namespace DSMEngine {

uint32_t Hash(const char* data, size_t n, uint32_t seed) {
  // MurmurHash1 - fast but mediocre quality
  // https://github.com/aappleby/smhasher/wiki/MurmurHash1
  //
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char* limit = data + n;
  uint32_t h = static_cast<uint32_t>(seed ^ (n * m));

  // Pick up four bytes at a time
  while (data + 4 <= limit) {
    uint32_t w = DecodeFixed32(data);
    data += 4;
    h += w;
    h *= m;
    h ^= (h >> 16);
  }

  // Pick up remaining bytes
  switch (limit - data) {
    // Note: The original hash implementation used data[i] << shift, which
    // promotes the char to int and then performs the shift. If the char is
    // negative, the shift is undefined behavior in C++. The hash algorithm is
    // part of the format definition, so we cannot change it; to obtain the same
    // behavior in a legal way we just cast to uint32_t, which will do
    // sign-extension. To guarantee compatibility with architectures where chars
    // are unsigned we first cast the char to int8_t.
    case 3:
      h += static_cast<uint32_t>(static_cast<int8_t>(data[2])) << 16;
      FALLTHROUGH_INTENDED;
    case 2:
      h += static_cast<uint32_t>(static_cast<int8_t>(data[1])) << 8;
      FALLTHROUGH_INTENDED;
    case 1:
      h += static_cast<uint32_t>(static_cast<int8_t>(data[0]));
      h *= m;
      h ^= (h >> r);
      break;
  }
  return h;
}

uint32_t Hash(const KeyType* key, uint32_t seed) {
  const uint32_t m = 0xc6a4a793;
  uint32_t h = static_cast<uint32_t>(seed ^ (9 * m));
  uint32_t data[9] = {key->SpcID >> 32, key->SpcID, key->DbID >> 32, key->DbID, key->RelID >> 32, key->RelID, key->ForkNum, key->BlkNum >> 32, key->BlkNum};
  uint32_t* limit = data + 9;
  for(uint32_t* p = data; p < limit; p++) {
    uint32_t w = DecodeFixed32((char*)p);
    h += w;
    h *= m;
    h ^= (h >> 16);
  }
  return h;
}

}  // namespace DSMEngine
