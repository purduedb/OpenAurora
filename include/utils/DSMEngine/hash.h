// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Simple hash function used for internal data structures

#ifndef STORAGE_DSMEngine_UTIL_HASH_H_
#define STORAGE_DSMEngine_UTIL_HASH_H_

#include <cstddef>
#include <cstdint>

namespace DSMEngine {

uint32_t Hash(const char* data, size_t n, uint32_t seed);

}  // namespace DSMEngine

#endif  // STORAGE_DSMEngine_UTIL_HASH_H_
