// Copyright (c) 2017 The DSMEngine Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_DSMEngine_INCLUDE_EXPORT_H_
#define STORAGE_DSMEngine_INCLUDE_EXPORT_H_

#if !defined(DSMEngine_EXPORT)

#if defined(DSMEngine_SHARED_LIBRARY)
#if defined(_WIN32)

#if defined(DSMEngine_COMPILE_LIBRARY)
#define DSMEngine_EXPORT __declspec(dllexport)
#else
#define DSMEngine_EXPORT __declspec(dllimport)
#endif  // defined(DSMEngine_COMPILE_LIBRARY)

#else  // defined(_WIN32)
#if defined(DSMEngine_COMPILE_LIBRARY)
#define DSMEngine_EXPORT __attribute__((visibility("default")))
#else
#define DSMEngine_EXPORT
#endif
#endif  // defined(_WIN32)

#else  // defined(DSMEngine_SHARED_LIBRARY)
#define DSMEngine_EXPORT
#endif

#endif  // !defined(DSMEngine_EXPORT)

#endif  // STORAGE_DSMEngine_INCLUDE_EXPORT_H_
