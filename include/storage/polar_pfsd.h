/*-------------------------------------------------------------------------
 *
 * polar_pfsd.h
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
 *    src/include/storage/polar_pfsd.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_PFSD_H
#define POLAR_PFSD_H


#define PFSD_MIN_MAX_IOSIZE         (4 * 1024)
#define PFSD_DEFAULT_MAX_IOSIZE (4 * 1024 * 1024)
#define PFSD_MAX_MAX_IOSIZE         (128 * 1024 * 1024)

extern  int max_pfsd_io_size;

extern ssize_t polar_pfsd_read(int fd, void *buf, size_t len);
extern ssize_t polar_pfsd_pread(int fd, void *buf, size_t len, off_t offset);
extern ssize_t polar_pfsd_write(int fd, const void *buf, size_t len);
extern ssize_t polar_pfsd_pwrite(int fd, const void *buf, size_t len, off_t offset);

#endif                          /* POLAR_PFSD_H */
