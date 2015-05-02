/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef APACHE_HTRACE_UTIL_CMP_UTIL_H
#define APACHE_HTRACE_UTIL_CMP_UTIL_H

/**
 * @file cmp_util.h
 *
 * Utilities for using the CMP library.
 *
 * This is an internal header, not intended for external use.
 */

#include "util/cmp.h"

#include <stdint.h> /* for uint64_t */

/**
 * CMP context for counting the number of bytes in the serialized form.
 */
struct cmp_counter_ctx {
    cmp_ctx_t base;
    uint64_t count;
};

/**
 * Initialize a CMP counter ctx.
 * This doesn't allocate any memory.
 *
 * @param ctx           The context to initialize.
 */
void cmp_counter_ctx_init(struct cmp_counter_ctx *ctx);

/**
 * CMP context for counting the number of bytes in the serialized form.
 */
struct cmp_bcopy_ctx {
    cmp_ctx_t base;
    uint64_t off;
    uint64_t len;
};

/**
 * Initialize a CMP writer ctx.
 * This doesn't allocate any memory.
 *
 * @param ctx           The context to initialize.
 */
void cmp_bcopy_ctx_init(struct cmp_bcopy_ctx *ctx, void *buf, uint64_t len);

/**
 * A version of the bcopy write function that does not perform bounds checking.
 * Useful if the serialized size has already been determined.
 */
size_t cmp_bcopy_write_nocheck_fn(struct cmp_ctx_s *c, const void *data,
                                  size_t count);

#endif

// vim: ts=4:sw=4:tw=79:et
