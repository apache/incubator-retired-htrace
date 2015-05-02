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

#include "util/cmp_util.h"

#include <stdint.h>
#include <string.h>

/**
 * @file cmp_util.c
 *
 * Utilities for using the CMP library.
 */

static size_t cmp_counter_write_fn(struct cmp_ctx_s *c, const void *data,
                                   size_t count)
{
    struct cmp_counter_ctx *ctx = (struct cmp_counter_ctx *)c;
    ctx->count += count;
    return count;
}

void cmp_counter_ctx_init(struct cmp_counter_ctx *ctx)
{
    cmp_init(&ctx->base, NULL, NULL, cmp_counter_write_fn);
    ctx->count = 0;
}

static size_t cmp_bcopy_write_fn(struct cmp_ctx_s *c, const void *data,
                                 size_t count)
{
    struct cmp_bcopy_ctx *ctx = (struct cmp_bcopy_ctx *)c;
    uint64_t rem, o = ctx->off;

    rem = ctx->len - o;
    if (rem < count) {
        count = rem;
    }
    memcpy(((uint8_t*)ctx->base.buf) + o, data, count);
    ctx->off = o + count;
    return count;
}

size_t cmp_bcopy_write_nocheck_fn(struct cmp_ctx_s *c, const void *data,
                                  size_t count)
{
    struct cmp_bcopy_ctx *ctx = (struct cmp_bcopy_ctx *)c;
    uint64_t o = ctx->off;
    memcpy(((uint8_t*)ctx->base.buf) + o, data, count);
    ctx->off = o + count;
    return count;
}

static int cmp_bcopy_reader(struct cmp_ctx_s *c, void *data, size_t limit)
{
    struct cmp_bcopy_ctx *ctx = (struct cmp_bcopy_ctx *)c;
    size_t count, o = ctx->off;

    count = ctx->len - o;
    if (count > limit) {
        count = limit;
    }
    memcpy(data, ((uint8_t*)ctx->base.buf) + o, count);
    ctx->off = o + count;
    return count;
}

void cmp_bcopy_ctx_init(struct cmp_bcopy_ctx *ctx, void *buf, uint64_t len)
{
    cmp_init(&ctx->base, buf, cmp_bcopy_reader, cmp_bcopy_write_fn);
    ctx->off = 0;
    ctx->len = len;
}

// vim:ts=4:sw=4:et
