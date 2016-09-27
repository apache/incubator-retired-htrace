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

#include "core/htrace.h"
#include "core/span_id.h"
#include "util/cmp.h"
#include "util/log.h"
#include "util/rand.h"
#include "util/string.h"
#include "util/time.h"

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * @file span_id.c
 *
 * Implementation of HTrace span IDs.
 *
 * Span IDs are 128 bits in total.  The upper 64 bits of a span ID is the same
 * as the upper 64 bits of the first parent span, if there is one.  The lower 64
 * bits are always random.
 */

const struct htrace_span_id INVALID_SPAN_ID;

static uint64_t parse_hex_range(const char *str, int start, int end,
                                char *err, size_t err_len)
{
    char *endptr = NULL;
    uint64_t ret;
    char substr[HTRACE_SPAN_ID_STRING_LENGTH + 1];

    err[0] = '\0';
    if (end - start >= HTRACE_SPAN_ID_STRING_LENGTH) {
        snprintf(err, err_len, "parse_hex_range buffer too short.");
        return 0;
    }
    memcpy(substr, str + start, end - start);
    substr[end - start] = '\0';
    errno = 0;
    ret = strtoull(substr, &endptr, 16);
    if (errno) {
        int e = errno;
        snprintf(err, err_len, "parse_hex_range error: %s", terror(e));
        return 0;
    }
    while (1) {
        char c = *endptr;
        if (c == '\0') {
            break;
        }
        if ((c != ' ') || (c != '\t')) {
            snprintf(err, err_len, "parse_hex_range error: garbage at end "
                     "of string.");
            return 0;
        }
        endptr++;
    }
    return ret;
}

void htrace_span_id_parse(struct htrace_span_id *id, const char *str,
                   char *err, size_t err_len)
{
    size_t len;

    err[0] = '\0';
    len = strlen(str);
    if (len < HTRACE_SPAN_ID_STRING_LENGTH) {
        snprintf(err, err_len, "too short: must be %d characters.",
                 HTRACE_SPAN_ID_STRING_LENGTH);
        return;
    }
    id->high = (parse_hex_range(str, 0, 8, err, err_len) << 32);
    if (err[0]) {
        return;
    }
    id->high |= (parse_hex_range(str, 8, 16, err, err_len));
    if (err[0]) {
        return;
    }
    id->low = (parse_hex_range(str, 16, 24, err, err_len) << 32);
    if (err[0]) {
        return;
    }
    id->low |= (parse_hex_range(str, 24, 32, err, err_len));
    if (err[0]) {
        return;
    }
}

int htrace_span_id_to_str(const struct htrace_span_id *id,
                          char *str, size_t len)
{
    int res = snprintf(str, len,
        "%08"PRIx32"%08"PRIx32"%08"PRIx32"%08"PRIx32,
        (uint32_t)(0xffffffffUL & (id->high >> 32)),
        (uint32_t)(0xffffffffUL & id->high),
        (uint32_t)(0xffffffffUL & (id->low >> 32)),
        (uint32_t)(0xffffffffUL & id->low));
    return (res == HTRACE_SPAN_ID_STRING_LENGTH);
}

void htrace_span_id_copy(struct htrace_span_id *dst,
                         const struct htrace_span_id *src)
{
    memmove(dst, src, sizeof(*dst));
}

int htrace_span_id_write_msgpack(const struct htrace_span_id *id,
                                 struct cmp_ctx_s *ctx)
{
    uint8_t buf[HTRACE_SPAN_ID_NUM_BYTES];
    buf[0] = (id->high >> 56) & 0xff;
    buf[1] = (id->high >> 48) & 0xff;
    buf[2] = (id->high >> 40) & 0xff;
    buf[3] = (id->high >> 32) & 0xff;
    buf[4] = (id->high >> 24) & 0xff;
    buf[5] = (id->high >> 16) & 0xff;
    buf[6] = (id->high >> 8) & 0xff;
    buf[7] = (id->high >> 0) & 0xff;
    buf[8] = (id->low >> 56) & 0xff;
    buf[9] = (id->low >> 48) & 0xff;
    buf[10] = (id->low >> 40) & 0xff;
    buf[11] = (id->low >> 32) & 0xff;
    buf[12] = (id->low >> 24) & 0xff;
    buf[13] = (id->low >> 16) & 0xff;
    buf[14] = (id->low >> 8) & 0xff;
    buf[15] = (id->low >> 0) & 0xff;
    return cmp_write_bin(ctx, buf, HTRACE_SPAN_ID_NUM_BYTES);
}

int htrace_span_id_read_msgpack(struct htrace_span_id *id,
                                struct cmp_ctx_s *ctx)
{
    uint8_t buf[HTRACE_SPAN_ID_NUM_BYTES];
    uint32_t size = HTRACE_SPAN_ID_NUM_BYTES;

    if (!cmp_read_bin(ctx, buf, &size)) {
        return 0;
    }
    if (size != HTRACE_SPAN_ID_NUM_BYTES) {
        return 0;
    }
    id->high =
        (((uint64_t)buf[0]) << 56) |
        (((uint64_t)buf[1]) << 48) |
        (((uint64_t)buf[2]) << 40) |
        (((uint64_t)buf[3]) << 32) |
        (((uint64_t)buf[4]) << 24) |
        (((uint64_t)buf[5]) << 16) |
        (((uint64_t)buf[6]) << 8) |
        (((uint64_t)buf[7]) << 0);
    id->low =
        (((uint64_t)buf[8]) << 56) |
        (((uint64_t)buf[9]) << 48) |
        (((uint64_t)buf[10]) << 40) |
        (((uint64_t)buf[11]) << 32) |
        (((uint64_t)buf[12]) << 24) |
        (((uint64_t)buf[13]) << 16) |
        (((uint64_t)buf[14]) << 8) |
        (((uint64_t)buf[15]) << 0);
    return 1;
}

void htrace_span_id_generate(struct htrace_span_id *id, struct random_src *rnd,
                             const struct htrace_span_id *parent)
{
    if (parent) {
        id->high = parent->high;
    } else {
        do {
            id->high = random_u64(rnd);
        } while (id->high == 0);
    }
    do {
        id->low = random_u64(rnd);
    } while (id->low == 0);
}

void htrace_span_id_clear(struct htrace_span_id *id)
{
    memset(id, 0, sizeof(*id));
}

int htrace_span_id_compare(const struct htrace_span_id *a,
                           const struct htrace_span_id *b)
{
    if (a->high < b->high) {
        return -1;
    } else if (a->high > b->high) {
        return 1;
    } else if (a->low < b->low) {
        return -1;
    } else if (a->low > b->low) {
        return 1;
    } else {
        return 0;
    }
}

// vim:ts=4:sw=4:et
