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
#include "core/span.h"
#include "receiver/receiver.h"
#include "sampler/sampler.h"
#include "util/cmp.h"
#include "util/log.h"
#include "util/rand.h"
#include "util/string.h"
#include "util/time.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * @file span.c
 *
 * Implementation of HTrace spans.
 */

struct htrace_span *htrace_span_alloc(const char *desc,
                uint64_t begin_ms, struct htrace_span_id *span_id)
{
    struct htrace_span *span;

    span = malloc(sizeof(*span));
    if (!span) {
        return NULL;
    }
    span->desc = strdup(desc);
    if (!span->desc) {
        free(span);
        return NULL;
    }
    span->begin_ms = begin_ms;
    span->end_ms = 0;
    htrace_span_id_copy(&span->span_id, span_id);
    span->trid = NULL;
    span->num_parents = 0;
    htrace_span_id_clear(&span->parent.single);
    span->parent.list = NULL;
    return span;
}

void htrace_span_free(struct htrace_span *span)
{
    if (!span) {
        return;
    }
    free(span->desc);
    free(span->trid);
    if (span->num_parents > 1) {
        free(span->parent.list);
    }
    free(span);
}

typedef int (*qsort_fn_t)(const void *, const void *);

void htrace_span_sort_and_dedupe_parents(struct htrace_span *span)
{
    int i, j, num_parents = span->num_parents;
    struct htrace_span_id prev;

    if (num_parents <= 1) {
        return;
    }
    qsort(span->parent.list, num_parents, sizeof(struct htrace_span_id),
          (qsort_fn_t)htrace_span_id_compare);
    prev = span->parent.list[0];
    htrace_span_id_copy(&prev, &span->parent.list[0]);
    j = 1;
    for (i = 1; i < num_parents; i++) {
        if (htrace_span_id_compare(&prev, span->parent.list + i) != 0) {
            htrace_span_id_copy(&prev, span->parent.list + i);
            htrace_span_id_copy(span->parent.list + j, span->parent.list + i);
            j++;
        }
    }
    span->num_parents = j;
    if (j == 1) {
        // After deduplication, there is now only one entry.  Switch to the
        // optimized no-malloc representation for 1 entry.
        free(span->parent.list);
        span->parent.single = prev;
    } else if (j != num_parents) {
        // After deduplication, there are now fewer entries.  Use realloc to
        // shrink the size of our dynamic allocation if possible.
        struct htrace_span_id *nlist =
            realloc(span->parent.list, sizeof(struct htrace_span_id) * j);
        if (nlist) {
            span->parent.list = nlist;
        }
    }
}

/**
 * Translate the span to a JSON string.
 *
 * This function can be called in two ways.  With buf == NULL, we will determine
 * the size of the buffer that would be required to hold a JSON string
 * containing the span contents.  With buf non-NULL, we will write the span
 * contents to the provided buffer.
 *
 * @param span              The span
 * @param max               The maximum number of bytes to write to buf.
 * @param buf               If non-NULL, where the string will be written.
 *
 * @return                  The number of bytes that the span json would take
 *                          up if it were written out.
 */
static int span_json_sprintf_impl(const struct htrace_span *span,
                                  int max, char *buf)
{
    int num_parents, i, ret = 0;
    const char *prefix = "";
    char sbuf[HTRACE_SPAN_ID_STRING_LENGTH + 1];

    // Note that we have validated the description and process ID strings to
    // make sure they don't contain anything evil.  So we don't need to escape
    // them here.

    htrace_span_id_to_str(&span->span_id, sbuf, sizeof(sbuf));
    ret += fwdprintf(&buf, &max, "{\"a\":\"%s\",\"b\":%" PRId64
                 ",\"e\":%" PRId64",", sbuf, span->begin_ms,
                 span->end_ms);
    if (span->desc[0]) {
        ret += fwdprintf(&buf, &max, "\"d\":\"%s\",", span->desc);
    }
    if (span->trid) {
        ret += fwdprintf(&buf, &max, "\"r\":\"%s\",", span->trid);
    }
    num_parents = span->num_parents;
    if (num_parents == 0) {
        ret += fwdprintf(&buf, &max, "\"p\":[]");
    } else if (num_parents == 1) {
        htrace_span_id_to_str(&span->parent.single, sbuf, sizeof(sbuf));
        ret += fwdprintf(&buf, &max, "\"p\":[\"%s\"]", sbuf);
    } else if (num_parents > 1) {
        ret += fwdprintf(&buf, &max, "\"p\":[");
        for (i = 0; i < num_parents; i++) {
            htrace_span_id_to_str(span->parent.list + i, sbuf, sizeof(sbuf));
            ret += fwdprintf(&buf, &max, "%s\"%s\"", prefix, sbuf);
            prefix = ",";
        }
        ret += fwdprintf(&buf, &max, "]");
    }
    ret += fwdprintf(&buf, &max, "}");
    // Add one to 'ret' to take into account the terminating null that we
    // need to write.
    return ret + 1;
}

int span_json_size(const struct htrace_span *span)
{
    return span_json_sprintf_impl(span, 0, NULL);
}

void span_json_sprintf(const struct htrace_span *span, int max, void *buf)
{
    span_json_sprintf_impl(span, max, buf);
}

int span_write_msgpack(const struct htrace_span *span, cmp_ctx_t *ctx)
{
    int i, num_parents;
    uint16_t map_size =
        1 + // desc
        1 + // begin_ms
        1 + // end_ms
        1; // span_id

    num_parents = span->num_parents;
    if (span->trid) {
        map_size++;
    }
    if (num_parents > 0) {
        map_size++;
    }
    if (!cmp_write_map16(ctx, map_size)) {
        return 0;
    }
    if (!cmp_write_fixstr(ctx, "a", 1)) {
        return 0;
    }
    if (!htrace_span_id_write_msgpack(&span->span_id, ctx)) {
        return 0;
    }
    if (!cmp_write_fixstr(ctx, "d", 1)) {
        return 0;
    }
    if (!cmp_write_str16(ctx, span->desc, strlen(span->desc))) {
        return 0;
    }
    if (!cmp_write_fixstr(ctx, "b", 1)) {
        return 0;
    }
    if (!cmp_write_u64(ctx, span->begin_ms)) {
        return 0;
    }
    if (!cmp_write_fixstr(ctx, "e", 1)) {
        return 0;
    }
    if (!cmp_write_u64(ctx, span->end_ms)) {
        return 0;
    }
    if (span->trid) {
        if (!cmp_write_fixstr(ctx, "r", 1)) {
            return 0;
        }
        if (!cmp_write_str16(ctx, span->trid, strlen(span->trid))) {
            return 0;
        }
    }
    if (num_parents > 0) {
        if (!cmp_write_fixstr(ctx, "p", 1)) {
            return 0;
        }
        if (!cmp_write_array16(ctx, num_parents)) {
            return 0;
        }
        if (num_parents == 1) {
            if (!htrace_span_id_write_msgpack(&span->parent.single, ctx)) {
                return 0;
            }
        } else {
            for (i = 0; i < num_parents; i++) {
                if (!htrace_span_id_write_msgpack(span->parent.list + i, ctx)) {
                    return 0;
                }
            }
        }
    }
    return 1;
}

// vim:ts=4:sw=4:et
