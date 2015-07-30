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

#include "core/span.h"
#include "test/span_table.h"
#include "test/span_util.h"
#include "test/test.h"
#include "util/htable.h"
#include "util/log.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct span_table *span_table_alloc(void)
{
    struct htable *ht;

    ht = htable_alloc(128, ht_hash_string, ht_compare_string);
    if (!ht) {
        return NULL;
    }
    return (struct span_table*)ht;
}

int span_table_get(struct span_table *st, struct htrace_span **out,
                   const char *desc, const char *trid)
{
    struct htable *ht = (struct htable *)st;
    struct htrace_span *span;

    span = htable_get(ht, desc);
    EXPECT_NONNULL(span);
    EXPECT_STR_EQ(desc, span->desc);
    EXPECT_UINT64_GE(span->begin_ms, span->end_ms);
    EXPECT_TRUE(0 !=
        htrace_span_id_compare(&INVALID_SPAN_ID, &span->span_id));
    EXPECT_NONNULL(span->trid);
    EXPECT_STR_EQ(trid, span->trid);
    *out = span;
    return EXIT_SUCCESS;
}

int span_table_put(struct span_table *st, struct htrace_span *span)
{
    struct htable *ht = (struct htable *)st;
    int res;

    res = htable_put(ht, span->desc, span);
    if (res) {
        htrace_span_free(span);
        return res;
    }
    return 0;
}

static void span_table_free_entry(void *tracer, void *key, void *val)
{
    struct htrace_span *span = val;
    htrace_span_free(span);
    // We don't need to free the key, since it's simply the span->desc string,
    // which is already freed by htrace_span_free.
}

void span_table_free(struct span_table *st)
{
    struct htable *ht = (struct htable *)st;

    // Free all entries
    htable_visit(ht, span_table_free_entry, NULL);
    // Free the table itself
    htable_free(ht);
}

uint32_t span_table_size(struct span_table *st)
{
    struct htable *ht = (struct htable *)st;
    return htable_used(ht);
}

int load_trace_span_file(const char *path, struct span_table *st)
{
    char line[8196], err[1024];
    size_t err_len = sizeof(err);
    FILE *fp = NULL;
    int lineno = 0, ret = EXIT_FAILURE;
    struct htrace_span *span;

    fp = fopen(path, "r");
    if (!fp) {
        int res = errno;
        fprintf(stderr, "failed to open %s: %s\n", path, terror(res));
        return -1;
    }
    while (1) {
        ++lineno;
        if (!fgets(line, sizeof(line), fp)) {
            if (ferror(fp)) {
                int res = errno;
                fprintf(stderr, "error reading from %s: %s\n",
                        path, terror(res));
                break;
            }
            ret = EXIT_SUCCESS;
            break;
        }
        span_json_parse(line, &span, err, err_len);
        if (err[0]) {
            fprintf(stderr, "error parsing line %d.  Failed to parse %s "
                    "from %s: %s\n", lineno, line, path, err);
            break;
        }
        span_table_put(st, span);
    }
    fclose(fp);
    if (ret == EXIT_SUCCESS) {
        //fprintf(stderr, "loaded %d spans from %s\n", lineno - 1, path);
        return lineno - 1;
    }
    return -1;
}

// vim: ts=4:sw=4:tw=79:et
