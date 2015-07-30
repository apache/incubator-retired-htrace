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

#include "core/conf.h"
#include "core/htrace.h"
#include "core/htracer.h"
#include "core/span.h"
#include "sampler/sampler.h"
#include "test/span_util.h"
#include "test/test.h"
#include "util/htable.h"
#include "util/log.h"

#include <errno.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_SPAN_JSON_LEN 100000

static struct htrace_conf *g_test_conf;

static struct htrace_log *g_test_lg;

static struct htracer *g_test_tracer;

static struct htrace_span *create_span(const char *desc,
        uint64_t begin_ms, uint64_t end_ms, uint64_t span_id,
        const char *trid, ...) __attribute__((sentinel));

static int test_span_to_json(const char *expected,
                             struct htrace_span *span)
{
    int buf_len;
    char *buf;
    struct htrace_span *rspan = NULL;
    char err[128];
    size_t err_len = sizeof(err);

    htrace_span_sort_and_dedupe_parents(span);
    buf_len = span_json_size(span);
    if ((0 > buf_len) || (buf_len > MAX_SPAN_JSON_LEN)) {
        fprintf(stderr, "invalid span_json_size %d.\n", buf_len);
        return EXIT_FAILURE;
    }
    buf = malloc(buf_len);
    EXPECT_NONNULL(buf);
    span_json_sprintf(span, buf_len, buf);
    EXPECT_STR_EQ(expected, buf);
    span_json_parse(buf, &rspan, err, err_len);
    if (err[0]) {
        fprintf(stderr, "Failed to parse span json %s: %s\n", buf, err);
        return EXIT_FAILURE;
    }
    EXPECT_NONNULL(rspan);
    if (span_compare(span, rspan) != 0) {
        htrace_span_free(rspan);
        fprintf(stderr, "Failed to parse the span json back into a span "
                "which was identical to the input.  JSON: %s\n", buf);
        return EXIT_FAILURE;
    }
    free(buf);
    htrace_span_free(rspan);
    return EXIT_SUCCESS;
}

static struct htrace_span *create_span(const char *desc,
        uint64_t begin_ms, uint64_t end_ms, uint64_t span_id,
        const char *trid, ...)
{
    struct htrace_span* span = NULL;
    uint64_t *parents, parent;
    int i, num_parents = 0;
    va_list ap, ap2;

    va_start(ap, trid);
    va_copy(ap2, ap);
    while (1) {
        parent = va_arg(ap2, uint64_t);
        if (!parent) {
            break;
        }
        num_parents++;
    } while (parent);
    va_end(ap2);
    if (num_parents > 0) {
        parents = xcalloc(sizeof(uint64_t) * num_parents);
        for (i = 0; i < num_parents; i++) {
            parents[i] = va_arg(ap, uint64_t);
        }
    }
    va_end(ap);
    span = htrace_span_alloc(desc, begin_ms, span_id);
    span->end_ms = end_ms;
    span->span_id = span_id;
    span->trid = xstrdup(trid);
    span->num_parents = num_parents;
    if (num_parents == 1) {
        span->parent.single = parents[0];
        free(parents);
    } else if (num_parents > 1) {
        span->parent.list = parents;
    }
    return span;
}

static int test_spans_to_str(void)
{
    struct htrace_span *span;

    span = create_span("foo", 123LLU, 456LLU, 789LLU, "span-unit",
                        NULL);
    EXPECT_INT_ZERO(test_span_to_json(
        "{\"s\":\"0000000000000315\",\"b\":123,\"e\":456,"
            "\"d\":\"foo\",\"r\":\"span-unit\""
            ",\"p\":[]}", span));
    htrace_span_free(span);

    span = create_span("myspan", 34359738368LLU,
                        34359739368LLU, 68719476736LLU, "span-unit2",
                        1LLU, 2LLU, 3LLU, NULL);
    EXPECT_INT_ZERO(test_span_to_json(
        "{\"s\":\"0000001000000000\",\"b\":34359738368,\"e\":34359739368,"
        "\"d\":\"myspan\",\"r\":\"span-unit2\"," "\"p\":[\"0000000000000001\","
        "\"0000000000000002\",\"0000000000000003\"]}", span));
    htrace_span_free(span);

    span = create_span("nextSpan", 14359739368LLU, 18719476736LLU,
                       0x8000001000000000LLU, "span-unit3",
                        1LLU, 1LLU, 1LLU, NULL);
    EXPECT_INT_ZERO(test_span_to_json(
        "{\"s\":\"8000001000000000\",\"b\":14359739368,\"e\":18719476736,"
        "\"d\":\"nextSpan\",\"r\":\"span-unit3\"," "\"p\":[\"0000000000000001\"]"
        "}", span));
    htrace_span_free(span);
    return EXIT_SUCCESS;
}

int main(void)
{
    g_test_conf = htrace_conf_from_strs("", HTRACE_TRACER_ID"=span-unit");
    EXPECT_NONNULL(g_test_conf);
    g_test_lg = htrace_log_alloc(g_test_conf);
    EXPECT_NONNULL(g_test_lg);
    g_test_tracer = htracer_create("span-unit", g_test_conf);
    EXPECT_NONNULL(g_test_tracer);

    EXPECT_INT_ZERO(test_spans_to_str());

    htracer_free(g_test_tracer);
    htrace_log_free(g_test_lg);
    htrace_conf_free(g_test_conf);
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
