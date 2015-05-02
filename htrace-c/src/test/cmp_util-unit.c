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
#include "test/span_util.h"
#include "test/test.h"
#include "util/cmp.h"
#include "util/cmp_util.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NUM_TEST_SPANS 3
#define TEST_BUF_LENGTH (8UL * 1024UL * 1024UL)

static struct htrace_span **setup_test_spans(void)
{
    struct htrace_span **spans =
        xcalloc(sizeof(struct htrace_span*) * NUM_TEST_SPANS);

    spans[0] = xcalloc(sizeof(struct htrace_span));
    spans[0]->desc = xstrdup("FirstSpan");
    spans[0]->begin_ms = 1927;
    spans[0]->end_ms = 2000;
    spans[0]->span_id = 1;

    spans[1] = xcalloc(sizeof(struct htrace_span));
    spans[1]->desc = xstrdup("SecondSpan");
    spans[1]->begin_ms = 1950;
    spans[1]->end_ms = 2000;
    spans[1]->span_id = 0xffffffffffffffffULL;
    spans[1]->prid = xstrdup("SecondSpanProc");
    spans[1]->num_parents = 1;
    spans[1]->parent.single = 1;

    spans[2] = xcalloc(sizeof(struct htrace_span));
    spans[2]->desc = xstrdup("ThirdSpan");
    spans[2]->begin_ms = 1969;
    spans[2]->end_ms = 1997;
    spans[2]->span_id = 0xcfcfcfcfcfcfcfcfULL;
    spans[2]->prid = xstrdup("ThirdSpanProc");
    spans[2]->num_parents = 2;
    spans[2]->parent.list = xcalloc(sizeof(uint64_t) * 2);
    spans[2]->parent.list[0] = 1;
    spans[2]->parent.list[1] = 0xffffffffffffffffULL;

    return spans;
}

static int serialize_test_spans(struct htrace_span **test_spans, cmp_ctx_t *ctx)
{
    int i;
    for (i = 0; i < NUM_TEST_SPANS; i++) {
        EXPECT_INT_EQ(1, span_write_msgpack(test_spans[i], ctx));
    }
    return EXIT_SUCCESS;
}

static int test_serialize_spans(struct htrace_span **test_spans)
{
    int i;
    struct htrace_span *span;
    struct cmp_counter_ctx cctx;
    struct cmp_bcopy_ctx bctx;
    char *buf;
    char err[1024];
    size_t err_len = sizeof(err);

    cmp_counter_ctx_init(&cctx);
    EXPECT_INT_ZERO(serialize_test_spans(test_spans, (cmp_ctx_t *)&cctx));

    buf = xcalloc(TEST_BUF_LENGTH);
    cmp_bcopy_ctx_init(&bctx, buf, TEST_BUF_LENGTH);
    EXPECT_INT_ZERO(serialize_test_spans(test_spans, (cmp_ctx_t *)&bctx));
    EXPECT_UINT64_EQ(cctx.count, bctx.off);
    EXPECT_UINT64_EQ(TEST_BUF_LENGTH, bctx.len);

    bctx.off = 0;
    for (i = 0; i < NUM_TEST_SPANS; i++) {
        span = span_read_msgpack((cmp_ctx_t*)&bctx, err, err_len);
        EXPECT_STR_EQ("", err);
        EXPECT_NONNULL(span);
        EXPECT_INT_ZERO(span_compare(test_spans[i], span));
        htrace_span_free(span);
    }

    free(buf);
    return EXIT_SUCCESS;
}

int main(void)
{
    int i;
    struct htrace_span **test_spans;

    test_spans = setup_test_spans();
    EXPECT_NONNULL(test_spans);
    EXPECT_INT_ZERO(test_serialize_spans(test_spans));
    for (i = 0; i < NUM_TEST_SPANS; i++) {
        htrace_span_free(test_spans[i]);
    }
    free(test_spans);
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
