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
#include "core/span.h"
#include "test/rtest.h"
#include "test/span_table.h"
#include "test/span_util.h"
#include "test/test.h"
#include "util/log.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define RECEIVER_TEST_TNAME "receiver-unit"

/**
 * @file rtestpp.cc
 *
 * The C receiver tests.
 */

struct rtest_data {
    struct htrace_conf *cnf;
    struct htrace_sampler *always;
    struct htracer *tracer;
};

static void get_receiver_test_trid(char *trid, size_t trid_len)
{
    snprintf(trid, trid_len, RECEIVER_TEST_TNAME "/%lld", (long long)getpid());
}

static int rtest_data_init(const char *conf_str, struct rtest_data **out)
{
    char *econf_str = NULL;
    struct rtest_data *rdata = calloc(1, sizeof(*(rdata)));
    EXPECT_NONNULL(rdata);
    if (asprintf(&econf_str, HTRACE_TRACER_ID"=%%{tname}/%%{pid};sampler=always;"
              "%s", conf_str) < 0) {
        fprintf(stderr, "asprintf(econf_str) failed: OOM\n");
        return EXIT_FAILURE;
    }
    rdata->cnf = htrace_conf_from_str(econf_str);
    free(econf_str);
    EXPECT_NONNULL(rdata->cnf);
    rdata->tracer = htracer_create(RECEIVER_TEST_TNAME, rdata->cnf);
    EXPECT_NONNULL(rdata->tracer);
    rdata->always = htrace_sampler_create(rdata->tracer, rdata->cnf);
    EXPECT_NONNULL(rdata->always);
    *out = rdata;
    return EXIT_SUCCESS;
}

static void rtest_data_free(struct rtest_data *rdata)
{
    htrace_sampler_free(rdata->always);
    rdata->always = NULL;
    htracer_free(rdata->tracer);
    rdata->tracer = NULL;
    htrace_conf_free(rdata->cnf);
    rdata->cnf = NULL;
    free(rdata);
}

static int rtest_verify_table_size(struct rtest *rt, struct span_table *st)
{
    EXPECT_INT_EQ(rt->spans_created, span_table_size(st));

    return EXIT_SUCCESS;
}

static int doit(struct rtest_data *rdata)
{
    struct htrace_scope *scope1, *scope2, *scope2_5;

    scope1 = htrace_start_span(rdata->tracer, NULL, "part1");
    EXPECT_UINT64_GT(0L, htrace_scope_get_span_id(scope1));
    htrace_scope_close(scope1);
    scope2 = htrace_start_span(rdata->tracer, NULL, "part2");
    EXPECT_UINT64_GT(0L, htrace_scope_get_span_id(scope2));
    scope2_5 = htrace_start_span(rdata->tracer, NULL, "part2.5");
    htrace_scope_close(scope2_5);
    htrace_scope_close(scope2);
    return EXIT_SUCCESS;
}

int rtest_simple_run(struct rtest *rt, const char *conf_str)
{
    struct htrace_scope *scope0;
    struct rtest_data *rdata = NULL;

    EXPECT_INT_ZERO(rtest_data_init(conf_str, &rdata));
    EXPECT_NONNULL(rdata);
    scope0 = htrace_start_span(rdata->tracer, rdata->always, "doit");
    doit(rdata);
    htrace_scope_close(scope0);
    rt->spans_created = 4;
    rtest_data_free(rdata);
    return EXIT_SUCCESS;
}

int rtest_simple_verify(struct rtest *rt, struct span_table *st)
{
    struct htrace_span *span;
    uint64_t doit_id, part2_id;
    char trid[128];

    EXPECT_INT_ZERO(rtest_verify_table_size(rt, st));
    get_receiver_test_trid(trid, sizeof(trid));
    EXPECT_INT_ZERO(span_table_get(st, &span, "doit", trid));
    doit_id = span->span_id;
    EXPECT_INT_ZERO(span->num_parents);

    EXPECT_INT_ZERO(span_table_get(st, &span, "part1", trid));
    EXPECT_INT_EQ(1, span->num_parents);
    EXPECT_UINT64_EQ(doit_id, span->parent.single);

    EXPECT_INT_ZERO(span_table_get(st, &span, "part2", trid));
    EXPECT_INT_EQ(1, span->num_parents);
    part2_id = span->span_id;
    EXPECT_UINT64_EQ(doit_id, span->parent.single);

    EXPECT_INT_ZERO(span_table_get(st, &span, "part2.5", trid));
    EXPECT_INT_EQ(1, span->num_parents);
    EXPECT_UINT64_EQ(part2_id, span->parent.single);

    return EXIT_SUCCESS;
}

static struct rtest g_rtest_simple = {
    "rtest_simple",
    rtest_simple_run,
    rtest_simple_verify,
};

struct rtest * const g_rtests[] = {
    &g_rtest_simple,
    NULL
};

// vim: ts=4:sw=4:tw=79:et
