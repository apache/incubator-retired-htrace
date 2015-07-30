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

#define __STDC_FORMAT_MACROS

#include "core/htrace.hpp"

extern "C" {
#include "core/conf.h"
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
}

/**
 * @file rtestpp.cc
 *
 * The C++ receiver tests.
 */

using std::string;

class RTestData {
public:
    RTestData(struct rtest *rt, const char *conf_str)
        : rt_(rt),
          cnf_(string(HTRACE_TRACER_ID"=%{tname}/%{pid};sampler=always;") +
               string(conf_str).c_str()),
          tracer_(RECEIVER_TEST_TNAME, cnf_),
          always_(&tracer_, cnf_)
    {
    }

    // Test that initialization succeeeded
    int TestInit() {
        EXPECT_STR_EQ(RECEIVER_TEST_TNAME, tracer_.Name().c_str());
        EXPECT_STR_EQ("AlwaysSampler", always_.ToString().c_str());
        return EXIT_SUCCESS;
    }

    struct rtest *rt_;
    htrace::Conf cnf_;
    htrace::Tracer tracer_;
    htrace::Sampler always_;

private:
    RTestData(const RTestData &other); // disallow copying
    RTestData& operator=(const RTestData &other); // disallow assignment
};

static void get_receiver_test_trid(char *trid, size_t trid_len)
{
    snprintf(trid, trid_len, RECEIVER_TEST_TNAME "/%lld", (long long)getpid());
}

static int rtest_generic_verify(struct rtest *rt, struct span_table *st)
{
    EXPECT_INT_EQ(rt->spans_created, span_table_size(st));

    return EXIT_SUCCESS;
}

static int doit(RTestData &tdata, struct rtest *rt)
{
    {
        htrace::Scope scope1(tdata.tracer_, "part1");
    }
    {
        htrace::Scope scope2(tdata.tracer_, "part2");
        {
            htrace::Scope scope2_5(tdata.tracer_, "part2.5");
        }
    }
    return EXIT_SUCCESS;
}

int rtestpp_simple_run(struct rtest *rt, const char *conf_str)
{
    RTestData tdata(rt, conf_str);
    EXPECT_INT_ZERO(tdata.TestInit());

    htrace::Scope scope0(tdata.tracer_, tdata.always_, "doit");
    doit(tdata, rt);
    rt->spans_created = 4;
    return EXIT_SUCCESS;
}

int rtestpp_simple_verify(struct rtest *rt, struct span_table *st)
{
    struct htrace_span *span;
    struct htrace_span_id doit_id, part2_id;
    char trid[128];

    EXPECT_INT_ZERO(rtest_generic_verify(rt, st));
    get_receiver_test_trid(trid, sizeof(trid));
    EXPECT_INT_ZERO(span_table_get(st, &span, "doit", trid));
    htrace_span_id_copy(&doit_id, &span->span_id);
    EXPECT_INT_ZERO(span->num_parents);

    EXPECT_INT_ZERO(span_table_get(st, &span, "part1", trid));
    EXPECT_INT_EQ(1, span->num_parents);
    EXPECT_TRUE(0 == htrace_span_id_compare(&doit_id, &span->parent.single));

    EXPECT_INT_ZERO(span_table_get(st, &span, "part2", trid));
    EXPECT_INT_EQ(1, span->num_parents);
    htrace_span_id_copy(&part2_id, &span->span_id);
    EXPECT_TRUE(0 == htrace_span_id_compare(&doit_id, &span->parent.single));

    EXPECT_INT_ZERO(span_table_get(st, &span, "part2.5", trid));
    EXPECT_INT_EQ(1, span->num_parents);
    EXPECT_TRUE(0 == htrace_span_id_compare(&part2_id, &span->parent.single));

    return EXIT_SUCCESS;
}

struct rtest g_rtestpp_simple = {
    "rtestpp_simple",
    rtestpp_simple_run,
    rtestpp_simple_verify,
};

struct rtest * const g_rtests[] = {
    &g_rtestpp_simple,
    NULL
};

// vim: ts=4:sw=4:tw=79:et
