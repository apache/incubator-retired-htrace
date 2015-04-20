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
#include "sampler/sampler.h"
#include "test/test.h"
#include "util/log.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static struct htrace_conf *g_test_conf;

static struct htrace_log *g_test_lg;

static struct htracer *g_test_tracer;

#define NUM_TEST_SAMPLES 1000

static int test_simple_sampler(const char *name, int expected)
{
    struct htrace_conf *conf;
    struct htrace_sampler *smp;
    char confstr[256] = { 0 };
    int i;

    if (name) {
        snprintf(confstr, sizeof(confstr), "%s=%s",
                 HTRACE_SAMPLER_KEY, name);
    }
    conf = htrace_conf_from_strs(confstr, "");
    EXPECT_NONNULL(conf);
    smp = htrace_sampler_create(g_test_tracer, conf);
    EXPECT_NONNULL(smp);
    for (i = 0; i < NUM_TEST_SAMPLES; i++) {
        int s = smp->ty->next(smp);
        EXPECT_INT_EQ(expected, s);
    }
    htrace_conf_free(conf);
    htrace_sampler_free(smp);
    return EXIT_SUCCESS;
}

static int test_unconfigured_sampler()
{
    return test_simple_sampler(NULL, 0);
}

static int test_never_sampler()
{
    return test_simple_sampler("never", 0);
}

static int test_always_sampler()
{
    return test_simple_sampler("always", 1);
}

#define NUM_PROB_TEST_SAMPLES 500000

static int test_prob_sampler(double target, double slop)
{
    struct htrace_conf *conf;
    struct htrace_sampler *smp;
    char confstr[256] = { 0 };
    double actual, diff;

    snprintf(confstr, sizeof(confstr),
             "sampler=prob;prob.sampler.fraction=%g", target);
    conf = htrace_conf_from_strs(confstr, "");
    EXPECT_NONNULL(conf);
    smp = htrace_sampler_create(g_test_tracer, conf);
    EXPECT_NONNULL(smp);
    do {
        int i;
        uint64_t total = 0;

        for (i = 0; i < NUM_PROB_TEST_SAMPLES; i++) {
            int val = smp->ty->next(smp);
            if ((val != 0) && (val != 1)) {
                htrace_log(g_test_lg, "Invalid return from sampler: "
                               "expected 0 or 1, but got %d\n", val);
                abort();
            }
            total += val;
        }
        actual = ((double)total) / (double)NUM_PROB_TEST_SAMPLES;
        diff = fabs(target - actual);
        htrace_log(g_test_lg, "After %d samples, fraction is %g.  Target "
                    "was %g.  %s\n", NUM_PROB_TEST_SAMPLES, actual, target,
                    (diff < slop) ? "Done. " : "Retrying.");
    } while (diff >= slop);
    htrace_conf_free(conf);
    htrace_sampler_free(smp);
    return EXIT_SUCCESS;
}

int main(void)
{
    g_test_conf = htrace_conf_from_strs("", HTRACE_PROCESS_ID"=sampler-unit");
    EXPECT_NONNULL(g_test_conf);
    g_test_lg = htrace_log_alloc(g_test_conf);
    EXPECT_NONNULL(g_test_lg);
    g_test_tracer = htracer_create("sampler-unit", g_test_conf);
    EXPECT_NONNULL(g_test_tracer);

    EXPECT_INT_ZERO(test_unconfigured_sampler());
    EXPECT_INT_ZERO(test_never_sampler());
    EXPECT_INT_ZERO(test_always_sampler());
    EXPECT_INT_ZERO(test_prob_sampler(0.5, 0.001));
    EXPECT_INT_ZERO(test_prob_sampler(0.01, 0.001));
    EXPECT_INT_ZERO(test_prob_sampler(0.1, 0.001));

    htracer_free(g_test_tracer);
    htrace_log_free(g_test_lg);
    htrace_conf_free(g_test_conf);
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
