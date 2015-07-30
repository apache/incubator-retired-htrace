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
#include "test/test.h"
#include "util/log.h"
#include "util/tracer_id.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

static struct htrace_conf *g_cnf;

static struct htrace_log *g_lg;

static int test_calculate_tracer_id(const char *expected, const char *fmt)
{
    char *tracer_id;

    tracer_id = calculate_tracer_id(g_lg, fmt, "fooproc");
    EXPECT_NONNULL(tracer_id);
    EXPECT_STR_EQ(expected, tracer_id);
    free(tracer_id);

    return EXIT_SUCCESS;
}

static int test_calculate_tracer_ids(void)
{
    char buf[128];

    EXPECT_INT_ZERO(test_calculate_tracer_id("my.name", "my.name"));
    EXPECT_INT_ZERO(test_calculate_tracer_id("my.fooproc", "my.%{tname}"));
    EXPECT_INT_ZERO(test_calculate_tracer_id("\%foo", "\%foo"));
    EXPECT_INT_ZERO(test_calculate_tracer_id("fooproc", "%{tname}"));
    EXPECT_INT_ZERO(test_calculate_tracer_id("\\fooproc", "\\\\%{tname}"));
    EXPECT_INT_ZERO(test_calculate_tracer_id("\%{tname}", "\\%{tname}"));

    snprintf(buf, sizeof(buf), "me.%lld", (long long)getpid());
    EXPECT_INT_ZERO(test_calculate_tracer_id(buf, "me.%{pid}"));

    get_best_ip(g_lg, buf, sizeof(buf));
    EXPECT_INT_ZERO(test_calculate_tracer_id(buf, "%{ip}"));

    return EXIT_SUCCESS;
}

int main(void)
{
    g_cnf = htrace_conf_from_strs("", "");
    EXPECT_NONNULL(g_cnf);
    g_lg = htrace_log_alloc(g_cnf);
    EXPECT_NONNULL(g_lg);
    EXPECT_INT_ZERO(test_calculate_tracer_ids());
    htrace_log_free(g_lg);
    htrace_conf_free(g_cnf);

    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
