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
#include "util/time.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TEST_STR1 "This is a test.  "
#define TEST_STR1_LEN (sizeof(TEST_STR1) - 1)

static struct htrace_conf *g_test_conf;

static struct htrace_log *g_test_lg;

static int test_timespec_conversions(void)
{
    uint64_t ms;
    struct timespec ts, ts2;
    ts.tv_sec = 1;
    ts.tv_nsec = 999999999;
    ms = timespec_to_ms(&ts);
    EXPECT_UINT64_EQ((uint64_t)1999, ms);
    ts2.tv_sec = 0;
    ts2.tv_nsec = 0;
    ms_to_timespec(ms, &ts2);
    EXPECT_UINT64_EQ(1LU, ts2.tv_sec);
    EXPECT_UINT64_EQ((uint64_t)999000000ULL, ts2.tv_nsec);
    return EXIT_SUCCESS;
}

static int test_now_increases(int monotonic)
{
    int i;
    uint64_t first = now_ms(g_test_lg);

    for (i = 0; i < 10; i++) {
        uint64_t next;
        sleep_ms(2); // At least 2 ms.
        if (monotonic) {
            next = monotonic_now_ms(g_test_lg);
        } else {
            next = now_ms(g_test_lg);
        }
        EXPECT_UINT64_GT(first, next);
    }
    return EXIT_SUCCESS;
}

int main(void)
{
    g_test_conf = htrace_conf_from_strs("", "");
    g_test_lg = htrace_log_alloc(g_test_conf);

    test_timespec_conversions();

    test_now_increases(0);
    test_now_increases(1);

    htrace_log_free(g_test_lg);
    htrace_conf_free(g_test_conf);
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
