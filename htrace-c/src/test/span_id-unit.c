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

#include "core/span_id.h"
#include "test/span_util.h"
#include "test/test.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int test_span_id_round_trip(const char *str)
{
    struct htrace_span_id id;
    char err[512], str2[HTRACE_SPAN_ID_STRING_LENGTH + 1];
    size_t err_len = sizeof(err);

    err[0] = '\0';
    htrace_span_id_parse(&id, str, err, err_len);
    EXPECT_STR_EQ("", err);
    EXPECT_INT_EQ(1, htrace_span_id_to_str(&id, str2, sizeof(str2)));
    EXPECT_STR_EQ(str, str2);
    return 0;
}

static int test_span_id_compare(int isLess,
                                const char *sa, const char *sb)
{
    struct htrace_span_id a, b;
    char err[512];
    size_t err_len = sizeof(err);
    int cmp;

    err[0] = '\0';

    htrace_span_id_parse(&a, sa, err, err_len);
    EXPECT_STR_EQ("", err);

    htrace_span_id_parse(&b, sb, err, err_len);
    EXPECT_STR_EQ("", err);

    cmp = htrace_span_id_compare(&a, &b);
    if (isLess) {
        EXPECT_INT_GT(cmp, 0);
    } else {
        EXPECT_INT_ZERO(cmp);
    }
    cmp = htrace_span_id_compare(&b, &a);
    if (isLess) {
        EXPECT_INT_GT(0, cmp);
    } else {
        EXPECT_INT_ZERO(cmp);
    }
    return 0;
}

static int test_span_id_less(const char *sa, const char *sb)
{
    return test_span_id_compare(1, sa, sb);
}

static int test_span_id_eq(const char *sa, const char *sb)
{
    return test_span_id_compare(0, sa, sb);
}

int main(void)
{
    EXPECT_INT_ZERO(test_span_id_round_trip("0123456789abcdef0011223344556677"));
    EXPECT_INT_ZERO(test_span_id_round_trip("a919f3d62ce111e5b345feff819cdc9f"));
    EXPECT_INT_ZERO(test_span_id_round_trip("00000000000000000000000000000000"));
    EXPECT_INT_ZERO(test_span_id_round_trip("ba85631c2ce111e5b345feff819cdc9f"));
    EXPECT_INT_ZERO(test_span_id_round_trip("ffffffffffffffffffffffffffffffff"));
    EXPECT_INT_ZERO(test_span_id_round_trip("ba85631c2ce111e5b345feff819cdc9f"));

    EXPECT_INT_ZERO(test_span_id_less("a919f3d62ce111e5b345feff819cdc9e",
                                      "a919f3d62ce111e5b345feff819cdc9f"));
    EXPECT_INT_ZERO(test_span_id_eq("a919f3d62ce111e5b345feff819cdc9f",
                                    "a919f3d62ce111e5b345feff819cdc9f"));
    EXPECT_INT_ZERO(test_span_id_eq("ffffffff2ce111e5b345feff819cdc9f",
                                    "ffffffff2ce111e5b345feff819cdc9f"));
    EXPECT_INT_ZERO(test_span_id_less("1919f3d62ce111e5b345feff819cdc9f",
                                      "f919f3d62ce111e5b345feff81900000"));
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
