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
#include "test/span_util.h"
#include "test/test.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int test_span_round_trip(const char *str)
{
    char err[512], *json = NULL;
    size_t err_len = sizeof(err);
    struct htrace_span *span = NULL;
    int json_size;

    err[0] = '\0';
    span_json_parse(str, &span, err, err_len);
    EXPECT_STR_EQ("", err);
    json_size = span_json_size(span);
    json = malloc(json_size);
    EXPECT_NONNULL(json);
    span_json_sprintf(span, json_size, json);
    EXPECT_STR_EQ(str, json);
    free(json);
    htrace_span_free(span);

    return 0;
}

int main(void)
{
    EXPECT_INT_ZERO(test_span_round_trip(
        "{\"a\":\"ba85631c2ce111e5b345feff819cdc9f\",\"b\":34359738368,"
        "\"e\":34359739368,\"d\":\"myspan\",\"r\":\"span-unit2\","
        "\"p\":[\"1549e8d42ce411e5b345feff819cdc9f\","
        "\"1b6a1d242ce411e5b345feff819cdc9f\","
        "\"25ab73822ce411e5b345feff819cdc9f\"]}"));
    EXPECT_INT_ZERO(test_span_round_trip(
        "{\"a\":\"000000002ce111e5b345feff819cdc9f\",\"b\":0,"
        "\"e\":0,\"d\":\"secondSpan\",\"r\":\"other-tracerid\","
        "\"p\":[]}"));
    EXPECT_INT_ZERO(test_span_round_trip(
        "{\"a\":\"6baba3842ce411e5b345feff819cdc9f\",\"b\":999,"
        "\"e\":1000,\"d\":\"thirdSpan\",\"r\":\"other-tracerid\","
        "\"p\":[\"000000002ce111e5b345feff819cdc9f\"]}"));
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
