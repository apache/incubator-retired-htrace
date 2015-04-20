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

#include "test/span_util.h"
#include "test/test.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int test_parse_hex_id_error(const char *in)
{
    char err[128];

    err[0] = '\0';
    parse_hex_id(in, err, sizeof(err));
    if (!err[0]) {
        fprintf(stderr, "test_parse_hex_id_error(%s): expected error, but "
                "was successful.\n", in);
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

static int test_parse_hex_id(uint64_t expected, const char *in)
{
    char err[128];
    size_t err_len = sizeof(err);
    uint64_t val;

    err[0] = '\0';
    val = parse_hex_id(in, err, err_len);
    if (err[0]) {
        fprintf(stderr, "test_parse_hex_id(%s): got error %s\n",
                in, err);
        return EXIT_FAILURE;
    }
    EXPECT_UINT64_EQ(expected, val);
    return EXIT_SUCCESS;
}

int main(void)
{
    EXPECT_INT_ZERO(test_parse_hex_id_error(""));
    EXPECT_INT_ZERO(test_parse_hex_id_error("z"));
    EXPECT_INT_ZERO(test_parse_hex_id_error("achoo"));
    EXPECT_INT_ZERO(test_parse_hex_id(1LLU, "00000000000000001"));
    EXPECT_INT_ZERO(test_parse_hex_id(0xffffffffffffffffLLU,
                                       "ffffffffffffffff"));
    EXPECT_INT_ZERO(test_parse_hex_id(0x8000000000000000LLU,
                                       "8000000000000000"));
    EXPECT_INT_ZERO(test_parse_hex_id(0x6297421fe159345fLLU,
                                       "6297421fe159345f"));
    EXPECT_INT_ZERO(test_parse_hex_id_error("6297421fe159345fzoo"));
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
