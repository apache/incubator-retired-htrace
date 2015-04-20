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

#include "test/test.h"
#include "util/string.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#pragma GCC diagnostic ignored "-Wformat-zero-length"

static int test_fwdprintf(void)
{
    char *b, buf[8];
    int rem = sizeof(buf);

    memset(&buf, 0, sizeof(buf));
    b = buf;
    EXPECT_INT_EQ(3, fwdprintf(NULL, NULL, "ab%c", 'c'));
    EXPECT_INT_EQ(3, fwdprintf(&b, &rem, "ab%c", 'c'));
    EXPECT_STR_EQ("abc", buf);
    EXPECT_INT_EQ(5, rem);
    EXPECT_INT_EQ(0, fwdprintf(NULL, NULL, ""));
    EXPECT_INT_EQ(0, fwdprintf(&b, &rem, ""));
    EXPECT_INT_EQ(5, rem);
    EXPECT_INT_EQ(2, fwdprintf(NULL, NULL, "de"));
    EXPECT_INT_EQ(2, fwdprintf(&b, &rem, "de"));
    EXPECT_STR_EQ("abcde", buf);
    EXPECT_INT_EQ(3, rem);
    EXPECT_INT_EQ(6, fwdprintf(NULL, NULL, "fghijk"));
    EXPECT_INT_EQ(6, fwdprintf(&b, &rem, "fghijk"));
    EXPECT_INT_EQ(0, rem);
    EXPECT_STR_EQ("abcdefg", buf);
    return EXIT_SUCCESS;
}

static int test_validate_json_string(void)
{
    EXPECT_INT_EQ(1, validate_json_string(NULL, ""));
    EXPECT_INT_EQ(1, validate_json_string(NULL, "abc"));
    EXPECT_INT_EQ(0, validate_json_string(NULL, "\\"));
    EXPECT_INT_EQ(0, validate_json_string(NULL, "\"FooBar\""));
    EXPECT_INT_EQ(1, validate_json_string(NULL, "Foo:bar:baz-whatever"));
    EXPECT_INT_EQ(0, validate_json_string(NULL, "\x01"));
    return EXIT_SUCCESS;
}

int main(void)
{
    EXPECT_INT_ZERO(test_fwdprintf());
    EXPECT_INT_ZERO(test_validate_json_string());
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
