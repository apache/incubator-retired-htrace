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
#include "util/rand.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct htrace_log *g_rand_unit_lg;

#define ARRAY_SIZE 128

static int compare_u32(const void *v1, const void *v2)
{
    uint32_t a = *(uint32_t *)v1;
    uint32_t b = *(uint32_t *)v2;

    if (a < b) {
        return -1;
    } else if (b < a) {
        return 1;
    } else {
        return 0;
    }
}

/**
 * Test that we can generate an array of unique uint32_t objects.
 */
static int test_u32_uniqueness(void)
{
    struct random_src *rnd = random_src_alloc(g_rand_unit_lg);
    uint32_t prev, *arr;
    int i, duplicate;

    do {
        arr = calloc(ARRAY_SIZE, sizeof(uint32_t));
        EXPECT_NONNULL(arr);
        for (i = 0; i < ARRAY_SIZE; i++) {
            arr[i] = random_u32(rnd);
        }
        qsort(arr, ARRAY_SIZE, sizeof(arr[0]), compare_u32);
        prev = 0;
        duplicate = 0;
        for (i = 0; i < ARRAY_SIZE; i++) {
            if (arr[i] == prev) {
                duplicate = 1;
            }
            prev = arr[i];
        }
    } while (duplicate);
    random_src_free(rnd);
    free(arr);
    return EXIT_SUCCESS;
}

int main(void)
{
    struct htrace_conf *conf;

    conf = htrace_conf_from_strs("", "");
    EXPECT_NONNULL(conf);
    g_rand_unit_lg = htrace_log_alloc(conf);
    EXPECT_NONNULL(g_rand_unit_lg);
    EXPECT_INT_ZERO(test_u32_uniqueness());
    htrace_log_free(g_rand_unit_lg);
    htrace_conf_free(conf);

    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
