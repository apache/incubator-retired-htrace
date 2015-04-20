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

#include "test/mini_htraced.h"
#include "test/test.h"

#include <stdlib.h>

static int test_mini_htraced_start_stop(void)
{
    char err[128];
    size_t err_len = sizeof(err);
    struct mini_htraced_params params;
    struct mini_htraced *ht = NULL;

    err[0] = '\0';
    params.name = "test_mini_htraced_start_stop";
    params.confstr = "";
    mini_htraced_build(&params, &ht, err, err_len);
    EXPECT_STR_EQ("", err);
    mini_htraced_stop(ht);
    mini_htraced_free(ht);

    return 0;
}

int main(void)
{
    EXPECT_INT_ZERO(test_mini_htraced_start_stop());
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
