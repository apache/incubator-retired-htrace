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
#include "test/rtest.h"
#include "test/span_table.h"
#include "test/span_util.h"
#include "test/temp_dir.h"
#include "test/test.h"
#include "util/log.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int local_file_rcv_test(struct rtest *rt)
{
    char err[512];
    size_t err_len = sizeof(err);
    char *local_path, *tdir, *conf_str = NULL;
    struct span_table *st;

    st = span_table_alloc();
    tdir = create_tempdir("local_file_rcv-unit", 0777, err, err_len);
    EXPECT_STR_EQ("", err);
    register_tempdir_for_cleanup(tdir);
    EXPECT_INT_GE(0, asprintf(&local_path, "%s/%s", tdir, "spans.json"));
    EXPECT_INT_GE(0, asprintf(&conf_str, "%s=%s;%s=%s",
                HTRACE_SPAN_RECEIVER_KEY, "local.file",
                HTRACE_LOCAL_FILE_RCV_PATH_KEY, local_path));
    EXPECT_INT_ZERO(rt->run(rt, conf_str));
    EXPECT_INT_GE(0, load_trace_span_file(local_path, st));
    EXPECT_INT_ZERO(rt->verify(rt, st));
    free(conf_str);
    free(local_path);
    free(tdir);
    span_table_free(st);

    return EXIT_SUCCESS;
}

int main(void)
{
    int i;

    for (i = 0; g_rtests[i]; i++) {
        struct rtest *rtest = g_rtests[i];
        if (local_file_rcv_test(rtest) != EXIT_SUCCESS) {
            fprintf(stderr, "rtest %s failed\n", rtest->name);
            return EXIT_FAILURE;
        }
    }

    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
