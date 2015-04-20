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
#include "test/mini_htraced.h"
#include "test/rtest.h"
#include "test/span_table.h"
#include "test/span_util.h"
#include "test/temp_dir.h"
#include "test/test.h"
#include "util/log.h"
#include "util/time.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int htraced_rcv_test(struct rtest *rt)
{
    char err[512], *conf_str, *json_path;
    size_t err_len = sizeof(err);
    struct mini_htraced_params params;
    struct mini_htraced *ht = NULL;
    struct span_table *st;
    uint64_t start_ms;

    params.name = rt->name;
    params.confstr = "";
    mini_htraced_build(&params, &ht, err, err_len);
    EXPECT_STR_EQ("", err);

    EXPECT_INT_GE(0, asprintf(&json_path, "%s/%s",
                ht->root_dir, "spans.json"));
    EXPECT_INT_GE(0, asprintf(&conf_str, "%s=%s;%s=%s",
                HTRACE_SPAN_RECEIVER_KEY, "htraced",
                HTRACED_ADDRESS_KEY, ht->htraced_http_addr));
    EXPECT_INT_ZERO(rt->run(rt, conf_str));
    start_ms = monotonic_now_ms(NULL);
    //
    // It may take a little while for htraced to commit the incoming spans sent
    // via RPC to its data store.  htraced does not have read-after-write
    // consistency, in other words.  This isn't normally an issue since trace
    // collection is done in the background.
    //
    // For this unit test, it means that we want to retry if we find too few
    // spans the first time we dump the htraced data store contents.
    //
    while (1) {
        int nspans;

        // This uses the bin/htrace program to dump the spans to a json file.
        mini_htraced_dump_spans(ht, err, err_len, json_path);
        EXPECT_STR_EQ("", err);
        st = span_table_alloc();
        EXPECT_NONNULL(st);
        nspans = load_trace_span_file(json_path, st);
        EXPECT_INT_GE(0, nspans);
        if (nspans >= rt->spans_created) {
            break;
        }
        span_table_free(st);
        st = NULL;
        EXPECT_UINT64_GE(start_ms, monotonic_now_ms(NULL) + 30000);
        sleep_ms(100);
        fprintf(stderr, "htraced_test_app1: retrying htrace dumpAll...\n");
    }
    EXPECT_INT_ZERO(rt->verify(rt, st));
    free(conf_str);
    free(json_path);
    span_table_free(st);
    mini_htraced_stop(ht);
    mini_htraced_free(ht);

    return EXIT_SUCCESS;
}

int main(void)
{
    int i;

    for (i = 0; g_rtests[i]; i++) {
        struct rtest *rtest = g_rtests[i];
        if (htraced_rcv_test(rtest) != EXIT_SUCCESS) {
            fprintf(stderr, "rtest %s failed\n", rtest->name);
            return EXIT_FAILURE;
        }
    }

    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
