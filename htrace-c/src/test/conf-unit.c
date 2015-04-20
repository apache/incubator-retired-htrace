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

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int test_simple_conf(void)
{
    struct htrace_conf *conf;
    struct htrace_log *lg;
    conf = htrace_conf_from_strs("foo=bar;foo2=baz;foo3=quux;foo5=123",
                                 "foo3=default3;foo4=default4");
    lg = htrace_log_alloc(conf);
    EXPECT_NONNULL(conf);
    EXPECT_STR_EQ("bar", htrace_conf_get(conf, "foo"));
    EXPECT_STR_EQ("quux", htrace_conf_get(conf, "foo3"));
    EXPECT_STR_EQ("default4", htrace_conf_get(conf, "foo4"));
    EXPECT_UINT64_EQ((uint64_t)123, htrace_conf_get_u64(lg, conf, "foo5"));
    EXPECT_UINT64_EQ((uint64_t)123, htrace_conf_get_u64(lg, conf, "foo5"));
    EXPECT_NULL(htrace_conf_get(conf, "unknown"));

    htrace_log_free(lg);
    htrace_conf_free(conf);
    return EXIT_SUCCESS;
}

static int test_double_conf(void)
{
    struct htrace_conf *conf;
    struct htrace_log *lg;
    double d;

    conf = htrace_conf_from_strs("my.double=5.4;bozo=wakkawakkaa",
                                 "my.double=1.1;bozo=2.0");
    EXPECT_NONNULL(conf);
    lg = htrace_log_alloc(conf);
    d = htrace_conf_get_double(lg, conf, "my.double");
    // Do a sloppy comparison to avoid thinking about IEEE float precision
    // issues
    if ((d > 5.401) || (d < 5.399)) {
        htrace_log(lg, "failed to parse my.double... expected 5.4, "
                   "got %g\n", d);
        return EXIT_FAILURE;
    }
    // 'bozo' should fall back on the default, since the configured value
    // cannot be parsed.
    d = htrace_conf_get_double(lg, conf, "bozo");
    if ((d > 2.001) || (d < 1.999)) {
        htrace_log(lg, "failed to parse bozo... expected 2.0, "
                   "got %g\n", d);
        return EXIT_FAILURE;
    }
    // 'unknown' should get 0.0, since there is no value or default.
    d = htrace_conf_get_double(lg, conf, "unknown");
    if ((d > 0.001) || (d < -0.001)) {
        htrace_log(lg, "failed to parse unknown... expected 0.0, "
                   "got %g\n", d);
        return EXIT_FAILURE;
    }

    htrace_log_free(lg);
    htrace_conf_free(conf);
    return EXIT_SUCCESS;
}

int main(void)
{
    test_simple_conf();
    test_double_conf();

    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
