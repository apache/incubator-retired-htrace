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
#include "test/temp_dir.h"
#include "test/test.h"
#include "util/log.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int verify_log_file(const char *path)
{
    FILE *fp;
    char contents[4096];
    size_t res;
    const char * const expected_contents =
        "foo 2, bar, and baz.\nquux as well.\n";

    fp = fopen(path, "r");
    if (!fp) {
        int e = errno;
        fprintf(stderr, "failed to open %s: error %d (%s)\n",
                path, e, terror(e));
        return EXIT_FAILURE;
    }
    memset(contents, 0, sizeof(contents));
    res = fread(contents, 1, sizeof(contents), fp);
    if (res < strlen(expected_contents)) {
        int e = errno;
        if (feof(fp)) {
            fprintf(stderr, "fread(%s): unexpected eof.\n", path);
            return EXIT_FAILURE;
        }
        fprintf(stderr, "fread(%s): error %d (%s)\n",
                path, e, terror(e));
        return EXIT_FAILURE;
    }
    fclose(fp);
    EXPECT_STR_EQ(expected_contents, contents);

    return EXIT_SUCCESS;
}

static int verify_log_to_file(void)
{
    struct htrace_conf *conf;
    struct htrace_log *lg;
    char *tdir, log_path[PATH_MAX], conf_str[PATH_MAX];
    char err[128];
    size_t err_len = sizeof(err);

    tdir = create_tempdir("verify_log_to_file", 0775, err, err_len);
    EXPECT_NONNULL(tdir);
    EXPECT_INT_ZERO(register_tempdir_for_cleanup(tdir));
    snprintf(log_path, sizeof(log_path), "%s/log.txt", tdir);
    snprintf(conf_str, sizeof(conf_str), "log.path=%s", log_path);
    conf = htrace_conf_from_strs(conf_str, "");
    EXPECT_NONNULL(conf);
    lg = htrace_log_alloc(conf);
    EXPECT_NONNULL(lg);
    htrace_log(lg, "foo %d, bar, and baz.\n", 2);
    htrace_log(lg, "quux as well.\n");
    htrace_log_free(lg);
    EXPECT_INT_ZERO(verify_log_file(log_path));
    htrace_conf_free(conf);
    free(tdir);

    return EXIT_SUCCESS;
}

int main(void)
{
    EXPECT_INT_ZERO(verify_log_to_file());

    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
