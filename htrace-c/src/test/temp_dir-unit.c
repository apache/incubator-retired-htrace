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

#include "test/temp_dir.h"
#include "test/test.h"

#include <limits.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static int test_create_tempdir(void)
{
    char *tdir = NULL;
    struct stat st_buf;
    char err[128];
    size_t err_len = sizeof(err);
    int ret;

    tdir = create_tempdir("test_create_tempdir", 0775,
                          err, err_len);
    EXPECT_STR_EQ("", err);
    ret = register_tempdir_for_cleanup(tdir);
    if (ret)
        return EXIT_FAILURE;
    if (stat(tdir, &st_buf) == -1) {
        return EXIT_FAILURE;
    }
    if (!S_ISDIR(st_buf.st_mode)) {
        return EXIT_FAILURE;
    }
    free(tdir);
    return 0;
}

static int test_create_tempdir_and_delete(void)
{
    char *tdir = NULL;
    struct stat st_buf;
    int ret;
    char err[128];
    size_t err_len = sizeof(err);

    tdir = create_tempdir("test_create_tempdir_and_delete", 0775,
                          err, err_len);
    EXPECT_STR_EQ("", err);
    ret = register_tempdir_for_cleanup(tdir);
    if (ret)
        return EXIT_FAILURE;
    if (stat(tdir, &st_buf) == -1) {
        return EXIT_FAILURE;
    }
    if (!S_ISDIR(st_buf.st_mode)) {
        return EXIT_FAILURE;
    }
    recursive_unlink(tdir);
    unregister_tempdir_for_cleanup(tdir);
    free(tdir);
    return 0;
}

int main(void)
{
    EXPECT_INT_ZERO(test_create_tempdir());
    EXPECT_INT_ZERO(test_create_tempdir());
    EXPECT_INT_ZERO(test_create_tempdir());
    EXPECT_INT_ZERO(test_create_tempdir_and_delete());
    EXPECT_INT_ZERO(test_create_tempdir_and_delete());
    EXPECT_INT_ZERO(test_create_tempdir_and_delete());
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
