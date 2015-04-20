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

#ifndef APACHE_HTRACE_TEST_TEMP_DIR_H
#define APACHE_HTRACE_TEST_TEMP_DIR_H

#include <unistd.h> /* for size_t */

/**
 * Create a temporary directory
 *
 * @param tempdir   The identifier to use for this temporary directory.  We will
 *                      add a random string to this identifier to create the
 *                      full path.
 * @param mode      The mode to use in mkdir.  Typically 0755.
 * @param err       The error buffer to be set on failure.
 * @param err_len   Length of the error buffer.
 *
 * @return          A malloc'ed string with the path to the temporary directory,
 *                      or NULL on failure.
 */
char *create_tempdir(const char *name, int mode, char *err, size_t err_len);

/**
 * Register a temporary directory to be deleted at the end of the program
 *
 * @param tempdir   The path of the temporary directory to register.  The string
 *                      will be deep-copied.
 *
 * @return      0 on success; error code otherwise
 */
int register_tempdir_for_cleanup(const char *tempdir);

/**
 * Unregister a temporary directory to be deleted at the end of the program
 *
 * @param tempdir   The path of the temporary directory to unregister.
 */
void unregister_tempdir_for_cleanup(const char *tempdir);

/**
 * Recursively unlink a directory.
 *
 * @param tempdir   The directory to remove.
 *
 * @return          Zero on success; the error code otherwise.
 */
int recursive_unlink(const char *path);

#endif

// vim:ts=4:sw=4:et
