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

#ifndef APACHE_HTRACE_TEST_MINI_HTRACED_H
#define APACHE_HTRACE_TEST_MINI_HTRACED_H

/**
 * @file mini_htraced.h
 *
 * Implements a mini htraced cluster which can be used in unit tests.
 *
 * This is useful for testing the htraced trace sink of the native client.
 *
 * This is an internal header, not intended for external use.
 */

#include <stdint.h> /* for uint64_t, etc. */
#include <unistd.h> /* for pid_t and size_t */

struct htrace_conf;

#define NUM_DATA_DIRS 2

struct mini_htraced_params {
    /**
     * The name of the mini htraced process to start.
     * This shows up in the test directory name and some other places.
     * The memory should be managed by the caller.
     */
    const char *name;

    /**
     * The configuration to use, in string form.
     * The memory should be managed by the caller.
     */
    const char *confstr;
};

struct mini_htraced {
    /**
     * The process ID of the mini htraced.
     */
    int pid;

    /**
     * The path to the root directory that all the state associated with this
     * mini_htraced will be placed under.  Malloced.
     */
    char *root_dir;

    /**
     * Paths to the data directories to use for htraced.  Malloced.
     */
    char *data_dir[NUM_DATA_DIRS];

    /**
     * Path to the server's log file.  Malloced.
     */
    char *htraced_log_path;

    /**
     * Path to the server's conf file.  Malloced.
     */
    char *htraced_conf_path;

    /**
     * The client configuration defaults.  Malloced.
     */
    char *client_conf_defaults;

    /**
     * The startup notification port.
     */
    int snport;

    /**
     * The startup notification socket, or -1 if the socket has been closed.
     */
    int snsock;

    /**
     * Nonzero if the htraced pid is valid.
     */
    int htraced_pid_valid;

    /**
     * The process ID of the htraced pid.
     */
    pid_t htraced_pid;

    /**
     * The HTTP address of the htraced, in hostname:port format.
     */
    char *htraced_http_addr;

    /**
     * The HRPC address of the htraced, in hostname:port format.
     */
    char *htraced_hrpc_addr;

    /**
     * The number of htrace commands that have been run.
     */
    uint64_t num_htrace_commands_run;
};

/**
 * Build the mini HTraced cluster.
 *
 * @param params            The parameters to use.
 * @param ht                (out param) The mini htraced object on success.
 * @param err               (out param) The error message if there was an
 *                              error.
 * @param err_len           The length of the error buffer provided by the
 *                              caller.
 */
void mini_htraced_build(const struct mini_htraced_params *params, struct mini_htraced **ht,
                        char *err, size_t err_len);

/**
 * Stop the htraced process.
 *
 * @param ht                The mini htraced object.
 */
void mini_htraced_stop(struct mini_htraced *ht);

/**
 * Free the memory associated with the mini htraced object.
 *
 * @param ht                The mini htraced object.
 */
void mini_htraced_free(struct mini_htraced *ht);

/**
 * Dump the spans contained in this htraced instance to a file.
 *
 * @param ht                The mini htraced object.
 * @param err               (out param) The error message if there was an
 *                              error.
 * @param err_len           The length of the error buffer provided by the
 *                              caller.
 * @param path              The path to dump the spans to, in json form.
 */
void mini_htraced_dump_spans(struct mini_htraced *ht,
                             char *err, size_t err_len,
                             const char *path);

#endif

// vim: ts=4:sw=4:tw=79:et
