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

#ifndef APACHE_HTRACE_TEST_RTEST_H
#define APACHE_HTRACE_TEST_RTEST_H

/**
 * @file rtest.h
 *
 * Declares receiver tests.
 *
 * This is an internal header, not intended for external use.
 */

struct span_table;

/**
 * A receiver test.
 */
struct rtest {
    /**
     * The name of the receiver test.
     */
    const char *name;

    /**
     * Run the receiver test.
     *
     * @param rt        The receiver test.
     * @param conf_str  The configuration string to use.
     *
     * @return zero on success
     */
    int (*run)(struct rtest *rt, const char *conf_str);

    /**
     * Verify that the receiver test succeeded.
     *
     * @param rt        The receiver test.
     * @param st        The span table.
     *
     * @return          zero on success
     */
    int (*verify)(struct rtest *rt, struct span_table *st);

    /**
     * The number of spans that have been created by this test.
     */
    int spans_created;
};

/**
 * A NULL-terminated list of pointers to rtests.
 */
extern struct rtest * const g_rtests[];

#define RECEIVER_TEST_TNAME "receiver-unit"

#endif

// vim: ts=4:sw=4:tw=79:et
