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

#include "core/htrace.h"
#include "test/test_config.h"

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * @file linkage-unit.c
 *
 * Tests the linkage of libhtrace.so.
 * Verifies that the functions in htrace.h are publicly visible, and that
 * functions not included in that header are not.
 *
 * This unit test links against the production libhtrace.so, not the test
 * library.  This also verifies that we can run a program compiled against the
 * production library without encountering unresolved symbols or similar.
 */

static const char * const PUBLIC_SYMS[] = {
    "htrace_conf_free",
    "htrace_conf_from_str",
    "htrace_restart_span",
    "htrace_sampler_create",
    "htrace_sampler_free",
    "htrace_sampler_to_str",
    "htrace_scope_close",
    "htrace_scope_detach",
    "htrace_start_span",
    "htracer_create",
    "htracer_free",
    "htracer_tname"
};

#define PUBLIC_SYMS_SIZE (sizeof(PUBLIC_SYMS) / sizeof(PUBLIC_SYMS[0]))

/**
 * Test that we can call the htrace_conf_from_strs function without aborting at
 * runtime.  This may seem like a trivial test, but keep in mind this is the
 * only unit test that uses the real libhtrace.so, not the testing library.
 */
static int test_call_htrace_conf_from_strs(void)
{
    struct htrace_conf *cnf;

    cnf = htrace_conf_from_str("foo=bar");
    htrace_conf_free(cnf);
    return EXIT_SUCCESS;
}

/**
 * Test that we can find all the public symbols in the library.
 */
static int find_public_symbols(void)
{
    int i;
    void *sym;

    for (i = 0; i < PUBLIC_SYMS_SIZE; i++) {
        sym = dlsym(RTLD_DEFAULT, PUBLIC_SYMS[i]);
        if (!sym) {
            fprintf(stderr, "Failed to find %s, or its value was NULL.\n",
                    PUBLIC_SYMS[i]);
            return EXIT_FAILURE;
        }
    }
    if (dlsym(RTLD_DEFAULT, "htrace_span_alloc")) {
        fprintf(stderr, "Found non-public symbol htrace_span_alloc.\n");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

int main(void)
{
    if (test_call_htrace_conf_from_strs()) {
        abort();
    }
    if (find_public_symbols()) {
        abort();
    }
    return EXIT_SUCCESS;
}

// vim: ts=4:sw=4:tw=79:et
