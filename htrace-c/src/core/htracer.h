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

#ifndef APACHE_HTRACE_CORE_TRACER_H
#define APACHE_HTRACE_CORE_TRACER_H

#include <pthread.h> /* for pthread_key_t */

/**
 * @file tracer.h
 *
 * The HTracer object.
 *
 * This is an internal header, not intended for external use.
 */

struct htrace_log;
struct htrace_rcv;
struct random_src;

struct htracer {
    /**
     * Key for thread-local data.
     */
    pthread_key_t tls;

    /**
     * The htrace log to use.
     */
    struct htrace_log *lg;

    /**
     * The name of this tracer.
     */
    char *tname;

    /**
     * The process id of this context.
     */
    char *prid;

    /**
     * The random source to use in this context.
     */
    struct random_src *rnd;

    /**
     * The span receiver to use.
     */
    struct htrace_rcv *rcv;
};

/**
 * Get the current scope in a given context.
 *
 * @param tracer            The context.
 *
 * @return                  The current scope, or NULL if there is none.
 */
struct htrace_scope *htracer_cur_scope(struct htracer *tracer);

/**
 * Push another scope on to the current context.
 *
 * @param tracer            The context.
 * @param cur               The current scope on the context.
 * @param next              The scope to push.
 *
 * @return                  0 on success; nonzero otherwise.
 */
int htracer_push_scope(struct htracer *tracer, struct htrace_scope *cur,
                       struct htrace_scope *next);

/**
 * Pop a scope from the current context.
 *
 * @param tracer            The context.
 * @param scope             The scope which should be the top of the stack.
 *
 * @return                  0 on success; nonzero otherwise.
 */
int htracer_pop_scope(struct htracer *tracer, struct htrace_scope *scope);

#endif

// vim: ts=4: sw=4: et
