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

#ifndef APACHE_HTRACE_SCOPE_H
#define APACHE_HTRACE_SCOPE_H

/**
 * @file scope.h
 *
 * Functions related to trace scopes.
 *
 * This is an internal header, not intended for external use.
 */

struct htrace_span_id;

#include <stdint.h>

/**
 * A trace scope.
 *
 * Currently, trace scopes contain span data (there is no separate object for
 * the span data.)
 */
struct htrace_scope {
    /**
     * The HTracer object associated with this scope.  Cannot be NULL.
     * This memory is managed externally from the htrace_scope object.
     */
    struct htracer *tracer;

    /**
     * The parent scope, or NULL if this is a top-level scope.
     */
    struct htrace_scope *parent;

    /**
     * The span object associated with this scope, or NULL if there is none.
     */
    struct htrace_span *span;
};

#endif

// vim: ts=4:sw=4:et
