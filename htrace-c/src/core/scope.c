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
#include "core/htracer.h"
#include "core/scope.h"
#include "core/span.h"
#include "receiver/receiver.h"
#include "sampler/sampler.h"
#include "util/log.h"
#include "util/rand.h"
#include "util/string.h"
#include "util/time.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/**
 * @file scope.c
 *
 * Implementation of HTrace scopes.
 */

struct htrace_scope* htrace_start_span(struct htracer *tracer,
        struct htrace_sampler *sampler, const char *desc)
{
    struct htrace_scope *cur_scope, *scope = NULL, *pscope;
    struct htrace_span *span = NULL;
    uint64_t span_id;

    // Validate the description string.  This ensures that it doesn't have
    // anything silly in it like embedded double quotes, backslashes, or control
    // characters.
    if (!validate_json_string(tracer->lg, desc)) {
        htrace_log(tracer->lg, "htrace_span_alloc(desc=%s): invalid "
                   "description string.\n", desc);
        return NULL;
    }
    cur_scope = htracer_cur_scope(tracer);
    if (!cur_scope) {
        if (!sampler->ty->next(sampler)) {
            return NULL;
        }
    }
    do {
        span_id = random_u64(tracer->rnd);
    } while (span_id == 0);
    span = htrace_span_alloc(desc, now_ms(tracer->lg), span_id);
    if (!span) {
        htrace_log(tracer->lg, "htrace_span_alloc(desc=%s): OOM\n", desc);
        return NULL;
    }
    scope = malloc(sizeof(*scope));
    if (!scope) {
        htrace_span_free(span);
        htrace_log(tracer->lg, "htrace_start_span(desc=%s): OOM\n", desc);
        return NULL;
    }
    scope->tracer = tracer;
    scope->span = span;

    // Search enclosing trace scopes for the first one that hasn't disowned
    // its trace span.
    for (pscope = cur_scope; pscope; pscope = pscope->parent) {
        struct htrace_span *pspan = pscope->span;
        if (pspan) {
            span->parent.single = pspan->span_id;
            span->num_parents = 1;
            break;
        }
        pscope = pscope->parent;
    }
    if (htracer_push_scope(tracer, cur_scope, scope) != 0) {
        htrace_span_free(span);
        free(scope);
        return NULL;
    }
    return scope;
}

struct htrace_span *htrace_scope_detach(struct htrace_scope *scope)
{
    struct htrace_span *span = scope->span;

    if (span == NULL) {
        htrace_log(scope->tracer->lg, "htrace_scope_detach: attempted to "
                   "detach a scope which was already detached.\n");
        return NULL;
    }
    scope->span = NULL;
    return span;
}

struct htrace_scope* htrace_restart_span(struct htracer *tracer,
                                         struct htrace_span *span)
{
    struct htrace_scope *cur_scope, *scope = NULL;

    scope = malloc(sizeof(*scope));
    if (!scope) {
        htrace_span_free(span);
        htrace_log(tracer->lg, "htrace_start_span(desc=%s, parent_id=%016"PRIx64
                   "): OOM\n", span->desc, span->span_id);
        return NULL;
    }
    scope->tracer = tracer;
    scope->parent = NULL;
    scope->span = span;
    cur_scope = htracer_cur_scope(tracer);
    if (htracer_push_scope(tracer, cur_scope, scope) != 0) {
        htrace_span_free(span);
        free(scope);
        return NULL;
    }
    return scope;
}

uint64_t htrace_scope_get_span_id(const struct htrace_scope *scope)
{
    struct htrace_span *span;

    if (!scope) {
        return 0;
    }
    span = scope->span;
    return span ? span->span_id : 0;
}

void htrace_scope_close(struct htrace_scope *scope)
{
    struct htracer *tracer;

    if (!scope) {
        return;
    }
    tracer = scope->tracer;
    if (htracer_pop_scope(tracer, scope) == 0) {
        struct htrace_span *span = scope->span;
        if (span) {
            struct htrace_rcv *rcv = tracer->rcv;
            span->end_ms = now_ms(tracer->lg);
            rcv->ty->add_span(rcv, span);
            htrace_span_free(span);
        }
        free(scope);
    }
}

// vim:ts=4:sw=4:et
