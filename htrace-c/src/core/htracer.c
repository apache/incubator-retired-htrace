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
#include "core/htrace.h"
#include "core/htracer.h"
#include "core/scope.h"
#include "core/span.h"
#include "receiver/receiver.h"
#include "util/log.h"
#include "util/process_id.h"
#include "util/rand.h"
#include "util/string.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/**
 * @file htracer.c
 *
 * Implementation of the Tracer object.
 */

struct htracer *htracer_create(const char *tname,
                               const struct htrace_conf *cnf)
{
    struct htracer *tracer;
    int ret;

    tracer = calloc(1, sizeof(*tracer));
    if (!tracer) {
        return NULL;
    }
    tracer->lg = htrace_log_alloc(cnf);
    if (!tracer->lg) {
        free(tracer);
        return NULL;
    }
    ret = pthread_key_create(&tracer->tls, NULL);
    if (ret) {
        htrace_log(tracer->lg, "htracer_create: pthread_key_create "
                   "failed: %s.\n", terror(ret));
        htrace_log_free(tracer->lg);
        return NULL;
    }
    tracer->tname = strdup(tname);
    if (!tracer->tname) {
        htrace_log(tracer->lg, "htracer_create: failed to "
                   "duplicate name string.\n");
        htracer_free(tracer);
        return NULL;
    }
    tracer->prid = calculate_process_id(tracer->lg,
            htrace_conf_get(cnf, HTRACE_PROCESS_ID), tname);
    if (!tracer->prid) {
        htrace_log(tracer->lg, "htracer_create: failed to "
                   "create process id string.\n");
        htracer_free(tracer);
        return NULL;
    }
    if (!validate_json_string(tracer->lg, tracer->prid)) {
        htrace_log(tracer->lg, "htracer_create: process ID string '%s' is "
                   "problematic.\n", tracer->prid);
        htracer_free(tracer);
        return NULL;
    }
    tracer->rnd = random_src_alloc(tracer->lg);
    if (!tracer->rnd) {
        htrace_log(tracer->lg, "htracer_create: failed to "
                   "allocate a random source.\n");
        htracer_free(tracer);
        return NULL;
    }
    tracer->rcv = htrace_rcv_create(tracer, cnf);
    if (!tracer->rcv) {
        htrace_log(tracer->lg, "htracer_create: failed to "
                   "create a receiver.\n");
        htracer_free(tracer);
        return NULL;
    }
    return tracer;
}

const char *htracer_tname(const struct htracer *tracer)
{
    return tracer->tname;
}

void htracer_free(struct htracer *tracer)
{
    struct htrace_rcv *rcv;

    if (!tracer) {
        return;
    }
    pthread_key_delete(tracer->tls);
    rcv = tracer->rcv;
    if (rcv) {
        rcv->ty->free(rcv);
    }
    random_src_free(tracer->rnd);
    free(tracer->tname);
    free(tracer->prid);
    htrace_log_free(tracer->lg);
    free(tracer);
}

struct htrace_scope *htracer_cur_scope(struct htracer *tracer)
{
    return pthread_getspecific(tracer->tls);
}

int htracer_push_scope(struct htracer *tracer, struct htrace_scope *cur,
                           struct htrace_scope *next)
{
    int ret;
    next->parent = cur;
    ret = pthread_setspecific(tracer->tls, next);
    if (ret) {
        htrace_log(tracer->lg, "htracer_push_scope: pthread_setspecific "
                   "failed: %s\n", terror(ret));
        return EIO;
    }
    return 0;
}

int htracer_pop_scope(struct htracer *tracer, struct htrace_scope *scope)
{
    struct htrace_scope *cur_scope;
    int ret;

    cur_scope = pthread_getspecific(tracer->tls);
    if (cur_scope != scope) {
        htrace_log(tracer->lg, "htracer_pop_scope: attempted to pop a scope "
                   "that wasn't the top of the stack.  Current top of stack: "
                   "%s.  Attempted to pop: %s.\n",
                   (cur_scope->span ? cur_scope->span->desc : "(detached)"),
                   (scope->span ? scope->span->desc : "(detached)"));
        return EIO;
    }
    ret = pthread_setspecific(tracer->tls, scope->parent);
    if (ret) {
        htrace_log(tracer->lg, "htracer_pop_scope: pthread_setspecific "
                   "failed: %s\n", terror(ret));
        return EIO;
    }
    return 0;
}

// vim:ts=4:sw=4:et
