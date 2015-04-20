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

#include "core/htracer.h"
#include "receiver/receiver.h"
#include "util/log.h"

/**
 * A span receiver that does nothing but discard all spans.
 */
struct noop_rcv {
    struct htrace_rcv base;
};

struct noop_rcv g_noop_rcv = {
    { &g_noop_rcv_ty },
};

static struct htrace_rcv *noop_rcv_create(struct htracer *tracer,
                                          const struct htrace_conf *conf)
{
    htrace_log(tracer->lg, "Using no-op htrace span receiver.\n");
    return (struct htrace_rcv *)&g_noop_rcv;
}

static void noop_rcv_add_span(struct htrace_rcv *rcv,
                              struct htrace_span *span)
{
    // do nothing
}

static void noop_rcv_flush(struct htrace_rcv *rcv)
{
    // do nothing
}

static void noop_rcv_free(struct htrace_rcv *rcv)
{
    // do nothing
}

const struct htrace_rcv_ty g_noop_rcv_ty = {
    "noop",
    noop_rcv_create,
    noop_rcv_add_span,
    noop_rcv_flush,
    noop_rcv_free,
};

// vim:ts=4:sw=4:et
