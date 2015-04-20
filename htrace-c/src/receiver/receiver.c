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
#include "receiver/receiver.h"
#include "util/log.h"

#include <stdint.h>
#include <string.h>

const struct htrace_rcv_ty * const g_rcv_tys[] = {
    &g_noop_rcv_ty,
    &g_local_file_rcv_ty,
    &g_htraced_rcv_ty,
    NULL,
};

static const struct htrace_rcv_ty *select_rcv_ty(struct htracer *tracer,
                                             const struct htrace_conf *conf)
{
    const char *tstr;
    const char *prefix = "";
    size_t i;
    char buf[256] = { 0 };

    tstr = htrace_conf_get(conf, HTRACE_SPAN_RECEIVER_KEY);
    if (!tstr) {
        htrace_log(tracer->lg, "No %s configured.\n", HTRACE_SPAN_RECEIVER_KEY);
        return &g_noop_rcv_ty;
    }
    for (i = 0; g_rcv_tys[i]; i++) {
        if (strcmp(g_rcv_tys[i]->name, tstr) == 0) {
            return g_rcv_tys[i];
        }
    }
    for (i = 0; g_rcv_tys[i]; i++) {
        if ((strlen(buf) + strlen(prefix) +
                 strlen(g_rcv_tys[i]->name)) < sizeof(buf)) {
            strcat(buf, prefix);
            strcat(buf, g_rcv_tys[i]->name);
            prefix = ", ";
        }
    }
    htrace_log(tracer->lg, "Unknown span receiver type as '%s'.  Valid "
               "span receiver types are: %s\n", tstr, buf);
    return &g_noop_rcv_ty;
}

struct htrace_rcv *htrace_rcv_create(struct htracer *tracer,
                                     const struct htrace_conf *conf)
{
    const struct htrace_rcv_ty *ty;

    ty = select_rcv_ty(tracer, conf);
    return ty->create(tracer, conf);
}

// vim:ts=4:sw=4:et
