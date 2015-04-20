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
#include "sampler/sampler.h"
#include "util/log.h"

#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

const struct htrace_sampler_ty * const g_sampler_tys[] = {
    &g_never_sampler_ty,
    &g_always_sampler_ty,
    &g_prob_sampler_ty,
    NULL,
};

static const struct htrace_sampler_ty *select_sampler_ty(
                        struct htracer *tracer, const struct htrace_conf *cnf)
{
    const char *tstr;
    const char *prefix = "";
    size_t i;
    char buf[256] = { 0 };

    tstr = htrace_conf_get(cnf, HTRACE_SAMPLER_KEY);
    if (!tstr) {
        htrace_log(tracer->lg, "No %s configured.\n", HTRACE_SAMPLER_KEY);
        return &g_never_sampler_ty;
    }
    for (i = 0; g_sampler_tys[i]; i++) {
        if (strcmp(g_sampler_tys[i]->name, tstr) == 0) {
            return g_sampler_tys[i];
        }
    }
    for (i = 0; g_sampler_tys[i]; i++) {
        if ((strlen(buf) + strlen(prefix) +
                 strlen(g_sampler_tys[i]->name)) < sizeof(buf)) {
            strcat(buf, prefix);
            strcat(buf, g_sampler_tys[i]->name);
            prefix = ", ";
        }
    }
    htrace_log(tracer->lg, "Unknown sampler type '%s'.  Valid "
               "sampler types are: %s\n", tstr, buf);
    return &g_never_sampler_ty;
}

struct htrace_sampler *htrace_sampler_create(struct htracer *tracer,
                                             struct htrace_conf *cnf)
{
    const struct htrace_sampler_ty *ty;

    ty = select_sampler_ty(tracer, cnf);
    return ty->create(tracer, cnf);
}

const char *htrace_sampler_to_str(struct htrace_sampler *smp)
{
    return smp->ty->to_str(smp);
}

void htrace_sampler_free(struct htrace_sampler *smp)
{
    return smp->ty->free(smp);
}

// vim: ts=4:sw=4:tw=79:et
