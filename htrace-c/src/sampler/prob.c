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
#include "util/rand.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/**
 * A sampler that fires with a certain chance.
 */
struct prob_sampler {
    struct htrace_sampler base;

    /**
     * A random source.
     */
    struct random_src *rnd;

    /**
     * The name of this probability sampler.
     */
    char *name;

    /**
     * The threshold at which we should sample.
     */
    uint32_t threshold;
};

static double get_prob_sampler_threshold(struct htrace_log *lg,
                                         const struct htrace_conf *conf);
static struct htrace_sampler *prob_sampler_create(struct htracer *tracer,
                                          const struct htrace_conf *conf);
static const char *prob_sampler_to_str(struct htrace_sampler *s);
static int prob_sampler_next(struct htrace_sampler *s);
static void prob_sampler_free(struct htrace_sampler *s);

const struct htrace_sampler_ty g_prob_sampler_ty = {
    "prob",
    prob_sampler_create,
    prob_sampler_to_str,
    prob_sampler_next,
    prob_sampler_free,
};

static double get_prob_sampler_threshold(struct htrace_log *lg,
                                       const struct htrace_conf *conf)
{
    double fraction =
        htrace_conf_get_double(lg, conf, HTRACE_PROB_SAMPLER_FRACTION_KEY);
    if (fraction < 0) {
        htrace_log(lg, "sampler_create: can't have a sampling fraction "
                   "less than 0.  Setting fraction to 0.\n");
        fraction = 0.0;
    } else if (fraction > 1.0) {
        htrace_log(lg, "sampler_create: can't have a sampling fraction "
                   "greater than 1.  Setting fraction to 1.\n");
        fraction = 1.0;
    }
    return fraction;
}

static struct htrace_sampler *prob_sampler_create(struct htracer *tracer,
                                          const struct htrace_conf *conf)
{
    struct prob_sampler *smp;
    double fraction;

    smp = calloc(1, sizeof(*smp));
    if (!smp) {
        htrace_log(tracer->lg, "prob_sampler_create: OOM\n");
        return NULL;
    }
    smp->base.ty = &g_prob_sampler_ty;
    smp->rnd = random_src_alloc(tracer->lg);
    if (!smp->rnd) {
        htrace_log(tracer->lg, "random_src_alloc failed.\n");
        free(smp);
        return NULL;
    }
    fraction = get_prob_sampler_threshold(tracer->lg, conf);
    smp->threshold = 0xffffffffLU * fraction;
    if (asprintf(&smp->name, "ProbabilitySampler(fraction=%.03g)",
                 fraction) < 0) {
        smp->name = NULL;
        random_src_free(smp->rnd);
        free(smp);
    }
    return (struct htrace_sampler *)smp;
}

static const char *prob_sampler_to_str(struct htrace_sampler *s)
{
    struct prob_sampler *smp = (struct prob_sampler *)s;
    return smp->name;
}

static int prob_sampler_next(struct htrace_sampler *s)
{
    struct prob_sampler *smp = (struct prob_sampler *)s;
    return random_u32(smp->rnd) < smp->threshold;
}

static void prob_sampler_free(struct htrace_sampler *s)
{
    struct prob_sampler *smp = (struct prob_sampler *)s;
    random_src_free(smp->rnd);
    free(smp->name);
    free(smp);
}

// vim: ts=4:sw=4:tw=79:et
