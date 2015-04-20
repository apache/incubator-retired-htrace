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

#include "sampler/sampler.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/**
 * A sampler that always fires.
 */
struct always_sampler {
    struct htrace_sampler base;
};

static struct htrace_sampler *always_sampler_create(struct htracer *tracer,
                                            const struct htrace_conf *conf);
static const char *always_sampler_to_str(struct htrace_sampler *sampler);
static int always_sampler_next(struct htrace_sampler *sampler);
static void always_sampler_free(struct htrace_sampler *sampler);

const struct htrace_sampler_ty g_always_sampler_ty = {
    "always",
    always_sampler_create,
    always_sampler_to_str,
    always_sampler_next,
    always_sampler_free,
};

const struct always_sampler g_always_sampler = {
    { (struct htrace_sampler_ty*) &g_always_sampler_ty },
};

static struct htrace_sampler *always_sampler_create(struct htracer *tracer,
                                            const struct htrace_conf *conf)
{
    return (struct htrace_sampler*)&g_always_sampler;
}

static const char *always_sampler_to_str(struct htrace_sampler *sampler)
{
    return "AlwaysSampler";
}

static int always_sampler_next(struct htrace_sampler *sampler)
{
    return 1;
}

static void always_sampler_free(struct htrace_sampler *sampler)
{
    // do nothing.
}

// vim: ts=4:sw=4:tw=79:et
