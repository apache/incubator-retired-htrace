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

#ifndef APACHE_HTRACE_SAMPLER_SAMPLER_H
#define APACHE_HTRACE_SAMPLER_SAMPLER_H

/**
 * @file sampler.h
 *
 * Functions related to HTrace samplers.
 *
 * This is an internal header, not intended for external use.
 */

#include <stdint.h>

struct htrace_conf;
struct htrace_log;
struct htracer;

/**
 * Base class for an HTrace sampler.
 *
 * Implementations should begin with this class as the first member.
 */
struct htrace_sampler {
    /**
     * The type of the sampler.
     */
    const struct htrace_sampler_ty *ty;
};

/**
 * A table of callbacks that implements an HTrace sampler.
 */
struct htrace_sampler_ty {
    /**
     * The name of this probability sampler type.
     *
     * This is used to select the sampler via the configuration.
     */
    const char *name;

    /**
     * Create an HTrace sampler of this type.
     *
     * @param tracer        The HTrace context to use.  The sampler may
     *                          hold on to this pointer.
     * @param conf          The HTrace configuration to use.  The sampler
     *                          must not hold on to this pointer.
     *
     * @return              The HTrace span receciver.
     */
    struct htrace_sampler *(*create)(struct htracer *tracer,
                                 const struct htrace_conf *conf);

    /**
     * Get the name of this HTrace sampler.
     *
     * @param smp           The sampler.
     *
     * @return              A description of this sampler object.  This string
     *                          must remain valid at least until the sampler
     *                          is freed.
     */
    const char*(*to_str)(struct htrace_sampler *smp);

    /**
     * Sampler callback.
     *
     * This callback must be able to be safely called by multiple threads
     * simultaneously.
     *
     * @param smp           The HTrace sampler.
     *
     * @return              1 to begin a new span; 0 otherwise.
     */
    int (*next)(struct htrace_sampler *smp);

    /**
     * Frees this HTrace sampler.
     *
     * @param rcv           The HTrace sampler.
     */
    void (*free)(struct htrace_sampler *smp);
};

/**
 * Get the configured fraction for the probability sampler.
 *
 * @param log           A log to send parse error messages to.
 * @param conf          The configuration to use.
 *
 * @return              A double between 0.0 and 1.0, inclusive.
 */
double get_prob_sampler_fraction(struct htrace_log *lg,
                                 struct htrace_conf *conf);

extern const struct htrace_sampler_ty g_never_sampler_ty;
extern const struct htrace_sampler_ty g_always_sampler_ty;
extern const struct htrace_sampler_ty g_prob_sampler_ty;
extern const struct always_sampler g_always_sampler;

#endif

// vim: ts=4:sw=4:et
