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

#include "util/log.h"
#include "util/rand.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/**
 * @file rand_posix.c
 *
 * A POSIX implementation of random number source.  We have to use a mutex here
 * to protect the rand_r() state.  If we used rand() instead, we would also
 * need to use a mutex.  The standard POSIX functions for getting random
 * numbers are actually really unfortunate in many ways.  Hopefully we can
 * provide platform-specific implementatinos of the probability sampler for all
 * the major platforms.
 */

/**
 * A sampler that fires with a certain probability.
 */
struct random_src {
    /**
     * Mutex protecting rand_r.
     */
    pthread_mutex_t lock;

    /**
     * State used with rand_r.
     */
    unsigned int rand_state;
};

struct random_src *random_src_alloc(struct htrace_log *lg)
{
    struct random_src *rnd;
    int ret;

    rnd = calloc(1, sizeof(*rnd));
    if (!rnd) {
        htrace_log(lg, "random_src_alloc: OOM\n");
        return NULL;
    }
    ret = pthread_mutex_init(&rnd->lock, NULL);
    if (ret) {
        htrace_log(lg, "random_src_alloc: pthread_mutex_create "
                   "failed: error %d (%s)\n", ret, terror(ret));
        free(rnd);
        return NULL;
    }
    return rnd;
}

void random_src_free(struct random_src *rnd)
{
    if (!rnd) {
        return;
    }
    pthread_mutex_destroy(&rnd->lock);
    free(rnd);
}

uint32_t random_u32(struct random_src *rnd)
{
    uint32_t val = 0;
    pthread_mutex_lock(&rnd->lock);
    // rand_r gives at least 15 bits of randomness.
    // So we need to xor it 3 times to get 32 bits' worth.
    val ^= rand_r(&rnd->rand_state);
    val <<= 15;
    val ^= rand_r(&rnd->rand_state);
    val <<= 15;
    val ^= rand_r(&rnd->rand_state);
    val <<= 15;
    pthread_mutex_unlock(&rnd->lock);
    return val;
}

uint64_t random_u64(struct random_src *rnd)
{
    uint64_t val = 0;
    pthread_mutex_lock(&rnd->lock);
    // rand_r gives at least 15 bits of randomness.
    // So we need to xor it 5 times to get 64 bits' worth.
    val ^= rand_r(&rnd->rand_state);
    val <<= 15;
    val ^= rand_r(&rnd->rand_state);
    val <<= 15;
    val ^= rand_r(&rnd->rand_state);
    val <<= 15;
    val ^= rand_r(&rnd->rand_state);
    val <<= 15;
    val ^= rand_r(&rnd->rand_state);
    val <<= 15;
    pthread_mutex_unlock(&rnd->lock);
    return val;
}

// vim: ts=4:sw=4:tw=79:et
