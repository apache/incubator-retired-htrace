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
#include <fcntl.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define URANDOM_PATH "/dev/urandom"

#define PSAMP_THREAD_LOCAL_BUF_LEN 256

/**
 * @file rand_linux.c
 *
 * A Linux implementation of a random number source.  This implementation reads
 * random numbers from /dev/urandom.  To avoid reading from /dev/urandom too
 * often, we have a thread-local cache of random data.  This is done using ELF
 * TLS.
 */

struct random_src {
    /**
     * The HTrace log.
     */
    struct htrace_log *lg;

    /**
     * File descriptor for /dev/urandom.
     */
    int urandom_fd;
};

/**
 * A thread-local cache of random bits.
 */
static __thread uint32_t g_rnd_cache[PSAMP_THREAD_LOCAL_BUF_LEN];

/**
 * An index into our thread-local cache of random bits.
 */
static __thread int g_rnd_cache_idx = PSAMP_THREAD_LOCAL_BUF_LEN;

static void refill_rand_cache(struct random_src *rnd)
{
    size_t total = 0;

    while (1) {
        ssize_t res;
        ssize_t rem = (PSAMP_THREAD_LOCAL_BUF_LEN * sizeof(uint32_t)) - total;
        if (rem == 0) {
            break;
        }
        res = read(rnd->urandom_fd, ((uint8_t*)&g_rnd_cache) + total, rem);
        if (res < 0) {
            int err = errno;
            if (err == EINTR) {
                continue;
            }
            htrace_log(rnd->lg, "refill_rand_cache: error refilling "
                       "random cache: %d (%s)\n", err,
                       terror(err));
            return;
        }
        total += res;
    }
    g_rnd_cache_idx = 0;
}

struct random_src *random_src_alloc(struct htrace_log *lg)
{
    struct random_src *rnd;
    int err;

    rnd = calloc(1, sizeof(*rnd));
    if (!rnd) {
        htrace_log(lg, "random_src_alloc: OOM\n");
        return NULL;
    }
    rnd->urandom_fd = open(URANDOM_PATH, O_RDONLY);
    if (rnd->urandom_fd < 0) {
        err = errno;
        htrace_log(lg, "random_src_alloc: failed to open "
                   URANDOM_PATH ": error %d (%s)\n", err,
                   terror(err));
        free(rnd);
        return NULL;
    }
    rnd->lg = lg;
    return rnd;
}

void random_src_free(struct random_src *rnd)
{
    if (!rnd) {
        return;
    }
    if (close(rnd->urandom_fd)) {
        int err = errno;
        htrace_log(rnd->lg, "linux_prob_sampler_free: close error: "
                   "%d (%s)\n", err, terror(err));
    }
    free(rnd);
}

uint32_t random_u32(struct random_src *rnd)
{
    if (g_rnd_cache_idx >= PSAMP_THREAD_LOCAL_BUF_LEN) {
        refill_rand_cache(rnd);
    }
    return g_rnd_cache[g_rnd_cache_idx++];
}

uint64_t random_u64(struct random_src *rnd)
{
    uint64_t val = random_u32(rnd);
    val <<= 32;
    return val | random_u32(rnd);
}

// vim: ts=4:sw=4:tw=79:et
