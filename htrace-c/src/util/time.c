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
#include "util/time.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

uint64_t timespec_to_ms(const struct timespec *ts)
{
    uint64_t seconds_ms, microseconds_ms;
    seconds_ms = ts->tv_sec;
    seconds_ms *= 1000LLU;
    microseconds_ms = ts->tv_nsec;
    microseconds_ms /= 1000000LLU;
    return seconds_ms + microseconds_ms;
}

void ms_to_timespec(uint64_t ms, struct timespec *ts)
{
    uint64_t sec = ms / 1000LLU;
    ts->tv_sec = sec;
    ms -= (sec * 1000LLU);
    ts->tv_nsec = ms * 1000000LLU;
}

uint64_t now_ms(struct htrace_log *lg)
{
    struct timespec ts;
    int err;

    if (clock_gettime(CLOCK_REALTIME, &ts)) {
        err = errno;
        if (lg) {
            htrace_log(lg, "clock_gettime(CLOCK_REALTIME) error: %d (%s)\n",
                       err, terror(err));
        }
        return 0;
    }
    return timespec_to_ms(&ts);
}

uint64_t monotonic_now_ms(struct htrace_log *lg)
{
    struct timespec ts;
    int err;

    if (clock_gettime(CLOCK_MONOTONIC, &ts)) {
        err = errno;
        if (lg) {
            htrace_log(lg, "clock_gettime(CLOCK_MONOTONIC) error: %d (%s)\n",
                       err, terror(err));
        }
        return 0;
    }
    return timespec_to_ms(&ts);
}

void sleep_ms(uint64_t ms)
{
    struct timespec req, rem;

    ms_to_timespec(ms, &req);
    memset(&rem, 0, sizeof(rem));
    do {
        if (nanosleep(&req, &rem) < 0) {
            if (errno == EINTR) {
                rem.tv_sec = req.tv_sec;
                rem.tv_nsec = req.tv_nsec;
                continue;
            }
        }
    } while (0);
}

// vim: ts=4:sw=4:et
