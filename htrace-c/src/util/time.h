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

#ifndef APACHE_HTRACE_UTIL_TIME_H
#define APACHE_HTRACE_UTIL_TIME_H

/**
 * @file time.h
 *
 * Functions for dealing with time.
 *
 * This is an internal header, not intended for external use.
 */

#include <stdint.h>

struct htrace_log;
struct timespec;
struct timeval;

/**
 * Convert a timespec into a time in milliseconds.
 *
 * @param ts            The timespec to convert.
 *
 * @return              The current time in milliseconds.
 */
uint64_t timespec_to_ms(const struct timespec *ts);

/**
 * Convert a time in milliseconds into a timespec.
 *
 * @param ms            The time in milliseconds to convert.
 * @param ts            (out param) The timespec to fill.
 */
void ms_to_timespec(uint64_t ms, struct timespec *ts);

/**
 * Convert a time in milliseconds into a timeval.
 *
 * @param ms            The time in milliseconds to convert.
 * @param tv            (out param) The timeval to fill.
 */
void ms_to_timeval(uint64_t ms, struct timeval *tv);

/**
 * Get the current wall-clock time in milliseconds.
 *
 * @param log           The log to use for error messsages.
 *
 * @return              The current wall-clock time in milliseconds.
 */
uint64_t now_ms(struct htrace_log *log);

/**
 * Get the current monotonic time in milliseconds.
 *
 * @param log           The log to use for error messsages.
 *
 * @return              The current monotonic clock time in milliseconds.
 */
uint64_t monotonic_now_ms(struct htrace_log *log);

/**
 * Sleep for at least a given number of milliseconds.
 *
 * @param ms            The number of milliseconds to sleep.
 */
void sleep_ms(uint64_t ms);

#endif

// vim: ts=4:sw=4:et
