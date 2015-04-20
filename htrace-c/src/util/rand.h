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

#ifndef APACHE_HTRACE_UTIL_RAND_H
#define APACHE_HTRACE_UTIL_RAND_H

/**
 * @file rand.h
 *
 * Functions for providing thread-safe random numbers.
 *
 * This is an internal header, not intended for external use.
 */

#include <stdint.h>

struct htrace_log;
struct random_src;

/**
 * Creates a random source.
 *
 * @param log     The htrace_log object to use.  We may hold on to a reference
 *                    to this log.
 * @return        NULL on OOM; the random source otherwise.
 */
struct random_src *random_src_alloc(struct htrace_log *lg);

/**
 * Frees a random source.
 *
 * @param rnd     The random source.
 */
void random_src_free(struct random_src *rnd);

/**
 * Gets a random 32-bit number from the random source.
 */
uint32_t random_u32(struct random_src *rnd);

/**
 * Gets a random 64-bit number from the random source.
 */
uint64_t random_u64(struct random_src *rnd);

#endif

// vim: ts=4:sw=4:et
