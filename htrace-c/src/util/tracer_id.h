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

#ifndef APACHE_HTRACE_UTIL_TRACER_ID_H
#define APACHE_HTRACE_UTIL_TRACER_ID_H

#include <unistd.h> /* for size_t */

/**
 * @file tracer_id.h
 *
 * Implements process IDs for the HTrace C client.
 *
 * This is an internal header, not intended for external use.
 */

struct htrace_log;

/**
 * Calculate the tracer ID.
 *
 * @param lg                A log object which will be used to report warnings.
 * @param fmt               The user-provided string to use when calculating the
 *                              tracer ID.
 * @param tname             The name supplied when creating the htracer.
 *
 * @return                  NULL on OOM; the tracer ID otherwise.
 */
char *calculate_tracer_id(struct htrace_log *lg, const char *fmt,
                           const char *tname);

/**
 * Get the best IP address representing this host.
 *
 * @param lg                A log object which will be used to report warnings.
 * @param ip_str            (out param) output string
 * @param ip_str_len        Length of output string
 */
void get_best_ip(struct htrace_log *lg, char *ip_str, size_t ip_str_len);

#endif

// vim: ts=4: sw=4: et
