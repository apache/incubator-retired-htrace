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

#ifndef APACHE_HTRACE_UTIL_LOG_H
#define APACHE_HTRACE_UTIL_LOG_H

/**
 * @file log.h
 *
 * Functions for HTrace logging.
 *
 * This is an internal header, not intended for external use.
 */

struct htrace_conf;

/**
 * Allocate a new htrace_log.
 *
 * @param conf          The configuration.  Will be deep copied.
 *
 * @return              The htrace_log, or NULL on OOM.
 */
struct htrace_log *htrace_log_alloc(const struct htrace_conf *conf);

/**
 * A thread-safe version of the strerror call.
 *
 * @param err           The POSIX error code.
 *
 * @return              A static or thread-local error string.
 */
const char *terror(int err);

/**
 * Free an htrace_log.
 *
 * @param lg            The log to be freed.
 */
void htrace_log_free(struct htrace_log *lg);

/**
 * Create an htrace log message.
 *
 * @param lg            The log to use.
 * @param fmt           The format string to use.
 * @param ...           Printf-style variable length arguments.
 */
void htrace_log(struct htrace_log *lg, const char *fmt, ...)
      __attribute__((format(printf, 2, 3)));

#endif

// vim: ts=4:sw=4:et
