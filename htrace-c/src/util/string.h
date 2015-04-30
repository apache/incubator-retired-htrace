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

#ifndef APACHE_HTRACE_UTIL_STRING_H
#define APACHE_HTRACE_UTIL_STRING_H

/**
 * @file string.h
 *
 * Functions for manipulating strings.
 *
 * This is an internal header, not intended for external use.
 */

struct htrace_log;

/**
 * Print to a buffer and move the pointer forward by the number of bytes
 * written.
 *
 * @param buf           (inout) The buffer to write to.  We will advance this
 *                          buffer by the number of bytes written.
 *                          If this buffer is NULL, nothing will be written.
 * @param rem           (inout) The maximum number of bytes to write to the
 *                          buffer.  If bytes are written to the buffer, this
 *                          number will be decremented by that amount.
 * @param fmt           Printf-style format string.
 *
 * @return              The number of bytes that would have been used if bytes
 *                          had been written
 */
int fwdprintf(char **buf, int* rem, const char *fmt, ...)
      __attribute__((format(printf, 3, 4)));

/**
 * Validate that a string could appear in a JSON expression without causing
 * problems.  We don't accept control characters, double quotes, backslashes,
 * tabs, newlines, or carriage returns.
 *
 * @param lg            The log to print messages about invalid strings to.
 * @param str           The string.
 *
 * @return              0 if the string is problematic; 1 if it's safe.
 */
int validate_json_string(struct htrace_log *lg, const char *str);

/**
 * Parse an endpoint string.
 *
 * We support a few different formats:
 * Hostname/port:
 *      my.hostname:9075
 * ipv4/port:
 *      127.0.0.1:9075
 * ipv6/port:
 *      [2001:db8:85a3:8d3:1319:8a2e:370:7348]:9075
 *
 * @param lg                    The htrace log object.
 * @param endpoint              The endpoint string.
 * @param default_port          The default port to use if no port is given.
 * @param remote                (out param) The remote name.  Malloced.
 * @param port                  (out param) The port.
 *
 * @return                      0 on failure; 1 on success.
 */
int parse_endpoint(struct htrace_log *lg, const char *endpoint,
                   int default_port, char **remote, int *port);

#endif

// vim: ts=4:sw=4:et
