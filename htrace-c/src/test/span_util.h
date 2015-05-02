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

#ifndef APACHE_HTRACE_TEST_SPAN_UTIL_H
#define APACHE_HTRACE_TEST_SPAN_UTIL_H

#include <stdint.h>
#include <unistd.h> /* for size_t */

struct cmp_ctx_s;
struct htrace_span;

/**
 * Parses 64-bit hex ID.
 *
 * @param in        The hex ID string.
 * @param err       (out param) On error, where the error message will be
 *                      written.  Will be set to the empty string on success.
 * @param err_len   The length of the error buffer.  Must be nonzero.
 */
uint64_t parse_hex_id(const char *in, char *err, size_t err_len);

/**
 * Parses a span JSON entry back into a span.
 *
 * This function is just used in unit tests and is not optimized.
 *
 * @param in        The span string.
 * @param span      (out param) On success, the dynamically allocated span
 *                      object.
 * @param err       (out param) On error, where the error message will be
 *                      written.  Will be set to the empty string on success.
 * @param err_len   The length of the error buffer.  Must be nonzero.
 */
void span_json_parse(const char *in, struct htrace_span **span,
                     char *err, size_t err_len);

/**
 * Compare two spans.
 *
 * @param a         The first span.
 * @param b         The second span.
 *
 * @return          A negative number if the first span is less;
 *                  0 if the spans are equivalent;
 *                  A positive number if the first span is greater.
 */
int span_compare(struct htrace_span *a, struct htrace_span *b);

/**
 * Read a span from the provided CMP context.
 *
 * @param ctx       The CMP context.
 * @param err       (out param) On error, where the error message will be
 *                      written.  Will be set to the empty string on success.
 * @param err_len   The length of the error buffer.  Must be nonzero.
 *
 * @return          The span on success; NULL otherwise.
 */
struct htrace_span *span_read_msgpack(struct cmp_ctx_s *ctx,
                                      char *err, size_t err_len);

#endif

// vim:ts=4:sw=4:et
