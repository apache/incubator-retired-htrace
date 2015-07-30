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

#ifndef APACHE_HTRACE_SPAN_ID_H
#define APACHE_HTRACE_SPAN_ID_H

/**
 * @file span_id.h
 *
 * Functions related to HTrace span IDs.
 *
 * This is an internal header, not intended for external use.
 */

#include <stdint.h> // for uint64_t
#include <unistd.h> // for size_t

struct cmp_ctx_s;
struct random_src;

/**
 * Length of an HTrace span ID in hexadecimal string form.
 */
#define HTRACE_SPAN_ID_STRING_LENGTH 32

/**
 * The number of bytes in the HTrace span ID
 */
#define HTRACE_SPAN_ID_NUM_BYTES 16

/**
 * The invalid span ID, which is all zeroes.
 */
extern const struct htrace_span_id INVALID_SPAN_ID;

/**
 * The HTrace span id.
 */
struct htrace_span_id {
    uint64_t high;
    uint64_t low;
};

/**
 * Parse a string containing an HTrace span ID.
 *
 * @param id            The HTrace span ID to fill in.
 * @param str           The string to parse.
 *
 */
void htrace_span_id_parse(struct htrace_span_id *id, const char *str,
                         char *err, size_t err_len);

/**
 * Write an HTrace span ID to a string.
 *
 * @param id            The HTrace span ID.
 * @param str           Where to put the string.
 * @param len           The length of the string buffer.
 * @param err           The error buffer to be set on failure.
 * @param err_len       Length of the error buffer.
 *
 * @return              1 on success; 0 if the length was not long enough, or
 *                          there was an internal snprintf error.
 */
int htrace_span_id_to_str(const struct htrace_span_id *id,
                          char *str, size_t len);

/**
 * Copy an htrace span ID.
 *
 * dst and src can be the same.
 *
 * @param dst           The destination span ID.
 * @param src           The source span ID.
 */
void htrace_span_id_copy(struct htrace_span_id *dst,
                         const struct htrace_span_id *src);

/**
 * Write this span ID to the provided CMP context.
 *
 * @param span          The span.
 * @param ctx           The CMP context.
 *
 * @return              0 on failure; 1 on success.
 */
int htrace_span_id_write_msgpack(const struct htrace_span_id *id,
                                 struct cmp_ctx_s *ctx);

/**
 * Read this span ID from the provided CMP context.
 *
 * @param span          The span.
 * @param ctx           The CMP context.
 *
 * @return              0 on failure; 1 on success.
 */
int htrace_span_id_read_msgpack(struct htrace_span_id *id,
                                struct cmp_ctx_s *ctx);

/**
 * Generate a new span ID.
 *
 * @param id            The span ID to alter.
 * @param rnd           The random source.
 * @param parent        The parent span ID, or null if there is none.
 */
void htrace_span_id_generate(struct htrace_span_id *id, struct random_src *rnd,
                             const struct htrace_span_id *parent);

/**
 * Set a span ID to the invalid span ID by clearing it.
 *
 * @param id            The span ID to clear.
 */
void htrace_span_id_clear(struct htrace_span_id *id);

/**
 * Compare two span IDs.
 *
 * @param a             The first span ID.
 * @param b             The second span ID.
 *
 * @return              A number less than 0 if the first span ID is less;
 *                      A number greater than 0 if the first span ID is greater;
 *                      0 if the span IDs are equal.
 */
int htrace_span_id_compare(const struct htrace_span_id *a,
                           const struct htrace_span_id *b);

#endif

// vim: ts=4:sw=4:et
