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

#ifndef APACHE_HTRACE_SPAN_H
#define APACHE_HTRACE_SPAN_H

/**
 * @file span.h
 *
 * Functions related to HTrace spans and trace scopes.
 *
 * This is an internal header, not intended for external use.
 */

#include <stdint.h>

struct cmp_ctx_s;
struct htracer;

struct htrace_span {
    /**
     * The name of this trace scope.
     * Dynamically allocated.  Will never be NULL.
     */
    char *desc;

    /**
     * The beginning time in wall-clock milliseconds.
     */
    uint64_t begin_ms;

    /**
     * The end time in wall-clock milliseconds.
     */
    uint64_t end_ms;

    /**
     * The span id.
     */
    uint64_t span_id;

    /**
     * The tracer ID of this trace scope.
     * Dynamically allocated.  May be null.
     */
    char *trid;

    /**
     * The number of parents.
     */
    int num_parents;

    union {
        /**
         * If there is 1 parent, this is the parent ID.
         */
        uint64_t single;

        /**
         * If there are multiple parents, this is a pointer to a dynamically
         * allocated array of parent IDs.
         */
        uint64_t *list;
    } parent;
};

/**
 * Allocate an htrace span.
 *
 * @param desc          The span name to use.  Will be deep-copied.
 * @param begin_ms      The value to use for begin_ms.
 * @param span_id       The span ID to use.
 *
 * @return              NULL on OOM; the span otherwise.
 */
struct htrace_span *htrace_span_alloc(const char *desc,
                uint64_t begin_ms, uint64_t span_id);

/**
 * Free the memory associated with an htrace span.
 *
 * @param span          The span to free.
 */
void htrace_span_free(struct htrace_span *span);

/**
 * Sort and deduplicate the parents array within the span.
 *
 * @param span          The span to process.
 */
void htrace_span_sort_and_dedupe_parents(struct htrace_span *span);

/**
 * Get the buffer size that would be needed to serialize this span to a buffer.
 *
 * @param span          The span.
 *
 * @return              The buffer size in bytes.  This will never be less
 *                          than 1.
 */
int span_json_size(const struct htrace_span *span);

/**
 * Get the buffer size that would be needed to serialize this span to a buffer.
 *
 * @param span          The span.
 *
 * @return              The buffer size in bytes.
 */
void span_json_sprintf(const struct htrace_span *span, int max, void *buf);

/**
 * Write a span to the provided CMP context.
 *
 * @param span          The span.
 * @param ctx           The CMP context.
 *
 * @return              0 on failure; 1 on success.
 */
int span_write_msgpack(const struct htrace_span *span, struct cmp_ctx_s *ctx);

#endif

// vim: ts=4:sw=4:et
