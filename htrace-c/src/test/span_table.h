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

#ifndef APACHE_HTRACE_TEST_SPAN_TABLE_H
#define APACHE_HTRACE_TEST_SPAN_TABLE_H

/**
 * @file span_table.h
 *
 * Implements a hash table containing trace spans.  They are indexed by span
 * description.
 *
 * This is an internal header, not intended for external use.
 */

struct htrace_span;
struct span_table;

/**
 * Allocate a span table.
 *
 * @return              NULL on OOM; the span table otherwise.
 */
struct span_table *span_table_alloc(void);

/**
 * Retrieve a span from the table.
 *
 * @param st            The span table.
 * @param out           (out param) the span.  This pointer will be valid until
 *                          the span table is freed.
 * @param desc          The span description to look for.
 * @param trid          The process ID to verify that the span has.
 *
 * @return              0 on success; nonzero otherwise.
 */
int span_table_get(struct span_table *st, struct htrace_span **out,
                   const char *desc, const char *trid);

/**
 * Add a span to the table.
 *
 * @param st            The span table.
 * @param span          The span to add.  Its memory will be managed by the
 *                          span table after span_table_put is called.
 *
 * @return              0 on success; nonzero otherwise.
 */
int span_table_put(struct span_table *st, struct htrace_span *span);

/**
 * Free a span table.  All spans inside will be freed.
 *
 * @param st            The span table.
 */
void span_table_free(struct span_table *st);

/**
 * Get the size of the span table.
 *
 * @return              The number of entries in the span table.
 */
uint32_t span_table_size(struct span_table *st);

/**
 * Load a file with newline-separated trace spans in JSON format into a span
 * table.  Note that this function assumes that every line contains a complete
 * span, and that each line is less than 8196 bytes.
 *
 * @param path              The path to read the file from.
 * @param st                The span table we will fill in.
 *
 * @return                  Negative numbers on failure; the number of lines we
 *                              read otherwise.
 */
int load_trace_span_file(const char *path, struct span_table *st);

#endif

// vim: ts=4:sw=4:tw=79:et
