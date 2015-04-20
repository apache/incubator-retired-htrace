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

#include "core/span.h"
#include "test/span_util.h"
#include "util/log.h"

#include <errno.h>
#include <json/json_object.h>
#include <json/json_tokener.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

uint64_t parse_hex_id(const char *in, char *err, size_t err_len)
{
    char *endptr;
    unsigned long long int ret;

    err[0] = '\0';
    errno = 0;
    ret = strtoull(in, &endptr, 16);
    if (errno) {
        int e = errno;
        snprintf(err, err_len, "parse_hex_id(%s) failed: error %s",
                 in, terror(e));
        return 0;
    }
    if (endptr == in) {
        snprintf(err, err_len, "parse_hex_id(%s) failed: empty string "
                 "found.", in);
        return 0;
    }
    while (1) {
        char c = *endptr++;
        if (c == '\0') {
            break;
        }
        if ((c != ' ') || (c != '\t')) {
            snprintf(err, err_len, "parse_hex_id(%s) failed: garbage at end "
                     "of string.", in);
            return 0;
        }
    }
    return ret;
}

static void span_json_parse_parents(struct json_object *root,
                    struct htrace_span *span, char *err, size_t err_len)
{
    char err2[128];
    struct json_object *p = NULL, *e = NULL;
    int i, np;

    if (!json_object_object_get_ex(root, "p", &p)) {
        return; // no parents
    }
    if (json_object_get_type(p) != json_type_array) {
        snprintf(err, err_len, "p element was not an array..");
        return;
    }
    np = json_object_array_length(p);
    if (np == 1) {
        span->num_parents = 1;
        e = json_object_array_get_idx(p, 0);
        span->parent.single = parse_hex_id(json_object_get_string(e),
                                            err2, sizeof(err2));
        if (err2[0]) {
            snprintf(err, err_len, "failed to parse parent ID 1/1: %s.", err2);
            return;
        }
    } else if (np > 1) {
        span->parent.list = malloc(sizeof(uint64_t) * np);
        if (!span->parent.list) {
            snprintf(err, err_len, "failed to allocate parent ID array of "
                     "%d elements", np);
            return;
        }
        span->num_parents = np;
        for (i = 0; i < np; i++) {
            e = json_object_array_get_idx(p, i);
            span->parent.list[i] = parse_hex_id(json_object_get_string(e),
                                            err2, sizeof(err2));
            if (err2[0]) {
                snprintf(err, err_len, "failed to parse parent ID %d/%d: %s",
                         i + 1, np, err2);
                return;
            }
        }
    }
}

static void span_json_parse_impl(struct json_object *root,
                    struct htrace_span *span, char *err, size_t err_len)
{
    char err2[128];
    struct json_object *d = NULL, *b = NULL, *e = NULL, *s = NULL, *r = NULL;
    int res;

    err[0] = '\0';
    if (!json_object_object_get_ex(root, "d", &d)) {
        d = NULL;
    }
    span->desc = strdup(d ? json_object_get_string(d) : "");
    if (!span->desc) {
        snprintf(err, err_len, "out of memory allocating description");
        return;
    }
    if (json_object_object_get_ex(root, "b", &b)) {
        errno = 0;
        span->begin_ms = json_object_get_int64(b);
        res = errno;
        if (res) {
            snprintf(err, err_len, "error parsing begin_ms: %s", terror(res));
            return;
        }
    }
    if (json_object_object_get_ex(root, "e", &e)) {
        errno = 0;
        span->end_ms = json_object_get_int64(e);
        res = errno;
        if (res) {
            snprintf(err, err_len, "error parsing end_ms: %s", terror(res));
            return;
        }
    }
    if (json_object_object_get_ex(root, "s", &s)) {
        span->span_id = parse_hex_id(json_object_get_string(s),
                                     err2, sizeof(err2));
        if (err2[0]) {
            snprintf(err, err_len, "error parsing span_id: %s", err2);
            return;
        }
    }
    if (json_object_object_get_ex(root, "r", &r)) {
        span->prid = strdup(json_object_get_string(r));
    } else {
        span->prid = strdup("");
    }
    if (!span->prid) {
        snprintf(err, err_len, "out of memory allocating process id");
        return;
    }
    span_json_parse_parents(root, span, err, err_len);
    if (err[0]) {
        return;
    }
}

void span_json_parse(const char *in, struct htrace_span **rspan,
                     char *err, size_t err_len)
{
    struct json_object *root = NULL;
    enum json_tokener_error jerr;
    struct htrace_span *span = NULL;

    err[0] = '\0';
    root = json_tokener_parse_verbose(in, &jerr);
    if (!root) {
        snprintf(err, err_len, "json_tokener_parse_verbose failed: %s",
                 json_tokener_error_desc(jerr));
        goto done;
    }
    span = calloc(1, sizeof(*span));
    if (!span) {
        snprintf(err, err_len, "failed to malloc span.");
        goto done;
    }
    span_json_parse_impl(root, span, err, err_len);

done:
    if (root) {
        json_object_put(root);
    }
    if (err[0]) {
        htrace_span_free(span);
        *rspan = NULL;
    } else {
        *rspan = span;
    }
}

/**
 * Compare two 64-bit numbers.
 *
 * We don't use subtraction here in order to avoid numeric overflow.
 */
static int uint64_cmp(uint64_t a, uint64_t b)
{
    if (a < b) {
        return -1;
    } else if (a > b) {
        return 1;
    } else {
        return 0;
    }
}

static int strcmp_handle_null(const char *a, const char *b)
{
    if (a == NULL) {
        a = "";
    }
    if (b == NULL) {
        b = "";
    }
    return strcmp(a, b);
}

static int compare_parents(struct htrace_span *a, struct htrace_span *b)
{
    int na, nb, i;

    htrace_span_sort_and_dedupe_parents(a);
    na = a->num_parents;
    htrace_span_sort_and_dedupe_parents(b);
    nb = b->num_parents;

    for (i = 0; ; i++) {
        uint64_t sa, sb;

        if (i >= na) {
            if (i >= nb) {
                return 0;
            } else {
                return -1;
            }
        } else if (i >= nb) {
            return 1;
        }
        if ((i == 0) && (na == 1)) {
            sa = a->parent.single;
        } else {
            sa = a->parent.list[i];
        }
        if ((i == 0) && (nb == 1)) {
            sb = b->parent.single;
        } else {
            sb = b->parent.list[i];
        }
        // Use explicit comparison rather than subtraction to avoid numeric
        // overflow issues.
        if (sa < sb) {
            return -1;
        } else if (sa > sb) {
            return 1;
        }
    }
}

int span_compare(struct htrace_span *a, struct htrace_span *b)
{
    int c;

    c = uint64_cmp(a->span_id, b->span_id);
    if (c) {
        return c;
    }
    c = strcmp(a->desc, b->desc);
    if (c) {
        return c;
    }
    c = uint64_cmp(a->begin_ms, b->begin_ms);
    if (c) {
        return c;
    }
    c = uint64_cmp(a->end_ms, b->end_ms);
    if (c) {
        return c;
    }
    c = strcmp_handle_null(a->prid, b->prid);
    if (c) {
        return c;
    }
    return compare_parents(a, b);
}

// vim:ts=4:sw=4:et
