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

#include "core/conf.h"
#include "core/htrace.h"
#include "util/htable.h"
#include "util/log.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define HTRACE_DEFAULT_CONF_KEYS (\
     HTRACE_PROB_SAMPLER_FRACTION_KEY "=0.01"\
     ";" HTRACED_BUFFER_SIZE_KEY "=67108864"\
     ";" HTRACED_FLUSH_INTERVAL_MS_KEY "=120000"\
     ";" HTRACED_WRITE_TIMEO_MS_KEY "=60000"\
     ";" HTRACED_READ_TIMEO_MS_KEY "=60000"\
     ";" HTRACE_PROCESS_ID "=%{tname}/%{ip}"\
     ";" HTRACED_ADDRESS_KEY "=localhost:9095"\
     ";" HTRACED_BUFFER_SEND_TRIGGER_FRACTION "=0.50"\
    )

static int parse_key_value(char *str, char **key, char **val)
{
    char *eq = strchr(str, '=');
    if (eq) {
        *eq = '\0';
        *val = strdup(eq + 1);
    } else {
        *val = strdup("true");
    }
    if (!*val) {
        return ENOMEM;
    }
    *key = strdup(str);
    if (!*key) {
        free(*val);
        return ENOMEM;
    }
    return 0;
}

static struct htable *htable_from_str(const char *str)
{
    struct htable *ht;
    char *cstr = NULL, *saveptr = NULL, *tok;
    int ret = ENOMEM;

    ht = htable_alloc(8, ht_hash_string, ht_compare_string);
    if (!ht) {
        goto done;
    }
    if (!str) {
        ret = 0;
        goto done;
    }
    cstr = strdup(str);
    if (!cstr) {
        goto done;
    }

    for (tok = strtok_r(cstr, ";", &saveptr); tok;
             tok = strtok_r(NULL, ";", &saveptr)) {
        char *key = NULL, *val = NULL;
        ret = parse_key_value(tok, &key, &val);
        if (ret) {
            goto done;
        }
        ret = htable_put(ht, key, val);
        if (ret) {
            goto done;
        }
    }
    ret = 0;
done:
    if (ret) {
        htable_free(ht);
        ht = NULL;
    }
    free(cstr);
    return ht;
}

struct htrace_conf *htrace_conf_from_strs(const char *values,
                                          const char *defaults)
{
    struct htrace_conf *cnf;

    cnf = calloc(1, sizeof(*cnf));
    if (!cnf) {
        return NULL;
    }
    cnf->values = htable_from_str(values);
    if (!cnf->values) {
        htrace_conf_free(cnf);
        return NULL;
    }
    cnf->defaults = htable_from_str(defaults);
    if (!cnf->defaults) {
        htrace_conf_free(cnf);
        return NULL;
    }
    return cnf;
}

struct htrace_conf *htrace_conf_from_str(const char *values)
{
    return htrace_conf_from_strs(values, HTRACE_DEFAULT_CONF_KEYS);
}

static void htrace_tuple_free(void *ctx, void *key, void *val)
{
    free(key);
    free(val);
}

void htrace_conf_free(struct htrace_conf *cnf)
{
    if (!cnf) {
        return;
    }
    if (cnf->values) {
        htable_visit(cnf->values, htrace_tuple_free, NULL);
        htable_free(cnf->values);
    }
    if (cnf->defaults) {
        htable_visit(cnf->defaults, htrace_tuple_free, NULL);
        htable_free(cnf->defaults);
    }
    free(cnf);
}

const char *htrace_conf_get(const struct htrace_conf *cnf, const char *key)
{
    const char *val;

    val = htable_get(cnf->values, key);
    if (val)
        return val;
    val = htable_get(cnf->defaults, key);
    return val;
}

static int convert_double(struct htrace_log *log, const char *key,
                   const char *in, double *out)
{
    char *endptr = NULL;
    int err;
    double ret;

    errno = 0;
    ret = strtod(in, &endptr);
    if (errno) {
        err = errno;
        htrace_log(log, "error parsing %s for %s: %d (%s)\n",
                   in, key, err, terror(err));
        return 0;
    }
    while (1) {
        char c = *endptr;
        if (c == '\0') {
            break;
        }
        if ((c != ' ') || (c != '\t')) {
            htrace_log(log, "error parsing %s for %s: garbage at end "
                       "of string.\n", in, key);
            return 0;
        }
    }
    *out = ret;
    return 1;
}

double htrace_conf_get_double(struct htrace_log *log,
                             const struct htrace_conf *cnf, const char *key)
{
    const char *val;
    double out = 0;

    val = htable_get(cnf->values, key);
    if (val) {
        if (convert_double(log, key, val, &out)) {
            return out;
        }
    }
    val = htable_get(cnf->defaults, key);
    if (val) {
        if (convert_double(log, key, val, &out)) {
            return out;
        }
    }
    return 0;
}

static int convert_u64(struct htrace_log *log, const char *key,
                   const char *in, uint64_t *out)
{
    char *endptr = NULL;
    int err;
    uint64_t ret;

    errno = 0;
    ret = strtoull(in, &endptr, 10);
    if (errno) {
        err = errno;
        htrace_log(log, "error parsing %s for %s: %d (%s)\n",
                   in, key, err, terror(err));
        return 0;
    }
    while (1) {
        char c = *endptr;
        if (c == '\0') {
            break;
        }
        if ((c != ' ') || (c != '\t')) {
            htrace_log(log, "error parsing %s for %s: garbage at end "
                       "of string.\n", in, key);
            return 0;
        }
    }
    *out = ret;
    return 1;
}

uint64_t htrace_conf_get_u64(struct htrace_log *log,
                             const struct htrace_conf *cnf, const char *key)
{
    const char *val;
    uint64_t out = 0;

    val = htable_get(cnf->values, key);
    if (val) {
        if (convert_u64(log, key, val, &out)) {
            return out;
        }
    }
    val = htable_get(cnf->defaults, key);
    if (val) {
        if (convert_u64(log, key, val, &out)) {
            return out;
        }
    }
    return 0;
}

// vim:ts=4:sw=4:et
