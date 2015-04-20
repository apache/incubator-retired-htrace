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

#ifndef APACHE_HTRACE_CORE_CONF_H
#define APACHE_HTRACE_CORE_CONF_H

#include <stdint.h>

/**
 * @file conf.h
 *
 * Functions to manipulate HTrace configuration objects.
 *
 * This is an internal header, not intended for external use.
 */

struct htable;
struct htrace_log;

struct htrace_conf {
    /**
     * A hash table mapping keys to the values that were set.
     */
    struct htable *values;

    /**
     * A hash table mapping keys to the default values that were set for those
     * keys.
     */
    struct htable *defaults;
};

/**
 * Create an HTrace conf object from a values string and a defaults string.
 *
 * See {@ref htrace_conf_from_str} for the format.
 *
 * The configuration object must be later freed with htrace_conf_free.
 *
 * @param str       The configuration string.
 *
 * @return          NULL on OOM; the htrace configuration otherwise.
 */
struct htrace_conf *htrace_conf_from_strs(const char *values,
                                          const char *defaults);

/**
 * Free an HTrace configuration object.
 *
 * @param cnf       The HTrace configuration object.
 */
void htrace_conf_free(struct htrace_conf *cnf);

/**
 * Get the value of a key in a configuration.
 *
 * @param cnf       The configuration.
 * @param key       The key.
 *
 * @return          NULL if the key was not found in the values or the
 *                      defaults; the value otherwise.
 */
const char *htrace_conf_get(const struct htrace_conf *cnf, const char *key);

/**
 * Get the value of a key in a configuration as a floating point double.
 *
 * @param log       Log to send parse error messages to.
 * @param cnf       The configuration.
 * @param key       The key.
 *
 * @return          The value if it was found.
 *                  The default value if it was not found.
 *                  0.0 if there was no default value.
 */
double htrace_conf_get_double(struct htrace_log *log,
                const struct htrace_conf *cnf, const char *key);

/**
 * Get the value of a key in a configuration as a uint64_t.
 *
 * @param log       Log to send parse error messages to.
 * @param cnf       The configuration.
 * @param key       The key.
 *
 * @return          The value if it was found.
 *                  The default value if it was not found.
 *                  0 if there was no default value.
 */
uint64_t htrace_conf_get_u64(struct htrace_log *log,
                const struct htrace_conf *cnf, const char *key);

#endif

// vim: ts=4: sw=4: et
