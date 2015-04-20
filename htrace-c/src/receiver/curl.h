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

#ifndef APACHE_HTRACE_RECEIVER_CURL_H
#define APACHE_HTRACE_RECEIVER_CURL_H

/**
 * @file curl.h
 *
 * Utility functions wrapping libcurl.
 *
 * This is an internal header, not intended for external use.
 */

#include <curl/curl.h> // for the CURL type

struct htrace_conf;
struct htrace_log;

/**
 * Initialize a libcurl handle.
 *
 * This function also takes care of calling curl_global_init if necessary.
 *
 * @param lg            The HTrace log to use for error messages.
 * @param conf          The HTrace configuration to use.
 *
 * @return              A libcurl handle, or NULL on failure.
 */
CURL* htrace_curl_init(struct htrace_log *lg, const struct htrace_conf *conf);

/**
 * Free a libcurl handle.
 *
 * This function also takes care of calling curl_global_cleanup if necessary.
 *
 * @param lg            The HTrace log to use for error messages.
 *
 * @param curl          The libcurl handle.
 */
void htrace_curl_free(struct htrace_log *lg, CURL *curl);

#endif

// vim: ts=4: sw=4: et
