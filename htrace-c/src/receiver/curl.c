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
#include "util/log.h"

#include <curl/curl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * Unfortunately, libcurl requires a non-threadsafe initialization function to
 * be called before it is usable.  This is unfortunate for a library like
 * libhtrace, which is designed to be used in a multi-threaded context.
 *
 * This mutex protects us against an application creating two htraced receivers
 * at around the same time, and calling that non-threadsafe initialization
 * function.
 *
 * Of course, this doesn't protect us against the application also initializing
 * libcurl.  We can protect against that by statically linking a private copy of
 * libcurl into libhtrace, so that we will be initializing and using our own
 * private copy of libcurl rather than the application's.
 */
static pthread_mutex_t g_curl_refcnt_lock = PTHREAD_MUTEX_INITIALIZER;

/**
 * The current number of CURL handles that are open.
 */
static int64_t g_curl_refcnt;

static int curl_addref(struct htrace_log *lg)
{
    int success = 0;
    CURLcode curl_err = 0;

    pthread_mutex_lock(&g_curl_refcnt_lock);
    if (g_curl_refcnt >= 1) {
        g_curl_refcnt++;
        success = 1;
        goto done;
    }
    curl_err = curl_global_init(CURL_GLOBAL_ALL);
    if (curl_err) {
        htrace_log(lg, "curl_global_init failed: error %d (%s)\n",
                   curl_err, curl_easy_strerror(curl_err));
        goto done;
    }
    htrace_log(lg, "successfully initialized libcurl...\n");
    g_curl_refcnt = 1;
    success = 1;

done:
    pthread_mutex_unlock(&g_curl_refcnt_lock);
    return success;
}

static void curl_unref(struct htrace_log *lg)
{
    pthread_mutex_lock(&g_curl_refcnt_lock);
    g_curl_refcnt--;
    if (g_curl_refcnt > 0) {
        goto done;
    }
    curl_global_cleanup();
    htrace_log(lg, "shut down libcurl...\n");
done:
    pthread_mutex_unlock(&g_curl_refcnt_lock);
}

CURL* htrace_curl_init(struct htrace_log *lg, const struct htrace_conf *conf)
{
    CURL *curl = NULL;
    int success = 0;

    if (!curl_addref(lg)) {
        return NULL;
    }
    curl = curl_easy_init();
    if (!curl) {
        htrace_log(lg, "curl_easy_init failed.\n");
        goto done;
    }
    success = 1;

done:
    if (!success) {
        if (curl) {
            curl_easy_cleanup(curl);
        }
        curl_unref(lg);
        return NULL;
    }
    return curl;
}

void htrace_curl_free(struct htrace_log *lg, CURL *curl)
{
    if (!curl) {
        return;
    }
    curl_easy_cleanup(curl);
    curl_unref(lg);
}

// vim:ts=4:sw=4:et
