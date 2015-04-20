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
#include "util/log.h"

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

struct htrace_log {
    /**
     * The lock which protects this log from concurrent writes.
     */
    pthread_mutex_t lock;

    /**
     * The log file.
     */
    FILE *fp;

    /**
     * Nonzero if we should close this file when closing the log.
     */
    int should_close;
};

struct htrace_log *htrace_log_alloc(const struct htrace_conf *conf)
{
    struct htrace_log *lg;
    const char *path;

    lg = calloc(1, sizeof(*lg));
    if (!lg) {
        fprintf(stderr, "htrace_log_alloc: out of memory.\n");
        return NULL;
    }
    path = htrace_conf_get(conf, HTRACE_LOG_PATH_KEY);
    if (!path) {
        lg->fp = stderr;
        return lg;
    }
    lg->fp = fopen(path, "a");
    if (!lg->fp) {
        int err = errno;
        fprintf(stderr, "htrace_log_alloc: failed to open %s for "
                "append: %d (%s).\n",
                path, err, terror(err));
        lg->fp = stderr;
        return lg;
    }
    // If we're logging to a file, we need to close the file when we close the
    // log.
    lg->should_close = 1;
    return lg;
}

void htrace_log_free(struct htrace_log *lg)
{
    if (!lg) {
        return;
    }
    pthread_mutex_destroy(&lg->lock);
    if (lg->should_close) {
        fclose(lg->fp);
    }
    free(lg);
}

static void htrace_logv(struct htrace_log *lg, const char *fmt, va_list ap)
{
    pthread_mutex_lock(&lg->lock);
    vfprintf(lg->fp, fmt, ap);
    pthread_mutex_unlock(&lg->lock);
}

void htrace_log(struct htrace_log *lg, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    htrace_logv(lg, fmt, ap);
    va_end(ap);
}

// vim: ts=4:sw=4:et
