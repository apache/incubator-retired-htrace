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
#include "core/htracer.h"
#include "core/span.h"
#include "receiver/receiver.h"
#include "util/log.h"

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * A span receiver that writes spans to a local file.
 */
struct local_file_rcv {
    struct htrace_rcv base;

    /**
     * The htracer object associated with this receciver.
     */
    struct htracer *tracer;

    /**
     * The local file.
     */
    FILE *fp;

    /**
     * Path to the local file.  Dynamically allocated.
     */
    char *path;

    /**
     * Lock protecting the local file from concurrent writes.
     */
    pthread_mutex_t lock;
};

static void local_file_rcv_free(struct htrace_rcv *r);

static struct htrace_rcv *local_file_rcv_create(struct htracer *tracer,
                                             const struct htrace_conf *conf)
{
    struct local_file_rcv *rcv;
    const char *path;
    int ret;

    path = htrace_conf_get(conf, HTRACE_LOCAL_FILE_RCV_PATH_KEY);
    if (!path) {
        htrace_log(tracer->lg, "local_file_rcv_create: no value found for %s. "
                   "You must set this configuration key to the path you wish "
                   "to write spans to.\n", HTRACE_LOCAL_FILE_RCV_PATH_KEY);
        return NULL;
    }
    rcv = calloc(1, sizeof(*rcv));
    if (!rcv) {
        htrace_log(tracer->lg, "local_file_rcv_create: OOM while "
                   "allocating local_file_rcv.\n");
        return NULL;
    }
    ret = pthread_mutex_init(&rcv->lock, NULL);
    if (ret) {
        htrace_log(tracer->lg, "local_file_rcv_create: failed to "
                   "create mutex while setting up local_file_rcv: "
                   "error %d (%s)\n", ret, terror(ret));
        free(rcv);
        return NULL;
    }
    rcv->base.ty = &g_local_file_rcv_ty;
    rcv->path = strdup(path);
    if (!rcv->path) {
        local_file_rcv_free((struct htrace_rcv*)rcv);
        return NULL;
    }
    rcv->tracer = tracer;
    rcv->fp = fopen(path, "a");
    if (!rcv->fp) {
        ret = errno;
        htrace_log(tracer->lg, "local_file_rcv_create: failed to "
                   "open '%s' for write: error %d (%s)\n",
                   path, ret, terror(ret));
        local_file_rcv_free((struct htrace_rcv*)rcv);
    }
    htrace_log(tracer->lg, "Initialized local_file receiver with path=%s.\n",
               rcv->path);
    return (struct htrace_rcv*)rcv;
}

static void local_file_rcv_add_span(struct htrace_rcv *r,
                                    struct htrace_span *span)
{
    int len, res, err;
    char *buf;
    struct local_file_rcv *rcv = (struct local_file_rcv *)r;

    span->prid = rcv->tracer->prid;
    len = span_json_size(span);
    buf = malloc(len + 1);
    if (!buf) {
        span->prid = NULL;
        htrace_log(rcv->tracer->lg, "local_file_rcv_add_span: OOM\n");
        return;
    }
    span_json_sprintf(span, len, buf);
    span->prid = NULL;
    buf[len - 1] = '\n';
    buf[len] = '\0';
    pthread_mutex_lock(&rcv->lock);
    res = fwrite(buf, 1, len, rcv->fp);
    err = errno;
    pthread_mutex_unlock(&rcv->lock);
    if (res < len) {
        htrace_log(rcv->tracer->lg, "local_file_rcv_add_span(%s): fwrite error: "
                   "%d (%s)\n", rcv->path, err, terror(err));
    }
    free(buf);
}

static void local_file_rcv_flush(struct htrace_rcv *r)
{
    struct local_file_rcv *rcv = (struct local_file_rcv *)r;
    if (fflush(rcv->fp) < 0) {
        int e = errno;
        htrace_log(rcv->tracer->lg, "local_file_rcv_flush(path=%s): fflush "
                   "error: %s\n", rcv->path, terror(e));
    }
}

static void local_file_rcv_free(struct htrace_rcv *r)
{
    struct local_file_rcv *rcv = (struct local_file_rcv *)r;
    int ret;
    struct htrace_log *lg;

    if (!rcv) {
        return;
    }
    lg = rcv->tracer->lg;
    htrace_log(lg, "Shutting down local_file receiver with path=%s\n",
               rcv->path);
    ret = pthread_mutex_destroy(&rcv->lock);
    if (ret) {
        htrace_log(lg, "local_file_rcv_free: pthread_mutex_destroy "
                   "error %d: %s\n", ret, terror(ret));
    }
    ret = fclose(rcv->fp);
    if (ret) {
        htrace_log(lg, "local_file_rcv_free: fclose error "
                   "%d: %s\n", ret, terror(ret));
    }
    free(rcv->path);
    free(rcv);
}

const struct htrace_rcv_ty g_local_file_rcv_ty = {
    "local.file",
    local_file_rcv_create,
    local_file_rcv_add_span,
    local_file_rcv_flush,
    local_file_rcv_free,
};

// vim:ts=4:sw=4:et
