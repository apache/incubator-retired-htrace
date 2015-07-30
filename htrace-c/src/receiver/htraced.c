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
#include "receiver/hrpc.h"
#include "receiver/receiver.h"
#include "test/test.h"
#include "util/cmp.h"
#include "util/cmp_util.h"
#include "util/log.h"
#include "util/string.h"
#include "util/time.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * @file htraced.c
 *
 * The htraced span receiver which implements sending spans on to the htraced
 * daemon.
 *
 * We send spans via the HRPC protocol.  HRPC consists of a simple fixed-size
 * header specifying a magic number, the length, and a message type, followed by
 * data in the msgpack serialization format.  The message type will determine
 * the type of msgpack data.  Messages bodies are sent as maps, essentially
 * making all fields optional and allowing the protocol to evolve.  See hrpc.c
 * and rpc.go for the implementation of HRPC.
 *
 * Spans are serialized immediately when they are added to the buffer.  This is
 * one of the advantages of msgpack-- it has a good streaming interface.  We do
 * not need to keep around the span objects after htraced_rcv_add_span.
 *
 * The htraced receiver keeps two equally sized buffers around internally.
 * While we are writing spans to one buffer, we can be sending the data from the
 * other buffer over the wire.  The intention here is to avoid copies as much as
 * possible.  In general, what we send over the wire is exactly what is in the
 * buffer, except that we have to add a short "prequel" to it containing the
 * other WriteSpansReq fields.
 *
 * Note that we may change the serialization in the future if we discover better
 * alternatives.  Sending spans over HTTP as JSON will always be supported
 * as a fallback.
 */

/**
 * The maximum length of the message we will send to the server.
 * This must be the same or shorter than MAX_HRPC_BODY_LENGTH in rpc.go.
 */
#define MAX_HRPC_LEN (64ULL * 1024ULL * 1024ULL)

/**
 * The maximum length of the prequel in a WriteSpans message.
 */
#define MAX_WRITESPANS_PREQUEL_LEN 1024

/**
 * The maximum length of the span data in a WriteSpans message.
 */
#define MAX_SPAN_DATA_LEN (MAX_HRPC_LEN - MAX_WRITESPANS_PREQUEL_LEN)

/**
 * The minimum total buffer size to allow.
 *
 * This should allow at least a few spans to be buffered.
 */
#define HTRACED_MIN_BUFFER_SIZE (4ULL * 1024ULL * 1024ULL)

/**
 * The maximum total buffer size to allow.
 */
#define HTRACED_MAX_BUFFER_SIZE (2ULL * MAX_SPAN_DATA_LEN)

/**
 * The minimum number of milliseconds to allow for flush_interval_ms.
 */
#define HTRACED_FLUSH_INTERVAL_MS_MIN 30000LL

/**
 * The maximum number of milliseconds to allow for flush_interval_ms.
 * This is mainly to avoid overflow.
 */
#define HTRACED_FLUSH_INTERVAL_MS_MAX 86400000LL

/**
 * The minimum number of milliseconds to allow for tcp write timeouts.
 */
#define HTRACED_WRITE_TIMEO_MS_MIN 50LL

/**
 * The minimum number of milliseconds to allow for tcp read timeouts.
 */
#define HTRACED_READ_TIMEO_MS_MIN 50LL

/**
 * The maximum number of times we will try to add a span to the circular buffer
 * before giving up.
 */
#define HTRACED_MAX_ADD_TRIES 3

/**
 * The maximum number of times to try to send some spans to the htraced daemon
 * before giving up.
 */
#define HTRACED_MAX_SEND_TRIES 3

/**
 * The number of milliseconds to sleep after failing to send some spans to the
 * htraced daemon.
 */
#define HTRACED_SEND_RETRY_SLEEP_MS 5000

/**
 * The number of buffers used by htraced.
 */
#define HTRACED_NUM_BUFS 2

/**
 * An HTraced send buffer.
 */
struct htraced_sbuf {
    /**
     * Current offset within the buffer.
     */
    uint64_t off;

    /**
     * Length of the buffer.
     */
    uint64_t len;

    /**
     * The number of spans in the buffer.
     */
    uint64_t num_spans;

    /**
     * The buffer data.  This field actually has size 'len,' not size 1.
     */
    char buf[1];
};

/*
 * A span receiver that writes spans to htraced.
 */
struct htraced_rcv {
    struct htrace_rcv base;

    /**
     * Nonzero if the receiver should shut down.
     */
    int shutdown;

    /**
     * The htracer object associated with this receciver.
     */
    struct htracer *tracer;

    /**
     * Buffered span data becomes eligible to be sent even if there isn't much
     * in the buffer after this timeout elapses.
     */
    uint64_t flush_interval_ms;

    /**
     * The maximum number of bytes we will buffer before waking the sending
     * thread.  We may sometimes send slightly more than this amount if the
     * thread takes a while to wake up.
     */
    uint64_t send_threshold;

    /**
     * The HRPC client.
     */
    struct hrpc_client *hcli;

    /**
     * The monotonic-clock time at which we last did a send operation.
     */
    uint64_t last_send_ms;

    /**
     * The index of the active buffer.
     */
    int active_buf;

    /**
     * The two send buffers.
     */
    struct htraced_sbuf *sbuf[HTRACED_NUM_BUFS];

    /**
     * Lock protecting the buffers from concurrent writes.
     */
    pthread_mutex_t lock;

    /**
     * Condition variable used to wake up the background thread.
     */
    pthread_cond_t bg_cond;

    /**
     * Condition variable used to wake up flushing threads.
     */
    pthread_cond_t flush_cond;

    /**
     * Background transmitter thread.
     */
    pthread_t xmit_thread;
};

void* run_htraced_xmit_manager(void *data);
static int should_xmit(struct htraced_rcv *rcv, uint64_t now);
static void htraced_xmit(struct htraced_rcv *rcv, uint64_t now);

static int htraced_sbufs_empty(struct htraced_rcv *rcv)
{
    int i;
    for (i = 0; i < HTRACED_NUM_BUFS; i++) {
        if (rcv->sbuf[i]->off) {
            return 0;
        }
    }
    return 1;
}

static struct htraced_sbuf *htraced_sbuf_alloc(uint64_t len)
{
    struct htraced_sbuf *sbuf;

    // The final field of the htraced_sbuf structure is declared as having size
    // 1, but really it has size 'len'.  This avoids a pointer dereference when
    // accessing data in the sbuf.
    sbuf = malloc(offsetof(struct htraced_sbuf, buf) + len);
    if (!sbuf) {
        return NULL;
    }
    sbuf->off = 0;
    sbuf->len = len;
    sbuf->num_spans = 0;
    return sbuf;
}

static void htraced_sbuf_free(struct htraced_sbuf *sbuf)
{
    free(sbuf);
}

static uint64_t htraced_sbuf_remaining(const struct htraced_sbuf *sbuf)
{
    return sbuf->len - sbuf->off;
}

static uint64_t htraced_get_bounded_u64(struct htrace_log *lg,
                const struct htrace_conf *cnf, const char *prop,
                uint64_t min, uint64_t max)
{
    uint64_t val = htrace_conf_get_u64(lg, cnf, prop);
    if (val < min) {
        htrace_log(lg, "htraced_rcv_create: can't set %s to %"PRId64
                   ".  Using minimum value of %"PRId64 " instead.\n",
                   prop, val, min);
        return min;
    } else if (val > max) {
        htrace_log(lg, "htraced_rcv_create: can't set %s to %"PRId64
                   ".  Using maximum value of %"PRId64 " instead.\n",
                   prop, val, max);
        return max;
    }
    return val;
}

static double htraced_get_bounded_double(struct htrace_log *lg,
                const struct htrace_conf *cnf, const char *prop,
                double min, double max)
{
    double val = htrace_conf_get_double(lg, cnf, prop);
    if (val < min) {
        htrace_log(lg, "htraced_rcv_create: can't set %s to %g"
                   ".  Using minimum value of %g instead.\n",
                   prop, val, min);
        return min;
    } else if (val > max) {
        htrace_log(lg, "htraced_rcv_create: can't set %s to %g"
                   ".  Using maximum value of %g instead.\n",
                   prop, val, max);
        return max;
    }
    return val;
}

static struct htrace_rcv *htraced_rcv_create(struct htracer *tracer,
                                             const struct htrace_conf *conf)
{
    struct htraced_rcv *rcv;
    const char *endpoint;
    int i, ret;
    uint64_t write_timeo_ms, read_timeo_ms, buf_len;
    double send_fraction;

    endpoint = htrace_conf_get(conf, HTRACED_ADDRESS_KEY);
    if (!endpoint) {
        htrace_log(tracer->lg, "htraced_rcv_create: no value found for %s. "
                   "You must set this configuration key to the "
                   "hostname:port identifying the htraced server.\n",
                   HTRACED_ADDRESS_KEY);
        goto error;
    }
    rcv = calloc(1, sizeof(*rcv));
    if (!rcv) {
        htrace_log(tracer->lg, "htraced_rcv_create: OOM while "
                   "allocating htraced_rcv.\n");
        goto error;
    }
    rcv->base.ty = &g_htraced_rcv_ty;
    rcv->shutdown = 0;
    rcv->tracer = tracer;

    rcv->flush_interval_ms = htraced_get_bounded_u64(tracer->lg, conf,
                HTRACED_FLUSH_INTERVAL_MS_KEY, HTRACED_FLUSH_INTERVAL_MS_MIN,
                HTRACED_FLUSH_INTERVAL_MS_MAX);
    write_timeo_ms = htraced_get_bounded_u64(tracer->lg, conf,
                HTRACED_WRITE_TIMEO_MS_KEY, HTRACED_WRITE_TIMEO_MS_MIN,
                0x7fffffffffffffffULL);
    read_timeo_ms = htraced_get_bounded_u64(tracer->lg, conf,
                HTRACED_READ_TIMEO_MS_KEY, HTRACED_READ_TIMEO_MS_MIN,
                0x7fffffffffffffffULL);
    rcv->hcli = hrpc_client_alloc(tracer->lg, write_timeo_ms,
                                  read_timeo_ms, endpoint);
    if (!rcv->hcli) {
        goto error_free_rcv;
    }
    buf_len = htraced_get_bounded_u64(tracer->lg, conf,
                HTRACED_BUFFER_SIZE_KEY, HTRACED_MIN_BUFFER_SIZE,
                HTRACED_MAX_BUFFER_SIZE) / 2;
    for (i = 0; i < HTRACED_NUM_BUFS; i++) {
        rcv->sbuf[i] = htraced_sbuf_alloc(buf_len);
        if (!rcv->sbuf[i]) {
            htrace_log(tracer->lg, "htraced_rcv_create: htraced_sbuf_alloc("
                       "buf_len=%"PRId64") failed: OOM.\n", buf_len);
            goto error_free_bufs;
        }
    }
    send_fraction = htraced_get_bounded_double(tracer->lg, conf,
                HTRACED_BUFFER_SEND_TRIGGER_FRACTION, 0.1, 1.0);
    rcv->send_threshold = buf_len * send_fraction;
    if (rcv->send_threshold > buf_len) {
        rcv->send_threshold = buf_len;
    }
    rcv->last_send_ms = monotonic_now_ms(tracer->lg);
    ret = pthread_mutex_init(&rcv->lock, NULL);
    if (ret) {
        htrace_log(tracer->lg, "htraced_rcv_create: pthread_mutex_init "
                   "error %d: %s\n", ret, terror(ret));
        goto error_free_bufs;
    }
    ret = pthread_cond_init(&rcv->bg_cond, NULL);
    if (ret) {
        htrace_log(tracer->lg, "htraced_rcv_create: pthread_cond_init("
                   "bg_cond) error %d: %s\n", ret, terror(ret));
        goto error_free_lock;
    }
    ret = pthread_cond_init(&rcv->flush_cond, NULL);
    if (ret) {
        htrace_log(tracer->lg, "htraced_rcv_create: pthread_cond_init("
                   "flush_cond) error %d: %s\n", ret, terror(ret));
        goto error_free_bg_cond;
    }
    ret = pthread_create(&rcv->xmit_thread, NULL, run_htraced_xmit_manager, rcv);
    if (ret) {
        htrace_log(tracer->lg, "htraced_rcv_create: failed to create xmit thread: "
                   "error %d: %s\n", ret, terror(ret));
        goto error_free_flush_cond;
    }
    htrace_log(tracer->lg, "Initialized htraced receiver for %s"
                ", flush_interval_ms=%" PRId64 ", send_threshold=%" PRId64
                ", write_timeo_ms=%" PRId64 ", read_timeo_ms=%" PRId64
                ", buf_len=%" PRId64 ".\n", hrpc_client_get_endpoint(rcv->hcli),
                rcv->flush_interval_ms, rcv->send_threshold,
                write_timeo_ms, read_timeo_ms, buf_len);
    return (struct htrace_rcv*)rcv;

error_free_flush_cond:
    pthread_cond_destroy(&rcv->flush_cond);
error_free_bg_cond:
    pthread_cond_destroy(&rcv->bg_cond);
error_free_lock:
    pthread_mutex_destroy(&rcv->lock);
error_free_bufs:
    for (i = 0; i < HTRACED_NUM_BUFS; i++) {
        htraced_sbuf_free(rcv->sbuf[i]);
    }
    hrpc_client_free(rcv->hcli);
error_free_rcv:
    free(rcv);
error:
    return NULL;
}

void* run_htraced_xmit_manager(void *data)
{
    struct htraced_rcv *rcv = data;
    struct htrace_log *lg = rcv->tracer->lg;
    uint64_t now, wakeup;
    struct timespec wakeup_ts;
    int ret;

    pthread_mutex_lock(&rcv->lock);
    while (1) {
        now = monotonic_now_ms(lg);
        while (should_xmit(rcv, now)) {
            htraced_xmit(rcv, now);
        }
        if (rcv->shutdown) {
            while (!htraced_sbufs_empty(rcv)) {
                htraced_xmit(rcv, now);
            }
            break;
        }
        // Wait for one of a few things to happen:
        // * Shutdown
        // * The wakeup timer to elapse, leading us to check if we should send
        //      because of send_timeo_ms.
        // * A writer to signal that we should wake up because enough bytes are
        //      buffered.
        wakeup = now + (rcv->flush_interval_ms / 2);
        ms_to_timespec(wakeup, &wakeup_ts);
        ret = pthread_cond_timedwait(&rcv->bg_cond, &rcv->lock, &wakeup_ts);
        if ((ret != 0) && (ret != ETIMEDOUT)) {
            htrace_log(lg, "run_htraced_xmit_manager: pthread_cond_timedwait "
                       "error: %d (%s)\n", ret, terror(ret));
        }
    }
    pthread_mutex_unlock(&rcv->lock);
    htrace_log(lg, "run_htraced_xmit_manager: shutting down the transmission "
               "manager thread.\n");
    return NULL;
}

/**
 * Determine if the xmit manager should send.
 * This function must be called with the lock held.
 *
 * @param rcv           The htraced receiver.
 * @param now           The current time in milliseconds.
 *
 * @return              nonzero if we should send now.
 */
static int should_xmit(struct htraced_rcv *rcv, uint64_t now)
{
    uint64_t off = rcv->sbuf[rcv->active_buf]->off;

    if (off > rcv->send_threshold) {
        // We have buffered a lot of bytes, so let's send.
        return 1;
    }
    if (now - rcv->last_send_ms > rcv->flush_interval_ms) {
        // It's been too long since the last transmission, so let's send.
        if (off > 0) {
            return 1;
        }
    }
    return 0; // Let's wait.
}

#define DEFAULT_TRID_STR        "DefaultTrid"
#define DEFAULT_TRID_STR_LEN    (sizeof(DEFAULT_TRID_STR) - 1)
#define SPANS_STR               "Spans"
#define SPANS_STR_LEN           (sizeof(SPANS_STR) - 1)

/**
 * Write the prequel to the WriteSpans message.
 */
static int add_writespans_prequel(struct htraced_rcv *rcv,
                                  struct htraced_sbuf *sbuf, uint8_t *prequel)
{
    struct cmp_bcopy_ctx bctx;
    struct cmp_ctx_s *ctx =  (struct cmp_ctx_s *)&bctx;
    cmp_bcopy_ctx_init(&bctx, prequel, MAX_WRITESPANS_PREQUEL_LEN);
    if (!cmp_write_fixmap(ctx, 2)) {
        return -1;
    }
    if (!cmp_write_fixstr(ctx, DEFAULT_TRID_STR, DEFAULT_TRID_STR_LEN)) {
        return -1;
    }
    if (!cmp_write_str(ctx, rcv->tracer->trid, strlen(rcv->tracer->trid))) {
        return -1;
    }
    if (!cmp_write_fixstr(ctx, SPANS_STR, SPANS_STR_LEN)) {
        return -1;
    }
    if (!cmp_write_array(ctx, sbuf->num_spans)) {
        return -1;
    }
    return bctx.off;
}

/**
 * Send all the spans which we have buffered.
 *
 * @param rcv           The htraced receiver.
 * @param sbuf          The span buffer to send.
 *
 * @return              1 on success; 0 otherwise.
 */
static int htraced_xmit_impl(struct htraced_rcv *rcv, struct htraced_sbuf *sbuf)
{
    struct htrace_log *lg = rcv->tracer->lg;
    uint8_t prequel[MAX_WRITESPANS_PREQUEL_LEN];
    int prequel_len, ret;
    char *err = NULL, *resp = NULL;
    size_t resp_len = 0;

    prequel_len = add_writespans_prequel(rcv, sbuf, prequel);
    if (prequel_len < 0) {
        htrace_log(lg, "htrace_xmit_impl: add_writespans_prequel failed.\n");
        ret = 0;
        goto done;
    }
    ret = hrpc_client_call(rcv->hcli, METHOD_ID_WRITE_SPANS,
                    prequel, prequel_len, sbuf->buf, sbuf->off,
                    &err, (void**)&resp, &resp_len);
    if (!ret) {
        htrace_log(lg, "htrace_xmit_impl: hrpc_client_call failed.\n");
        goto done;
    } else if (err) {
        htrace_log(lg, "htrace_xmit_impl: server returned error: %s\n", err);
        ret = 0;
        goto done;
    }
    ret = 1;
done:
    free(err);
    free(resp);
    return ret;
}

static void htraced_xmit(struct htraced_rcv *rcv, uint64_t now)
{
    int tries = 0;
    struct htraced_sbuf *sbuf;

    // Flip to the other buffer.
    sbuf = rcv->sbuf[rcv->active_buf];
    rcv->active_buf = !rcv->active_buf;

    // Release the lock while doing network I/O, so that we don't block threads
    // adding spans.
    pthread_mutex_unlock(&rcv->lock);
    while (1) {
        int retry, success = htraced_xmit_impl(rcv, sbuf);
        if (success) {
            break;
        }
        tries++;
        retry = (tries < HTRACED_MAX_SEND_TRIES);
        htrace_log(rcv->tracer->lg, "htraced_xmit(%s) failed on try %d.  %s\n",
                   hrpc_client_get_endpoint(rcv->hcli), tries,
                   (retry ? "Retrying after a delay." : "Giving up."));
        if (!retry) {
            break;
        }
    }
    sbuf->off = 0;
    sbuf->num_spans = 0;
    pthread_mutex_lock(&rcv->lock);
    rcv->last_send_ms = now;
    pthread_cond_broadcast(&rcv->flush_cond);
}

static void htraced_rcv_add_span(struct htrace_rcv *r,
                                 struct htrace_span *span)
{
    int tries, retry;
    uint64_t rem, off;
    struct htraced_rcv *rcv = (struct htraced_rcv *)r;
    struct htraced_sbuf *sbuf;
    struct htrace_log *lg = rcv->tracer->lg;
    struct cmp_counter_ctx cctx;
    struct cmp_bcopy_ctx bctx;
    uint64_t msgpack_len;

    // Determine the length of the span when serialized to msgpack.
    cmp_counter_ctx_init(&cctx);
    if (!span_write_msgpack(span, (cmp_ctx_t*)&cctx)) {
        htrace_log(lg, "htraced_rcv_add_span: span_write_msgpack failed.\n");
        return;
    }
    msgpack_len = cctx.count;

    // Try to get enough space in the current buffer.
    tries = 0;
    do {
        pthread_mutex_lock(&rcv->lock);
        sbuf = rcv->sbuf[rcv->active_buf];
        rem = htraced_sbuf_remaining(sbuf);
        if (rem < msgpack_len) {
            pthread_cond_signal(&rcv->bg_cond);
            pthread_mutex_unlock(&rcv->lock);
            tries++;
            retry = tries < HTRACED_MAX_ADD_TRIES;
            htrace_log(lg, "htraced_rcv_add_span: not enough space in the "
                           "current buffer.  Have %" PRId64 ", need %"
                           PRId64 ".  %s...\n", rem, msgpack_len,
                           (retry ? "Retrying" : "Giving up"));
            if (retry) {
                pthread_yield();
                continue;
            }
            pthread_mutex_unlock(&rcv->lock);
            return;
        }
    } while (0);
    // OK, now we have the lock, and we know that there is enough space in the
    // current buffer.
    off = sbuf->off;
    cmp_bcopy_ctx_init(&bctx, sbuf->buf + off, msgpack_len);
    bctx.base.write = cmp_bcopy_write_nocheck_fn;
    span_write_msgpack(span, (cmp_ctx_t*)&bctx);
    off += msgpack_len;
    sbuf->off = off;
    sbuf->num_spans++;
    if (off > rcv->send_threshold) {
        pthread_cond_signal(&rcv->bg_cond);
    }
    pthread_mutex_unlock(&rcv->lock);
}

static void htraced_rcv_flush(struct htrace_rcv *r)
{
    struct htraced_rcv *rcv = (struct htraced_rcv *)r;
    uint64_t now;

    // Note: This assumes that we only flush one buffer at once, and
    // that we flush buffers in order.  If we revisit those assumptions we'll
    // need to change this.
    // The SpanReceiver flush is only used for testing anyway.
    pthread_mutex_lock(&rcv->lock);
    now = monotonic_now_ms(rcv->tracer->lg);
    while (1) {
        if (rcv->last_send_ms >= now) {
            break;
        }
        if (htraced_sbufs_empty(rcv)) {
            break;
        }
        rcv->last_send_ms = 0;
        pthread_cond_wait(&rcv->flush_cond, &rcv->lock);
    }
    pthread_mutex_unlock(&rcv->lock);
}

static void htraced_rcv_free(struct htrace_rcv *r)
{
    struct htraced_rcv *rcv = (struct htraced_rcv *)r;
    struct htrace_log *lg;
    int i, ret;

    if (!rcv) {
        return;
    }
    lg = rcv->tracer->lg;
    htrace_log(lg, "Shutting down htraced receiver for %s\n",
               hrpc_client_get_endpoint(rcv->hcli));
    pthread_mutex_lock(&rcv->lock);
    rcv->shutdown = 1;
    pthread_cond_signal(&rcv->bg_cond);
    pthread_mutex_unlock(&rcv->lock);
    ret = pthread_join(rcv->xmit_thread, NULL);
    if (ret) {
        htrace_log(lg, "htraced_rcv_free: pthread_join "
                   "error %d: %s\n", ret, terror(ret));
    }
    for (i = 0; i < HTRACED_NUM_BUFS; i++) {
        htraced_sbuf_free(rcv->sbuf[i]);
    }
    hrpc_client_free(rcv->hcli);
    ret = pthread_mutex_destroy(&rcv->lock);
    if (ret) {
        htrace_log(lg, "htraced_rcv_free: pthread_mutex_destroy "
                   "error %d: %s\n", ret, terror(ret));
    }
    ret = pthread_cond_destroy(&rcv->bg_cond);
    if (ret) {
        htrace_log(lg, "htraced_rcv_free: pthread_cond_destroy(bg_cond) "
                   "error %d: %s\n", ret, terror(ret));
    }
    ret = pthread_cond_destroy(&rcv->flush_cond);
    if (ret) {
        htrace_log(lg, "htraced_rcv_free: pthread_cond_destroy(flush_cond) "
                   "error %d: %s\n", ret, terror(ret));
    }
    free(rcv);
}

const struct htrace_rcv_ty g_htraced_rcv_ty = {
    "htraced",
    htraced_rcv_create,
    htraced_rcv_add_span,
    htraced_rcv_flush,
    htraced_rcv_free,
};

// vim:ts=4:sw=4:et
