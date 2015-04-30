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
#include "util/log.h"
#include "util/string.h"
#include "util/time.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * @file htraced.c
 *
 * The htraced span receiver which implements sending spans on to htraced.
 */

/**
 * The minimum buffer size to allow for the htraced circular buffer.
 *
 * This should hopefully allow at least a few spans to be buffered.
 */
#define HTRACED_MIN_BUFFER_SIZE (4ULL * 1024ULL * 1024ULL)

/**
 * The maximum buffer size to allow for the htraced circular buffer.
 * This is mainly to avoid overflow.  Of course, you couldn't allocate a buffer
 * anywhere near this size anyway.
 */
#define HTRACED_MAX_BUFFER_SIZE 0x7ffffffffffffffLL

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
 * The maximum size of the message we will send over the wire.
 * This also sets the size of the transmission buffer.
 * This constant must not be more than 2^^32 on 32-bit systems.
 */
#define HTRACED_MAX_MSG_LEN (8ULL * 1024ULL * 1024ULL)

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
     * Length of the circular buffer.
     */
    uint64_t clen;

    /**
     * A circular buffer containing span data.
     */
    uint8_t *cbuf;

    /**
     * 'start' pointer of the circular buffer.
     */
    uint64_t cstart;

    /**
     * 'end' pointer of the circular buffer.
     */
    uint64_t cend;

    /**
     * The monotonic-clock time at which we last did a send operation.
     */
    uint64_t last_send_ms;

    /**
     * RPC messages are copied into this buffer before being sent.
     * Its length is HTRACED_MAX_MSG_LEN.
     */
    uint8_t *sbuf;

    /**
     * Lock protecting the circular buffer from concurrent writes.
     */
    pthread_mutex_t lock;

    /**
     * Condition variable used to wake up the background thread.
     */
    pthread_cond_t cond;

    /**
     * Background transmitter thread.
     */
    pthread_t xmit_thread;
};

void* run_htraced_xmit_manager(void *data);
static int should_xmit(struct htraced_rcv *rcv, uint64_t now);
static void htraced_xmit(struct htraced_rcv *rcv, uint64_t now);
static uint64_t cbuf_used(const struct htraced_rcv *rcv);
static int32_t cbuf_to_sbuf(struct htraced_rcv *rcv);

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

static struct htrace_rcv *htraced_rcv_create(struct htracer *tracer,
                                             const struct htrace_conf *conf)
{
    struct htraced_rcv *rcv;
    const char *endpoint;
    int ret;
    uint64_t write_timeo_ms, read_timeo_ms;

    endpoint = htrace_conf_get(conf, HTRACED_ADDRESS_KEY);
    if (!endpoint) {
        htrace_log(tracer->lg, "htraced_rcv_create: no value found for %s. "
                   "You must set this configuration key to the "
                   "hostname:port identifying the htraced server.\n",
                   HTRACED_ADDRESS_KEY);
        goto error;
    }
    rcv = malloc(sizeof(*rcv));
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
    rcv->clen = htrace_conf_get_u64(tracer->lg, conf, HTRACED_BUFFER_SIZE_KEY);
    if (rcv->clen < HTRACED_MIN_BUFFER_SIZE) {
        htrace_log(tracer->lg, "htraced_rcv_create: invalid buffer size %" PRId64
                   ".  Setting the minimum buffer size of %llu"
                   " instead.\n", rcv->clen, HTRACED_MIN_BUFFER_SIZE);
        rcv->clen = HTRACED_MIN_BUFFER_SIZE;
    } else if (rcv->clen > HTRACED_MAX_BUFFER_SIZE) {
        htrace_log(tracer->lg, "htraced_rcv_create: invalid buffer size %" PRId64
                   ".  Setting the maximum buffer size of %lld"
                   " instead.\n", rcv->clen, HTRACED_MAX_BUFFER_SIZE);
        rcv->clen = HTRACED_MAX_BUFFER_SIZE;
    }
    rcv->cbuf = malloc(rcv->clen);
    if (!rcv->cbuf) {
        htrace_log(tracer->lg, "htraced_rcv_create: failed to malloc %"PRId64
                   " bytes for the htraced circular buffer.\n", rcv->clen);
        goto error_free_hcli;
    }
    // Send when the buffer gets 1/4 full.
    rcv->send_threshold = rcv->clen * 0.25;
    rcv->cstart = 0;
    rcv->cend = 0;
    rcv->last_send_ms = monotonic_now_ms(tracer->lg);
    rcv->sbuf = malloc(HTRACED_MAX_MSG_LEN);
    if (!rcv->sbuf) {
        goto error_free_cbuf;
    }
    ret = pthread_mutex_init(&rcv->lock, NULL);
    if (ret) {
        htrace_log(tracer->lg, "htraced_rcv_create: pthread_mutex_init "
                   "error %d: %s\n", ret, terror(ret));
        goto error_free_sbuf;
    }
    ret = pthread_cond_init(&rcv->cond, NULL);
    if (ret) {
        htrace_log(tracer->lg, "htraced_rcv_create: pthread_cond_init "
                   "error %d: %s\n", ret, terror(ret));
        goto error_free_lock;
    }
    ret = pthread_create(&rcv->xmit_thread, NULL, run_htraced_xmit_manager, rcv);
    if (ret) {
        htrace_log(tracer->lg, "htraced_rcv_create: failed to create xmit thread: "
                   "error %d: %s\n", ret, terror(ret));
        goto error_free_cvar;
    }
    htrace_log(tracer->lg, "Initialized htraced receiver for %s"
                ", flush_interval_ms=%" PRId64 ", send_threshold=%" PRId64
                ", write_timeo_ms=%" PRId64 ", read_timeo_ms=%" PRId64
                ", clen=%" PRId64 ".\n", hrpc_client_get_endpoint(rcv->hcli),
                rcv->flush_interval_ms, rcv->send_threshold,
                write_timeo_ms, read_timeo_ms, rcv->clen);
    return (struct htrace_rcv*)rcv;

error_free_cvar:
    pthread_cond_destroy(&rcv->cond);
error_free_lock:
    pthread_mutex_destroy(&rcv->lock);
error_free_sbuf:
    free(rcv->sbuf);
error_free_cbuf:
    free(rcv->cbuf);
error_free_hcli:
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
            while (cbuf_used(rcv) > 0) {
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
        ret = pthread_cond_timedwait(&rcv->cond, &rcv->lock, &wakeup_ts);
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
    uint64_t used;

    used = cbuf_used(rcv);
    if (used > rcv->send_threshold) {
        // We have buffered a lot of bytes, so let's send.
        return 1;
    }
    if (now - rcv->last_send_ms > rcv->flush_interval_ms) {
        // It's been too long since the last transmission, so let's send.
        if (used > 0) {
            return 1;
        }
    }
    return 0; // Let's wait.
}

/**
 * Send all the spans which we have buffered.
 *
 * @param rcv           The htraced receiver.
 * @param slen          The length of the buffer to send.
 *
 * @return              1 on success; 0 otherwise.
 */
static int htraced_xmit_impl(struct htraced_rcv *rcv, int32_t slen)
{
    struct htrace_log *lg = rcv->tracer->lg;
    int res, retval = 0;
    char *prequel = NULL, *err = NULL, *resp = NULL;
    size_t resp_len = 0;

    res = hrpc_client_call(rcv->hcli, METHOD_ID_WRITE_SPANS,
                     rcv->sbuf, slen, &err, (void**)&resp, &resp_len);
    if (!res) {
        htrace_log(lg, "htrace_xmit_impl: hrpc_client_call failed.\n");
        retval = 0;
    } else if (err) {
        htrace_log(lg, "htrace_xmit_impl: server returned error: %s\n", err);
        retval = 0;
    } else {
        retval = 1;
    }
    free(prequel);
    free(err);
    free(resp);
    return retval;
}

static void htraced_xmit(struct htraced_rcv *rcv, uint64_t now)
{
    int32_t slen;
    int tries = 0;

    // Move span data from the circular buffer into the transmission buffer.
    slen = cbuf_to_sbuf(rcv);

    // Release the lock while doing network I/O, so that we don't block threads
    // adding spans.
    pthread_mutex_unlock(&rcv->lock);
    while (1) {
        int retry, success = htraced_xmit_impl(rcv, slen);
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
    pthread_mutex_lock(&rcv->lock);
    rcv->last_send_ms = now;
}

/**
 * Move data from the circular buffer into the transmission buffer, advancing
 * the circular buffer's start offset.
 *
 * This function must be called with the lock held.
 *
 * Note that we rely on HTRACED_MAX_MSG_LEN being < 4GB in this function for
 * correctness on 32-bit systems.
 *
 * @param rcv           The htraced receiver.
 *
 * @return              The amount of data copied.
 */
static int32_t cbuf_to_sbuf(struct htraced_rcv *rcv)
{
    const char * const SUFFIX = "]}";
    int SUFFIX_LEN = sizeof(SUFFIX) - 1;
    int rem = HTRACED_MAX_MSG_LEN - SUFFIX_LEN;
    size_t amt;
    char *sbuf = (char*)rcv->sbuf;

    fwdprintf(&sbuf, &rem, "{\"DefaultPid\":\"%s\",\"Spans\":[",
             rcv->tracer->prid);
    if (rcv->cstart < rcv->cend) {
        amt = rcv->cend - rcv->cstart;
        if (amt > rem) {
            amt = rem;
        }
        memcpy(sbuf, rcv->cbuf + rcv->cstart, amt);
        sbuf += amt;
        rem -= amt;
        rcv->cstart += amt;
    } else {
        amt = rcv->clen - rcv->cstart;
        if (amt > rem) {
            amt = rem;
        }
        memcpy(sbuf, rcv->cbuf + rcv->cstart, amt);
        sbuf += amt;
        rem -= amt;
        rcv->cstart += amt;
        if (rem > 0) {
            amt = rcv->cend;
            if (amt > rem) {
                amt = rem;
            }
            memcpy(sbuf, rcv->cbuf, amt);
            sbuf += amt;
            rem -= amt;
            rcv->cstart = amt;
        }
    }
    // overwrite last comma
    rem++;
    sbuf--;
    rem += SUFFIX_LEN;
    fwdprintf(&sbuf, &rem, "%s", SUFFIX);
    return HTRACED_MAX_MSG_LEN - rem;
}

/**
 * Returns the current number of bytes used in the htraced circular buffer.
 * Must be called under the lock.
 *
 * @param rcv           The htraced receiver.
 *
 * @return              The number of bytes used.
 */
static uint64_t cbuf_used(const struct htraced_rcv *rcv)
{
    if (rcv->cstart <= rcv->cend) {
        return rcv->cend - rcv->cstart;
    }
    return rcv->clen - (rcv->cstart - rcv->cend);
}

static void htraced_rcv_add_span(struct htrace_rcv *r,
                                 struct htrace_span *span)
{
    int json_len, tries, retry;
    uint64_t used, rem;
    struct htraced_rcv *rcv = (struct htraced_rcv *)r;
    struct htrace_log *lg = rcv->tracer->lg;

    json_len = span_json_size(span);
    tries = 0;
    do {
        pthread_mutex_lock(&rcv->lock);
        used = cbuf_used(rcv);
        if (used + json_len >= rcv->clen) {
            pthread_cond_signal(&rcv->cond);
            pthread_mutex_unlock(&rcv->lock);
            tries++;
            retry = tries < HTRACED_MAX_ADD_TRIES;
            htrace_log(lg, "htraced_rcv_add_span: not enough space in the "
                           "circular buffer.  Have %" PRId64 ", need %d"
                           ".  %s...\n", (rcv->clen - used), json_len,
                           (retry ? "Retrying" : "Giving up"));
            if (retry) {
                pthread_yield();
                continue;
            }
            return;
        }
    } while (0);
    // OK, now we have the lock, and we know that there is enough space in the
    // circular buffer.
    rem = rcv->clen - rcv->cend;
    if (rem < json_len) {
        // Handle a 'torn write' where the circular buffer loops around to the
        // beginning in the middle of the write.
        char *temp = malloc(json_len);
        if (!temp) {
            htrace_log(lg, "htraced_rcv_add_span: failed to malloc %d byte "
                       "buffer for torn write.\n", json_len);
            goto done;
        }
        span_json_sprintf(span, json_len, temp);
        temp[json_len - 1] = ',';
        memcpy(rcv->cbuf + rcv->cend, temp, rem);
        memcpy(rcv->cbuf, temp + rem, json_len - rem);
        rcv->cend = json_len - rem;
        free(temp);
    } else {
        span_json_sprintf(span, json_len, rcv->cbuf + rcv->cend);
        rcv->cbuf[rcv->cend + json_len - 1] = ',';
        rcv->cend += json_len;
    }
    used += json_len;
    if (used > rcv->send_threshold) {
        pthread_cond_signal(&rcv->cond);
    }
done:
    pthread_mutex_unlock(&rcv->lock);
}

static void htraced_rcv_flush(struct htrace_rcv *r)
{
    struct htraced_rcv *rcv = (struct htraced_rcv *)r;

    while (1) {
        pthread_mutex_lock(&rcv->lock);
        if (cbuf_used(rcv) == 0) {
            // If the buffer is empty, we're done.
            // Note that there is no guarantee that we'll ever be done if spans
            // are being added continuously throughout the flush.  This is OK,
            // since flush() is actually only used by unit tests.
            // We could do something more clever here, but it would be a lot more
            // complex.
            pthread_mutex_unlock(&rcv->lock);
            break;
        }
        // Get the xmit thread to send what it can, by resetting the "last send
        // time" to the oldest possible monotonic time.
        rcv->last_send_ms = 0;
        pthread_cond_signal(&rcv->cond);
        pthread_mutex_unlock(&rcv->lock);
    }
}

static void htraced_rcv_free(struct htrace_rcv *r)
{
    struct htraced_rcv *rcv = (struct htraced_rcv *)r;
    struct htrace_log *lg;
    int ret;

    if (!rcv) {
        return;
    }
    lg = rcv->tracer->lg;
    htrace_log(lg, "Shutting down htraced receiver for %s\n",
               hrpc_client_get_endpoint(rcv->hcli));
    pthread_mutex_lock(&rcv->lock);
    rcv->shutdown = 1;
    pthread_cond_signal(&rcv->cond);
    pthread_mutex_unlock(&rcv->lock);
    ret = pthread_join(rcv->xmit_thread, NULL);
    if (ret) {
        htrace_log(lg, "htraced_rcv_free: pthread_join "
                   "error %d: %s\n", ret, terror(ret));
    }
    free(rcv->cbuf);
    free(rcv->sbuf);
    hrpc_client_free(rcv->hcli);
    ret = pthread_mutex_destroy(&rcv->lock);
    if (ret) {
        htrace_log(lg, "htraced_rcv_free: pthread_mutex_destroy "
                   "error %d: %s\n", ret, terror(ret));
    }
    ret = pthread_cond_destroy(&rcv->cond);
    if (ret) {
        htrace_log(lg, "htraced_rcv_free: pthread_cond_destroy "
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
