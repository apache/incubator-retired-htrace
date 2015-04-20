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
#include "receiver/curl.h"
#include "receiver/receiver.h"
#include "test/test.h"
#include "util/log.h"
#include "util/time.h"

#include <errno.h>
#include <curl/curl.h>
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
 * The minimum number of milliseconds to allow for send_timeo_ms.
 */
#define HTRACED_SEND_TIMEO_MS_MIN 30000LL

/**
 * The maximum number of milliseconds to allow for send_timeo_ms.
 * This is mainly to avoid overflow.
 */
#define HTRACED_SEND_TIMEO_MS_MAX 86400000LL

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
     * The HTraced server URL.
     * Dynamically allocated.
     */
    char *url;

    /**
     * Buffered span data becomes eligible to be sent even if there isn't much
     * in the buffer after this timeout elapses.
     */
    uint64_t send_timeo_ms;

    /**
     * The maximum number of bytes we will buffer before waking the sending
     * thread.  We may sometimes send slightly more than this amount if the
     * thread takes a while to wake up.
     */
    uint64_t send_threshold;

    /**
     * The CURL handle.
     */
    CURL *curl;

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

static struct htrace_rcv *htraced_rcv_create(struct htracer *tracer,
                                             const struct htrace_conf *conf)
{
    struct htraced_rcv *rcv;
    const char *url;
    int ret;

    url = htrace_conf_get(conf, HTRACED_ADDRESS_KEY);
    if (!url) {
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
    if (asprintf(&rcv->url, "%s/writeSpans", url) < 0) {
        rcv->url = NULL;
        goto error_free_rcv;
    }
    rcv->send_timeo_ms = htrace_conf_get_u64(tracer->lg, conf,
                    HTRACED_SEND_TIMEOUT_MS_KEY);
    if (rcv->send_timeo_ms < HTRACED_SEND_TIMEO_MS_MIN) {
        htrace_log(tracer->lg, "htraced_rcv_create: invalid send timeout of %"
                   PRId64 " ms.  Setting the minimum timeout of %lld"
                   " ms instead.\n", rcv->send_timeo_ms, HTRACED_SEND_TIMEO_MS_MIN);
        rcv->send_timeo_ms = HTRACED_SEND_TIMEO_MS_MIN;
    } else if (rcv->send_timeo_ms > HTRACED_SEND_TIMEO_MS_MAX) {
        htrace_log(tracer->lg, "htraced_rcv_create: invalid send timeout of %"
                   PRId64 " ms.  Setting the maximum timeout of %lld"
                   " ms instead.\n", rcv->send_timeo_ms, HTRACED_SEND_TIMEO_MS_MAX);
        rcv->send_timeo_ms = HTRACED_SEND_TIMEO_MS_MAX;
    }
    rcv->curl = htrace_curl_init(tracer->lg, conf);
    if (!rcv->curl) {
        goto error_free_url;
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
        goto error_free_curl;
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
    htrace_log(tracer->lg, "Initialized htraced receiver with url=%s, "
               "send_timeo_ms=%" PRId64 ", send_threshold=%" PRId64 ", clen=%"
               PRId64 ".\n", rcv->url, rcv->send_timeo_ms, rcv->send_threshold,
               rcv->clen);
    return (struct htrace_rcv*)rcv;

error_free_cvar:
    pthread_cond_destroy(&rcv->cond);
error_free_lock:
    pthread_mutex_destroy(&rcv->lock);
error_free_sbuf:
    free(rcv->sbuf);
error_free_cbuf:
    free(rcv->cbuf);
error_free_curl:
    htrace_curl_free(tracer->lg, rcv->curl);
error_free_url:
    free(rcv->url);
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
        wakeup = now + (rcv->send_timeo_ms / 2);
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
    if (now - rcv->last_send_ms > rcv->send_timeo_ms) {
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
    CURLcode res;
    char *pid_header = NULL;
    struct curl_slist *headers = NULL;
    int ret = 0;

    // Disable the use of SIGALARM to interrupt DNS lookups.
    curl_easy_setopt(rcv->curl, CURLOPT_NOSIGNAL, 1);
    // Do not use a global DNS cache.
    curl_easy_setopt(rcv->curl, CURLOPT_DNS_USE_GLOBAL_CACHE, 0);
    // Disable verbosity.
    curl_easy_setopt(rcv->curl, CURLOPT_VERBOSE, 0);
    // The user agent is libhtraced.
    curl_easy_setopt(rcv->curl, CURLOPT_USERAGENT, "libhtraced");
    // Set URL
    curl_easy_setopt(rcv->curl, CURLOPT_URL, rcv->url);
    // Set POST
    curl_easy_setopt(rcv->curl, CURLOPT_POST, 1L);
    // Set the size that we're copying from rcv->sbuf
    curl_easy_setopt(rcv->curl, CURLOPT_POSTFIELDSIZE, (long)slen);
    if (asprintf(&pid_header, "htrace-pid: %s", rcv->tracer->prid) < 0) {
        htrace_log(lg, "htraced_xmit(%s) failed: OOM allocating htrace-pid\n",
                   rcv->url);
        goto done;
    }
    curl_easy_setopt(rcv->curl, CURLOPT_POSTFIELDS, rcv->sbuf);
    headers = curl_slist_append(headers, pid_header);
    if (!headers) {
        htrace_log(lg, "htraced_xmit(%s) failed: OOM allocating headers\n",
                   rcv->url);
        return 0;
    }
    headers = curl_slist_append(headers, "Content-Type: application/json");
    if (!headers) {
        htrace_log(lg, "htraced_xmit(%s) failed: OOM allocating headers\n",
                   rcv->url);
        return 0;
    }
    curl_easy_setopt(rcv->curl, CURLOPT_HTTPHEADER, headers);
    res = curl_easy_perform(rcv->curl);
    if (res != CURLE_OK) {
        htrace_log(lg, "htraced_xmit(%s) failed: error %lld (%s)\n",
                   rcv->url, (long long)res, curl_easy_strerror(res));
    }
    ret = res == CURLE_OK;
done:
    curl_easy_reset(rcv->curl);
    free(pid_header);
    curl_slist_free_all(headers);
    return ret;
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
                   rcv->url, tries,
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
    int32_t rem = HTRACED_MAX_MSG_LEN;
    size_t amt;

    if (rcv->cstart < rcv->cend) {
        amt = rcv->cend - rcv->cstart;
        if (amt > rem) {
            amt = rem;
        }
        memcpy(rcv->sbuf, rcv->cbuf + rcv->cstart, amt);
        rem -= amt;
        rcv->cstart += amt;
    } else {
        amt = rcv->clen - rcv->cstart;
        if (amt > rem) {
            amt = rem;
        }
        memcpy(rcv->sbuf, rcv->cbuf + rcv->cstart, amt);
        rem -= amt;
        rcv->cstart += amt;
        if (rem > 0) {
            amt = rcv->cend;
            if (amt > rem) {
                amt = rem;
            }
            memcpy(rcv->sbuf, rcv->cbuf, amt);
            rem -= amt;
            rcv->cstart = amt;
        }
    }
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

    {
        char buf[4096];
        span_json_sprintf(span, sizeof(buf), buf);
    }

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
        char *temp = alloca(json_len);
        span_json_sprintf(span, json_len, temp);
        temp[json_len - 1] = '\n';
        memcpy(rcv->cbuf + rcv->cend, temp, rem);
        memcpy(rcv->cbuf, temp + rem, json_len - rem);
        rcv->cend = json_len - rem;
    } else {
        span_json_sprintf(span, json_len, rcv->cbuf + rcv->cend);
        rcv->cbuf[rcv->cend + json_len - 1] = '\n';
        rcv->cend += json_len;
    }
    used += json_len;
    if (used > rcv->send_threshold) {
        pthread_cond_signal(&rcv->cond);
    }
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
    htrace_log(lg, "Shutting down htraced receiver with url=%s\n", rcv->url);
    pthread_mutex_lock(&rcv->lock);
    rcv->shutdown = 1;
    pthread_cond_signal(&rcv->cond);
    pthread_mutex_unlock(&rcv->lock);
    ret = pthread_join(rcv->xmit_thread, NULL);
    if (ret) {
        htrace_log(lg, "htraced_rcv_free: pthread_join "
                   "error %d: %s\n", ret, terror(ret));
    }
    free(rcv->url);
    free(rcv->cbuf);
    free(rcv->sbuf);
    htrace_curl_free(lg, rcv->curl);
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
