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
package org.apache.htrace.impl;


import java.io.IOException;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;

/**
 * A {@link SpanReceiver} that passes Spans to htraced via REST. Implementation minimizes
 * dependencies and aims for small footprint since this client will be the guest of another,
 * the process traced.
 *
 * <p>Logs via commons-logging. Uses jetty client. Jetty has its own logging. To connect, see
 * jetty logging to commons-logging and see https://issues.apache.org/jira/browse/HADOOP-6807
 * and http://blogs.bytecode.com.au/glen/2005/06/21/getting-your-logging-working-in-jetty.html.
 *
 * <p>This client depends on the REST defined in <code>rest.go</code> in the htraced REST server.
 *
 * <p>Create an instance by doing:
 * <code>SpanReceiver receiver = new HTracedRESTReceiver(conf);</code> where conf is an instance
 * of {@link HTraceConfiguration}. See the public keys defined below for what we will look for in
 * the configuration.  For example, set {@link #HTRACED_REST_URL_KEY} if
 * <code>htraced</code> is in a non-standard location. Then call
 * <code>receiver.receiveSpan(Span);</code> to send spans to an htraced
 * instance. This method returns immediately. It sends the spans in background.
 *
 * <p>TODO: Shading works?
 * TODO: Add lazy start; don't start background thread till a span gets queued.
 * TODO: Add some metrics; how many times we've run, how many spans and what size we've sent.
 */
public class HTracedRESTReceiver implements SpanReceiver {
  private static final Log LOG = LogFactory.getLog(HTracedRESTReceiver.class);

  /**
   * The HttpClient to use for this receiver.
   */
  private final HttpClient httpClient;

  /**
   * The maximum number of spans to buffer.
   */
  private final int capacity;

  /**
   * REST URL to use writing Spans.
   */
  private final String url;

  /**
   * The maximum number of spans to send in a single PUT.
   */
  private final int maxToSendAtATime;

  /**
   * Runs background task to do the REST PUT.
   */
  private final PostSpans postSpans;

  /**
   * Thread for postSpans
   */
  private final Thread postSpansThread;

  /**
   * The connection timeout in milliseconds.
   */
  public static final String CLIENT_CONNECT_TIMEOUT_MS_KEY = "client.connect.timeout.ms";
  private static final int CLIENT_CONNECT_TIMEOUT_MS_DEFAULT = 30000;

  /**
   * The idle timeout in milliseconds.
   */
  public static final String CLIENT_IDLE_TIMEOUT_MS_KEY = "client.idle.timeout.ms";
  private static final int CLIENT_IDLE_TIMEOUT_MS_DEFAULT = 120000;

  /**
   * URL of the htraced REST server we are to talk to.
   */
  public static final String HTRACED_REST_URL_KEY = "htraced.rest.url";
  private static final String HTRACED_REST_URL_DEFAULT = "http://localhost:9095/";

  /**
   * Maximum size of the queue to accumulate spans in.
   * Cleared by the background thread that does the REST POST to htraced.
   */
  public static final String CLIENT_REST_QUEUE_CAPACITY_KEY = "client.rest.queue.capacity";
  private static final int CLIENT_REST_QUEUE_CAPACITY_DEFAULT = 1000000;

  /**
   * Period at which the background thread that does the REST POST to htraced in ms.
   */
  public static final String CLIENT_REST_PERIOD_MS_KEY = "client.rest.period.ms";
  private static final int CLIENT_REST_PERIOD_MS_DEFAULT = 30000;

  /**
   * Maximum spans to post to htraced at a time.
   */
  public static final String CLIENT_REST_MAX_SPANS_AT_A_TIME_KEY =
    "htrace.client.rest.batch.size";
  private static final int CLIENT_REST_MAX_SPANS_AT_A_TIME_DEFAULT = 100;

  /**
   * Lock protecting the PostSpans data.
   */
  private ReentrantLock lock = new ReentrantLock();

  /**
   * Condition variable used to wake up the PostSpans thread.
   */
  private Condition cond = lock.newCondition();

  /**
   * True if we should shut down.
   * Protected by the lock.
   */
  private boolean shutdown = false;

  /**
   * Simple bounded queue to hold spans between periodic runs of the httpclient.
   * Protected by the lock.
   */
  private final ArrayDeque<Span> spans;

  /**
   * Keep last time we logged we were at capacity; used to prevent flooding of logs with
   * "at capacity" messages.
   */
  private AtomicLong lastAtCapacityWarningLog = new AtomicLong(0L);

  /**
   * True if we should flush as soon as possible.  Protected by the lock.
   */
  private boolean mustStartFlush;

  /**
   * The process ID to use for all spans.
   */
  private final ProcessId processId;

  /**
   * Create an HttpClient instance.
   *
   * @param connTimeout         The timeout to use for connecting.
   * @param idleTimeout         The idle timeout to use.
   */
  HttpClient createHttpClient(long connTimeout, long idleTimeout) {
    HttpClient httpClient = new HttpClient();
    httpClient.setUserAgentField(new HttpField(HttpHeader.USER_AGENT,
      this.getClass().getSimpleName()));
    httpClient.setConnectTimeout(connTimeout);
    httpClient.setIdleTimeout(idleTimeout);
    return httpClient;
  }

  /**
   * Constructor.
   * You must call {@link #close()} post construction when done.
   * @param conf
   * @throws Exception
   */
  public HTracedRESTReceiver(final HTraceConfiguration conf) throws Exception {
    int connTimeout = conf.getInt(CLIENT_CONNECT_TIMEOUT_MS_KEY,
                                  CLIENT_CONNECT_TIMEOUT_MS_DEFAULT);
    int idleTimeout = conf.getInt(CLIENT_IDLE_TIMEOUT_MS_KEY,
                                  CLIENT_IDLE_TIMEOUT_MS_DEFAULT);
    this.httpClient = createHttpClient(connTimeout, idleTimeout);
    this.capacity = conf.getInt(CLIENT_REST_QUEUE_CAPACITY_KEY, CLIENT_REST_QUEUE_CAPACITY_DEFAULT);
    this.spans = new ArrayDeque<Span>(capacity);
    // Build up the writeSpans URL.
    URL restServer = new URL(conf.get(HTRACED_REST_URL_KEY, HTRACED_REST_URL_DEFAULT));
    URL url = new URL(restServer.getProtocol(), restServer.getHost(), restServer.getPort(), "/writeSpans");
    this.url = url.toString();
    // Period at which we run the background thread that does the REST POST to htraced.
    int periodInMs = conf.getInt(CLIENT_REST_PERIOD_MS_KEY, CLIENT_REST_PERIOD_MS_DEFAULT);
    // Maximum spans to send in one go
    this.maxToSendAtATime =
      conf.getInt(CLIENT_REST_MAX_SPANS_AT_A_TIME_KEY, CLIENT_REST_MAX_SPANS_AT_A_TIME_DEFAULT);
    // Start up the httpclient.
    this.httpClient.start();
    // Start the background thread.
    this.postSpans = new PostSpans(periodInMs);
    this.postSpansThread = new Thread(postSpans);
    this.postSpansThread.setDaemon(true);
    this.postSpansThread.setName("PostSpans");
    this.postSpansThread.start();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created new HTracedRESTReceiver with connTimeout=" +
            connTimeout + ", idleTimeout = " + idleTimeout + ", capacity=" +
            capacity + ", url=" + url +  ", periodInMs=" + periodInMs +
            ", maxToSendAtATime=" + maxToSendAtATime);
    }
    processId = new ProcessId(conf);
  }

  /**
   * POST spans runnable.
   * Run on a period. Services the passed in queue taking spans and sending them to traced via http.
   */
  private class PostSpans implements Runnable {
    private final long periodInNs;
    private final ArrayDeque<Span> spanBuf;

    private PostSpans(long periodInMs) {
      this.periodInNs = TimeUnit.NANOSECONDS.
          convert(periodInMs, TimeUnit.MILLISECONDS);
      this.spanBuf = new ArrayDeque<Span>(maxToSendAtATime);
    }

    /**
     * The span sending thread.
     *
     * We send a batch of spans for one of two reasons: there are already
     * maxToSendAtATime spans in the buffer, or the client.rest.period.ms
     * has elapsed.  The idea is that we want to strike a balance between
     * sending a lot of spans at a time, for efficiency purposes, and
     * making sure that we don't buffer spans locally for too long.
     *
     * The longer we buffer spans locally, the longer we will have to wait
     * to see the results of our client operations in the GUI, and the higher
     * the risk of losing them if the client crashes.
     */
    @Override
    public void run() {
      long waitNs;
      try {
        waitNs = periodInNs;
        while (true) {
          lock.lock();
          try {
            if (shutdown) {
              if (spans.isEmpty()) {
                LOG.debug("Shutting down PostSpans thread...");
                break;
              }
            } else {
              try {
                waitNs = cond.awaitNanos(waitNs);
                if (mustStartFlush) {
                  waitNs = 0;
                  mustStartFlush = false;
                }
              } catch (InterruptedException e) {
                LOG.info("Got InterruptedException");
                waitNs = 0;
              }
            }
            if ((spans.size() > maxToSendAtATime) || (waitNs <= 0) ||
                    shutdown) {
              loadSpanBuf();
              waitNs = periodInNs;
            }
          } finally {
            lock.unlock();
          }
          // Once the lock has been safely released, we can do some network
          // I/O without blocking the client process.
          if (!spanBuf.isEmpty()) {
            sendSpans();
            spanBuf.clear();
          }
        }
      } finally {
        if (httpClient != null) {
          try {
            httpClient.stop();
          } catch (Exception e) {
            LOG.error("Error shutting down httpClient", e);
          }
        }
        spans.clear();
      }
    }

    private void loadSpanBuf() {
      for (int loaded = 0; loaded < maxToSendAtATime; loaded++) {
        Span span = spans.pollFirst();
        if (span == null) {
          return;
        }
        spanBuf.add(span);
      }
    }

    private void sendSpans() {
      try {
        Request request = httpClient.newRequest(url).method(HttpMethod.POST);
        request.header(HttpHeader.CONTENT_TYPE, "application/json");
        request.header("htrace-pid", processId.get());
        StringBuilder bld = new StringBuilder();
        for (Span span : spanBuf) {
          bld.append(span.toJson());
        }
        request.content(new StringContentProvider(bld.toString()));
        ContentResponse response = request.send();
        if (response.getStatus() == HttpStatus.OK_200) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("POSTED " + spanBuf.size() + " spans");
          }
        } else {
          LOG.error("Status: " + response.getStatus());
          LOG.error(response.getHeaders());
          LOG.error(response.getContentAsString());
        }
      } catch (InterruptedException e) {
        LOG.error(e);
      } catch (TimeoutException e) {
        LOG.error(e);
      } catch (ExecutionException e) {
        LOG.error(e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    LOG.debug("Closing HTracedRESTReceiver(" + url + ").");
    lock.lock();
    try {
      this.shutdown = true;
      cond.signal();
    } finally {
      lock.unlock();
    }
    try {
      postSpansThread.join(120000);
      if (postSpansThread.isAlive()) {
        LOG.error("Timed out without closing HTracedRESTReceiver(" +
                  url + ").");
      } else {
        LOG.debug("Closed HTracedRESTReceiver(" + url + ").");
      }
    } catch (InterruptedException e) {
      LOG.error("Interrupted while joining postSpans", e);
    }
  }

  /**
   * Start flushing the buffered spans.
   *
   * Note that even after calling this function, you will still have to wait
   * for the flush to finish happening.  This function just starts the flush;
   * it does not block until it has completed.  You also do not get
   * "read-after-write consistency" with htraced... the spans that are
   * written may be buffered for a short period of time prior to being
   * readable.  This is not a problem for production use (since htraced is not
   * a database), but it means that most unit tests will need a loop in their
   * "can I read what I wrote" tests.
   */
  void startFlushing() {
    LOG.info("Triggering HTracedRESTReceiver flush.");
    lock.lock();
    try {
      mustStartFlush = true;
      cond.signal();
    } finally {
      lock.unlock();
    }
  }

  private static long WARN_TIMEOUT_MS = 300000;

  @Override
  public void receiveSpan(Span span) {
    boolean added = false;
    lock.lock();
    try {
      if (shutdown) {
        LOG.trace("receiveSpan(span=" + span + "): HTracedRESTReceiver " +
            "is already shut down.");
        return;
      }
      if (spans.size() < capacity) {
        spans.add(span);
        added = true;
        if (spans.size() >= maxToSendAtATime) {
          cond.signal();
        }
      } else {
        cond.signal();
      }
    } finally {
      lock.unlock();
    }
    if (!added) {
      long now = TimeUnit.MILLISECONDS.convert(System.nanoTime(),
          TimeUnit.NANOSECONDS);
      long last = lastAtCapacityWarningLog.get();
      if (now - last > WARN_TIMEOUT_MS) {
        // Only log every 5 minutes. Any more than this for a guest process
        // is obnoxious.
        if (lastAtCapacityWarningLog.compareAndSet(last, now)) {
          // If the atomic-compare-and-set succeeds, we should log.  Otherwise,
          // we should assume another thread already logged and bumped up the
          // value of lastAtCapacityWarning sometime between our get and the
          // "if" statement.
          LOG.warn("There are too many HTrace spans to buffer!  We have " +
              "already buffered " + capacity + " spans.  Dropping spans.");
        }
      }
    }
  }
}
