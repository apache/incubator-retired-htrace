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
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
 * <p>Logs via commons-logging. Uses jetty client. Jetty has its own logging (TODO: connect
 * jetty logging to commons-logging, see https://issues.apache.org/jira/browse/HADOOP-6807
 * and http://blogs.bytecode.com.au/glen/2005/06/21/getting-your-logging-working-in-jetty.html).
 *
 * <p>This client depends on the REST defined in <code>rest.go</code> in the htraced REST server.
 * For example, a <code>GET</code> on <code>/server/info</code> returns the htraced server info.
 *
 * <p>Create an instance by doing:
 * <code>SpanReceiver receiver = new HTracedRESTReceiver(conf);</code> where conf is an instance
 * of {@link HTraceConfiguration}. See the public keys defined below for what we will look for in
 * the configuration.  For example, set {@link #HTRACED_REST_URL_KEY} if htraced is in non-standard
 * location. Then call <code>receiver.receiveSpan(Span);</code> to send spans to an htraced
 * instance. This method returns immediately. It sends the spans in background.
 *
 * <p>TODO: How to be more dependent on rest.go so we break if it changes?
 * TODO: Shading works?
 * TODO: Add lazy start; don't start background thread till a span gets queued.
 * TODO: Add some metrics; how many times we've run, how many spans and what size we've sent.
 */
public class HTracedRESTReceiver implements SpanReceiver {
  private static final Log LOG = LogFactory.getLog(HTracedRESTReceiver.class);

  // TODO: Take process name and add this to user agent?  Would help debugging?
  // @VisibleForTesting Protected so accessible from tests.
  final HttpClient httpClient;

  /**
   * REST URL to use writing Spans.
   */
  private final String writeSpansRESTURL;

  /**
   * Runs background task to do the REST PUT.
   * TODO: Make period configurable. TODO: Make instantiation lazy.
   */
  private final ScheduledExecutorService scheduler;

  /**
   * Keep around reference so can cancel on close any running scheduled task.
   */
  private final ScheduledFuture<?> scheduledFuture;

  /**
   * Timeout in milliseconds.
   * For now, it is read and connect timeout.
   */
  public static final String CLIENT_REST_TIMEOUT_MS_KEY = "client.rest.timeout.ms";
  private static final int CLIENT_REST_TIMEOUT_MS_DEFAULT = 60000;

  /**
   * URL of the htraced REST server we are to talk to.
   */
  public static final String HTRACED_REST_URL_KEY = "htraced.rest.url";
  private static final String HTRACED_REST_URL_DEFAULT = "http://locahost:9095/";

  /**
   * Maximum size of the queue to accumulate spans in.
   * Cleared by the background thread that does the REST POST to htraced.
   */
  public static final String CLIENT_REST_QUEUE_CAPACITY_KEY = "client.rest.queue.capacity";
  private static final int CLIENT_REST_QUEUE_CAPACITY_DEFAULT = 1000000;

  /**
   * Period at which the background thread that does the REST POST to htraced in ms.
   */
  public static final String CLIENT_REST_PERIOD_MS_KEY = "client.reset.period.ms";
  private static final int CLIENT_REST_PERIOD_MS_DEFAULT = 1000;

  /**
   * Maximum spans to post to htraced at a time.
   */
  public static final String CLIENT_REST_MAX_SPANS_AT_A_TIME_KEY =
    "client.rest.max.spans.at.a.time";
  private static final int CLIENT_REST_MAX_SPANS_AT_A_TIME_DEFAULT = 1000;

  /**
   * Simple bounded queue to hold spans between periodic runs of the httpclient.
   * TODO: Make size configurable.
   */
  private final Queue<Span> queue;

  /**
   * Keep last time we logged we were at capacity; used to prevent flooding of logs with
   * "at capacity" messages.
   */
  private volatile long lastAtCapacityWarningLog = 0L;

  public HTracedRESTReceiver(final HTraceConfiguration conf) throws Exception {
    this.httpClient = new HttpClient();
    this.httpClient.setUserAgentField(new HttpField(HttpHeader.USER_AGENT,
      this.getClass().getSimpleName()));
    // Use same timeout for connection and idle for now.
    int timeout = conf.getInt(CLIENT_REST_TIMEOUT_MS_KEY, CLIENT_REST_TIMEOUT_MS_DEFAULT);
    this.httpClient.setConnectTimeout(timeout);
    this.httpClient.setIdleTimeout(timeout);
    int capacity =
      conf.getInt(CLIENT_REST_QUEUE_CAPACITY_KEY, CLIENT_REST_QUEUE_CAPACITY_DEFAULT);
    this.queue = new ArrayBlockingQueue<Span>(capacity, true);
    // Build up the writeSpans URL.
    URL restServer = new URL(conf.get(HTRACED_REST_URL_KEY, HTRACED_REST_URL_DEFAULT));
    URL url =
      new URL(restServer.getProtocol(), restServer.getHost(), restServer.getPort(), "/writeSpans");
    this.writeSpansRESTURL = url.toString();
    // Make a scheduler with one thread to run our POST of spans on a period.
    this.scheduler = Executors.newScheduledThreadPool(1);
    // Period at which we run the background thread that does the REST POST to htraced.
    int periodInMs =
      conf.getInt(CLIENT_REST_PERIOD_MS_KEY, CLIENT_REST_PERIOD_MS_DEFAULT);
    // Maximum spans to send in one go
    int maxToSendAtATime =
      conf.getInt(CLIENT_REST_MAX_SPANS_AT_A_TIME_KEY, CLIENT_REST_MAX_SPANS_AT_A_TIME_DEFAULT);
    this.scheduledFuture =
      this.scheduler.scheduleAtFixedRate(new PostSpans(this.queue, maxToSendAtATime),
          periodInMs, periodInMs, TimeUnit.MILLISECONDS);
    // Start up the httpclient.
    this.httpClient.start();
  }

  /**
   * POST spans runnable.
   * Run on a period. Services the passed in queue taking spans and sending them to traced via http.
   */
  private class PostSpans implements Runnable {
    private final Queue<Span> q;
    private final int maxToSendAtATime;

    private PostSpans(final Queue<Span> q, final int maxToSendAtATime) {
      this.q = q;
      this.maxToSendAtATime = maxToSendAtATime;
    }

    @Override
    public void run() {
      Span span = null;
      // Cycle until we drain the queue. Seen maxToSendAtATime at a time if more than we can send
      // in one go.
      while ((span = this.q.poll()) != null) {
        // We got a span. Send at least this one span.
        Request request = httpClient.newRequest(writeSpansRESTURL).method(HttpMethod.POST);
        request.header(HttpHeader.CONTENT_TYPE, "application/json");
        int count = 1;
        request.content(new StringContentProvider(span.toJson()));
        // Drain queue of spans if more than just one.
        while ((span = this.q.poll()) != null) {
          request.content(new StringContentProvider(span.toJson()));
          count++;
          // If we've accumulated sufficient to send, go ahead and send what we have. Can do the
          // rest in out next go around.
          if (count > this.maxToSendAtATime) break;
        }
        try {
          ContentResponse response = request.send();
          if (response.getStatus() == HttpStatus.OK_200) {
            LOG.info("POSTED " + count + " spans");
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
  }

  @Override
  public void close() throws IOException {
    if (this.scheduledFuture != null) this.scheduledFuture.cancel(true);
    if (this.scheduler == null) this.scheduler.shutdown();
    if (this.httpClient != null) {
      try {
        this.httpClient.stop();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }


  @Override
  public void receiveSpan(Span span) {
    if (!this.queue.offer(span)) {
      // TODO: If failed the offer, run the background thread now. I can't block though?
      long now = System.currentTimeMillis();
      // Only log every 5 minutes. Any more than this for a guest process is obnoxious
      if (now - lastAtCapacityWarningLog > 300000) {
        LOG.warn("At capacity");
        this.lastAtCapacityWarningLog = now;
      }
    }
  }
}