/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;

public class FlumeSpanReceiver implements SpanReceiver {
  private static final Log LOG = LogFactory.getLog(FlumeSpanReceiver.class);

  public static final String NUM_THREADS_KEY = "htrace.flume.num-threads";
  public static final int DEFAULT_NUM_THREADS = 1;
  public static final String FLUME_HOSTNAME_KEY = "htrace.flume.hostname";
  public static final String DEFAULT_FLUME_HOSTNAME = "localhost";
  public static final String FLUME_PORT_KEY = "htrace.flume.port";
  public static final String FLUME_BATCHSIZE_KEY = "htrace.flume.batchsize";
  public static final int DEFAULT_FLUME_BATCHSIZE = 100;
  
  /**
   * How long this receiver will try and wait for all threads to shutdown.
   */
  private static final int SHUTDOWN_TIMEOUT = 30;

  /**
   * How many errors in a row before we start dropping traces on the floor.
   */
  private static final int MAX_ERRORS = 10;

  /**
   * The queue that will get all HTrace spans that are to be sent.
   */
  private final BlockingQueue<Span> queue;

  /**
   * Boolean used to signal that the threads should end.
   */
  private final AtomicBoolean running = new AtomicBoolean(true);

  /**
   * The thread factory used to create new ExecutorService.
   * <p/>
   * This will be the same factory for the lifetime of this object so that
   * no thread names will ever be duplicated.
   */
  private final ThreadFactory tf;

  ////////////////////
  /// Variables that will change on each call to configure()
  ///////////////////
  private ExecutorService service;
  private int maxSpanBatchSize;
  private String flumeHostName;
  private int flumePort;
  private final TracerId tracerId;

  public FlumeSpanReceiver(HTraceConfiguration conf) {
    this.queue = new ArrayBlockingQueue<Span>(1000);
    this.tf = new SimpleThreadFactory();
    this.tracerId = new TracerId(conf);
    configure(conf);
  }

  private class SimpleThreadFactory implements ThreadFactory {
    final AtomicLong count = new AtomicLong(0);
    @Override
    public Thread newThread(Runnable arg0) {
      String name = String.format("flumeSpanReceiver-%d", count.getAndIncrement());
      Thread t = new Thread(arg0, name);
      t.setDaemon(true);
      return t;
    }
  }

  private void configure (HTraceConfiguration conf) {

    // Read configuration
    int numThreads = conf.getInt(NUM_THREADS_KEY, DEFAULT_NUM_THREADS);
    this.flumeHostName = conf.get(FLUME_HOSTNAME_KEY, DEFAULT_FLUME_HOSTNAME);
    this.flumePort = conf.getInt(FLUME_PORT_KEY, 0);
    if (this.flumePort == 0) {
      throw new IllegalArgumentException(FLUME_PORT_KEY + " is required in configuration.");
    }
    this.maxSpanBatchSize = conf.getInt(FLUME_BATCHSIZE_KEY, DEFAULT_FLUME_BATCHSIZE);

    // Initialize executors
    // If there are already threads running tear them down.
    if (this.service != null) {
      this.service.shutdownNow();
      this.service = null;
    }
    this.service = Executors.newFixedThreadPool(numThreads, tf);
    for (int i = 0; i < numThreads; i++) {
      this.service.submit(new WriteSpanRunnable());
    }
  }

  private class WriteSpanRunnable implements Runnable {
    private RpcClient flumeClient = null;

    /**
     * This runnable sends a HTrace span to the Flume.
     */
    @Override
    public void run() {
      List<Span> dequeuedSpans = new ArrayList<Span>(maxSpanBatchSize);
      long errorCount = 0;

      while (running.get() || queue.size() > 0) {
        Span firstSpan = null;
        try {
          // Block for up to a second. to try and get a span.
          // We only block for a little bit in order to notice
          // if the running value has changed
          firstSpan = queue.poll(1, TimeUnit.SECONDS);

          // If the poll was successful then it's possible that there
          // will be other spans to get. Try and get them.
          if (firstSpan != null) {
            // Add the first one that we got
            dequeuedSpans.add(firstSpan);
            // Try and get up to 100 queues
            queue.drainTo(dequeuedSpans, maxSpanBatchSize - 1);
          }
        } catch (InterruptedException ie) {
          // Ignored.
        }

        startClient();
        if (dequeuedSpans.isEmpty()) {
          continue;
        }

        try {
          List<Event> events = new ArrayList<Event>(dequeuedSpans.size());
          for (Span span : dequeuedSpans) {
            // Headers allow Flume to filter
            Map<String, String> headers = new HashMap<String, String>();
            headers.put("SpanId",       span.toString());
            headers.put("TracerId",     span.getTracerId());
            headers.put("Description",  span.getDescription());

            String body = span.toJson();

            Event evt = EventBuilder.withBody(body, Charset.forName("UTF-8"), headers);
            events.add(evt);
          }
          flumeClient.appendBatch(events);

          // clear the list for the next time through.
          dequeuedSpans.clear();
          // reset the error counter.
          errorCount = 0;
        } catch (Exception e) {
          errorCount += 1;
          // If there have been ten errors in a row start dropping things.
          if (errorCount < MAX_ERRORS) {
            try {
              queue.addAll(dequeuedSpans);
            } catch (IllegalStateException ex) {
              LOG.error("Drop " + dequeuedSpans.size() +
                        " span(s) because writing to HBase failed.");
            }
          }
          closeClient();
          try {
            // Since there was an error sleep just a little bit to try and allow the
            // HBase some time to recover.
            Thread.sleep(500);
          } catch (InterruptedException e1) {
            // Ignored
          }
        }
      }
      closeClient();
    }

    /**
     * Close Flume RPC client
     */
    private void closeClient() {
      if (flumeClient != null) {
        try {
          flumeClient.close();
        } catch (FlumeException ex) {
          LOG.warn("Error while trying to close Flume Rpc Client.", ex);
        } finally {
          flumeClient = null;
        }
      }
    }

    /**
     * Create / reconnect Flume RPC client
     */
    private void startClient() {
      // If current client is inactive, close it
      if (flumeClient != null && !flumeClient.isActive()) {
        flumeClient.close();
        flumeClient = null;
      }
      // Create client if needed
      if (flumeClient == null) {
        try {
          flumeClient = RpcClientFactory.getDefaultInstance(flumeHostName, flumePort, maxSpanBatchSize);
        } catch (FlumeException e) {
          LOG.warn("Failed to create Flume RPC Client. " + e.getMessage());
        }
      }
    }
  }

  /**
   * Close the receiver.
   * <p/>
   * This tries to shutdown thread pool.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    running.set(false);
    service.shutdown();
    try {
      if (!service.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
        LOG.error("Was not able to process all remaining spans upon closing in: " +
            SHUTDOWN_TIMEOUT + " " + TimeUnit.SECONDS +
            ". Left Spans could be dropped.");
       }
    } catch (InterruptedException e1) {
      LOG.warn("Thread interrupted when terminating executor.", e1);
    }
  }

  @Override
  public void receiveSpan(Span span) {
    if (running.get()) {
      try {
        if (span.getTracerId().isEmpty()) {
          span.setTracerId(tracerId.get());
        }
        this.queue.add(span);
      } catch (IllegalStateException e) {
        LOG.error("Error trying to append span (" +
            span.getDescription() +
            ") to the queue. Blocking Queue was full.");
      }
    }
  }
}
