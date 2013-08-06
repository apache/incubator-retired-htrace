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

package org.cloudera.htrace.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.zipkin.gen.LogEntry;
import com.twitter.zipkin.gen.Scribe;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.cloudera.htrace.HTraceConfiguration;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.SpanReceiver;
import org.cloudera.htrace.zipkin.HTraceToZipkinConverter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Zipkin is an open source tracing library. This span receiver acts as a bridge between HTrace and
 * Zipkin, that converts HTrace Span objects into Zipkin Span objects.
 *
 * HTrace spans are queued into a blocking queue.  From there background worker threads will
 * batch the spans together and then send them through to a Zipkin collector.
 */
public class ZipkinSpanReceiver implements SpanReceiver {
  private static final Log LOG = LogFactory.getLog(ZipkinSpanReceiver.class);

  /**
   * Default hostname to fall back on.
   */
  private static final String DEFAULT_COLLECTOR_HOSTNAME = "localhost";

  /**
   * Default collector port.
   */
  private static final int DEFAULT_COLLECTOR_PORT = 9410; // trace collector default port.

  /**
   *  this is used to tell scribe that the entries are for zipkin..
   */
  private static final String CATEGORY = "zipkin";

  /**
   * Whether the service which is traced is in client or a server mode. It is used while creating
   * the Endpoint.
   */
  private static final boolean DEFAULT_IN_CLIENT_MODE = false;

  /**
   * How long this receiver will try and wait for all threads to shutdown.
   */
  private static final int SHUTDOWN_TIMEOUT = 30;

  /**
   * How many spans this receiver will try and send in one batch.
   */
  private static final int MAX_SPAN_BATCH_SIZE = 100;

  /**
   * How many errors in a row before we start dropping traces on the floor.
   */
  private static final int MAX_ERRORS = 10;

  /**
   * The queue that will get all HTrace spans that are to be sent.
   */
  private final BlockingQueue<Span> queue;

  /**
   * Factory used to encode a Zipkin Span to bytes.
   */
  private final TProtocolFactory protocolFactory;

  /**
   * Boolean used to signal that the threads should end.
   */
  private final AtomicBoolean running = new AtomicBoolean(true);

  /**
   * The thread factory used to create new ExecutorService.
   *
   * This will be the same factory for the lifetime of this object so that
   * no thread names will ever be duplicated.
   */
  private final ThreadFactory tf;

  ////////////////////
  /// Variables that will change on each call to configure()
  ///////////////////
  private HTraceToZipkinConverter converter;
  private ExecutorService service;
  private HTraceConfiguration conf;
  private String collectorHostname;
  private int collectorPort;

  public ZipkinSpanReceiver() {
    this.queue = new ArrayBlockingQueue<Span>(1000);
    this.protocolFactory = new TBinaryProtocol.Factory();

    tf = new ThreadFactoryBuilder().setDaemon(true)
                                   .setNameFormat("zipkinSpanReceiver-%d")
                                   .build();
  }

  @Override
  public void configure(HTraceConfiguration conf) {
    this.conf = conf;

    this.collectorHostname = conf.get("zipkin.collector-hostname",
      DEFAULT_COLLECTOR_HOSTNAME);
    this.collectorPort = conf.getInt("zipkin.collector-port",
      DEFAULT_COLLECTOR_PORT);

    // initialize the endpoint. This endpoint is used while writing the Span.
    initConverter();

    int numThreads = conf.getInt("zipkin.num-threads", 1);

    // If there are already threads runnnig tear them down.
    if (this.service != null) {
      this.service.shutdownNow();
      this.service = null;
    }

    this.service = Executors.newFixedThreadPool(numThreads, tf);

    for (int i = 0; i< numThreads; i++) {
      this.service.submit(new WriteSpanRunnable());
    }
  }

  /**
   * Set up the HTrace to Zipkin converter.
   */
  private void initConverter() {
    boolean inClientMode = conf.getBoolean("zipkin.is-in-client-mode", DEFAULT_IN_CLIENT_MODE);
    InetAddress tracedServiceHostname = null;
    // Try and get the hostname.  If it's not configured try and get the local hostname.
    try {
      String host = conf.get("zipkin.traced-service-hostname",
                             InetAddress.getLocalHost().getHostAddress());

      tracedServiceHostname = InetAddress.getByName(host);
    } catch (UnknownHostException e) {
      LOG.error("Couldn't get the localHost address", e);
    }
    short tracedServicePort = (short) conf.getInt("zipkin.traced-service-port", 80);
    byte[] address = tracedServiceHostname != null
                     ? tracedServiceHostname.getAddress() : DEFAULT_COLLECTOR_HOSTNAME.getBytes();
    int ipv4 = ByteBuffer.wrap(address).getInt();
    this.converter = new HTraceToZipkinConverter(ipv4, tracedServicePort, inClientMode);
  }



  private class WriteSpanRunnable implements Runnable {
    /**
     * scribe client to push zipkin spans
     */
    private Scribe.Client scribeClient = null;
    private final ByteArrayOutputStream baos;
    private final TProtocol streamProtocol;

    public WriteSpanRunnable() {
      baos = new ByteArrayOutputStream();
      streamProtocol = protocolFactory.getProtocol(new TIOStreamTransport(baos));
    }

    /**
     * This runnable converts a HTrace span to a Zipkin span and sends it across the zipkin
     * collector as a thrift object. The scribe client which is used for rpc writes a list of
     * LogEntry objects, so the span objects are first transformed into LogEntry objects before
     * sending to the zipkin-collector.
     * 
     * Here is a little ascii art which shows the above transformation:
     *  <pre>
     *  +------------+   +------------+   +------------+              +-----------------+
     *  | HTrace Span|-->|Zipkin Span |-->| (LogEntry) | ===========> | Zipkin Collector|
     *  +------------+   +------------+   +------------+ (Scribe rpc) +-----------------+
     *  </pre>
     */
    @Override
    public void run() {

      List<Span> dequeuedSpans = new ArrayList<Span>(MAX_SPAN_BATCH_SIZE);

      long errorCount = 0;

      while (running.get() || queue.size() > 0) {
        Span firstSpan = null;
        try {
          // Block for up to a second. to try and get a span.
          // We only block for a little bit in order to notice if the running value has changed
          firstSpan =  queue.poll(1, TimeUnit.SECONDS);

          // If the poll was successful then it's possible that there
          // will be other spans to get. Try and get them.
          if (firstSpan != null) {
            // Add the first one that we got
            dequeuedSpans.add(firstSpan);
            // Try and get up to 100 queues
            queue.drainTo(dequeuedSpans, MAX_SPAN_BATCH_SIZE - 1);
          }

        } catch (InterruptedException ie) {
          // Ignored.
        }

        if (dequeuedSpans.isEmpty()) continue;

        // If this is the first time through or there was an error re-connect
        if (scribeClient == null) {
          startClient();
        }
        // Create a new list every time through so that the list doesn't change underneath
        // thrift as it's sending.
        List<LogEntry> entries = new ArrayList<LogEntry>(dequeuedSpans.size());
        try {
          // Convert every de-queued span
          for (Span htraceSpan : dequeuedSpans) {
            // convert the HTrace span to Zipkin span
            com.twitter.zipkin.gen.Span zipkinSpan = converter.toZipkinSpan(htraceSpan);
            // Clear any old data.
            baos.reset();
            // Write the span to a BAOS
            zipkinSpan.write(streamProtocol);

            // Do Base64 encoding and put the string into a log entry.
            LogEntry logEntry =
                new LogEntry(CATEGORY, Base64.encodeBase64String(baos.toByteArray()));
            entries.add(logEntry);
          }

          // Send the entries
          scribeClient.Log(entries);
          // clear the list for the next time through.
          dequeuedSpans.clear();
          // reset the error counter.
          errorCount = 0;
        } catch (Exception e) {
          LOG.error("Error when writing to the zipkin collector: " +
              collectorHostname + ":" + collectorPort);

          errorCount += 1;
          // If there have been ten errors in a row start dropping things.
          if (errorCount < MAX_ERRORS) {
            dequeuedSpans.addAll(dequeuedSpans);
          }

          closeClient();
          try {
            // Since there was an error sleep just a little bit to try and allow the
            // zipkin collector some time to recover.
            Thread.sleep(500);
          } catch (InterruptedException e1) {
            // Ignored
          }
        }
      }
      closeClient();
    }

    /**
     * Close out the connection.
     */
    private void closeClient() {
      // close out the transport.
      if (scribeClient != null) {
        scribeClient.getInputProtocol().getTransport().close();
        scribeClient = null;
      }
    }

    /**
     * Re-connect to Zipkin.
     */
    private void startClient() {
      if (this.scribeClient == null) {
        TTransport transport = new TFramedTransport(new TSocket(collectorHostname, collectorPort));
        try {
          transport.open();
        } catch (TTransportException e) {
          e.printStackTrace();
        }
        TProtocol protocol = protocolFactory.getProtocol(transport);
        this.scribeClient = new Scribe.Client(protocol);
      }
    }
  }

  /**
   * Close the receiver.
   *
   * This tries to shut
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    running.set(false);
    service.shutdown();
    try {
      if (!service.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
        LOG.error("Was not able to process all remaining spans to write upon closing in: " +
          SHUTDOWN_TIMEOUT + " " + TimeUnit.SECONDS +". There could be un-sent spans still left." +
          "  They have been dropped.");
      }
    } catch (InterruptedException e1) {
      LOG.warn("Thread interrupted when terminating executor.", e1);
    }
  }

  @Override
  public void receiveSpan(Span span) {
    if (running.get()) {
      try {
        this.queue.add(span);
      } catch (IllegalStateException e) {
        LOG.error("Error trying to append span (" + span.getDescription() + ") to the queue."
          +"  Blocking Queue was full.");
      }
    }
  }
}
