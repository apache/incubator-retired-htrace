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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.Transport;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanReceiver;
import org.apache.htrace.zipkin.HTraceToZipkinConverter;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Zipkin is an open source tracing library. This span receiver acts as a bridge between HTrace and
 * Zipkin, that converts HTrace Span objects into Zipkin Span objects.
 * <p/>
 * HTrace spans are queued into a blocking queue.  From there background worker threads will
 * batch the spans together and then send them through to a Zipkin collector.
 *
 * Pluggable Zipkin transports are supported through the "zipkin.transport.class" configuration
 * Implementations for Scribe (ScribeTransport) (default) and Kafka (KafkaTransport) are available
 *
 */
public class ZipkinSpanReceiver extends SpanReceiver {

  private static final Log LOG = LogFactory.getLog(ZipkinSpanReceiver.class);

  /**
   * Default number of threads to use.
   */
  private static final int DEFAULT_NUM_THREAD = 1;
  public static final String NUM_THREAD_KEY = "zipkin.num-threads";

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

  private static final String DEFAULT_TRANSPORT_CLASS = "org.apache.htrace.impl.ScribeTransport";
  public static final String TRANSPORT_CLASS_KEY = "zipkin.transport.class";

  /**
   * The transport that the spans will be sent trough
   */
  private Transport transport;

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
   * <p/>
   * This will be the same factory for the lifetime of this object so that
   * no thread names will ever be duplicated.
   */
  private final ThreadFactory tf = new ThreadFactory() {
    private final AtomicLong receiverIdx = new AtomicLong(0);

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setDaemon(true);
      t.setName(String.format("zipkinSpanReceiver-%d",
                receiverIdx.getAndIncrement()));
      return t;
    }
  };

  ////////////////////
  /// Variables that will change on each call to configure()
  ///////////////////
  private HTraceToZipkinConverter converter;
  private ExecutorService service;
  private HTraceConfiguration conf;

  public ZipkinSpanReceiver(HTraceConfiguration conf) {
    this.transport = createTransport(conf);
    this.queue = new ArrayBlockingQueue<Span>(1000);
    this.protocolFactory = new TBinaryProtocol.Factory();
    configure(conf);
  }

  private void logAndThrow(Throwable exception) {
    LOG.error(ExceptionUtils.getStackTrace(exception));
    throw new RuntimeException(exception);
  }

  protected Transport createTransport(HTraceConfiguration conf) {
    ClassLoader classLoader = Builder.class.getClassLoader();
    String className = conf.get(TRANSPORT_CLASS_KEY, DEFAULT_TRANSPORT_CLASS);
    Transport transport = null;
    try {
      Class<?> cls = classLoader.loadClass(className);
      transport = (Transport)cls.newInstance();
    } catch (ClassNotFoundException
        | InstantiationException
        | IllegalAccessException e) {
      logAndThrow(e);
    }
    return transport;
  }

  private void configure(HTraceConfiguration conf) {
    this.conf = conf;


    // initialize the endpoint. This endpoint is used while writing the Span.
    initConverter();

    int numThreads = conf.getInt(NUM_THREAD_KEY, DEFAULT_NUM_THREAD);

    // If there are already threads runnnig tear them down.
    if (this.service != null) {
      this.service.shutdownNow();
      this.service = null;
    }

    this.service = Executors.newFixedThreadPool(numThreads, tf);

    for (int i = 0; i < numThreads; i++) {
      this.service.submit(new WriteSpanRunnable());
    }
  }

  /**
   * Set up the HTrace to Zipkin converter.
   */
  private void initConverter() {
    InetAddress tracedServiceHostname = null;
    // Try and get the hostname.  If it's not configured try and get the local hostname.
    try {
      //TODO (clehene) extract conf to constant
      //TODO (clehene) has this been deprecated?
      String host = conf.get("zipkin.traced-service-hostname",
          InetAddress.getLocalHost().getHostAddress());
      tracedServiceHostname = InetAddress.getByName(host);
    } catch (UnknownHostException e) {
      LOG.error("Couldn't get the localHost address", e);
    }
    short tracedServicePort = (short) conf.getInt("zipkin.traced-service-port", -1);
    byte[] address = tracedServiceHostname != null
        ? tracedServiceHostname.getAddress() : InetAddress.getLoopbackAddress().getAddress();
    int ipv4 = ByteBuffer.wrap(address).getInt();
    this.converter = new HTraceToZipkinConverter(ipv4, tracedServicePort);
  }


  private class WriteSpanRunnable implements Runnable {
    private final ByteArrayOutputStream baos;
    private final TProtocol streamProtocol;

    public WriteSpanRunnable() {
      baos = new ByteArrayOutputStream();
      streamProtocol = protocolFactory.getProtocol(new TIOStreamTransport(baos));
    }

    /**
     *
     * This runnable converts an HTrace span to a Zipkin span and sends it across the transport
     * as a Thrift object.
     * <p/>
     * Here is a little ascii art which shows the above transformation:
     * <pre>
     *  +------------+   +------------+              +-----------------+
     *  | HTrace Span|-->|Zipkin Span | ===========> | Zipkin Collector|
     *  +------------+   +------------+ (transport)  +-----------------+
     *  </pre>
     */
    @Override
    public void run() {

      List<Span> dequeuedSpans = new ArrayList<Span>(MAX_SPAN_BATCH_SIZE);

      long errorCount = 0;

      while (running.get() || queue.size() > 0) {
        Span firstSpan = null;
        //TODO (clenene) the following code (try / catch) is duplicated in / from FlumeSpanReceiver
        try {
          // Block for up to a second. to try and get a span.
          // We only block for a little bit in order to notice if the running value has changed
          firstSpan = queue.poll(1, TimeUnit.SECONDS);

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

        if (!transport.isOpen()) {
          try {
            transport.open(conf);
          } catch (Throwable e) {
            logAndThrow(e);
          }
        }

        // Create a new list every time through so that the list doesn't change underneath
        // thrift as it's sending.
        List<byte[]> entries = new ArrayList<>(dequeuedSpans.size());
        try {
          // Convert every de-queued span
          for (Span htraceSpan : dequeuedSpans) {
            // convert the HTrace span to Zipkin span
            com.twitter.zipkin.gen.Span zipkinSpan = converter.convert(htraceSpan);
            // Clear any old data.
            baos.reset();
            // Write the span to a BAOS
            zipkinSpan.write(streamProtocol);

            entries.add(baos.toByteArray());
          }

          // Send the entries
          transport.send(entries);

          // clear the list for the next time through.
          dequeuedSpans.clear();
          // reset the error counter.
          errorCount = 0;
        } catch (Exception e) {
          errorCount = handleException(dequeuedSpans, errorCount, e);
        }
      }
      closeClient();
    }

    private long handleException(List<Span> dequeuedSpans, long errorCount, Exception e) {
      LOG.error("Error when writing to the zipkin transport: " + transport, e);

      errorCount += 1;
      // If there have been ten errors in a row start dropping things.
      if (errorCount < MAX_ERRORS) {
        try {
          queue.addAll(dequeuedSpans);
        } catch (IllegalStateException ex) {
          LOG.error("Drop " + dequeuedSpans.size() + " span(s) because queue is full");
        }
      }
      closeClient();
      try {
        // Since there was an error sleep just a little bit to try and allow the
        // zipkin collector some time to recover.
        Thread.sleep(500);
      } catch (InterruptedException e1) {
        // Ignored
      }
      return errorCount;
    }

    /**
     * Close out the connection.
     */
    private void closeClient(){
      try {
        transport.close();
      } catch (IOException e) {
        LOG.warn("Failed to close transport", e);
      }
    }

  }

  /**
   * Close the receiver.
   * <p/>
   * This tries to shut
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    running.set(false);
    service.shutdown();
    try {
      if (!service.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
        LOG.error("Was not able to process all remaining spans to write upon closing in: " +
            SHUTDOWN_TIMEOUT + " " + TimeUnit.SECONDS + ". There could be un-sent spans still left." +
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
            + "  Blocking Queue was full.");
      }
    }
  }
}
