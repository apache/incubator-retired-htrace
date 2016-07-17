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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanReceiver;
import org.apache.htrace.core.TimelineAnnotation;
import org.apache.htrace.protobuf.generated.SpanProtos;
import org.kududb.client.Bytes;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.Insert;
import org.kududb.client.PartialRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class KuduSpanReceiver extends SpanReceiver {

  private static final Log LOG = LogFactory.getLog(KuduSpanReceiver.class);

  private static final int SHUTDOWN_TIMEOUT = 30;
  private static final int MAX_ERRORS = 10;
  private final BlockingQueue<Span> queue;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final KuduClientConfiguration clientConf;
  private final int maxSpanBatchSize;
  private final ThreadFactory threadFactory = new ThreadFactory() {
    private final AtomicLong receiverIndex = new AtomicLong(0);

    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable);
      thread.setDaemon(true);
      thread.setName(String.format("kuduSpanReceiver-%d",
              receiverIndex.getAndIncrement()));
      return thread;
    }
  };
  private ExecutorService service;
  private String table;
  private String column_span_id;
  private String column_span;
  private String column_root_span;
  private String column_root_span_start_time;

  public KuduSpanReceiver(HTraceConfiguration conf) {
    this.clientConf =
            new KuduClientConfiguration(conf.get(KuduReceiverConstants.KUDU_MASTER_HOST_KEY,
                    KuduReceiverConstants.DEFAULT_KUDU_MASTER_HOST),
                    conf.get(KuduReceiverConstants.KUDU_MASTER_PORT_KEY,
                            KuduReceiverConstants.DEFAULT_KUDU_MASTER_PORT));
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_BOSS_COUNT_KEY) != null) {
      this.clientConf.setBossCount(Integer.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_BOSS_COUNT_KEY)));
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_WORKER_COUNT_KEY) != null) {
      this.clientConf.setWorkerCount(Integer.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_WORKER_COUNT_KEY)));
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_STATISTICS_ENABLED_KEY) != null) {
      this.clientConf.setIsStatisticsEnabled(Boolean.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_STATISTICS_ENABLED_KEY)));
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_ADMIN_OPERATION_KEY) != null) {
      this.clientConf
              .setAdminOperationTimeout(Long.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_ADMIN_OPERATION_KEY)));
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_OPERATION_KEY) != null) {
      this.clientConf
              .setOperationTimeout(Long.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_OPERATION_KEY)));
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_SOCKET_READ_KEY) != null) {
      this.clientConf
              .setSocketReadTimeout(Long.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_SOCKET_READ_KEY)));
    }
    this.queue = new ArrayBlockingQueue<Span>(conf.getInt(KuduReceiverConstants.SPAN_BLOCKING_QUEUE_SIZE_KEY,
            KuduReceiverConstants.DEFAULT_SPAN_BLOCKING_QUEUE_SIZE));
    this.table = conf.get(KuduReceiverConstants.KUDU_TABLE_KEY, KuduReceiverConstants.DEFAULT_KUDU_TABLE);
    this.column_span_id = conf.get(KuduReceiverConstants.KUDU_COLUMN_SPAN_ID_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_ID);
    this.column_span = conf.get(KuduReceiverConstants.KUDU_COLUMN_SPAN_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN);
    this.column_root_span = conf.get(KuduReceiverConstants.KUDU_COLUMN_ROOT_SPAN_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_ROOT_SPAN);
    this.column_root_span_start_time = conf.get(KuduReceiverConstants.KUDU_COLUMN_ROOT_SPAN_START_TIME_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_ROOT_SPAN_START_TIME);
    this.maxSpanBatchSize = conf.getInt(KuduReceiverConstants.MAX_SPAN_BATCH_SIZE_KEY,
            KuduReceiverConstants.DEFAULT_MAX_SPAN_BATCH_SIZE);
    if (this.service != null) {
      this.service.shutdownNow();
      this.service = null;
    }
    int numThreads = conf.getInt(KuduReceiverConstants.NUM_PARALLEL_THREADS_KEY,
            KuduReceiverConstants.DEFAULT_NUM_PARALLEL_THREADS);
    this.service = Executors.newFixedThreadPool(numThreads, threadFactory);
    for (int i = 0; i < numThreads; i++) {
      this.service.submit(new KuduSpanReceiver.WriteSpanRunnable());
    }
  }

  @Override
  public void close() throws IOException {
    running.set(false);
    service.shutdown();
    try {
      if (!service.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
        LOG.error("Timeout " + SHUTDOWN_TIMEOUT + " " + TimeUnit.SECONDS +
                " reached while shutting worker threads which process enqued spans." +
                " Enqueued spans which are left in blocking queue is dropped.");
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted exception occured while terminating thread service executor.", e);
    }
  }

  @Override
  public void receiveSpan(Span span) {
    if (running.get()) {
      try {
        this.queue.add(span);
      } catch (IllegalStateException e) {
        LOG.error("Error trying to enqueue span ("
                + span.getDescription()
                + ") to the queue. Blocking Queue is currently reached its capacity.");
      }
    }
  }

  private class WriteSpanRunnable implements Runnable {

    private KuduSession session;
    private KuduClient client;

    @Override
    public void run() {
      SpanProtos.Span.Builder sbuilder = SpanProtos.Span.newBuilder();
      SpanProtos.TimelineAnnotation.Builder tlbuilder =
              SpanProtos.TimelineAnnotation.newBuilder();
      List<Span> dequeuedSpans = new ArrayList<Span>(maxSpanBatchSize);
      long errorCount = 0;
      while (running.get() || queue.size() > 0) {
        Span firstSpan = null;
        try {
          firstSpan = queue.poll(1, TimeUnit.SECONDS);
          if (firstSpan != null) {
            dequeuedSpans.add(firstSpan);
            queue.drainTo(dequeuedSpans, maxSpanBatchSize - 1);
          }
        } catch (InterruptedException ie) {
          LOG.error("Interrupted Exception occurred while polling to " +
                  "retrieve first span from blocking queue");
        }
        startSession();
        if (dequeuedSpans.isEmpty()) {
          try {
            this.session.flush();
          } catch (java.lang.Exception e) {
            LOG.error("Failed to flush writes to Kudu.");
            closeSession();
          }
          continue;
        }
        try {
          for (Span span : dequeuedSpans) {
            sbuilder.clear()
                    .setTraceId(span.getSpanId().getHigh())
                    .setStart(span.getStartTimeMillis())
                    .setStop(span.getStopTimeMillis())
                    .setSpanId(span.getSpanId().getLow())
                    .setProcessId(span.getTracerId())
                    .setDescription(span.getDescription());

            if (span.getParents().length == 0) {
              sbuilder.setParentId(0);
            } else if (span.getParents().length > 0) {
              sbuilder.setParentId(span.getParents()[0].getLow());
            }
            for (TimelineAnnotation ta : span.getTimelineAnnotations()) {
              sbuilder.addTimeline(tlbuilder.clear()
                      .setTime(ta.getTime())
                      .setMessage(ta.getMessage())
                      .build());
            }
            KuduTable tableRef = client.openTable(table);
            Insert insert = tableRef.newInsert();
            PartialRow row = insert.getRow();
            row.addBinary(column_span_id, Bytes.fromLong(span.getSpanId().getHigh()));
            row.addBinary(column_span, sbuilder.build().toByteArray());
            if (span.getParents().length == 0) {
              row.addBinary(column_root_span_start_time, Bytes.fromLong(span.getStartTimeMillis()));
              row.addBinary(column_root_span, sbuilder.build().toByteArray());
            }
            this.session.apply(insert);
          }
          dequeuedSpans.clear();
          errorCount = 0;
        } catch (Exception e) {
          errorCount += 1;
          if (errorCount < MAX_ERRORS) {
            try {
              queue.addAll(dequeuedSpans);
            } catch (IllegalStateException ex) {
              LOG.error("Exception occured while writing spans kudu datastore. " +
                      "Trying to re-enqueue de-queued spans to blocking queue for writing but failed. " +
                      "Dropped " + dequeuedSpans.size() + " dequeued span(s) which were due written" +
                      "into kudu datastore");
            }
          }
          closeSession();
          try {
            Thread.sleep(500);
          } catch (InterruptedException e1) {
            LOG.error("Interrupted Exception occurred while allowing kudu to re-stabilized");
          }
        }
      }
      closeSession();
    }

    private void closeSession() {
      try {
        if (this.session != null) {
          this.session.close();
          this.session = null;
        }
      } catch (java.lang.Exception e) {
        LOG.warn("Failed to close Kudu session. " + e.getMessage());
      }
    }

    private void startSession() {
      if (this.session == null) {
        if (this.client == null) {
          client = clientConf.buildClient();
        }
        session = client.newSession();
      }
    }
  }

}
