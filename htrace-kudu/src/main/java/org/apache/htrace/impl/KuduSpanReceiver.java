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

  private String table_span;
  private String column_span_trace_id;
  private String column_span_start_time;
  private String column_span_stop_time;
  private String column_span_span_id;
  private String column_span_process_id;
  private String column_span_parent_id;
  private String column_span_description;
  private String column_span_parent;

  private String table_timeline;
  private String column_timeline_timeline_id;
  private String column_timeline_time;
  private String column_timeline_message;
  private String column_timeline_span_id;

  public KuduSpanReceiver(HTraceConfiguration conf) {

    String masterHost;
    String masterPort;
    Integer workerCount;
    Integer bossCount;
    Boolean isStatisticsEnabled;
    Long adminOperationTimeout;
    Long operationTimeout;
    Long socketReadTimeout;

    masterHost = conf.get(KuduReceiverConstants.KUDU_MASTER_HOST_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_MASTER_HOST);
    masterPort = conf.get(KuduReceiverConstants.KUDU_MASTER_PORT_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_MASTER_PORT);

    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_BOSS_COUNT_KEY) != null) {
      bossCount = Integer.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_BOSS_COUNT_KEY));
    } else {
      bossCount = null;
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_WORKER_COUNT_KEY) != null) {
      workerCount = Integer.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_WORKER_COUNT_KEY));
    } else {
      workerCount = null;
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_STATISTICS_ENABLED_KEY) != null) {
      isStatisticsEnabled = Boolean.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_STATISTICS_ENABLED_KEY));
    } else {
      isStatisticsEnabled = null;
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_ADMIN_OPERATION_KEY) != null) {
      adminOperationTimeout = Long.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_ADMIN_OPERATION_KEY));
    } else {
      adminOperationTimeout = null;
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_OPERATION_KEY) != null) {
      operationTimeout = Long.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_OPERATION_KEY));
    } else {
      operationTimeout = null;
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_SOCKET_READ_KEY) != null) {
      socketReadTimeout = Long.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_SOCKET_READ_KEY));
    } else {
      socketReadTimeout = null;
    }

    this.clientConf = new KuduClientConfiguration(masterHost,
            masterPort,
            workerCount,
            bossCount,
            isStatisticsEnabled,
            adminOperationTimeout,
            operationTimeout,
            socketReadTimeout);

    this.queue = new ArrayBlockingQueue<Span>(conf.getInt(KuduReceiverConstants.SPAN_BLOCKING_QUEUE_SIZE_KEY,
            KuduReceiverConstants.DEFAULT_SPAN_BLOCKING_QUEUE_SIZE));

    this.table_span = conf.get(KuduReceiverConstants.KUDU_SPAN_TABLE_KEY, KuduReceiverConstants.DEFAULT_KUDU_SPAN_TABLE);
    this.table_timeline= conf.get(KuduReceiverConstants.KUDU_SPAN_TIMELINE_ANNOTATION_TABLE_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_SPAN_TIMELINE_ANNOTATION_TABLE);

    this.column_span_trace_id = conf.get(KuduReceiverConstants.KUDU_COLUMN_SPAN_TRACE_ID_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID);
    this.column_span_start_time = conf.get(KuduReceiverConstants.KUDU_COLUMN_SPAN_START_TIME_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_START_TIME);
    this.column_span_stop_time = conf.get(KuduReceiverConstants.KUDU_COLUMN_SPAN_STOP_TIME_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_STOP_TIME);
    this.column_span_span_id = conf.get(KuduReceiverConstants.KUDU_COLUMN_SPAN_SPAN_ID_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_SPAN_ID);
    this.column_span_process_id = conf.get(KuduReceiverConstants.KUDU_COLUMN_SPAN_PROCESS_ID_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PROCESS_ID);
    this.column_span_parent_id = conf.get(KuduReceiverConstants.KUDU_COLUMN_SPAN_PARENT_ID_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID);
    this.column_span_description = conf.get(KuduReceiverConstants.KUDU_COLUMN_SPAN_DESCRIPTION_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_DESCRIPTION);
    this.column_span_parent = conf.get(KuduReceiverConstants.KUDU_COLUMN_SPAN_PARENT_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT);
    this.column_timeline_time = conf.get(KuduReceiverConstants.KUDU_COLUMN_TIMELINE_TIME_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_TIME);
    this.column_timeline_message = conf.get(KuduReceiverConstants.KUDU_COLUMN_TIMELINE_MESSAGE_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_MESSAGE);
    this.column_timeline_span_id = conf.get(KuduReceiverConstants.KUDU_COLUMN_TIMELINE_SPANID_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_SPANID);
    this.column_timeline_timeline_id = conf.get(KuduReceiverConstants.KUDU_COLUMN_TIMELINE_TIMELINEID_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_TIMELINEID);

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
            KuduTable tableSpan = client.openTable(table_span);
            Insert spanInsert = tableSpan.newInsert();
            PartialRow spanRow = spanInsert.getRow();
            spanRow.addLong(column_span_trace_id,span.getSpanId().getHigh());
            spanRow.addLong(column_span_start_time,span.getStartTimeMillis());
            spanRow.addLong(column_span_stop_time,span.getStopTimeMillis());
            spanRow.addLong(column_span_span_id,span.getSpanId().getLow());
            spanRow.addString(column_span_process_id,span.getTracerId());
            if (span.getParents().length == 0) {
              spanRow.addLong(column_span_parent_id,0);
              spanRow.addBoolean(column_span_parent,false);
            } else if (span.getParents().length > 0) {
              spanRow.addLong(column_span_parent_id,span.getParents()[0].getLow());
              spanRow.addBoolean(column_span_parent,true);
            }
            spanRow.addString(column_span_description,span.getDescription());
            this.session.apply(spanInsert);
            long annotationCounter = 0;
            for (TimelineAnnotation ta : span.getTimelineAnnotations()) {
              annotationCounter++;
              KuduTable tableTimeline = client.openTable(table_timeline);
              Insert timelineInsert = tableTimeline.newInsert();
              PartialRow timelineRow = timelineInsert.getRow();
              timelineRow.addLong(column_timeline_timeline_id,span.getSpanId().getHigh()+annotationCounter);
              timelineRow.addLong(column_timeline_time,ta.getTime());
              timelineRow.addString(column_timeline_message,ta.getMessage());
              timelineRow.addLong(column_timeline_span_id,span.getSpanId().getHigh());
              this.session.apply(timelineInsert);
            }
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
