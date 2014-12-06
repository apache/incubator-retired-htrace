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
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.mortbay.util.ajax.JSON;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Writes the spans it receives to a local file.
 * A production LocalFileSpanReceiver should use a real CSV format.
 */
public class LocalFileSpanReceiver implements SpanReceiver {
  public static final Log LOG = LogFactory.getLog(LocalFileSpanReceiver.class);
  public static final String PATH_KEY = "local-file-span-receiver.path";
  public static final String CAPACITY_KEY = "local-file-span-receiver.capacity";
  // default capacity for the executors blocking queue
  public static final int CAPACITY_DEFAULT = 5000;
  // default timeout duration when calling executor.awaitTermination()
  public static final long EXECUTOR_TERMINATION_TIMEOUT_DURATION_DEFAULT = 60;
  private String file;
  private FileWriter fwriter;
  private BufferedWriter bwriter;
  private Map<String, Object> values;
  private ExecutorService executor;
  private long executorTerminationTimeoutDuration;

  public LocalFileSpanReceiver() {
  }


  @Override
  public void configure(HTraceConfiguration conf) {
    this.executorTerminationTimeoutDuration = EXECUTOR_TERMINATION_TIMEOUT_DURATION_DEFAULT;
    int capacity = conf.getInt(CAPACITY_KEY, CAPACITY_DEFAULT);
    this.file = conf.get(PATH_KEY);
    if (file == null || file.isEmpty()) {
      throw new IllegalArgumentException("must configure " + PATH_KEY);
    }
    this.executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(capacity));
    try {
      this.fwriter = new FileWriter(this.file, true);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    this.bwriter = new BufferedWriter(fwriter);
    this.values = new LinkedHashMap<String, Object>();
  }


  private class WriteSpanRunnable implements Runnable {
    public final Span span;

    public WriteSpanRunnable(Span span) {
      this.span = span;
    }

    @Override
    public void run() {
      try {
        values.put("TraceID", span.getTraceId());
        values.put("SpanID", span.getSpanId());
        values.put("ParentID", span.getParentId());
        values.put("ProcessID", span.getProcessId());
        values.put("Start", span.getStartTimeMillis());
        values.put("Stop", span.getStopTimeMillis());
        values.put("Description", span.getDescription());
        values.put("KVAnnotations", span.getKVAnnotations());
        values.put("TLAnnotations", span.getTimelineAnnotations());
        bwriter.write(JSON.toString(values));
        bwriter.newLine();
        bwriter.flush();
        values.clear();
      } catch (IOException e) {
        LOG.error("Error when writing to file: " + file, e);
      }
    }
  }

  @Override
  public void receiveSpan(Span span) {
    executor.submit(new WriteSpanRunnable(span));
  }

  @Override
  public void close() throws IOException {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(this.executorTerminationTimeoutDuration,
          TimeUnit.SECONDS)) {
        LOG.warn("Was not able to process all remaining spans to write upon closing in: "
            + this.executorTerminationTimeoutDuration + "s");
      }
    } catch (InterruptedException e1) {
      LOG.warn("Thread interrupted when terminating executor.", e1);
    }

    try {
      fwriter.close();
    } catch (IOException e) {
      LOG.error("Error closing filewriter for file: " + file, e);
    }
    try {
      bwriter.close();
    } catch (IOException e) {
      LOG.error("Error closing bufferedwriter for file: " + file, e);
    }
  }
}