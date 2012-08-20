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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.SpanReceiver;
import org.mortbay.util.ajax.JSON;

/**
 * Writes the spans it receives to a local file. For now I am ignoring the data
 * (annotations) portion of the spans. A production LocalFileSpanReceiver should
 * use a real CSV format.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LocalFileSpanReceiver implements SpanReceiver {
  // default capacity for the executors blocking queue
  public static final int DEFAULT_CAPACITY = 5000;
  // default timeout duration when calling executor.awaitTermination()
  public static final long DEFAULT_EXECUTOR_TERMINATION_TIMEOUT_DURATION = 1;
  // default time unit for the above duration when calling
  // executor.awaitTermination()
  public static final TimeUnit DEFAULT_EXECUTOR_TERMINATION_TIMEOUT_UNITS = TimeUnit.MINUTES;
  public static final Log LOG = LogFactory.getLog(LocalFileSpanReceiver.class);
  private final String file;
  private final FileWriter fwriter;
  private final BufferedWriter bwriter;
  private final Map<String, Object> values;
  private final ExecutorService executor;
  private final long executorTerminationTimeoutDuration;
  private final TimeUnit executorTerminationTimeoutUnits;

  /**
   * 
   * @param file
   *          name of the file to write spans to.
   * @throws IOException
   */
  public LocalFileSpanReceiver(String file) throws IOException {
    this(file, DEFAULT_CAPACITY, DEFAULT_EXECUTOR_TERMINATION_TIMEOUT_DURATION,
        DEFAULT_EXECUTOR_TERMINATION_TIMEOUT_UNITS);
  }

  /**
   * 
   * @param file
   *          name of the file to write spans to.
   * @param capacity
   *          max number of spans to hold in work queue (default = 5000).
   * @throws IOException
   */
  public LocalFileSpanReceiver(String file, int capacity) throws IOException{
    this(file, capacity, DEFAULT_EXECUTOR_TERMINATION_TIMEOUT_DURATION,
        DEFAULT_EXECUTOR_TERMINATION_TIMEOUT_UNITS);
  }

  /**
   * 
   * @param file
   *          name of the file to write spans to.
   * @param executorTerminationTimeoutDuration
   *          how much time should the executor wait when terminating before
   *          timing out
   * @param executorTerminationTimeoutUnits
   *          units for above duration
   * @throws IOException
   */
  public LocalFileSpanReceiver(String file, long executorTerminationTimeoutDuration,
      TimeUnit executorTerminationTimeoutUnits) throws IOException{
    this(file, DEFAULT_CAPACITY, executorTerminationTimeoutDuration,
        executorTerminationTimeoutUnits);
  }

  /**
   * 
   * @param file
   *          name of the file to write spans to.
   * @param capacity
   *          max number of spans to hold in work queue (default = 5000).
   * @param executorTerminationTimeoutDuration
   *          how much time should the executor wait when terminating before
   *          timing out
   * @param executorTerminationTimeoutUnits
   *          units for above duration
   * @throws IOException
   */
  public LocalFileSpanReceiver(String file, int capacity,
      long executorTerminationTimeoutDuration,
      TimeUnit executorTerminationTimeoutUnits) throws IOException{
    this.executorTerminationTimeoutDuration = executorTerminationTimeoutDuration;
    this.executorTerminationTimeoutUnits = executorTerminationTimeoutUnits;
    this.executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(capacity));
    this.file = file;
    this.fwriter = new FileWriter(this.file, true);
    this.bwriter = new BufferedWriter(fwriter);
    this.values = new HashMap<String, Object>();
  }

  private class WriteSpanRunnable implements Runnable {
    public final Span span;

    public WriteSpanRunnable(Span span) {
      this.span = span;
    }

    @Override
    public void run() {
      try {
        values.put("SpanID", span.getSpanId());
        values.put("TraceID", span.getTraceId());
        values.put("ParentID", span.getParentId());
        values.put("Start", span.getStartTimeMillis());
        values.put("Stop", span.getStopTimeMillis());
        values.put("Description", span.getDescription());
        values.put("Annotations", span.getAnnotations());
        bwriter.write(JSON.toString(values));
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
          this.executorTerminationTimeoutUnits)) {
        LOG.warn("Was not able to process all remaining spans to write upon closing in: "
            + this.executorTerminationTimeoutDuration
            + " "
            + this.executorTerminationTimeoutUnits);
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