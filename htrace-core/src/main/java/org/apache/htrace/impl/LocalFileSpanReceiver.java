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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.UUID;
import java.io.OutputStreamWriter;
import java.io.Writer;
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
  private static ObjectWriter JSON_WRITER = new ObjectMapper().writer();
  private String file;
  private Writer writer;
  private ExecutorService executor;
  private long executorTerminationTimeoutDuration;

  public LocalFileSpanReceiver(HTraceConfiguration conf) {
    this.executorTerminationTimeoutDuration = EXECUTOR_TERMINATION_TIMEOUT_DURATION_DEFAULT;
    int capacity = conf.getInt(CAPACITY_KEY, CAPACITY_DEFAULT);
    this.file = conf.get(PATH_KEY);
    if (file == null || file.isEmpty()) {
      throw new IllegalArgumentException("must configure " + PATH_KEY);
    }
    this.executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(capacity));
    boolean success = false;
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(file, true);
      this.writer = new BufferedWriter(
          new OutputStreamWriter(fos,"UTF-8"));
      success = true;
    } catch (IOException ioe) {
      LOG.warn("Error opening " + file + ": " + ioe.getMessage());
      throw new RuntimeException(ioe);
    } finally {
      if (!success) {
        if (fos != null) {
          try {
            fos.close();
          } catch (IOException e) {
            LOG.error("Error closing output stream for " + file, e);
          }
        }
      }
    }
  }

  private class WriteSpanRunnable implements Runnable {
    public final Span span;

    public WriteSpanRunnable(Span span) {
      this.span = span;
    }

    @Override
    public void run() {
      try {
        JSON_WRITER.writeValue(writer, span);
        writer.write("%n");
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
      writer.close();
    } catch (IOException e) {
      LOG.error("Error closing writer for file: " + file, e);
    }
  }

  public static String getUniqueLocalTraceFileName() {
    String tmp = System.getProperty("java.io.tmpdir", "/tmp");
    String nonce = null;
    BufferedReader reader = null;
    try {
      // On Linux we can get a unique local file name by reading the process id
      // out of /proc/self/stat.  (There isn't any portable way to get the
      // process ID from Java.)
      reader = new BufferedReader(
          new InputStreamReader(new FileInputStream("/proc/self/stat"),
                                "UTF-8"));
      String line = reader.readLine();
      if (line == null) {
        throw new EOFException();
      }
      nonce = line.split(" ")[0];
    } catch (IOException e) {
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch(IOException e) {
          LOG.warn("Exception in closing " + reader, e);
        }
      }
    }
    if (nonce == null) {
      // If we can't use the process ID, use a random nonce.
      nonce = UUID.randomUUID().toString();
    }
    return new File(tmp, nonce).getAbsolutePath();
  }
}
