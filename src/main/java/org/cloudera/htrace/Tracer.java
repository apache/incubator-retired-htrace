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
package org.cloudera.htrace;

import java.lang.management.ManagementFactory;
import java.security.SecureRandom;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudera.htrace.impl.MilliSpan;

/**
 * A Tracer provides the implementation for collecting and distributing Spans
 * within a process.
 */
public class Tracer {
  public static final Log LOG = LogFactory.getLog(Tracer.class);
  private final static Random random = new SecureRandom();
  private final List<SpanReceiver> receivers = new CopyOnWriteArrayList<SpanReceiver>();
  private static final ThreadLocal<Span> currentSpan = new ThreadLocal<Span>() {
    @Override
    protected Span initialValue() {
      return null;
    }
  };
  public static final TraceInfo DONT_TRACE = new TraceInfo(-1, -1);
  protected static String processId = null;

  private static Tracer instance = null;

  synchronized protected static Tracer getInstance() {
    if (instance == null) {
      instance = new Tracer();
    }
    return instance;
  }

  protected Span createNew(String description) {
    Span parent = currentSpan.get();
    if (parent == null) {
      return new MilliSpan(description,
          /* traceId = */ random.nextLong(),
          /* parentSpanId = */ Span.ROOT_SPAN_ID,
          /* spanId = */ random.nextLong(),
          getProcessId());
    } else {
      return parent.child(description);
    }
  }

  protected boolean isTracing() {
    return currentSpan.get() != null;
  }

  protected Span currentSpan() {
    return currentSpan.get();
  }

  protected void deliver(Span span) {
    for (SpanReceiver receiver : receivers) {
      receiver.receiveSpan(span);
    }
  }

  protected void addReceiver(SpanReceiver receiver) {
    receivers.add(receiver);
  }

  protected void removeReceiver(SpanReceiver receiver) {
    receivers.remove(receiver);
  }

  protected Span setCurrentSpan(Span span) {
    currentSpan.set(span);
    return span;
  }
  

  public TraceScope continueSpan(Span s) {
    Span oldCurrent = currentSpan();
    setCurrentSpan(s);
    return new TraceScope(s, oldCurrent);
  }

  protected int numReceivers() {
    return receivers.size();
  }

  static String getProcessId() {
    if (processId == null) {
      String mxBeanName = ManagementFactory.getRuntimeMXBean().getName();
      String cmdLine = System.getProperty("sun.java.command");
      if (cmdLine != null && !cmdLine.isEmpty()) {
        String fullClassName = cmdLine.split("\\s+")[0];
        String[] classParts = fullClassName.split("\\.");
        cmdLine = classParts[classParts.length - 1];
      }

      processId = cmdLine + " (" + mxBeanName + ")";
    }
    return processId;
  }
}