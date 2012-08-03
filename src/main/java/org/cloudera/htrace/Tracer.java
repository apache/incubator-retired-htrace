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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.cloudera.htrace.impl.NullSpan;
import org.cloudera.htrace.impl.RootMilliSpan;

/**
 * A Tracer provides the implementation for collecting and distributing Spans
 * within a process.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Tracer {
  private final static Random random = new SecureRandom();
  private final List<SpanReceiver> receivers = new ArrayList<SpanReceiver>();
  private static final ThreadLocal<Span> currentTrace = new ThreadLocal<Span>();
  private static final TraceInfo DONT_TRACE = new TraceInfo(0, 0);
  protected static String processId = "";

  private static Tracer instance = null;

  synchronized protected static void setInstance(Tracer tracer) {
    instance = tracer;
  }

  synchronized protected static Tracer getInstance() {
    if (instance == null) {
      instance = new Tracer();
    }
    return instance;
  }

  protected static TraceInfo traceInfo() {
    Span span = currentTrace.get();
    if (span != null) {
      return new TraceInfo(span.getTraceId(), span.getSpanId());
    }
    return DONT_TRACE;
  }

  protected Span start(String description) {
    Span parent = currentTrace.get();
    if (parent == null)
      return NullSpan.getInstance();
    return push(parent.child(description));
  }

  protected Span on(String description) {
    Span parent = currentTrace.get();
    Span root;
    if (parent == null) {
      root = new RootMilliSpan(description, random.nextLong(),
          random.nextLong(), Span.ROOT_SPAN_ID, processId);
    } else {
      root = parent.child(description);
    }
    return push(root);
  }

  protected void stop() {
    stop(currentTrace());
  }

  protected void stop(Span span) {
    if (span != null) {
      span.stop();
      currentTrace.set(null);
    }
  }

  protected boolean isTracing() {
    return currentTrace.get() != null;
  }

  protected Span currentTrace() {
    return currentTrace.get();
  }

  protected void deliver(Span span) {
    for (SpanReceiver receiver : receivers) {
      receiver.receiveSpan(span);
    }
  }

  protected synchronized void addReceiver(SpanReceiver receiver) {
    receivers.add(receiver);
  }

  protected synchronized void removeReceiver(SpanReceiver receiver) {
    receivers.remove(receiver);
  }

  protected Span push(Span span) {
    if (span != null) {
      currentTrace.set(span);
      span.start();
    }
    return span;
  }

  protected void pop(Span span) {
    if (span != null) {
      deliver(span);
      currentTrace.set(span.getParent());
    } else {
      currentTrace.set(null);
    }
  }

  protected Span continueTrace(Span parent, String activity) {
    return push(parent.child(activity));
  }

  protected Span continueTrace(String description, long traceId, long parentId) {
    return push(new RootMilliSpan(description, traceId, random.nextLong(),
        parentId, processId));
  }

  protected int numReceivers() {
    return receivers.size();
  }
}
