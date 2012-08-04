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
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.cloudera.htrace.impl.NullSpan;
import org.cloudera.htrace.impl.RootMilliSpan;
import org.cloudera.htrace.impl.TrueIfTracingSampler;
import org.cloudera.htrace.wrappers.TraceCallable;
import org.cloudera.htrace.wrappers.TraceRunnable;

@SuppressWarnings("unchecked")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Trace {
  private final static Random random = new SecureRandom();

  public static Span startSpan(String description) {
    return startSpan(description, TrueIfTracingSampler.getInstance());
  }

  public static Span startSpan(String description, Span parent) {
    return startSpan(description, parent, TrueIfTracingSampler.getInstance());
  }

  public static Span startSpan(String description, TraceInfo tinfo) {
    return startSpan(description, tinfo, TrueIfTracingSampler.getInstance());
  }
  
  public static Span startSpan(String description, long traceId, long parentId) {
    return startSpan(description, traceId, parentId,
        TrueIfTracingSampler.getInstance());
  }
  
  public static <T> Span startSpan(String description, Sampler<T> s) {
    return startSpan(description, s, null);
  }

  public static <T> Span startSpan(String description, Span parent, Sampler<T> s) {
    return startSpan(description, parent, s, null);
  }

  public static <T> Span startSpan(String description, TraceInfo tinfo,
      Sampler<T> s) {
    return startSpan(description, tinfo, s, null);
  }

  public static <T> Span startSpan(String description, long traceId,
      long parentId, Sampler<T> s) {
    return startSpan(description, traceId, parentId, s, null);
  }

  public static <T> Span startSpan(String description, Sampler<T> s, T info) {
    if (s.next(info)) {
      return Tracer.getInstance().on(description);
    }
    return NullSpan.getInstance();
  }

  public static <T> Span startSpan(String description, Span parent,
      Sampler<T> s, T info) {
    if (s.next(info)) {
      return Tracer.getInstance().push(parent.child(description));
    }
    return NullSpan.getInstance();
  }

  public static <T> Span startSpan(String description, TraceInfo tinfo,
      Sampler<T> s, T info) {
    if (s.next(info)) {
      return Tracer.getInstance().push(
          new RootMilliSpan(description, tinfo.traceId, random.nextLong(),
              tinfo.parentSpanId, Tracer.processId));
    }
    return NullSpan.getInstance();
  }

  public static <T> Span startSpan(String description, long traceId,
      long parentId, Sampler<T> s, T info) {
    if (s.next(info)) {
      return Tracer.getInstance().push(
          new RootMilliSpan(description, traceId, random.nextLong(), parentId,
              Tracer.processId));
    }
    return NullSpan.getInstance();
  }

  /**
   * Set the processId to be used for all Spans created by this Tracer.
   *
   * @see Span.java
   * @param processId
   */
  public static void setProcessId(String processId) {
    Tracer.processId = processId;
  }


  /**
   * Adds the given SpanReceiver to the current Tracer instance's list of
   * SpanReceivers.
   *
   * @param rcvr
   */
  public static void addReceiver(SpanReceiver rcvr) {
    Tracer.getInstance().addReceiver(rcvr);
  }

  /**
   * Adds a data annotation to the current span if tracing is currently on.
   *
   * @param key
   * @param value
   */
  public static void addAnnotation(byte[] key, byte[] value) {
    if (isTracing()) {
      currentTrace().addAnnotation(key, value);
    }
  }

  /**
   * Provides the current trace's span ID and parent ID, or the TInfo: (0,0) if
   * tracing is currently off.
   *
   * @return TINfo current trace or the sentinel no-trace (0,0) if tracing off
   */
  public static TraceInfo traceInfo() {
    return Tracer.traceInfo();
  }

  /**
   * If 'span' is not null, it is delivered to any registered SpanReceiver's,
   * and sets the current span to 'span's' parent. If 'span' is null, the
   * current trace is set to null.
   *
   * @param span
   *          The Span to be popped
   */
  public static void pop(Span span) {
    Tracer.getInstance().pop(span);
  }

  /**
   * Returns true if the current thread is a part of a trace, false otherwise.
   *
   * @return
   */
  public static boolean isTracing() {
    return Tracer.getInstance().isTracing();
  }

  /**
   * If we are tracing, return the current span, else null
   *
   * @return Span representing the current trace, or null if not tracing.
   */
  public static Span currentTrace() {
    return Tracer.getInstance().currentTrace();
  }

  /**
   * Wrap the callable in a TraceCallable, if tracing.
   *
   * @param callable
   * @return The callable provided, wrapped if tracing, 'callable' if not.
   */
  public static <V> Callable<V> wrap(Callable<V> callable) {
    if (isTracing()) {
      return new TraceCallable<V>(Trace.currentTrace(), callable);
    } else {
      return callable;
    }
  }

  /**
   * Wrap the runnable in a TraceRunnable, if tracing
   *
   * @param runnable
   * @return The runnable provided, wrapped if tracing, 'runnable' if not.
   */
  public static Runnable wrap(Runnable runnable) {
    if (isTracing()) {
      return new TraceRunnable(Trace.currentTrace(), runnable);
    } else {
      return runnable;
    }
  }
}
