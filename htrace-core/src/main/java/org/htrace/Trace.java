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
package org.htrace;

import org.htrace.impl.MilliSpan;
import org.htrace.impl.TrueIfTracingSampler;
import org.htrace.wrappers.TraceCallable;
import org.htrace.wrappers.TraceRunnable;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * The primary way to interact with the library. Provides methods to start
 * spans, as well as set necessary tracing information.
 */
public class Trace {
  private final static Random random = new SecureRandom();

  /**
   * Starts and returns a new span as the child of the current span if the
   * default sampler (TrueIfTracingSampler) returns true, otherwise returns the
   * NullSpan.
   *
   * @param description Description of the span to be created.
   * @return
   */
  public static TraceScope startSpan(String description) {
    return startSpan(description, TrueIfTracingSampler.INSTANCE);
  }

  /**
   * Starts and returns a new span as the child of the parameter 'parent'. This
   * will always return a new span, even if tracing wasn't previously enabled for
   * this thread.
   *
   * @param description Description of the span to be created.
   * @param parent      The parent that should be used to create the child span that is to
   *                    be returned.
   * @return
   */
  public static TraceScope startSpan(String description, Span parent) {
    if (parent == null) return startSpan(description);
    return continueSpan(parent.child(description));
  }

  public static TraceScope startSpan(String description, TraceInfo tinfo) {
    if (tinfo == null) return continueSpan(null);
    Span newSpan = new MilliSpan(description, tinfo.traceId, tinfo.spanId,
        random.nextLong(), Tracer.getProcessId());
    return continueSpan(newSpan);
  }

  public static <T> TraceScope startSpan(String description, Sampler<T> s) {
    return startSpan(description, s, null);
  }

  public static TraceScope startSpan(String description, Sampler<TraceInfo> s, TraceInfo tinfo) {
    Span span = null;
    if (isTracing() || s.next(tinfo)) {
      span = new MilliSpan(description, tinfo.traceId, tinfo.spanId,
          random.nextLong(), Tracer.getProcessId());
    }
    return continueSpan(span);
  }

  public static <T> TraceScope startSpan(String description, Sampler<T> s, T info) {
    Span span = null;
    if (isTracing() || s.next(info)) {
      span = Tracer.getInstance().createNew(description);
    }
    return continueSpan(span);
  }

  /**
   * Pick up an existing span from another thread.
   */
  public static TraceScope continueSpan(Span s) {
    // Return an empty TraceScope that does nothing on
    // close
    if (s == null) return new TraceScope(null, null);
    return Tracer.getInstance().continueSpan(s);
  }

  /**
   * Set the processId to be used for all Spans created by this Tracer.
   *
   * @param processId
   * @see Span.java
   */
  public static void setProcessId(String processId) {
    Tracer.processId = processId;
  }

  /**
   * Removes the given SpanReceiver from the list of SpanReceivers.
   *
   * @param rcvr
   */
  public static void removeReceiver(SpanReceiver rcvr) {
    Tracer.getInstance().removeReceiver(rcvr);
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
   */
  public static void addKVAnnotation(byte[] key, byte[] value) {
    Span s = currentSpan();
    if (s != null) {
      s.addKVAnnotation(key, value);
    }
  }

  /**
   * Annotate the current span with the given message.
   */
  public static void addTimelineAnnotation(String msg) {
    Span s = currentSpan();
    if (s != null) {
      s.addTimelineAnnotation(msg);
    }
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
  public static Span currentSpan() {
    return Tracer.getInstance().currentSpan();
  }

  /**
   * Wrap the callable in a TraceCallable, if tracing.
   *
   * @param callable
   * @return The callable provided, wrapped if tracing, 'callable' if not.
   */
  public static <V> Callable<V> wrap(Callable<V> callable) {
    if (isTracing()) {
      return new TraceCallable<V>(Trace.currentSpan(), callable);
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
      return new TraceRunnable(Trace.currentSpan(), runnable);
    } else {
      return runnable;
    }
  }

  /**
   * Wrap the runnable in a TraceRunnable, if tracing
   *
   * @param description name of the span to be created.
   * @param runnable
   * @return The runnable provided, wrapped if tracing, 'runnable' if not.
   */
  public static Runnable wrap(String description, Runnable runnable) {
    if (isTracing()) {
      return new TraceRunnable(Trace.currentSpan(), runnable, description);
    } else {
      return runnable;
    }
  }
}
