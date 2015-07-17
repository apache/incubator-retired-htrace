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
package org.apache.htrace;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.impl.MilliSpan;
import org.apache.htrace.impl.NeverSampler;
import org.apache.htrace.wrappers.TraceCallable;
import org.apache.htrace.wrappers.TraceRunnable;

import java.util.concurrent.Callable;

/**
 * The Trace class is the primary way to interact with the library.  It provides
 * methods to create and manipulate spans.
 *
 * A 'Span' represents a length of time.  It has many other attributes such as a
 * description, ID, and even potentially a set of key/value strings attached to
 * it.
 *
 * Each thread in your application has a single currently active currentSpan
 * associated with it.  When this is non-null, it represents the current
 * operation that the thread is doing.  Spans are NOT thread-safe, and must
 * never be used by multiple threads at once.  With care, it is possible to
 * safely pass a Span object between threads, but in most cases this is not
 * necessary.
 *
 * A 'TraceScope' can either be empty, or contain a Span.  TraceScope objects
 * implement the Java's Closeable interface.  Similar to file descriptors, they
 * must be closed after they are created.  When a TraceScope contains a Span,
 * this span is closed when the scope is closed.
 *
 * The 'startSpan' methods in this class do a few things:
 * <ul>
 *   <li>Create a new Span which has this thread's currentSpan as one of its parents.</li>
 *   <li>Set currentSpan to the new Span.</li>
 *   <li>Create a TraceSpan object to manage the new Span.</li>
 * </ul>
 *
 * Closing a TraceScope does a few things:
 * <ul>
 *   <li>It closes the span which the scope was managing.</li>
 *   <li>Set currentSpan to the previous currentSpan (which may be null).</li>
 * </ul>
 */
public class Trace {
  private static final Log LOG = LogFactory.getLog(Trace.class);

  /**
   * Creates a new trace scope.
   *
   * If this thread has a currently active trace span, the trace scope we create
   * here will contain a new span descending from the currently active span.
   * If there is no currently active trace span, the trace scope we create will
   * be empty.
   *
   * @param description   The description field for the new span to create.
   */
  public static TraceScope startSpan(String description) {
    return startSpan(description, NeverSampler.INSTANCE);
  }

  public static TraceScope startSpan(String description, TraceInfo tinfo) {
    if (tinfo == null) return continueSpan(null);
    Span newSpan = new MilliSpan.Builder().
        begin(System.currentTimeMillis()).
        end(0).
        description(description).
        traceId(tinfo.traceId).
        spanId(Tracer.nonZeroRandom64()).
        parents(new long[] { tinfo.spanId }).
        build();
    return continueSpan(newSpan);
  }

  /**
   * Creates a new trace scope.
   *
   * If this thread has a currently active trace span, it must be the 'parent'
   * span that you pass in here as a parameter.  The trace scope we create here
   * will contain a new span which is a child of 'parent'.
   *
   * @param description   The description field for the new span to create.
   */
  public static TraceScope startSpan(String description, Span parent) {
    if (parent == null) {
      return startSpan(description);
    }
    Span currentSpan = currentSpan();
    if ((currentSpan != null) && (currentSpan != parent)) {
      Tracer.clientError("HTrace client error: thread " +
          Thread.currentThread().getName() + " tried to start a new Span " +
          "with parent " + parent.toString() + ", but there is already a " +
          "currentSpan " + currentSpan);
    }
    return continueSpan(parent.child(description));
  }

  public static <T> TraceScope startSpan(String description, Sampler<T> s) {
    return startSpan(description, s, null);
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
    // Return an empty TraceScope that does nothing on close
    if (s == null) return NullScope.INSTANCE;
    return Tracer.getInstance().continueSpan(s);
  }

  /**
   * Removes the given SpanReceiver from the list of SpanReceivers.
   */
  public static void removeReceiver(SpanReceiver rcvr) {
    Tracer.getInstance().removeReceiver(rcvr);
  }

  /**
   * Adds the given SpanReceiver to the current Tracer instance's list of
   * SpanReceivers.
   */
  public static void addReceiver(SpanReceiver rcvr) {
    Tracer.getInstance().addReceiver(rcvr);
  }

  /**
   * Adds a data annotation to the current span if tracing is currently on.
   */
  public static void addKVAnnotation(String key, String value) {
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
   * @param runnable The runnable that will have tracing info associated with it if tracing.
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
