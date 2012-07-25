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
package org.apache.hadoop.hbase.htrace;

import java.util.concurrent.Callable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.htrace.impl.NullSpan;
import org.apache.hadoop.hbase.htrace.wrappers.TraceCallable;
import org.apache.hadoop.hbase.htrace.wrappers.TraceRunnable;

/**
 * NOTE: The *withSampling functions work, but the sampling and general control
 * of tracing has yet to be completely decided (color of the bike shed).
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Trace {

  /**
   * Sets the sampler that will be used when calling a *withSampling method with
   * no explicit Sampler passed in.
   * 
   * @param sampler
   */
  public static void setSampler(Sampler sampler) {
    Tracer.getInstance().setSampler(sampler);
  }

  /**
   * Equivalent to 'startTraceIfNotStarted', but only calls
   * 'startTraceIfNotStarted' if the current Trace's sampler is not null and
   * returns true on a next() call.
   * 
   * @param description
   * @return The Span just started if this trace is to be sampled, otherwise the
   *         NullSpan
   */
  public static Span startTraceIfNotStartedWithSampling(String description) {
    return startTraceIfNotStartedWithSampling(description, Tracer.getInstance()
        .getSampler());
  }

  /**
   * Equivalent to 'startTraceIfNotStarted', but only calls
   * 'startTraceIfNotStarted' if the current Trace's sampler is not null and
   * returns true on a next(info) call.
   * 
   * @param description
   * @param info
   *          Information necessary for the Sampler to decide whether or not to
   *          sample a trace.
   * @return The Span just started if this trace is to be sampled, otherwise the
   *         NullSpan
   */
  public static Span startTraceIfNotStartedWithSampling(String description,
      Object info) {
    return startTraceIfNotStartedWithSampling(description, info, Tracer
        .getInstance().getSampler());
  }

  /**
   * Equivalent to 'startTraceIfNotStarted', but only calls
   * 'startTraceIfNotStarted' if 'sampler' is not null and returns true on a
   * next() call.
   * 
   * @param description
   * @param sampler
   * @return The Span just started if this trace is to be sampled, otherwise the
   *         NullSpan
   */
  public static Span startTraceIfNotStartedWithSampling(String description,
      Sampler sampler) {
    if (sampler != null && sampler.next()) {
      return startTraceIfNotStarted(description);
    } else {
      return NullSpan.getInstance();
    }
  }

  /**
   * Equivalent to 'startTraceIfNotStarted', but only calls
   * 'startTraceIfNotStarted' if 'sampler' is not null and returns true on a
   * next(info) call.
   * 
   * @param description
   * @param info
   *          Information necessary for the Sampler to decide whether or not to
   *          sample a trace.
   * @param sampler
   * @return The Span just started if this trace is to be sampled, otherwise the
   *         NullSpan
   */

  public static Span startTraceIfNotStartedWithSampling(String description,
      Object info, Sampler sampler) {
    if (sampler != null && sampler.next(info)) {
      return startTraceIfNotStarted(description);
    } else {
      return NullSpan.getInstance();
    }
  }

  /**
   * Starts tracing in the current thread if it has not already been started. If
   * a trace has already started, just begins a new Span in the current trace.
   * 
   * @return Span The span that has just been started.
   */
  public static Span startTraceIfNotStarted(String description) {
    return Tracer.getInstance().on(description);
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
  public static void data(String key, String value) {
    if (isTracing()) {
      currentTrace().data(key, value);
    }
  }

  /**
   * Provides the current trace's span ID and parent ID, or the TInfo: (0,0) if
   * tracing is currently off.
   * 
   * @return TINfo current trace or the sentinel no-trace (0,0) if tracing off
   */
  public static TInfo traceInfo() {
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
   * Stop a trace in this thread, simply sets current trace to null if 'span' is
   * null.
   * 
   * @param span
   *          The Span to stop.
   */
  public static void off(Span span) {
    Tracer.getInstance().stop(span);
  }

  /**
   * Stops the current trace, sets the current trace to 'null', and calls flush
   * on all registered SpanReceiver's.
   */
  public static void off() {
    off(true);
  }

  /**
   * Stops the current trace, and sets the current trace to 'null'.
   * 
   * @param shouldFlush
   *          True if 'flush' should be called on registered SpanReceivers,
   *          false otherwise.
   */
  public static void off(boolean shouldFlush) {
    Tracer.getInstance().stop();
    if (shouldFlush) {
      Tracer.getInstance().flush();
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
  public static Span currentTrace() {
    return Tracer.getInstance().currentTrace();
  }

  /**
   * Create a new time span (if tracing is on), with the given description.
   * 
   * @param description
   *          Description of the trace to be started.
   * @return If tracing is on, the Span just started, otherwise the NULL Span.
   */
  public static Span startSpanInCurrentTrace(String description) {
    return Tracer.getInstance().start(description);
  }

  /**
   * Start a trace in the current thread from information passed via RPC.
   * 
   * @param traceId
   * @param parentId
   * @param description
   * @return The Span just started.
   */
  public static Span continueTrace(long traceId, long parentId,
      String description) {
    return continueTrace(new TInfo(traceId, parentId), description);
  }

  /**
   * Continue a trace with info from RPC.
   * 
   * @param info
   * @param description
   * @return The Span just started.
   */
  public static Span continueTrace(TInfo info, String description) {
    if (info.traceId == 0) {
      return NullSpan.getInstance();
    }
    return Tracer.getInstance().continueTrace(description, info.traceId,
        info.parentId);
  }

  /**
   * Continue a trace with info from a Span parent.
   * 
   * @param parent
   * @param description
   * @return The Span just started.
   */
  public static Span continueTrace(Span parent, String description) {
    if (parent.traceId() == 0) {
      return NullSpan.getInstance();
    }
    return Tracer.getInstance().continueTrace(parent, description);
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

  /**
   * Trace's can have process id's, which are set on a per-Tracer basis. The id
   * can be an ip address or a host name, etc.
   * 
   * @param processId
   */
  public static void setProcessId(String processId) {
    Tracer.processId = processId;
  }
}
