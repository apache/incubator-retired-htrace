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
package org.apache.htrace.core;

import java.io.Closeable;

/**
 * Create a new TraceScope at major transitions. Hosts current tracing context.
 */
public class TraceScope implements Closeable {
  /**
   * The tracer to use for this scope.
   */
  final Tracer tracer;

  /**
   * The trace span for this scope, or null if the scope is closed.
   *
   * If the scope is closed, it must also be detached.
   */
  private final Span span;

  /**
   * The parent of this trace scope, or null if there is no parent.
   */
  private TraceScope parent;

  /**
   * True if this scope is detached.
   */
  boolean detached;

  TraceScope(Tracer tracer, Span span, TraceScope parent) {
    this.tracer = tracer;
    this.span = span;
    this.parent = parent;
    this.detached = false;
  }

  /**
   * Returns the span which this scope is managing.
   */
  public Span getSpan() {
    return span;
  }

  /**
   * Returns the span ID which this scope is managing.
   */
  public SpanId getSpanId() {
    return span.getSpanId();
  }

  TraceScope getParent() {
    return parent;
  }

  void setParent(TraceScope parent) {
    this.parent = parent;
  }

  /**
   * Detach this TraceScope from the current thread.
   *
   * It is OK to "leak" TraceScopes which have been detached.  They will not
   * consume any resources other than a small amount of memory until they are
   * garbage collected.  On the other hand, trace scopes which are still
   * attached must never be leaked.
   */
  public void detach() {
    if (detached) {
      Tracer.throwClientError("Can't detach this TraceScope  because " +
          "it is already detached.");
    }
    tracer.detachScope(this);
    detached = true;
    parent = null;
  }

  /**
   * Attach this TraceScope to the current thread.
   */
  public void reattach() {
    if (!detached) {
      Tracer.throwClientError("Can't reattach this TraceScope  because " +
          "it is not detached.");
    }
    tracer.reattachScope(this);
    detached = false;
  }

  /**
   * Close this TraceScope, ending the trace span it is managing.
   */
  @Override
  public void close() {
    tracer.closeScope(this);
  }

  public void addKVAnnotation(String key, String value) {
    span.addKVAnnotation(key, value);
  }

  public void addTimelineAnnotation(String msg) {
    span.addTimelineAnnotation(msg);
  }

  @Override
  public String toString() {
    return "TraceScope(tracerId=" + tracer.getTracerId() +
        ", span=" + span.toJson() +
        ", detached=" + detached + ")";
  }
}
