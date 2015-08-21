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

/**
 * An empty {@link TraceScope}.
 */
class NullScope extends TraceScope {
  NullScope(Tracer tracer) {
    super(tracer, null, null);
  }

  @Override
  public SpanId getSpanId() {
    return SpanId.INVALID;
  }

  @Override
  public void detach() {
    if (detached) {
      Tracer.throwClientError("Can't detach this TraceScope  because " +
          "it is already detached.");
    }
    detached = true;
  }

  @Override
  public void reattach() {
    if (!detached) {
      Tracer.throwClientError("Can't reattach this TraceScope  because " +
          "it is not detached.");
    }
    detached = false;
  }

  @Override
  public void close() {
    tracer.popNullScope();
  }

  @Override
  public String toString() {
    return "NullScope";
  }

  @Override
  public void addKVAnnotation(String key, String value) {
    // do nothing
  }

  @Override
  public void addTimelineAnnotation(String msg) {
    // do nothing
  }
}
