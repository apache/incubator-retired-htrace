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

import java.util.concurrent.Callable;

/**
 * Wrap a Callable with a Span that survives a change in threads.
 */
public class TraceCallable<V> implements Callable<V> {
  private final Tracer tracer;
  private final Callable<V> impl;
  private final SpanId parentId;
  private final String description;

  public TraceCallable(Tracer tracer, SpanId parentId, Callable<V> impl,
      String description) {
    this.tracer = tracer;
    this.impl = impl;
    this.parentId = parentId;
    this.description = description;
  }

  @Override
  public V call() throws Exception {
    String description = this.description;
    if (description == null) {
      description = Thread.currentThread().getName();
    }
    try (TraceScope chunk = tracer.newScope(description, parentId)) {
      return impl.call();
    }
  }

  public Callable<V> getImpl() {
    return impl;
  }
}
