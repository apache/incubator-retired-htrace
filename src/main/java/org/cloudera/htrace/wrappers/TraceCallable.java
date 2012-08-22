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
package org.cloudera.htrace.wrappers;

import java.util.concurrent.Callable;

import org.cloudera.htrace.Sampler;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.Trace;

/**
 * Wrap a Callable with a Span that survives a change in threads.
 * 
 */
public class TraceCallable<V> implements Callable<V> {
  private final Callable<V> impl;
  private final Span parent;

  public TraceCallable(Callable<V> impl) {
    this(Trace.currentTrace(), impl);
  }

  public TraceCallable(Span parent, Callable<V> impl) {
    this.impl = impl;
    this.parent = parent;
  }

  @Override
  public V call() throws Exception {
    if (parent != null) {
      Span chunk = Trace.startSpan(Thread.currentThread().getName(), parent,
          Sampler.ALWAYS);

      try {
        return impl.call();
      } finally {
        chunk.stop();
      }
    } else {
      return impl.call();
    }
  }

  public Callable<V> getImpl() {
    return impl;
  }
}
