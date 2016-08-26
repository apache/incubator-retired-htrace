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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A convenience wrapper around a {@link ScheduledExecutorService} for
 * automatically propagating trace scopes to executable tasks.
 * <p>
 * Recurring tasks will use independent scopes per execution, but will all be
 * tied to the same parent scope (if any).
 */
public class ScheduledTraceExecutorService extends TraceExecutorService
    implements ScheduledExecutorService {
  final ScheduledExecutorService impl;

  ScheduledTraceExecutorService(Tracer tracer, String scopeName,
      ScheduledExecutorService impl) {
    super(tracer, scopeName, impl);
    this.impl = impl;
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay,
      TimeUnit unit) {
    return impl.schedule(wrap(command), delay, unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay,
      TimeUnit unit) {
    return impl.schedule(wrap(callable), delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
      long initialDelay, long period, TimeUnit unit) {
    return impl.scheduleAtFixedRate(wrap(command), initialDelay, period, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
      long initialDelay, long delay, TimeUnit unit) {
    return impl.scheduleWithFixedDelay(wrap(command), initialDelay, delay,
        unit);
  }

}
