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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TraceExecutorService implements ExecutorService {
  private final Tracer tracer;
  private final String scopeName;
  private final ExecutorService impl;

  TraceExecutorService(Tracer tracer, String scopeName,
                       ExecutorService impl) {
    this.tracer = tracer;
    this.scopeName = scopeName;
    this.impl = impl;
  }

  @Override
  public void execute(Runnable command) {
    impl.execute(tracer.wrap(command, scopeName));
  }

  @Override
  public void shutdown() {
    impl.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return impl.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return impl.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return impl.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit)
      throws InterruptedException {
    return impl.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return impl.submit(tracer.wrap(task, scopeName));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return impl.submit(tracer.wrap(task, scopeName), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return impl.submit(tracer.wrap(task, scopeName));
  }

  private <T> Collection<? extends Callable<T>> wrapCollection(
      Collection<? extends Callable<T>> tasks) {
    List<Callable<T>> result = new ArrayList<Callable<T>>();
    for (Callable<T> task : tasks) {
      result.add(tracer.wrap(task, scopeName));
    }
    return result;
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return impl.invokeAll(wrapCollection(tasks));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                       long timeout, TimeUnit unit) throws InterruptedException {
    return impl.invokeAll(wrapCollection(tasks), timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return impl.invokeAny(wrapCollection(tasks));
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout,
                         TimeUnit unit) throws InterruptedException, ExecutionException,
      TimeoutException {
    return impl.invokeAny(wrapCollection(tasks), timeout, unit);
  }

}
