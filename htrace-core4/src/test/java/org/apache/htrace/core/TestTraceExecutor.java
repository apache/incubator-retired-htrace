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

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

public class TestTraceExecutor {
  private static final int WAIT_TIME_SECONDS = 60;

  @Test
  public void testExecutorWithNullScope() throws Exception {
    HTraceConfiguration conf = HTraceConfiguration.fromKeyValuePairs("sampler.classes", "AlwaysSampler");
    ExecutorService es = null;
    try (Tracer tracer = new Tracer.Builder("TestTraceExecutor").conf(conf).build()) {
      es = Executors.newSingleThreadExecutor(new NamingThreadFactory());
      es = tracer.newTraceExecutorService(es);

      try (TraceScope scope = tracer.newScope("TestRunnable")) {
        final AtomicReference<String> description = new AtomicReference<String>("");
        es.submit(new Runnable() {
          @Override
          public void run() {
            description.set(Tracer.getCurrentSpan().getDescription());
          }
        }).get(WAIT_TIME_SECONDS, TimeUnit.SECONDS);
        assertEquals("TraceRunnable did not set description correctly", "child-thread-1", description.get());
      }

      try (TraceScope scope = tracer.newScope("TestCallable")) {
        String description = es.submit(new Callable<String>() {
          @Override
          public String call() throws Exception {
            return Tracer.getCurrentSpan().getDescription();
          }
        }).get(WAIT_TIME_SECONDS, TimeUnit.SECONDS);
        assertEquals("TraceCallable did not set description correctly", "child-thread-1", description);
      }
    } finally {
      if (es != null) {
        es.shutdown();
      }
    }
  }

  /*
   * Inspired by org.apache.solr.util.DefaultSolrThreadFactory
   */
  static class NamingThreadFactory implements ThreadFactory {
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    public NamingThreadFactory() {
      SecurityManager s = System.getSecurityManager();
      group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
    }

    @Override
    public Thread newThread(Runnable r) {
      return new Thread(group, r, "child-thread-" + threadNumber.getAndIncrement(), 0);
    }
  }
}
