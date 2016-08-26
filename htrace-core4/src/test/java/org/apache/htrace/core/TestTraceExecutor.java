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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.htrace.core.Tracer.Builder;
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

  @Test
  public void testWrappingFromSpan() throws Exception {
    HTraceConfiguration conf = HTraceConfiguration.fromKeyValuePairs("sampler.classes", "AlwaysSampler");

    ExecutorService es = Executors.newSingleThreadExecutor();
    try (Tracer tracer = new Tracer.Builder("TestTraceExecutor").conf(conf).build()) {
      SpanId random = SpanId.fromRandom();
      try (TraceScope parentScope = tracer.newScope("parent")) {
        Callable<SpanId> callable = new TraceCallable<SpanId>(tracer, random, new Callable<SpanId>() {
          @Override
          public SpanId call() throws Exception {
            return Tracer.getCurrentSpan().getParents()[0];
          }
        }, "child");
        SpanId result = es.submit(callable).get(WAIT_TIME_SECONDS, TimeUnit.SECONDS);
        assertEquals(random, result);
      }
    } finally {
      es.shutdown();
    }
  }

  @Test
  public void testScheduledExecutor() throws Exception {
    final int TASK_COUNT = 3;
    final int DELAY = 500;

    HTraceConfiguration conf = HTraceConfiguration.fromKeyValuePairs(
        Tracer.SAMPLER_CLASSES_KEY, AlwaysSampler.class.getName());

    ScheduledExecutorService ses = null;
    Builder builder = new Tracer.Builder("TestTraceExecutor").conf(conf);
    try (Tracer tracer = builder.build()) {
      final ThreadFactory tf = new NamingThreadFactory();
      ses = Executors.newScheduledThreadPool(TASK_COUNT, tf);
      ses = tracer.newTraceExecutorService(ses);

      final CountDownLatch startLatch = new CountDownLatch(TASK_COUNT);
      final CountDownLatch continueLatch = new CountDownLatch(1);
      Callable<String> task = new Callable<String>() {
        @Override
        public String call() throws InterruptedException {
          startLatch.countDown();
          // Prevent any task from exiting until every task has started
          assertTrue(continueLatch.await(WAIT_TIME_SECONDS, TimeUnit.SECONDS));
          // Annotate on the presumed child trace
          Tracer.getCurrentSpan().addTimelineAnnotation(
              Thread.currentThread().getName());
          return Tracer.getCurrentSpan().getDescription();
        }
      };

      try (TraceScope scope = tracer.newScope("TestRunnable")) {
        Collection<Future<String>> futures = new ArrayList<>();

        for (int i = 0; i < TASK_COUNT; i++) {
          futures.add(ses.schedule(task, DELAY, TimeUnit.MILLISECONDS));
        }

        // Wait for all tasks to start
        assertTrue(startLatch.await(WAIT_TIME_SECONDS, TimeUnit.SECONDS));
        continueLatch.countDown();
        // Collect the expected results
        Collection<String> results = new HashSet<>();
        for (Future<String> future : futures) {
          results.add(future.get(WAIT_TIME_SECONDS, TimeUnit.SECONDS));
        }

        assertTrue("Timeline Annotations should have gone to child traces.",
            Tracer.getCurrentSpan().getTimelineAnnotations().isEmpty());
        assertEquals("Duplicated child span descriptions.", TASK_COUNT,
            results.size());
      }
    } finally {
      if (ses != null) {
        ses.shutdown();
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
