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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Does some stuff and traces it.
 */
public class TraceCreator {
  public static final String RPC_TRACE_ROOT = "createSampleRpcTrace";
  public static final String THREADED_TRACE_ROOT = "createThreadedTrace";
  public static final String SIMPLE_TRACE_ROOT = "createSimpleTrace";

  private final Tracer tracer;

  public TraceCreator(Tracer tracer) {
    this.tracer = tracer;
  }

  public void createSampleRpcTrace() {
    try (TraceScope s = tracer.newScope(RPC_TRACE_ROOT)) {
      pretendRpcSend();
    }
  }

  public void createSimpleTrace() {
    try (TraceScope s = tracer.newScope(SIMPLE_TRACE_ROOT)) {
      importantWork1();
    }
  }

  /**
   * Creates the demo trace (will create different traces from call to call).
   */
  public void createThreadedTrace() {
    try (TraceScope s = tracer.newScope(THREADED_TRACE_ROOT)) {
      Random r = ThreadLocalRandom.current();
      int numThreads = r.nextInt(4) + 1;
      Thread[] threads = new Thread[numThreads];

      for (int i = 0; i < numThreads; i++) {
        threads[i] = new Thread(tracer.wrap(new MyRunnable(), null));
      }
      for (int i = 0; i < numThreads; i++) {
        threads[i].start();
      }
      for (int i = 0; i < numThreads; i++) {
        try {
          threads[i].join();
        } catch (InterruptedException e) {
        }
      }
      importantWork1();
    }
  }

  private void importantWork1() {
    try (TraceScope cur = tracer.newScope("important work 1")) {
      Thread.sleep((long) (2000 * Math.random()));
      importantWork2();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void importantWork2() {
    try (TraceScope cur = tracer.newScope("important work 2")) {
      Thread.sleep((long) (2000 * Math.random()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private class MyRunnable implements Runnable {
    @Override
    public void run() {
      try {
        Thread.sleep(750);
        Random r = ThreadLocalRandom.current();
        int importantNumber = 100 / r.nextInt(3);
        System.out.println("Important number: " + importantNumber);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      } catch (ArithmeticException ae) {
        try (TraceScope c = tracer.newScope("dealing with arithmetic exception.")) {
          Thread.sleep((long) (3000 * Math.random()));
        } catch (InterruptedException ie1) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  public void pretendRpcSend() {
    Span span = tracer.getCurrentSpan();
    pretendRpcReceiveWithTraceInfo(span.getSpanId());
  }

  public void pretendRpcReceiveWithTraceInfo(SpanId parentId) {
    try (TraceScope s = tracer.newScope("received RPC", parentId)) {
      importantWork1();
    }
  }
}
