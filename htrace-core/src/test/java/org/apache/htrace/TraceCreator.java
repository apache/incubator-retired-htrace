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
package org.apache.htrace;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.htrace.Sampler;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;

/**
 * Does some stuff and traces it.
 */
public class TraceCreator {

  public static final String RPC_TRACE_ROOT = "createSampleRpcTrace";
  public static final String THREADED_TRACE_ROOT = "createThreadedTrace";
  public static final String SIMPLE_TRACE_ROOT = "createSimpleTrace";

  /**
   * Takes as input the SpanReceiver that should used as the sink for Spans when
   * createDemoTrace() is called.
   *
   * @param receiver
   */
  public TraceCreator(SpanReceiver receiver) {
    Trace.addReceiver(receiver);
  }

  /**
   * Takes as input the SpanReceivers that should used as the sink for Spans
   * when createDemoTrace() is called.
   *
   * @param receivers
   */
  public TraceCreator(Collection<SpanReceiver> receivers) {
    for (SpanReceiver receiver : receivers) {
      Trace.addReceiver(receiver);
    }
  }

  public void createSampleRpcTrace() {
    TraceScope s = Trace.startSpan(RPC_TRACE_ROOT, Sampler.ALWAYS);
    try {
      pretendRpcSend();
    } finally {
      s.close();
    }
  }

  public void createSimpleTrace() {
    TraceScope s = Trace.startSpan(SIMPLE_TRACE_ROOT, Sampler.ALWAYS);
    try {
      importantWork1();
    } finally {
      s.close();
    }
  }

  /**
   * Creates the demo trace (will create different traces from call to call).
   */
  public void createThreadedTrace() {
    TraceScope s = Trace.startSpan(THREADED_TRACE_ROOT, Sampler.ALWAYS);
    try {
      Random r = ThreadLocalRandom.current();
      int numThreads = r.nextInt(4) + 1;
      Thread[] threads = new Thread[numThreads];

      for (int i = 0; i < numThreads; i++) {
        threads[i] = new Thread(Trace.wrap(new MyRunnable()));
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
    } finally {
      s.close();
    }
  }

  private void importantWork1() {
    TraceScope cur = Trace.startSpan("important work 1");
    try {
      Thread.sleep((long) (2000 * Math.random()));
      importantWork2();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      cur.close();
    }
  }

  private void importantWork2() {
    TraceScope cur = Trace.startSpan("important work 2");
    try {
      Thread.sleep((long) (2000 * Math.random()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      cur.close();
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
        TraceScope c = Trace.startSpan("dealing with arithmetic exception.");
        try {
          Thread.sleep((long) (3000 * Math.random()));
        } catch (InterruptedException ie1) {
          Thread.currentThread().interrupt();
        } finally {
          c.close();
        }
      }
    }
  }

  public void pretendRpcSend() {
    pretendRpcReceiveWithTraceInfo(Trace.currentSpan());
  }

  public void pretendRpcReceiveWithTraceInfo(Span parent) {
    TraceScope s = Trace.startSpan("received RPC", parent);
    try {
      importantWork1();
    } finally {
      s.close();
    }
  }
}
