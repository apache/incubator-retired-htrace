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
package org.cloudera.htrace;

import java.util.Random;

/**
 * Does some stuff and traces it.
 */
public class TraceCreator {

  /**
   * Takes as input the SpanReceiver that should used as the sink for Spans when
   * createDemoTrace() is called.
   * 
   * @param receiver
   */
  public TraceCreator(SpanReceiver receiver) {
    Trace.addReceiver(receiver);
  }

  public void createSimpleTrace() {
    Span s = Trace.startSpan("beginning the trace.", Sampler.ALWAYS);
    try {
      importantWork1();
    } finally {
      s.stop();
    }
  }

  /**
   * Creates the demo trace (will create different traces from call to call).
   */
  public void createDemoTrace() {
    Span s = Trace.startSpan("beginning the trace.", Sampler.ALWAYS);
    try {
      Random r = new Random();
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
      s.stop();
    }
  }

  private void importantWork1() {
    Span cur = Trace.startSpan("important work 1");
    try {
      Thread.sleep((long) (5000 * Math.random()));
      importantWork2();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      cur.stop();
    }
  }

  private void importantWork2() {
    Span cur = Trace.startSpan("important work 2");
    try {
      Thread.sleep((long) (5000 * Math.random()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      cur.stop();
    }
  }

  private class MyRunnable implements Runnable {
    @Override
    public void run() {
      try {
        Thread.sleep(1000);
        Random r = new Random();
        int importantNumber = 100 / r.nextInt(3);
        System.out.println("Important number: " + importantNumber);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      } catch (ArithmeticException ae) {
        Span c = Trace.startSpan("dealing with arithmetic exception.");
        try {
          Thread.sleep((long) (5000 * Math.random()));
        } catch (InterruptedException ie1) {
          Thread.currentThread().interrupt();
        } finally {
          c.stop();
        }
      }
    }
  }
}