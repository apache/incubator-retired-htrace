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

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.containsString;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class TestBadClient {
  /**
   * Test closing an outer scope when an inner one is still active.
   */
  @Test
  public void TestClosingOuterScope() throws Exception {
    boolean gotException = false;
    TraceScope outerScope = Trace.startSpan("outer", AlwaysSampler.INSTANCE);
    TraceScope innerScope = Trace.startSpan("inner");
    try {
      outerScope.close();
    } catch (RuntimeException e) {
      assertThat(e.getMessage(),
          containsString("You have probably forgotten to close or detach"));
      gotException = true;
    }
    assertTrue("Expected to get exception because of improper " +
        "scope closure.", gotException);
    innerScope.close();
  }

  /**
   * Test calling detach() two times on a scope object.
   */
  @Test
  public void TestDoubleDetach() throws Exception {
    boolean gotException = false;
    TraceScope myScope = Trace.startSpan("myScope", AlwaysSampler.INSTANCE);
    myScope.detach();
    try {
      myScope.detach();
    } catch (RuntimeException e) {
      assertThat(e.getMessage(),
          containsString("it has already been detached."));
      gotException = true;
    }
    assertTrue("Expected to get exception because of double TraceScope " +
        "detach.", gotException);
  }

  private static class SpanHolder {
    Span span;

    void set(Span span) {
      this.span = span;
    }
  }

  /**
   * Test correctly passing spans between threads using detach().
   */
  @Test
  public void TestPassingSpanBetweenThreads() throws Exception {
    final SpanHolder spanHolder = new SpanHolder();
    Thread th = new Thread(new Runnable() {
      @Override
      public void run() {
        TraceScope workerScope = Trace.startSpan("workerSpan",
            AlwaysSampler.INSTANCE);
        spanHolder.set(workerScope.getSpan());
        workerScope.detach();
      }
    });
    th.start();
    th.join();

    // Create new scope whose parent is the worker thread's span. 
    TraceScope outermost = Trace.startSpan("outermost", spanHolder.span);
    TraceScope nested = Trace.startSpan("nested");
    nested.close();
    outermost.close();
    // Create another span which also descends from the worker thread's span.
    TraceScope nested2 = Trace.startSpan("nested2", spanHolder.span);
    nested2.close();

    // Close the worker thread's span.
    spanHolder.span.stop();

    // We can create another descendant, even though the worker thread's span
    // has been stopped.
    TraceScope lateChildScope = Trace.startSpan("lateChild", spanHolder.span);
    lateChildScope.close();
  }

  /**
   * Test trying to manually set our TraceScope's parent in a case where there
   * is a currently active span.
   */
  @Test
  public void TestIncorrectStartSpan() throws Exception {
    // Create new scope
    TraceScope outermost = Trace.startSpan("outermost",
        AlwaysSampler.INSTANCE);
    // Create nested scope
    TraceScope nested = Trace.startSpan("nested", outermost.getSpan()); 
    // Error
    boolean gotException = false;
    try {
      TraceScope error = Trace.startSpan("error", outermost.getSpan()); 
      error.close();
    } catch (RuntimeException e) {
      assertThat(e.getMessage(),
          containsString("there is already a currentSpan"));
      gotException = true;
    }
    assertTrue("Expected to get exception because of incorrect startSpan.",
        gotException);
  }

  @After
  public void resetCurrentSpan() {
    Tracer.getInstance().setCurrentSpan(null);
  }
}
