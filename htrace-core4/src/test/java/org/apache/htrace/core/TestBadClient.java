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

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.containsString;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestBadClient {
  @After
  public void clearBadState() {
    // Clear the bad trace state so that we don't disrupt other unit tests
    // that run in this JVM.
    Tracer.threadLocalScope.set(null);
  }

  /**
   * Test closing an outer scope when an inner one is still active.
   */
  @Test
  public void TestClosingOuterScope() throws Exception {
    Tracer tracer = new Tracer.Builder().
        name("TestClosingOuterScopeTracer").
        tracerPool(new TracerPool("TestClosingOuterScope")).
        conf(HTraceConfiguration.
            fromKeyValuePairs("sampler.classes", "AlwaysSampler")).build();
    boolean gotException = false;
    TraceScope outerScope = tracer.newScope("outer");
    TraceScope innerScope = tracer.newScope("inner");
    try {
      outerScope.close();
    } catch (RuntimeException e) {
      assertThat(e.getMessage(),
          containsString("it is not the current TraceScope"));
      gotException = true;
    }
    assertTrue("Expected to get exception because of improper " +
        "scope closure.", gotException);
    innerScope.close();
    tracer.close();
  }

  /**
   * Test calling detach() two times on a scope object.
   */
  @Test
  public void TestDoubleDetachIsCaught() throws Exception {
    Tracer tracer = new Tracer.Builder().
        name("TestDoubleDetach").
        tracerPool(new TracerPool("TestDoubleDetachIsCaught")).
        conf(HTraceConfiguration.
            fromKeyValuePairs("sampler.classes", "AlwaysSampler")).build();
    boolean gotException = false;
    TraceScope myScope = tracer.newScope("myScope");
    myScope.detach();
    try {
      myScope.detach();
    } catch (RuntimeException e) {
      assertThat(e.getMessage(),
          containsString("it is already detached."));
      gotException = true;
    }
    assertTrue("Expected to get exception because of double TraceScope " +
        "detach.", gotException);
    tracer.close();
  }

  /**
   * Test calling detach() two times on a scope object.
   */
  @Test
  public void TestDoubleDetachOnNullScope() throws Exception {
    Tracer tracer = new Tracer.Builder().
        name("TestDoubleDetachOnNullScope").
        tracerPool(new TracerPool("TestDoubleDetachOnNullScope")).
        conf(HTraceConfiguration.
            fromKeyValuePairs("sampler.classes", "NeverSampler")).build();
    boolean gotException = false;
    TraceScope myScope = tracer.newScope("myScope");
    myScope.detach();
    try {
      myScope.detach();
    } catch (RuntimeException e) {
      assertThat(e.getMessage(),
          containsString("it is already detached."));
      gotException = true;
    }
    assertTrue("Expected to get exception because of double TraceScope " +
        "detach on NullScope.", gotException);
    tracer.close();
  }

  /**
   * Test calling reattach() two times on a scope object.
   */
  @Test
  public void TestDoubleReattachIsCaught() throws Exception {
    Tracer tracer = new Tracer.Builder().
        name("TestDoubleReattach").
        tracerPool(new TracerPool("TestDoubleReattachIsCaught")).
        conf(HTraceConfiguration.
            fromKeyValuePairs("sampler.classes", "AlwaysSampler")).build();
    boolean gotException = false;
    TraceScope myScope = tracer.newScope("myScope");
    myScope.detach();
    myScope.reattach();
    try {
      myScope.reattach();
    } catch (RuntimeException e) {
      assertThat(e.getMessage(),
          containsString("it is not detached."));
      gotException = true;
    }
    assertTrue("Expected to get exception because of double TraceScope " +
        "reattach.", gotException);
    tracer.close();
  }

  private static class ScopeHolder {
    TraceScope scope;

    void set(TraceScope scope) {
      this.scope = scope;
    }
  }

  /**
   * Test correctly passing spans between threads using detach().
   */
  @Test
  public void TestPassingSpanBetweenThreads() throws Exception {
    final Tracer tracer = new Tracer.Builder().
        name("TestPassingSpanBetweenThreads").
        tracerPool(new TracerPool("TestPassingSpanBetweenThreads")).
        conf(HTraceConfiguration.
            fromKeyValuePairs("sampler.classes", "AlwaysSampler")).build();
    POJOSpanReceiver receiver =
        new POJOSpanReceiver(HTraceConfiguration.EMPTY);
    tracer.getTracerPool().addReceiver(receiver);
    final ScopeHolder scopeHolder = new ScopeHolder();
    Thread th = new Thread(new Runnable() {
      @Override
      public void run() {
        TraceScope workerScope = tracer.newScope("workerSpan");
        workerScope.detach();
        scopeHolder.set(workerScope);
      }
    });
    th.start();
    th.join();
    TraceScope workerScope = scopeHolder.scope;
    SpanId workerScopeId = workerScope.getSpan().getSpanId();

    // Create new scope whose parent is the worker thread's span.
    workerScope.reattach();
    TraceScope nested = tracer.newScope("nested");
    nested.close();
    // Create another span which also descends from the worker thread's span.
    TraceScope nested2 = tracer.newScope("nested2");
    nested2.close();

    // Close the worker thread's span.
    workerScope.close();

    // We can create another descendant, even though the worker thread's span
    // has been stopped.
    TraceScope lateChildScope = tracer.newScope("lateChild", workerScopeId);
    lateChildScope.close();
    tracer.close();

    TraceGraph traceGraph = new TraceGraph(receiver.getSpans());
    Collection<Span> rootSpans =
        traceGraph.getSpansByParent().find(SpanId.INVALID);
    Assert.assertEquals(1, rootSpans.size());
    Assert.assertEquals(workerScopeId,
        rootSpans.iterator().next().getSpanId());
    Collection<Span> childSpans =
        traceGraph.getSpansByParent().find(workerScopeId);
    Assert.assertEquals(3, childSpans.size());
  }
}
