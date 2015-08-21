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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.htrace.core.TraceGraph.SpansByParent;

import org.junit.Assert;
import org.junit.Test;

public class TestHTrace {
  @Test
  public void TestTracerCreateAndClose() throws Exception {
    Tracer tracer = new TracerBuilder().
        name("TestSimpleScope").
        tracerPool(new TracerPool("TestTracerCreateAndClose")).
        conf(HTraceConfiguration.fromKeyValuePairs(
            "sampler.classes", "AlwaysSampler")).
        build();
    POJOSpanReceiver receiver =
        new POJOSpanReceiver(HTraceConfiguration.EMPTY);
    tracer.getTracerPool().addReceiver(receiver);
    tracer.close();
    Assert.assertTrue(receiver.getSpans().isEmpty());
  }

  @Test
  public void TestSimpleScope() throws Exception {
    Tracer tracer = new TracerBuilder().
        name("TestSimpleScope").
        tracerPool(new TracerPool("TestSimpleScope")).
        conf(HTraceConfiguration.fromKeyValuePairs(
            "sampler.classes", "AlwaysSampler")).
        build();
    POJOSpanReceiver receiver =
        new POJOSpanReceiver(HTraceConfiguration.EMPTY);
    tracer.getTracerPool().addReceiver(receiver);
    TraceScope scope = tracer.newScope("Foo");
    scope.close();
    tracer.close();
    Assert.assertEquals(1, receiver.getSpans().size());
    Span span = receiver.getSpans().iterator().next();
    Assert.assertEquals(0, span.getParents().length);
  }

  @Test
  public void TestCreateSpans() throws Exception {
    Tracer tracer = new TracerBuilder().
        name("TestCreateSpans").
        tracerPool(new TracerPool("TestCreateSpans")).
        conf(HTraceConfiguration.fromKeyValuePairs(
            "sampler.classes", "AlwaysSampler")).
        build();
    POJOSpanReceiver receiver =
        new POJOSpanReceiver(HTraceConfiguration.EMPTY);
    tracer.getTracerPool().addReceiver(receiver);
    TraceCreator traceCreator = new TraceCreator(tracer);
    traceCreator.createSampleRpcTrace();
    traceCreator.createSimpleTrace();
    traceCreator.createThreadedTrace();
    tracer.close();
    TraceGraph traceGraph = new TraceGraph(receiver.getSpans());
    Collection<Span> roots = traceGraph.getSpansByParent().find(SpanId.INVALID);
    Assert.assertTrue("Trace tree must have roots", !roots.isEmpty());
    Assert.assertEquals(3, roots.size());

    Map<String, Span> descriptionToRootSpan = new HashMap<String, Span>();
    for (Span root : roots) {
      descriptionToRootSpan.put(root.getDescription(), root);
    }

    Assert.assertTrue(descriptionToRootSpan.keySet().contains(
        TraceCreator.RPC_TRACE_ROOT));
    Assert.assertTrue(descriptionToRootSpan.keySet().contains(
        TraceCreator.SIMPLE_TRACE_ROOT));
    Assert.assertTrue(descriptionToRootSpan.keySet().contains(
        TraceCreator.THREADED_TRACE_ROOT));

    SpansByParent spansByParentId = traceGraph.getSpansByParent();

    Span rpcTraceRoot = descriptionToRootSpan.get(TraceCreator.RPC_TRACE_ROOT);
    Assert.assertEquals(1, spansByParentId.find(rpcTraceRoot.getSpanId()).size());

    Span rpcTraceChild1 = spansByParentId.find(rpcTraceRoot.getSpanId())
        .iterator().next();
    Assert.assertEquals(1, spansByParentId.find(rpcTraceChild1.getSpanId()).size());

    Span rpcTraceChild2 = spansByParentId.find(rpcTraceChild1.getSpanId())
        .iterator().next();
    Assert.assertEquals(1, spansByParentId.find(rpcTraceChild2.getSpanId()).size());

    Span rpcTraceChild3 = spansByParentId.find(rpcTraceChild2.getSpanId())
        .iterator().next();
    Assert.assertEquals(0, spansByParentId.find(rpcTraceChild3.getSpanId()).size());
  }

  @Test(timeout=60000)
  public void testRootSpansHaveNonZeroSpanId() throws Exception {
    Tracer tracer = new TracerBuilder().
        name("testRootSpansHaveNonZeroSpanId").
        tracerPool(new TracerPool("testRootSpansHaveNonZeroSpanId")).
        conf(HTraceConfiguration.fromKeyValuePairs(
            "sampler.classes", "AlwaysSampler")).build();
    TraceScope scope = tracer.
        newScope("myRootSpan", new SpanId(100L, 200L));
    Assert.assertNotNull(scope);
    Assert.assertEquals("myRootSpan", scope.getSpan().getDescription());
    Assert.assertTrue(scope.getSpan().getSpanId().isValid());
    Assert.assertEquals(100L, scope.getSpan().getSpanId().getHigh());
    Assert.assertNotEquals(0L, scope.getSpan().getSpanId().getLow());
    scope.close();
  }
}
