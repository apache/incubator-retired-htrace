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

package org.apache.htrace.impl;

import org.apache.htrace.core.AlwaysSampler;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.MilliSpan;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceCreator;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.apache.htrace.core.TracerPool;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class TestFlumeSpanReceiver {
  @Rule
  public FakeFlume flumeServer = new FakeFlume();

  private Tracer newTracer() {
    return new Tracer.Builder().
        name("FlumeTracer").
        tracerPool(new TracerPool("newTracer")).
        conf(HTraceConfiguration.fromKeyValuePairs(
            FlumeSpanReceiver.FLUME_PORT_KEY,
            Integer.toString(flumeServer.getPort()),
            "span.receiver.classes", FlumeSpanReceiver.class.getName(),
            "sampler.classes", AlwaysSampler.class.getName()
        )).build();
  }

  @Test(timeout=120000)
  public void testSimpleTraces() throws IOException, InterruptedException {
    Tracer tracer = newTracer();
    Span rootSpan = new MilliSpan.Builder().
        description("root").
        spanId(new SpanId(100, 100)).
        tracerId("test").
        begin(System.currentTimeMillis()).
        build();
    TraceScope rootScope = tracer.newScope("root");
    TraceScope innerOne = tracer.newScope("innerOne");
    TraceScope innerTwo = tracer.newScope("innerTwo");
    innerTwo.close();
    Assert.assertTrue(flumeServer.nextEventBodyAsString().contains("innerTwo"));
    innerOne.close();
    Assert.assertTrue(flumeServer.nextEventBodyAsString().contains("innerOne"));
    rootSpan.addKVAnnotation("foo", "bar");
    rootSpan.addTimelineAnnotation("timeline");
    rootScope.close();
    Assert.assertTrue(flumeServer.nextEventBodyAsString().contains("root"));
    tracer.close();
  }

  @Test(timeout=120000)
  public void testConcurrency() throws IOException {
    Tracer tracer = newTracer();
    TraceCreator traceCreator = new TraceCreator(tracer);
    flumeServer.alwaysOk();
    traceCreator.createThreadedTrace();
  }

  @Test(timeout=120000)
  public void testResilience() throws IOException {
    Tracer tracer = newTracer();
    TraceCreator traceCreator = new TraceCreator(tracer);
    flumeServer.alwaysFail();
    traceCreator.createThreadedTrace();
  }
}
