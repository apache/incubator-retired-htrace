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

import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.MilliSpan;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceCreator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class TestFlumeSpanReceiver {
  @Rule
  public TraceCreator traceCreator = new TraceCreator();
  @Rule
  public FakeFlume flumeServer = new FakeFlume();

  @Test
  public void testSimpleTraces() throws IOException, InterruptedException {
    traceCreator.addReceiver(new FlumeSpanReceiver(
        HTraceConfiguration.fromKeyValuePairs(
            FlumeSpanReceiver.FLUME_PORT_KEY, Integer.toString(flumeServer.getPort())
        )
    ));

    Span rootSpan = new MilliSpan.Builder().
        description("root").
        spanId(new SpanId(100, 100)).
        tracerId("test").
        begin(System.currentTimeMillis()).
        build();
    Span innerOne = rootSpan.child("Some good work");
    Span innerTwo = innerOne.child("Some more good work");
    innerTwo.stop();
    Assert.assertTrue(flumeServer.nextEventBodyAsString().contains(innerTwo.getDescription()));
    innerOne.stop();
    Assert.assertTrue(flumeServer.nextEventBodyAsString().contains(innerOne.getDescription()));
    rootSpan.addKVAnnotation("foo", "bar");
    rootSpan.addTimelineAnnotation("timeline");
    rootSpan.stop();
    Assert.assertTrue(flumeServer.nextEventBodyAsString().contains(rootSpan.getDescription()));
  }

  @Test
  public void testConcurrency() throws IOException {
    traceCreator.addReceiver(new FlumeSpanReceiver(
        HTraceConfiguration.fromKeyValuePairs(
            FlumeSpanReceiver.FLUME_PORT_KEY, Integer.toString(flumeServer.getPort())
        )
    ));

    flumeServer.alwaysOk();
    traceCreator.createThreadedTrace();
  }

  @Test
  public void testResilience() throws IOException {
    traceCreator.addReceiver(new FlumeSpanReceiver(
        HTraceConfiguration.fromKeyValuePairs(
            FlumeSpanReceiver.FLUME_PORT_KEY, Integer.toString(flumeServer.getPort())
        )
    ));

    flumeServer.alwaysFail();
    traceCreator.createThreadedTrace();
  }
}
