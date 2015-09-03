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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class TestSpanReceiverBuilder {
  private static final Log LOG =
      LogFactory.getLog(TestSpanReceiverBuilder.class);

  private List<SpanReceiver> createSpanReceivers(String classes) {
    Tracer tracer = new Tracer.Builder().
        name("MyTracer").
        tracerPool(new TracerPool("createSpanReceivers")).
        conf(HTraceConfiguration.fromKeyValuePairs(
            "span.receiver.classes", classes)).
        build();
    SpanReceiver[] receivers = tracer.getTracerPool().getReceivers();
    tracer.close();
    LinkedList<SpanReceiver> receiverList = new LinkedList<SpanReceiver>();
    for (SpanReceiver item: receivers) {
      receiverList.add(item);
    }
    return receiverList;
  }

  @Test
  public void TestCreateStandardSpanReceivers() {
    List<SpanReceiver> receivers;
    receivers = createSpanReceivers("");
    Assert.assertTrue(receivers.isEmpty());
    receivers = createSpanReceivers("POJOSpanReceiver");
    Assert.assertTrue(receivers.get(0).getClass().getName().
        equals("org.apache.htrace.core.POJOSpanReceiver"));
    receivers = createSpanReceivers(
               "org.apache.htrace.core.StandardOutSpanReceiver");
    Assert.assertTrue(receivers.get(0).getClass().getName().
        equals("org.apache.htrace.core.StandardOutSpanReceiver"));
    receivers = createSpanReceivers(
               "POJOSpanReceiver;StandardOutSpanReceiver");
    Assert.assertEquals(2, receivers.size());
    for (Iterator<SpanReceiver> iter = receivers.iterator(); iter.hasNext();) {
      SpanReceiver receiver = iter.next();
      if (receiver.getClass().getName().equals(
          "org.apache.htrace.core.POJOSpanReceiver")) {
        iter.remove();
        break;
      }
    }
    for (Iterator<SpanReceiver> iter = receivers.iterator(); iter.hasNext();) {
      SpanReceiver receiver = iter.next();
      if (receiver.getClass().getName().equals(
          "org.apache.htrace.core.StandardOutSpanReceiver")) {
        iter.remove();
        break;
      }
    }
    Assert.assertEquals(0, receivers.size());
  }

  public static class GoodSpanReceiver extends SpanReceiver {
    public GoodSpanReceiver(HTraceConfiguration conf) {
    }

    @Override
    public void receiveSpan(Span span) {
    }

    @Override
    public void close() throws IOException {
    }
  }

  public static class BadSpanReceiver extends SpanReceiver {
    public BadSpanReceiver(HTraceConfiguration conf) {
      throw new RuntimeException("Can't create BadSpanReceiver");
    }

    @Override
    public void receiveSpan(Span span) {
    }

    @Override
    public void close() throws IOException {
    }
  }

  /**
   * Test trying to create a SpanReceiver that experiences an error in the
   * constructor.
   */
  @Test
  public void testGetSpanReceiverWithConstructorError() throws Exception {
    List<SpanReceiver> receivers;
    receivers = createSpanReceivers(
        GoodSpanReceiver.class.getName());
    Assert.assertEquals(1, receivers.size());
    Assert.assertTrue(receivers.get(0).getClass().getName().
        contains("GoodSpanReceiver"));
    receivers = createSpanReceivers(
        BadSpanReceiver.class.getName());
    Assert.assertEquals(0, receivers.size());
  }
}
