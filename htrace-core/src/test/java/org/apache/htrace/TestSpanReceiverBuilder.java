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

import org.apache.htrace.impl.LocalFileSpanReceiver;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestSpanReceiverBuilder {
  /**
   * Test that if no span receiver is configured, the builder returns null.
   */
  @Test
  public void testGetNullSpanReceiver() {
    SpanReceiverBuilder builder =
        new SpanReceiverBuilder(HTraceConfiguration.EMPTY).logErrors(false);
    SpanReceiver rcvr = builder.build();
    Assert.assertEquals(null, rcvr);
  }

  private static SpanReceiver createSpanReceiver(Map<String, String> m) {
    HTraceConfiguration hconf = HTraceConfiguration.fromMap(m);
    SpanReceiverBuilder builder =
        new SpanReceiverBuilder(hconf).
            logErrors(false);
    return builder.build();
  }

  /**
   * Test getting various SpanReceiver objects.
   */
  @Test
  public void testGetSpanReceivers() throws Exception {
    HashMap<String, String> confMap = new HashMap<String, String>();

    // Create LocalFileSpanReceiver
    confMap.put(LocalFileSpanReceiver.PATH_KEY, "/tmp/foo");
    confMap.put(SpanReceiverBuilder.SPAN_RECEIVER_CONF_KEY,
        "org.apache.htrace.impl.LocalFileSpanReceiver");
    SpanReceiver rcvr = createSpanReceiver(confMap);
    Assert.assertEquals("org.apache.htrace.impl.LocalFileSpanReceiver",
        rcvr.getClass().getName());
    rcvr.close();

    // Create POJOSpanReceiver
    confMap.remove(LocalFileSpanReceiver.PATH_KEY);
    confMap.put(SpanReceiverBuilder.SPAN_RECEIVER_CONF_KEY, "POJOSpanReceiver");
    rcvr = createSpanReceiver(confMap);
    Assert.assertEquals("org.apache.htrace.impl.POJOSpanReceiver",
        rcvr.getClass().getName());
    rcvr.close();

    // Create StandardOutSpanReceiver
    confMap.remove(LocalFileSpanReceiver.PATH_KEY);
    confMap.put(SpanReceiverBuilder.SPAN_RECEIVER_CONF_KEY,
        "org.apache.htrace.impl.StandardOutSpanReceiver");
    rcvr = createSpanReceiver(confMap);
    Assert.assertEquals("org.apache.htrace.impl.StandardOutSpanReceiver",
        rcvr.getClass().getName());
    rcvr.close();
  }

  public static class TestSpanReceiver implements SpanReceiver {
    final static String SUCCEEDS = "test.span.receiver.succeeds";

    public TestSpanReceiver(HTraceConfiguration conf) {
      if (conf.get(SUCCEEDS) == null) {
        throw new RuntimeException("Can't create TestSpanReceiver: " +
            "invalid configuration.");
      }
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
    HashMap<String, String> confMap = new HashMap<String, String>();

    // Create TestSpanReceiver
    confMap.put(SpanReceiverBuilder.SPAN_RECEIVER_CONF_KEY,
        TestSpanReceiver.class.getName());
    confMap.put(TestSpanReceiver.SUCCEEDS, "true");
    SpanReceiver rcvr = createSpanReceiver(confMap);
    Assert.assertEquals(TestSpanReceiver.class.getName(),
        rcvr.getClass().getName());
    rcvr.close();

    // Fail to create TestSpanReceiver
    confMap.remove(TestSpanReceiver.SUCCEEDS);
    rcvr = createSpanReceiver(confMap);
    Assert.assertEquals(null, rcvr);
  }
}
