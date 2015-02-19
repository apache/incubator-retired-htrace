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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcTestUtils;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceCreator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFlumeSpanReceiver {
  private static final Log LOG = LogFactory.getLog(TestFlumeSpanReceiver.class);

  private static final String ROOT_SPAN_DESC = "ROOT";

  private SpanReceiver spanReceiver;
  private Server flumeServer;
  private TraceCreator traceCreator;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Test
  public void testSimpleTraces() throws FlumeException,
      EventDeliveryException, IOException {
    AvroHandler avroHandler = null;
    List<Span> spans = null;
    try {
      avroHandler = new AvroHandler();
      startReceiver(null, avroHandler);
      
      spans = new ArrayList<Span>();
      Span rootSpan = new MilliSpan.Builder().
                  description(ROOT_SPAN_DESC).
                  traceId(1).
                  spanId(100).
                  processId("test").
                  begin(System.currentTimeMillis()).
                  build();
      Span innerOne = rootSpan.child("Some good work");
      Span innerTwo = innerOne.child("Some more good work");
      innerTwo.stop();
      spans.add(innerTwo);
      innerOne.stop();
      spans.add(innerOne);
      rootSpan.addKVAnnotation("foo".getBytes(), "bar".getBytes());
      rootSpan.addTimelineAnnotation("timeline");
      rootSpan.stop();
      spans.add(rootSpan);

    } finally {
      stopReceiver();
    }
    List<AvroFlumeEvent> events = avroHandler.getAllEvents();
    Assert.assertEquals(spans.size(), events.size());
    for (int i = 0; i < spans.size(); i ++) {
      String json = new String(events.get(i).getBody().array(), Charset.forName("UTF-8"));
      Assert.assertTrue(json.contains(spans.get(i).getDescription()));
    }
  }

  @Test
  public void testConcurrency() throws FlumeException,
      EventDeliveryException, IOException {
    try {
      Map<String, String> extraConf = new HashMap<String, String>();
      extraConf.put(FlumeSpanReceiver.NUM_THREADS_KEY, "5");
      startReceiver(extraConf, new RpcTestUtils.OKAvroHandler());
      traceCreator.createThreadedTrace();
    } finally {
      stopReceiver();
    }
  }

  @Test
  public void testResilience() throws FlumeException,
      EventDeliveryException, IOException {
    try {
      startReceiver(null, new RpcTestUtils.FailedAvroHandler());
      traceCreator.createThreadedTrace();
    } finally {
      stopReceiver();
    }
  }

  private void startReceiver(Map<String, String> extraConf, AvroSourceProtocol avroHandler) {
    // Start Flume server
    Assert.assertNull(flumeServer);
    flumeServer = RpcTestUtils.startServer(avroHandler);

    // Create and configure span receiver
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(FlumeSpanReceiver.FLUME_HOSTNAME_KEY, "127.0.0.1");
    conf.put(FlumeSpanReceiver.FLUME_PORT_KEY, Integer.toString(flumeServer.getPort()));
    if (extraConf != null) {
      conf.putAll(extraConf);
    }
    
    spanReceiver = new FlumeSpanReceiver(HTraceConfiguration.fromMap(conf));

    // Create trace creator, it will register our receiver
    traceCreator = new TraceCreator(spanReceiver);
  }

  private void stopReceiver() throws IOException {
    // Close span receiver
    if (spanReceiver != null) {
      Trace.removeReceiver(spanReceiver);
      spanReceiver.close();
      spanReceiver = null;
    }

    // Close Flume server
    if (flumeServer != null) {
      RpcTestUtils.stopServer(flumeServer);
      flumeServer = null;
    }
  }
  
  private static class AvroHandler implements AvroSourceProtocol {
    private ArrayList<AvroFlumeEvent> all_events = new ArrayList<AvroFlumeEvent>();
    
    public List<AvroFlumeEvent> getAllEvents() {
      return new ArrayList<AvroFlumeEvent>(all_events);
    }
    
    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      all_events.add(event);
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) throws
        AvroRemoteException {
      all_events.addAll(events);
      return Status.OK;
    }
  }
}
