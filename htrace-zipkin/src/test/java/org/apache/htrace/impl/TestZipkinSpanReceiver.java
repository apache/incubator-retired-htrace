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

import org.apache.htrace.Transport;
import org.apache.htrace.core.AlwaysSampler;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.MilliSpan;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceCreator;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.apache.htrace.core.TracerPool;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestZipkinSpanReceiver {

  private Tracer newTracer(final Transport transport) {
    TracerPool pool = new TracerPool("newTracer");
    pool.addReceiver(new ZipkinSpanReceiver(HTraceConfiguration.EMPTY) {
      @Override
      protected Transport createTransport(HTraceConfiguration conf) {
        return transport;
      }
    });
    return new Tracer.Builder("ZipkinTracer").
        tracerPool(pool).
        conf(HTraceConfiguration.fromKeyValuePairs(
            "sampler.classes", AlwaysSampler.class.getName()
        )).
        build();
  }

  @Test
  public void testSimpleTraces() throws IOException, InterruptedException {
    FakeZipkinTransport transport = new FakeZipkinTransport();
    Tracer tracer = newTracer(transport);
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
    Assert.assertTrue(transport.nextMessageAsSpan().getName().contains("innerTwo"));
    innerOne.close();
    Assert.assertTrue(transport.nextMessageAsSpan().getName().contains("innerOne"));
    rootSpan.addKVAnnotation("foo", "bar");
    rootSpan.addTimelineAnnotation("timeline");
    rootScope.close();
    Assert.assertTrue(transport.nextMessageAsSpan().getName().contains("root"));
    tracer.close();
  }

  @Test
  public void testConcurrency() throws IOException {
    Tracer tracer = newTracer(new FakeZipkinTransport(){
      @Override public void send(List<byte[]> spans) throws IOException { /*do nothing*/ }
    });
    TraceCreator traceCreator = new TraceCreator(tracer);
    traceCreator.createThreadedTrace();
  }

  private static class FakeZipkinTransport implements Transport {

    private final BlockingQueue<com.twitter.zipkin.gen.Span> receivedSpans =
        new ArrayBlockingQueue<com.twitter.zipkin.gen.Span>(1);

    com.twitter.zipkin.gen.Span nextMessageAsSpan() throws InterruptedException {
      return receivedSpans.take();
    }


    @Override
    public void open(HTraceConfiguration conf) throws IOException {

    }

    @Override
    public boolean isOpen() {
      return false;
    }

    @Override
    public void send(List<byte[]> spans) throws IOException {
      for (byte[] message : spans) {
        TMemoryBuffer transport = new TMemoryBuffer(message.length);
        try {
          transport.write(message);
          com.twitter.zipkin.gen.Span zSpan = new com.twitter.zipkin.gen.Span();
          zSpan.read(new TBinaryProtocol(transport));
          receivedSpans.add(zSpan);
        } catch (TException e) {
          throw new IOException(e);
        }
      }
    }

    @Override
    public void close() throws IOException {

    }
  }
}
