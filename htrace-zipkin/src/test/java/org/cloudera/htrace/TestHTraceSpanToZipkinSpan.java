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

import com.twitter.zipkin.gen.Endpoint;
import org.cloudera.htrace.zipkin.HTraceToZipkinConverter;
import org.cloudera.htrace.impl.POJOSpanReceiver;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Creates HTrace and then convert it to Zipkin trace and checks whether it is a valid span or not.
 */
public class TestHTraceSpanToZipkinSpan {

  private static final String ROOT_SPAN_DESC = "ROOT";

  @Test
  public void testHTraceToZipkin() throws IOException {
    Endpoint ep = new Endpoint(12345, (short) 12, "test");
    POJOSpanReceiver psr = new POJOSpanReceiver();
    Trace.addReceiver(psr);
    runWorkerMethods();
    psr.close();
    Collection<Span> spans = psr.getSpans();
    for (Span s : spans) {
      com.twitter.zipkin.gen.Span zs =
          new HTraceToZipkinConverter(12345, (short) 12, true).toZipkinSpan(s);
      assertSpansAreEquivalent(s, zs);
    }
  }

  private void assertSpansAreEquivalent(Span s, com.twitter.zipkin.gen.Span zs) {
    assertEquals(s.getTraceId(), zs.getTrace_id());
    if (s.getParentId() != Span.ROOT_SPAN_ID) {
      assertEquals(s.getParentId(), zs.getParent_id());
    }
    assertEquals(s.getSpanId(), zs.getId());
    Assert.assertNotNull(zs.getAnnotations());
    if (ROOT_SPAN_DESC.equals(zs.getName())) {
      assertEquals(3, zs.getAnnotations().size());// two start/stop + one timeline annotation
      assertEquals(1, zs.getBinary_annotations().size());
    } else {
      assertEquals(2, zs.getAnnotations().size());
    }

  }

  private void runWorkerMethods() {
    TraceScope root = Trace.startSpan(ROOT_SPAN_DESC, Sampler.ALWAYS);
    try {
      doSomeWork();
      root.getSpan().addKVAnnotation("foo".getBytes(), "bar".getBytes());
      root.getSpan().addTimelineAnnotation("timeline");
    } finally {
      root.close();
    }
  }

  private void doSomeWork() {
    TraceScope tScope = Trace.startSpan("Some good work");
    try {
      Thread.sleep((long) (2000 * Math.random()));
      doSomeMoreWork();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      tScope.close();
    }
  }

  private void doSomeMoreWork() {
    TraceScope tScope = Trace.startSpan("Some more good work");
    try {
      Thread.sleep((long) (2000 * Math.random()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      tScope.close();
    }
  }
}
