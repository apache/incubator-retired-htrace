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

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.Assert.assertEquals;

import org.apache.htrace.Span;
import org.apache.htrace.TimelineAnnotation;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestMilliSpan {
  private void compareSpans(Span expected, Span got) throws Exception {
    assertEquals(expected.getStartTimeMillis(), got.getStartTimeMillis());
    assertEquals(expected.getStopTimeMillis(), got.getStopTimeMillis());
    assertEquals(expected.getDescription(), got.getDescription());
    assertEquals(expected.getTraceId(), got.getTraceId());
    assertEquals(expected.getSpanId(), got.getSpanId());
    assertEquals(expected.getProcessId(), got.getProcessId());
    assertEquals(expected.getParentId(), got.getParentId());
    Map<byte[], byte[]> expectedT = expected.getKVAnnotations();
    Map<byte[], byte[]> gotT = got.getKVAnnotations();
    if (expectedT == null) {
      assertEquals(null, gotT);
    } else {
      assertEquals(expectedT.size(), gotT.size());
      Map<String, String> expectedTMap = new HashMap<String, String>();
      for (byte[] key : expectedT.keySet()) {
        expectedTMap.put(new String(key, "UTF-8"),
            new String(expectedT.get(key), "UTF-8"));
      }
      Map<String, String> gotTMap = new HashMap<String, String>();
      for (byte[] key : gotT.keySet()) {
        gotTMap.put(new String(key, "UTF-8"),
            new String(gotT.get(key), "UTF-8"));
      }
      for (String key : expectedTMap.keySet()) {
        assertEquals(expectedTMap.get(key), gotTMap.get(key));
      }
    }
    List<TimelineAnnotation> expectedTimeline =
        expected.getTimelineAnnotations();
    List<TimelineAnnotation> gotTimeline =
        got.getTimelineAnnotations();
    if (expectedTimeline == null) {
      assertEquals(null, gotTimeline);
    } else {
      assertEquals(expectedTimeline.size(), gotTimeline.size());
      Iterator<TimelineAnnotation> iter = gotTimeline.iterator();
      for (TimelineAnnotation expectedAnn : expectedTimeline) {
        TimelineAnnotation gotAnn =  iter.next();
        assertEquals(expectedAnn.getMessage(), gotAnn.getMessage());
        assertEquals(expectedAnn.getTime(), gotAnn.getTime());
      }
    }
  }

  @Test
  public void testJsonSerialization() throws Exception {
    MilliSpan span = new MilliSpan.Builder().
        description("foospan").
        begin(123L).
        end(456L).
        parents(new long[] { 7L }).
        processId("b2404.halxg.com:8080").
        spanId(989L).
        traceId(444).build();
    String json = span.toJson();
    ObjectMapper mapper = new ObjectMapper();
    MilliSpan dspan = mapper.readValue(json, MilliSpan.class);
    compareSpans(span, dspan);
  }

  @Test
  public void testJsonSerializationWithNegativeLongValue() throws Exception {
    MilliSpan span = new MilliSpan.Builder().
        description("foospan").
        begin(-1L).
        end(-1L).
        parents(new long[] { -1L }).
        processId("b2404.halxg.com:8080").
        spanId(-1L).
        traceId(-1L).build();
    String json = span.toJson();
    ObjectMapper mapper = new ObjectMapper();
    MilliSpan dspan = mapper.readValue(json, MilliSpan.class);
    compareSpans(span, dspan);
  }

  @Test
  public void testJsonSerializationWithRandomLongValue() throws Exception {
    Random random = new SecureRandom();
    MilliSpan span = new MilliSpan.Builder().
        description("foospan").
        begin(random.nextLong()).
        end(random.nextLong()).
        parents(new long[] { random.nextLong() }).
        processId("b2404.halxg.com:8080").
        spanId(random.nextLong()).
        traceId(random.nextLong()).build();
    String json = span.toJson();
    ObjectMapper mapper = new ObjectMapper();
    MilliSpan dspan = mapper.readValue(json, MilliSpan.class);
    compareSpans(span, dspan);
  }

  @Test
  public void testJsonSerializationWithOptionalFields() throws Exception {
    MilliSpan.Builder builder = new MilliSpan.Builder().
        description("foospan").
        begin(300).
        end(400).
        parents(new long[] { }).
        processId("b2408.halxg.com:8080").
        spanId(111111111L).
        traceId(4443);
    Map<byte[], byte[]> traceInfo = new HashMap<byte[], byte[]>();
    traceInfo.put("abc".getBytes("UTF-8"), "123".getBytes("UTF-8"));
    traceInfo.put("def".getBytes("UTF-8"), "456".getBytes("UTF-8"));
    builder.traceInfo(traceInfo);
    List<TimelineAnnotation> timeline = new LinkedList<TimelineAnnotation>();
    timeline.add(new TimelineAnnotation(310L, "something happened"));
    timeline.add(new TimelineAnnotation(380L, "something else happened"));
    timeline.add(new TimelineAnnotation(390L, "more things"));
    builder.timeline(timeline);
    MilliSpan span = builder.build();
    String json = span.toJson();
    ObjectMapper mapper = new ObjectMapper();
    MilliSpan dspan = mapper.readValue(json, MilliSpan.class);
    compareSpans(span, dspan);
  }
}
