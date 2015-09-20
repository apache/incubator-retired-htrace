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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TestMilliSpan {
  private void compareSpans(Span expected, Span got) throws Exception {
    assertEquals(expected.getStartTimeMillis(), got.getStartTimeMillis());
    assertEquals(expected.getStopTimeMillis(), got.getStopTimeMillis());
    assertEquals(expected.getDescription(), got.getDescription());
    assertEquals(expected.getSpanId(), got.getSpanId());
    assertEquals(expected.getTracerId(), got.getTracerId());
    assertTrue(Arrays.equals(expected.getParents(), got.getParents()));
    Map<String, String> expectedT = expected.getKVAnnotations();
    Map<String, String> gotT = got.getKVAnnotations();
    if (expectedT == null) {
      assertEquals(null, gotT);
    } else {
      assertEquals(expectedT.size(), gotT.size());
      for (String key : expectedT.keySet()) {
        assertEquals(expectedT.get(key), gotT.get(key));
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
        parents(new SpanId[] { new SpanId(7L, 7L) }).
        tracerId("b2404.halxg.com:8080").
        spanId(new SpanId(7L, 8L)).
        build();
    String json = span.toJson();
    MilliSpan dspan = MilliSpan.fromJson(json);
    compareSpans(span, dspan);
  }

  @Test
  public void testJsonSerializationWithNegativeLongValue() throws Exception {
    MilliSpan span = new MilliSpan.Builder().
        description("foospan").
        begin(-1L).
        end(-1L).
        parents(new SpanId[] { new SpanId(-1L, -1L) }).
        tracerId("b2404.halxg.com:8080").
        spanId(new SpanId(-1L, -2L)).
        build();
    String json = span.toJson();
    MilliSpan dspan = MilliSpan.fromJson(json);
    compareSpans(span, dspan);
  }

  @Test
  public void testJsonSerializationWithRandomLongValue() throws Exception {
    SpanId parentId = SpanId.fromRandom();
    MilliSpan span = new MilliSpan.Builder().
        description("foospan").
        begin(ThreadLocalRandom.current().nextLong()).
        end(ThreadLocalRandom.current().nextLong()).
        parents(new SpanId[] { parentId }).
        tracerId("b2404.halxg.com:8080").
        spanId(parentId.newChildId()).
        build();
    String json = span.toJson();
    MilliSpan dspan = MilliSpan.fromJson(json);
    compareSpans(span, dspan);
  }

  @Test
  public void testJsonSerializationWithOptionalFields() throws Exception {
    MilliSpan.Builder builder = new MilliSpan.Builder().
        description("foospan").
        begin(300).
        end(400).
        parents(new SpanId[] { }).
        tracerId("b2408.halxg.com:8080").
        spanId(new SpanId(111111111L, 111111111L));
    Map<String, String> traceInfo = new HashMap<String, String>();
    traceInfo.put("abc", "123");
    traceInfo.put("def", "456");
    builder.traceInfo(traceInfo);
    List<TimelineAnnotation> timeline = new LinkedList<TimelineAnnotation>();
    timeline.add(new TimelineAnnotation(310L, "something happened"));
    timeline.add(new TimelineAnnotation(380L, "something else happened"));
    timeline.add(new TimelineAnnotation(390L, "more things"));
    builder.timeline(timeline);
    MilliSpan span = builder.build();
    String json = span.toJson();
    MilliSpan dspan = MilliSpan.fromJson(json);
    compareSpans(span, dspan);
  }

  @Test
  public void testJsonSerializationWithFieldsNotSet() throws Exception {
    MilliSpan span = new MilliSpan.Builder().build();
    String json = span.toJson();
    MilliSpan dspan = MilliSpan.fromJson(json);
    compareSpans(span, dspan);
  }
}
