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

package org.apache.htrace.viewer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.protobuf.generated.SpanProtos.Span;
import org.apache.htrace.protobuf.generated.SpanProtos.TimelineAnnotation;
import org.apache.htrace.viewer.HBaseSpanViewer;
import org.junit.Test;
import org.junit.Assert;

public class TestHBaseSpanViewer {
  private static final Log LOG = LogFactory.getLog(TestHBaseSpanViewer.class);

  @Test
  public void testProtoToJson() {
    Span.Builder sbuilder = Span.newBuilder();
    TimelineAnnotation.Builder tlbuilder = TimelineAnnotation.newBuilder();
    sbuilder.clear().setTraceId(1)
                    .setParentId(2)
                    .setStart(3)
                    .setStop(4)
                    .setSpanId(5)
                    .setProcessId("pid")
                    .setDescription("description");
    for (int i = 0; i < 3; i++) {
      sbuilder.addTimeline(tlbuilder.clear()
                           .setTime(i)
                           .setMessage("message" + i)
                           .build());
    }
    Span span = sbuilder.build();
    try {
      String json = HBaseSpanViewer.toJsonString(span);
      String expected =
          "{\"trace_id\":\"1\","
          + "\"parent_id\":\"2\","
          + "\"start\":\"3\","
          + "\"stop\":\"4\","
          + "\"span_id\":\"5\","
          + "\"process_id\":\"pid\","
          + "\"description\":\"description\","
          + "\"timeline\":["
          + "{\"time\":\"0\",\"message\":\"message0\"},"
          + "{\"time\":\"1\",\"message\":\"message1\"},"
          + "{\"time\":\"2\",\"message\":\"message2\"}]}";
      Assert.assertEquals(json, expected);
    } catch (IOException e) {
      Assert.fail("failed to get json from span. " + e.getMessage());
    }
  }

  @Test
  public void testProtoWithoutTimelineToJson() {
    Span.Builder sbuilder = Span.newBuilder();
    sbuilder.clear().setTraceId(1)
                    .setParentId(2)
                    .setStart(3)
                    .setStop(4)
                    .setSpanId(5)
                    .setProcessId("pid")
                    .setDescription("description");
    Span span = sbuilder.build();
    try {
      String json = HBaseSpanViewer.toJsonString(span);
      String expected =
          "{\"trace_id\":\"1\","
          + "\"parent_id\":\"2\","
          + "\"start\":\"3\","
          + "\"stop\":\"4\","
          + "\"span_id\":\"5\","
          + "\"process_id\":\"pid\","
          + "\"description\":\"description\"}";
      Assert.assertEquals(json, expected);
    } catch (IOException e) {
      Assert.fail("failed to get json from span. " + e.getMessage());
    }
  }
}
