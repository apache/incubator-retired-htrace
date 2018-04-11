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
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.Tracer;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.MilliSpan;
import org.apache.htrace.core.TracerPool;
import org.apache.htrace.core.TimelineAnnotation;
import org.apache.htrace.viewer.KuduSpanViewer;
import org.junit.*;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.BaseKuduTest;
import org.kududb.client.KuduClient;
import org.kududb.client.CreateTableOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TestKuduSpanViewer extends BaseKuduTest {

  private static final String BIN_DIR_PROP = "binDir";
  private static final String BIN_DIR_PROP_DEFAULT = "../build/release/bin";
  //set kudu binary location and enable test execution from here
  private static final boolean TEST_ENABLE = false;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    if (TEST_ENABLE) {
      System.setProperty(BIN_DIR_PROP, BIN_DIR_PROP_DEFAULT);
      BaseKuduTest.setUpBeforeClass();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if(TEST_ENABLE) {
      BaseKuduTest.tearDownAfterClass();
    }
  }

  private void createTable() throws Exception {
    KuduClient client = BaseKuduTest.syncClient;
    List<ColumnSchema> span_columns = new ArrayList();
    span_columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID,
            Type.INT64)
            .key(true)
            .build());
    span_columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_START_TIME,
            Type.INT64)
            .build());
    span_columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_STOP_TIME,
            Type.INT64)
            .build());
    span_columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_SPAN_ID,
            Type.INT64)
            .build());
    span_columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT,
            Type.BOOL)
            .build());
    span_columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_DESCRIPTION,
            Type.STRING)
            .build());

    List<String> rangeKeys = new ArrayList<>();
    rangeKeys.add(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID);
    Schema schema = new Schema(span_columns);
    client.createTable(KuduReceiverConstants.DEFAULT_KUDU_SPAN_TABLE, schema,
            new CreateTableOptions().setRangePartitionColumns(rangeKeys));

    List<ColumnSchema> timeline_columns = new ArrayList();
    timeline_columns.add(new ColumnSchema.ColumnSchemaBuilder
            (KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_TIMELINEID, Type.INT64)
            .key(true)
            .build());
    timeline_columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_TIME,
            Type.INT64)
            .build());
    timeline_columns.add(new ColumnSchema.ColumnSchemaBuilder
            (KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_MESSAGE, Type.STRING)
            .build());
    timeline_columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_SPANID,
            Type.INT64)
            .build());
    List<String> rangeKeysTimeline = new ArrayList<>();
    rangeKeysTimeline.add(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_TIMELINEID);
    Schema timelineSchema = new Schema(timeline_columns);
    client.createTable(KuduReceiverConstants.DEFAULT_KUDU_SPAN_TIMELINE_ANNOTATION_TABLE, timelineSchema,
            new CreateTableOptions().setRangePartitionColumns(rangeKeysTimeline));
  }


  @Test
  public void testSpanToJson() {
    SpanId[] parent = new SpanId[1];
    parent[0] = new SpanId(1,1);
    MilliSpan.Builder builder = new MilliSpan.Builder()
            .parents(parent)
            .begin(1)
            .end(2)
            .spanId(new SpanId(10,20))
            .description("description");
    List<TimelineAnnotation> timelineList = new LinkedList<TimelineAnnotation>();
    for (int i = 0; i < 3; i++) {
      timelineList.add(new TimelineAnnotation(i,"message" + i));
    }
    builder.timeline(timelineList);
    Span span = builder.build();
    try {
      String json = KuduSpanViewer.toJsonString(span);
      String expected =
              "{\"trace_id\":\"20\",\"span_id\":\"10\",\"description\":\"description\",\"parent_id\":\"1\"," +
                      "\"start\":\"1\",\"stop\":\"2\",\"timeline\":[{\"time\":\"0\",\"message\":\"message0\",}{\"time\":\"1\"," +
                      "\"message\":\"message1\",}{\"time\":\"2\",\"message\":\"message2\",}]}";
      Assert.assertEquals(json, expected);
    } catch (IOException e) {
      Assert.fail("failed to get json from span. " + e.getMessage());
    }
  }

  @Test
  public void testSpanWithoutTimelineToJson() {
    SpanId[] parent = new SpanId[1];
    parent[0] = new SpanId(200,111);
    MilliSpan.Builder builder = new MilliSpan.Builder()
            .parents(parent)
            .begin(1)
            .end(2)
            .spanId(new SpanId(10,20))
            .tracerId("pid")
            .description("description");
    Span span = builder.build();
    try {
      String json = KuduSpanViewer.toJsonString(span);
      String expected =
              "{\"trace_id\":\"20\",\"span_id\":\"10\",\"description\":\"description\"," +
                      "\"parent_id\":\"111\",\"start\":\"1\",\"stop\":\"2\",}";
      Assert.assertEquals(json, expected);
    } catch (IOException e) {
      Assert.fail("failed to get json from span. " + e.getMessage());
    }
  }

  @Ignore
  @Test
  public void TestKuduSpanViewer() throws Exception {
    createTable();
    Tracer tracer = new Tracer.Builder().
            name("testKuduSpanReceiver").
            tracerPool(new TracerPool("testKuduSpanReceiver")).
            conf(HTraceConfiguration.fromKeyValuePairs(
                    "sampler.classes", "AlwaysSampler",
                    "span.receiver.classes", "org.apache.htrace.impl.KuduSpanReceiver",
                    KuduReceiverConstants.KUDU_MASTER_HOST_KEY, BaseKuduTest.getMasterAddresses().split(":")[0],
                    KuduReceiverConstants.KUDU_MASTER_PORT_KEY, BaseKuduTest.getMasterAddresses().split(":")[1]))
            .build();
    TraceScope scope = tracer.newScope("testKuduScope");
    scope.addTimelineAnnotation("test");
    Span testSpan = scope.getSpan();
    TraceScope childScope = tracer.newScope("testKuduChildScope", new SpanId(100,200));
    Span childScopeSpan = childScope.getSpan();
    childScope.addTimelineAnnotation("testChild");
    childScope.close();
    scope.close();
    tracer.close();
    HTraceConfiguration conf = HTraceConfiguration
            .fromKeyValuePairs(KuduReceiverConstants.KUDU_MASTER_HOST_KEY,
                    BaseKuduTest.getMasterAddresses().split(":")[0],
            KuduReceiverConstants.KUDU_MASTER_PORT_KEY, BaseKuduTest.getMasterAddresses().split(":")[1]);
    KuduSpanViewer viewer = new KuduSpanViewer(conf);
    List<Span> list = viewer.getRootSpans();
    Assert.assertEquals(list.size(), 1);
    Span span = viewer.getRootSpans().get(0);
    try {
      String json = KuduSpanViewer.toJsonString(span);
      String expected = KuduSpanViewer.toJsonString(testSpan);
      Assert.assertEquals(json, expected);
    } catch (IOException e) {
      Assert.fail("failed to get json from span. " + e.getMessage());
    }
    List<Span> list2 = viewer.getSpans(span.getSpanId().getHigh());
    Assert.assertEquals(list2.size(), 2);
    Span span2 = list2.get(0);
    try {
      String json = KuduSpanViewer.toJsonString(span2);
      String expected = null;
      if(span2.getParents().length != 0) {
        expected = KuduSpanViewer.toJsonString(childScopeSpan);
      } else {
        expected = KuduSpanViewer.toJsonString(testSpan);
      }
      Assert.assertEquals(json, expected);
    } catch (IOException e) {
      Assert.fail("failed to get json from span. " + e.getMessage());
    }
    Span span3 = list2.get(1);
    try {
      String json = KuduSpanViewer.toJsonString(span3);
      String expected = null;
      if(span3.getParents().length != 0) {
        expected = KuduSpanViewer.toJsonString(childScopeSpan);
      } else {
        expected = KuduSpanViewer.toJsonString(testSpan);
      }
      Assert.assertEquals(json, expected);
    } catch (IOException e) {
      Assert.fail("failed to get json from span. " + e.getMessage());
    }
  }
}
