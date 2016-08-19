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

import org.apache.htrace.core.Tracer;
import org.apache.htrace.core.TracerPool;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.MilliSpan;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.TimelineAnnotation;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;
import org.junit.AfterClass;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.ColumnSchema;
import org.kududb.client.BaseKuduTest;
import org.kududb.client.KuduClient;
import org.kududb.client.CreateTableOptions;
import org.kududb.client.KuduScanner;
import org.kududb.client.RowResultIterator;
import org.kududb.client.RowResult;
import org.kududb.client.KuduPredicate;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TestKuduSpanReceiver extends BaseKuduTest {

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
    span_columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_HIGH,
            Type.INT64)
            .build());
    span_columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_LOW,
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

  @Ignore
  @Test
  public void TestKuduSpanReceiver() throws Exception {
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
    scope.close();
    tracer.close();
    KuduClient client = BaseKuduTest.syncClient;
    List<String> spanColumns = new ArrayList<>();
    spanColumns.add(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID);
    spanColumns.add(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_SPAN_ID);
    spanColumns.add(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_DESCRIPTION);
    spanColumns.add(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_START_TIME);
    spanColumns.add(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_STOP_TIME);
    spanColumns.add(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_HIGH);
    spanColumns.add(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_LOW);
    KuduScanner scanner = client.newScannerBuilder(client.openTable(KuduReceiverConstants.DEFAULT_KUDU_SPAN_TABLE))
            .setProjectedColumnNames(spanColumns)
            .build();
    MilliSpan dbSpan = null;
    while (scanner.hasMoreRows()) {
      RowResultIterator results = scanner.nextRows();
      while (results.hasNext()) {
        RowResult result = results.next();
        long traceId = result.getLong(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID);
        MilliSpan.Builder builder = new MilliSpan.Builder()
                .spanId(new SpanId(result.getLong(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_SPAN_ID),
                        result.getLong(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID)))
                .description(result.getString(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_DESCRIPTION))
                .begin(result.getLong(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_START_TIME))
                .end(result.getLong(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_STOP_TIME));
        if (!(result.getLong(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_HIGH) == 0 &&
                result.getLong(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_LOW) == 0)) {
          SpanId[] parents = new SpanId[1];
          parents[0] = new SpanId(result.getLong(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_HIGH),
                  result.getLong(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_LOW));
          builder.parents(parents);
        }
        List<String> timelineColumns = new ArrayList<>();
        timelineColumns.add(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_TIME);
        timelineColumns.add(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_MESSAGE);
        KuduScanner timelineScanner = client
                .newScannerBuilder(client.openTable(KuduReceiverConstants.DEFAULT_KUDU_SPAN_TIMELINE_ANNOTATION_TABLE))
                .setProjectedColumnNames(timelineColumns)
                .addPredicate(KuduPredicate
                        .newComparisonPredicate(new ColumnSchema.ColumnSchemaBuilder
                                (KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_SPANID, Type.INT64)
                                .build(), KuduPredicate.ComparisonOp.EQUAL, traceId))
                .build();
        List<TimelineAnnotation> timelineList = new LinkedList<TimelineAnnotation>();
        while (timelineScanner.hasMoreRows()) {
          RowResultIterator timelineResults = timelineScanner.nextRows();
          while (timelineResults.hasNext()) {
            RowResult timelineRow = timelineResults.next();
            timelineList.add(new TimelineAnnotation
                    (timelineRow.getLong(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_TIME),
                            timelineRow.getString(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_MESSAGE)));
          }
        }
        builder.timeline(timelineList);
        dbSpan = builder.build();
        break;
      }
    }
    Assert.assertEquals(testSpan.getSpanId().getHigh(), dbSpan.getSpanId().getHigh());
    Assert.assertEquals(testSpan.getSpanId().getLow(), dbSpan.getSpanId().getLow());
    Assert.assertEquals(testSpan.getStartTimeMillis(), dbSpan.getStartTimeMillis());
    Assert.assertEquals(testSpan.getStopTimeMillis(), dbSpan.getStopTimeMillis());
    Assert.assertEquals(testSpan.getDescription(), dbSpan.getDescription());
    Assert.assertEquals(testSpan.getTimelineAnnotations().get(0).getMessage(),
            dbSpan.getTimelineAnnotations().get(0).getMessage());
    Assert.assertEquals(testSpan.getTimelineAnnotations().get(0).getTime(),
            dbSpan.getTimelineAnnotations().get(0).getTime());
    syncClient.deleteTable(KuduReceiverConstants.DEFAULT_KUDU_SPAN_TABLE);
    syncClient.deleteTable(KuduReceiverConstants.DEFAULT_KUDU_SPAN_TIMELINE_ANNOTATION_TABLE);
  }

}
