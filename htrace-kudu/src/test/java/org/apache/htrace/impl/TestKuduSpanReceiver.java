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
import org.apache.htrace.core.Span;
import org.apache.htrace.core.TracerPool;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.protobuf.generated.SpanProtos;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.ColumnSchema;
import org.kududb.client.BaseKuduTest;
import org.kududb.client.KuduClient;
import org.kududb.client.CreateTableOptions;
import org.kududb.client.KuduScanner;
import org.kududb.client.RowResultIterator;
import org.kududb.client.RowResult;


import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

public class TestKuduSpanReceiver extends BaseKuduTest {

  private static final String BIN_DIR_PROP = "binDir";
  private static final String BIN_DIR_PROP_DEFAULT = "../build/release/bin";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    System.setProperty(BIN_DIR_PROP, BIN_DIR_PROP_DEFAULT);
    BaseKuduTest.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    BaseKuduTest.tearDownAfterClass();
  }

  private void createTable() throws Exception {
    KuduClient client = BaseKuduTest.syncClient;
    List<ColumnSchema> columns = new ArrayList(4);
    columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_ID,
            Type.BINARY)
            .key(true)
            .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN,
            Type.BINARY)
            .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_ROOT_SPAN_START_TIME,
            Type.BINARY)
            .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_ROOT_SPAN,
            Type.BINARY)
            .build());
    List<String> rangeKeys = new ArrayList<>();
    rangeKeys.add(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_ID);

    Schema schema = new Schema(columns);
    client.createTable(KuduReceiverConstants.DEFAULT_KUDU_TABLE, schema,
            new CreateTableOptions().setRangePartitionColumns(rangeKeys));
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
    Span testSpan = scope.getSpan();
    SpanProtos.Span dbSpan = null;
    scope.close();
    tracer.close();
    KuduClient client = BaseKuduTest.syncClient;
    List<String> projectColumns = new ArrayList<>(1);
    projectColumns.add(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN);
    KuduScanner scanner = client.newScannerBuilder(client.openTable(KuduReceiverConstants.DEFAULT_KUDU_TABLE))
            .setProjectedColumnNames(projectColumns)
            .build();
    while (scanner.hasMoreRows()) {
      RowResultIterator results = scanner.nextRows();
      while (results.hasNext()) {
        RowResult result = results.next();
        ByteArrayInputStream in = new
                ByteArrayInputStream(result.getBinaryCopy(KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN));
        dbSpan = SpanProtos.Span.parseFrom(in);
        break;
      }
    }
    Assert.assertEquals(testSpan.getSpanId().getHigh(), dbSpan.getTraceId());
    Assert.assertEquals(testSpan.getSpanId().getLow(), dbSpan.getSpanId());
    Assert.assertEquals(testSpan.getStartTimeMillis(), dbSpan.getStart());
    Assert.assertEquals(testSpan.getStopTimeMillis(), dbSpan.getStop());
    Assert.assertEquals(testSpan.getDescription(), dbSpan.getDescription());
    syncClient.deleteTable(KuduReceiverConstants.DEFAULT_KUDU_TABLE);
  }

}
