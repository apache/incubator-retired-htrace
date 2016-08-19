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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.MilliSpan;
import org.apache.htrace.core.TimelineAnnotation;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.impl.KuduClientConfiguration;
import org.kududb.ColumnSchema;
import org.kududb.Type;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduPredicate;
import org.kududb.client.KuduScanner;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;

import java.io.OutputStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;

public class KuduSpanViewer {

  private static final Log LOG = LogFactory.getLog(KuduSpanViewer.class);
  private static final String JSON_FIELD_TRACE_ID = "trace_id";
  private static final String JSON_FIELD_PARENT_ID = "parent_id";
  private static final String JSON_FIELD_START = "start";
  private static final String JSON_FIELD_STOP = "stop";
  private static final String JSON_FIELD_SPAN_ID = "span_id";
  private static final String JSON_FIELD_DESCRIPTION = "description";
  private static final String JSON_FIELD_TIMELINE = "timeline";
  private static final String JSON_FIELD_TIMELINE_TIME = "time";
  private static final String JSON_FIELD_TIMELINE_MESSEGE = "message";
  private KuduClient client;
  private KuduClientConfiguration clientConf;


  public KuduSpanViewer(HTraceConfiguration conf) {
    String masterHost;
    Integer masterPort;
    Integer workerCount;
    Integer bossCount;
    Boolean isStatisticsEnabled;
    Long adminOperationTimeout;
    Long operationTimeout;
    Long socketReadTimeout;
    masterHost = conf.get(KuduClientConstants.KUDU_MASTER_HOST_KEY,
            KuduClientConstants.DEFAULT_KUDU_MASTER_HOST);
    masterPort = Integer.valueOf(conf.get(KuduClientConstants.KUDU_MASTER_PORT_KEY,
            KuduClientConstants.DEFAULT_KUDU_MASTER_PORT));

    if (conf.get(KuduClientConstants.KUDU_CLIENT_BOSS_COUNT_KEY) != null) {
      bossCount = Integer.valueOf(conf.get(KuduClientConstants.KUDU_CLIENT_BOSS_COUNT_KEY));
    } else {
      bossCount = null;
    }
    if (conf.get(KuduClientConstants.KUDU_CLIENT_WORKER_COUNT_KEY) != null) {
      workerCount = Integer.valueOf(conf.get(KuduClientConstants.KUDU_CLIENT_WORKER_COUNT_KEY));
    } else {
      workerCount = null;
    }
    if (conf.get(KuduClientConstants.KUDU_CLIENT_STATISTICS_ENABLED_KEY) != null) {
      isStatisticsEnabled = Boolean.valueOf(conf.get(KuduClientConstants.KUDU_CLIENT_STATISTICS_ENABLED_KEY));
    } else {
      isStatisticsEnabled = null;
    }
    if (conf.get(KuduClientConstants.KUDU_CLIENT_TIMEOUT_ADMIN_OPERATION_KEY) != null) {
      adminOperationTimeout = Long.valueOf(conf.get(KuduClientConstants.KUDU_CLIENT_TIMEOUT_ADMIN_OPERATION_KEY));
    } else {
      adminOperationTimeout = null;
    }
    if (conf.get(KuduClientConstants.KUDU_CLIENT_TIMEOUT_OPERATION_KEY) != null) {
      operationTimeout = Long.valueOf(conf.get(KuduClientConstants.KUDU_CLIENT_TIMEOUT_OPERATION_KEY));
    } else {
      operationTimeout = null;
    }
    if (conf.get(KuduClientConstants.KUDU_CLIENT_TIMEOUT_SOCKET_READ_KEY) != null) {
      socketReadTimeout = Long.valueOf(conf.get(KuduClientConstants.KUDU_CLIENT_TIMEOUT_SOCKET_READ_KEY));
    } else {
      socketReadTimeout = null;
    }
    this.clientConf = new KuduClientConfiguration(masterHost,
            masterPort,
            workerCount,
            bossCount,
            isStatisticsEnabled,
            adminOperationTimeout,
            operationTimeout,
            socketReadTimeout);
    this.client = clientConf.buildClient();
  }

  public List<Span> getSpans(long spanId) throws Exception {
    List<Span> spans = new ArrayList<Span>();
    List<String> spanColumns = new ArrayList<>();
    spanColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID);
    spanColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_SPAN_ID);
    spanColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_DESCRIPTION);
    spanColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_START_TIME);
    spanColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_STOP_TIME);
    spanColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_HIGH);
    spanColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_LOW);
    KuduScanner scanner = client.newScannerBuilder(client.openTable(KuduClientConstants.DEFAULT_KUDU_SPAN_TABLE))
            .setProjectedColumnNames(spanColumns)
            .addPredicate(KuduPredicate
                    .newComparisonPredicate(new ColumnSchema.ColumnSchemaBuilder
                            (KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_SPAN_ID, Type.INT64)
                            .build(), KuduPredicate.ComparisonOp.EQUAL, spanId))
            .build();
    MilliSpan dbSpan;
    while (scanner.hasMoreRows()) {
      RowResultIterator results = scanner.nextRows();
      while (results.hasNext()) {
        RowResult result = results.next();
        long traceId = result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID);
        MilliSpan.Builder builder = new MilliSpan.Builder()
                .spanId(new SpanId(result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_SPAN_ID),
                        result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID)))
                .description(result.getString(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_DESCRIPTION))
                .begin(result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_START_TIME))
                .end(result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_STOP_TIME));
        if (!(result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_HIGH) == 0 &&
                result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_LOW) == 0)) {
          SpanId[] parents = new SpanId[1];
          parents[0] = new SpanId(result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_HIGH),
                  result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_LOW));
          builder.parents(parents);
        }
        List<String> timelineColumns = new ArrayList<>();
        timelineColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_TIMELINE_TIME);
        timelineColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_TIMELINE_MESSAGE);
        KuduScanner timelineScanner = client
                .newScannerBuilder(client.openTable(KuduClientConstants.DEFAULT_KUDU_SPAN_TIMELINE_ANNOTATION_TABLE))
                .setProjectedColumnNames(timelineColumns)
                .addPredicate(KuduPredicate
                        .newComparisonPredicate(new ColumnSchema.ColumnSchemaBuilder
                                (KuduClientConstants.DEFAULT_KUDU_COLUMN_TIMELINE_SPANID, Type.INT64)
                                .build(), KuduPredicate.ComparisonOp.EQUAL, traceId))
                .build();
        List<TimelineAnnotation> timelineList = new LinkedList<TimelineAnnotation>();
        while (timelineScanner.hasMoreRows()) {
          RowResultIterator timelineResults = timelineScanner.nextRows();
          while (timelineResults.hasNext()) {
            RowResult timelineRow = timelineResults.next();
            timelineList.add(new TimelineAnnotation
                    (timelineRow.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_TIMELINE_TIME),
                            timelineRow.getString(KuduClientConstants.DEFAULT_KUDU_COLUMN_TIMELINE_MESSAGE)));
          }
        }
        builder.timeline(timelineList);
        dbSpan = builder.build();
        spans.add(dbSpan);
      }
    }
    return spans;
  }

  public List<Span> getRootSpans() throws Exception {
    List<Span> spans = new ArrayList<Span>();
    List<String> spanColumns = new ArrayList<>();
    spanColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID);
    spanColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_SPAN_ID);
    spanColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_DESCRIPTION);
    spanColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_START_TIME);
    spanColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_STOP_TIME);
    KuduScanner scanner = client.newScannerBuilder(client.openTable(KuduClientConstants.DEFAULT_KUDU_SPAN_TABLE))
            .setProjectedColumnNames(spanColumns)
            .addPredicate(KuduPredicate
                    .newComparisonPredicate(new ColumnSchema.ColumnSchemaBuilder
                            (KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT, Type.BOOL)
                            .build(), KuduPredicate.ComparisonOp.EQUAL, true))
            .build();
    MilliSpan dbSpan;
    while (scanner.hasMoreRows()) {
      RowResultIterator results = scanner.nextRows();
      while (results.hasNext()) {
        RowResult result = results.next();
        long traceId = result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID);
        MilliSpan.Builder builder = new MilliSpan.Builder()
                .spanId(new SpanId(result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_SPAN_ID),
                        result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID)))
                .description(result.getString(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_DESCRIPTION))
                .begin(result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_START_TIME))
                .end(result.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_SPAN_STOP_TIME));
        List<String> timelineColumns = new ArrayList<>();
        timelineColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_TIMELINE_TIME);
        timelineColumns.add(KuduClientConstants.DEFAULT_KUDU_COLUMN_TIMELINE_MESSAGE);
        KuduScanner timelineScanner = client
                .newScannerBuilder(client.openTable(KuduClientConstants.DEFAULT_KUDU_SPAN_TIMELINE_ANNOTATION_TABLE))
                .setProjectedColumnNames(timelineColumns)
                .addPredicate(KuduPredicate
                        .newComparisonPredicate(new ColumnSchema.ColumnSchemaBuilder
                                (KuduClientConstants.DEFAULT_KUDU_COLUMN_TIMELINE_SPANID, Type.INT64)
                                .build(), KuduPredicate.ComparisonOp.EQUAL, traceId))
                .build();
        List<TimelineAnnotation> timelineList = new LinkedList<TimelineAnnotation>();
        while (timelineScanner.hasMoreRows()) {
          RowResultIterator timelineResults = timelineScanner.nextRows();
          while (timelineResults.hasNext()) {
            RowResult timelineRow = timelineResults.next();
            timelineList.add(new TimelineAnnotation
                    (timelineRow.getLong(KuduClientConstants.DEFAULT_KUDU_COLUMN_TIMELINE_TIME),
                            timelineRow.getString(KuduClientConstants.DEFAULT_KUDU_COLUMN_TIMELINE_MESSAGE)));
          }
        }
        builder.timeline(timelineList);
        dbSpan = builder.build();
        spans.add(dbSpan);
      }
    }
    return spans;
  }

  public void close() {
    try {
      this.client.close();
    } catch (java.lang.Exception ex) {
      LOG.error("Couln't close the Kudu DB client connection.", ex);
    }
  }


  public static String toJsonString(final Span span) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    OutputStreamWriter writer =
            new OutputStreamWriter(out, Charset.defaultCharset());
    appendJsonString(span, writer);
    writer.flush();
    out.flush();
    return out.toString();
  }

  public static void appendJsonString(Span span, OutputStreamWriter writer) throws IOException {
    writer.append("{");
    appendField(JSON_FIELD_TRACE_ID, span.getSpanId().getLow(), writer);
    appendField(JSON_FIELD_SPAN_ID, span.getSpanId().getHigh(), writer);
    appendField(JSON_FIELD_DESCRIPTION, span.getDescription(), writer);
    if (span.getParents().length != 0) {
      appendField(JSON_FIELD_PARENT_ID, span.getParents()[0].getLow(), writer);
    }
    appendField(JSON_FIELD_START, span.getStartTimeMillis(), writer);
    appendField(JSON_FIELD_STOP, span.getStopTimeMillis(), writer);
    if (!span.getTimelineAnnotations().isEmpty()) {
      writer.append("\"");
      writer.append(JSON_FIELD_TIMELINE);
      writer.append("\"");
      writer.append(":");
      writer.append("[");
      for (TimelineAnnotation annotation : span.getTimelineAnnotations()) {
        writer.append("{");
        appendField(JSON_FIELD_TIMELINE_TIME, annotation.getTime(), writer);
        appendField(JSON_FIELD_TIMELINE_MESSEGE, annotation.getMessage(), writer);
        writer.append("}");
      }
      writer.append("]");
    }
    writer.append("}");
  }

  private static void appendField(String field,
                                  Object value,
                                  OutputStreamWriter writer) throws IOException {
    writer.append("\"");
    writer.append(field);
    writer.append("\"");
    writer.append(":");
    appendStringValue(value.toString(), writer);
    writer.append(",");
  }

  private static void appendStringValue(String value,
                                        OutputStreamWriter writer) throws IOException {
    writer.append("\"");
    writer.append(value.toString());
    writer.append("\"");
  }

}