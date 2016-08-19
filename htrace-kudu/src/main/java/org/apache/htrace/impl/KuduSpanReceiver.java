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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanReceiver;
import org.apache.htrace.core.TimelineAnnotation;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.Insert;
import org.kududb.client.PartialRow;

import java.io.IOException;

public class KuduSpanReceiver extends SpanReceiver {

  private static final Log LOG = LogFactory.getLog(KuduSpanReceiver.class);

  private final KuduClientConfiguration clientConf;
  private KuduSession session;
  private KuduClient client;

  private String table_span;
  private String column_span_trace_id;
  private String column_span_start_time;
  private String column_span_stop_time;
  private String column_span_span_id;
  private String column_span_parent_id_low;
  private String column_span_parent_id_high;
  private String column_span_description;
  private String column_span_parent;

  private String table_timeline;
  private String column_timeline_timeline_id;
  private String column_timeline_time;
  private String column_timeline_message;
  private String column_timeline_span_id;

  public KuduSpanReceiver(HTraceConfiguration conf) {

    String masterHost;
    Integer masterPort;
    Integer workerCount;
    Integer bossCount;
    Boolean isStatisticsEnabled;
    Long adminOperationTimeout;
    Long operationTimeout;
    Long socketReadTimeout;

    masterHost = conf.get(KuduReceiverConstants.KUDU_MASTER_HOST_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_MASTER_HOST);
    masterPort = Integer.valueOf(conf.get(KuduReceiverConstants.KUDU_MASTER_PORT_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_MASTER_PORT));

    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_BOSS_COUNT_KEY) != null) {
      bossCount = Integer.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_BOSS_COUNT_KEY));
    } else {
      bossCount = null;
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_WORKER_COUNT_KEY) != null) {
      workerCount = Integer.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_WORKER_COUNT_KEY));
    } else {
      workerCount = null;
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_STATISTICS_ENABLED_KEY) != null) {
      isStatisticsEnabled = Boolean.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_STATISTICS_ENABLED_KEY));
    } else {
      isStatisticsEnabled = null;
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_ADMIN_OPERATION_KEY) != null) {
      adminOperationTimeout = Long.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_ADMIN_OPERATION_KEY));
    } else {
      adminOperationTimeout = null;
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_OPERATION_KEY) != null) {
      operationTimeout = Long.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_OPERATION_KEY));
    } else {
      operationTimeout = null;
    }
    if (conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_SOCKET_READ_KEY) != null) {
      socketReadTimeout = Long.valueOf(conf.get(KuduReceiverConstants.KUDU_CLIENT_TIMEOUT_SOCKET_READ_KEY));
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
    //table names made configurable
    this.table_span = conf.get(KuduReceiverConstants.KUDU_SPAN_TABLE_KEY, KuduReceiverConstants.DEFAULT_KUDU_SPAN_TABLE);
    this.table_timeline = conf.get(KuduReceiverConstants.KUDU_SPAN_TIMELINE_ANNOTATION_TABLE_KEY,
            KuduReceiverConstants.DEFAULT_KUDU_SPAN_TIMELINE_ANNOTATION_TABLE);
    //default column names have used
    this.column_span_trace_id = KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID;
    this.column_span_start_time = KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_START_TIME;
    this.column_span_stop_time = KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_STOP_TIME;
    this.column_span_span_id = KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_SPAN_ID;
    this.column_span_parent_id_low = KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_LOW;
    this.column_span_parent_id_high = KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID_HIGH;
    this.column_span_description = KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_DESCRIPTION;
    this.column_span_parent = KuduReceiverConstants.DEFAULT_KUDU_COLUMN_SPAN_PARENT;
    this.column_timeline_time = KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_TIME;
    this.column_timeline_message = KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_MESSAGE;
    this.column_timeline_span_id = KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_SPANID;
    this.column_timeline_timeline_id = KuduReceiverConstants.DEFAULT_KUDU_COLUMN_TIMELINE_TIMELINEID;
    //kudu backend session initialization
    if (this.session == null) {
      if (this.client == null) {
        client = clientConf.buildClient();
      }
      session = client.newSession();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (this.session != null) {
        if (this.session.isClosed()) {
          this.session.close();
        }
        this.client.close();
      }
    } catch (java.lang.Exception e) {
      LOG.warn("Failed to close Kudu session. " + e.getMessage());
    }
  }

  @Override
  public void receiveSpan(Span span) {
   try {
      KuduTable tableSpan = client.openTable(table_span);
      Insert spanInsert = tableSpan.newInsert();
      PartialRow spanRow = spanInsert.getRow();
      spanRow.addLong(column_span_trace_id, span.getSpanId().getLow());
      spanRow.addLong(column_span_start_time, span.getStartTimeMillis());
      spanRow.addLong(column_span_stop_time, span.getStopTimeMillis());
      spanRow.addLong(column_span_span_id, span.getSpanId().getHigh());
      if (span.getParents().length == 0) {
        spanRow.addLong(column_span_parent_id_low, 0);
        spanRow.addLong(column_span_parent_id_high, 0);
        spanRow.addBoolean(column_span_parent, true);
      } else if (span.getParents().length > 0) {
        spanRow.addLong(column_span_parent_id_low, span.getParents()[0].getLow());
        spanRow.addLong(column_span_parent_id_high, span.getParents()[0].getHigh());
        spanRow.addBoolean(column_span_parent, false);
      }
      spanRow.addString(column_span_description, span.getDescription());
      session.apply(spanInsert);
      long annotationCounter = 0;
      for (TimelineAnnotation ta : span.getTimelineAnnotations()) {
        annotationCounter++;
        KuduTable tableTimeline = client.openTable(table_timeline);
        Insert timelineInsert = tableTimeline.newInsert();
        PartialRow timelineRow = timelineInsert.getRow();
        timelineRow.addLong(column_timeline_timeline_id, span.getSpanId().getLow() + annotationCounter);
        timelineRow.addLong(column_timeline_time, ta.getTime());
        timelineRow.addString(column_timeline_message, ta.getMessage());
        timelineRow.addLong(column_timeline_span_id, span.getSpanId().getLow());
        session.apply(timelineInsert);
      }
    } catch (java.lang.Exception ex) {
      LOG.error("Failed to write span to Kudu backend", ex);
    } finally {
      try {
        session.flush();
      } catch (java.lang.Exception ex) {
        //Ignore
      }
    }
  }

}
