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

public class KuduReceiverConstants {

  static final String KUDU_MASTER_HOST_KEY = "kudu.master.host";
  static final String DEFAULT_KUDU_MASTER_HOST = "127.0.0.1";
  static final String KUDU_MASTER_PORT_KEY = "kudu.master.port";
  static final String DEFAULT_KUDU_MASTER_PORT = "7051";
  static final String SPAN_BLOCKING_QUEUE_SIZE_KEY = "kudu.span.queue.size";
  static final int DEFAULT_SPAN_BLOCKING_QUEUE_SIZE = 1000;
  static final String KUDU_SPAN_TABLE_KEY = "kudu.span.table";
  static final String DEFAULT_KUDU_SPAN_TABLE = "span";
  static final String KUDU_SPAN_TIMELINE_ANNOTATION_TABLE_KEY = "kudu.span.timeline.annotation.table";
  static final String DEFAULT_KUDU_SPAN_TIMELINE_ANNOTATION_TABLE = "span.timeline";
  static final String MAX_SPAN_BATCH_SIZE_KEY = "kudu.batch.size";
  static final int DEFAULT_MAX_SPAN_BATCH_SIZE = 100;
  static final String NUM_PARALLEL_THREADS_KEY = "kudu.num.threads";
  static final int DEFAULT_NUM_PARALLEL_THREADS = 1;
  static final String KUDU_COLUMN_SPAN_TRACE_ID_KEY = "kudu.column.span.traceid";
  static final String DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID = "trace_id";
  static final String KUDU_COLUMN_SPAN_START_TIME_KEY = "kudu.column.span.starttime";
  static final String DEFAULT_KUDU_COLUMN_SPAN_START_TIME = "start_time";
  static final String KUDU_COLUMN_SPAN_STOP_TIME_KEY = "kudu.column.span.stoptime";
  static final String DEFAULT_KUDU_COLUMN_SPAN_STOP_TIME = "stop_time";
  static final String KUDU_COLUMN_SPAN_SPAN_ID_KEY = "kudu.column.span.spanid";
  static final String DEFAULT_KUDU_COLUMN_SPAN_SPAN_ID = "span_id";
  static final String KUDU_COLUMN_SPAN_PROCESS_ID_KEY = "kudu.column.span.processid";
  static final String DEFAULT_KUDU_COLUMN_SPAN_PROCESS_ID = "process_id";
  static final String KUDU_COLUMN_SPAN_PARENT_ID_KEY = "kudu.column.span.parentid";
  static final String DEFAULT_KUDU_COLUMN_SPAN_PARENT_ID = "parent_id";
  static final String KUDU_COLUMN_SPAN_DESCRIPTION_KEY = "kudu.column.span.description";
  static final String DEFAULT_KUDU_COLUMN_SPAN_DESCRIPTION = "description";
  static final String KUDU_COLUMN_SPAN_PARENT_KEY = "kudu.column.span.parent";
  static final String DEFAULT_KUDU_COLUMN_SPAN_PARENT = "parent";
  static final String KUDU_COLUMN_TIMELINE_TIME_KEY = "kudu.column.timeline.time";
  static final String DEFAULT_KUDU_COLUMN_TIMELINE_TIME = "time";
  static final String KUDU_COLUMN_TIMELINE_MESSAGE_KEY = "kudu.column.timeline.message";
  static final String DEFAULT_KUDU_COLUMN_TIMELINE_MESSAGE = "message";
  static final String KUDU_COLUMN_TIMELINE_SPANID_KEY = "kudu.column.timeline.spanid";
  static final String DEFAULT_KUDU_COLUMN_TIMELINE_SPANID = "spanid";
  static final String KUDU_COLUMN_TIMELINE_TIMELINEID_KEY = "kudu.column.timeline.timelineid";
  static final String DEFAULT_KUDU_COLUMN_TIMELINE_TIMELINEID = "timelineid";
  static final String KUDU_CLIENT_WORKER_COUNT_KEY = "kudu.client.worker.count";
  static final String KUDU_CLIENT_BOSS_COUNT_KEY = "kudu.client.boss.count";
  static final String KUDU_CLIENT_STATISTICS_ENABLED_KEY = "kudu.client.statistics.enabled";
  static final String KUDU_CLIENT_TIMEOUT_ADMIN_OPERATION_KEY = "kudu.client.timeout.admin.operation";
  static final String KUDU_CLIENT_TIMEOUT_OPERATION_KEY = "kudu.client.timeout.operation";
  static final String KUDU_CLIENT_TIMEOUT_SOCKET_READ_KEY = "kudu.client.timeout.socket.read";

}



