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
  static final String KUDU_SPAN_TABLE_KEY = "kudu.span.table";
  static final String DEFAULT_KUDU_SPAN_TABLE = "span";
  static final String KUDU_SPAN_PARENT_TABLE_KEY = "kudu.span.parent.table";
  static final String DEFAULT_KUDU_SPAN_PARENT_TABLE = "span.parent";
  static final String KUDU_SPAN_TIMELINE_ANNOTATION_TABLE_KEY = "kudu.span.timeline.annotation.table";
  static final String DEFAULT_KUDU_SPAN_TIMELINE_ANNOTATION_TABLE = "span.timeline";
  static final String DEFAULT_KUDU_COLUMN_SPAN_TRACE_ID = "trace_id";
  static final String DEFAULT_KUDU_COLUMN_SPAN_START_TIME = "start_time";
  static final String DEFAULT_KUDU_COLUMN_SPAN_STOP_TIME = "stop_time";
  static final String DEFAULT_KUDU_COLUMN_SPAN_SPAN_ID = "span_id";
  static final String DEFAULT_KUDU_COLUMN_PARENT_ID_LOW = "parent_id_low";
  static final String DEFAULT_KUDU_COLUMN_PARENT_ID_HIGH = "parent_id_high";
  static final String DEFAULT_KUDU_COLUMN_PARENT_CHILD_SPANID = "parent_child_span_id";
  static final String DEFAULT_KUDU_COLUMN_SPAN_DESCRIPTION = "description";
  static final String DEFAULT_KUDU_COLUMN_SPAN_PARENT = "parent";
  static final String DEFAULT_KUDU_COLUMN_TIMELINE_TIME = "time";
  static final String DEFAULT_KUDU_COLUMN_TIMELINE_MESSAGE = "message";
  static final String DEFAULT_KUDU_COLUMN_TIMELINE_SPANID = "spanid";
  static final String DEFAULT_KUDU_COLUMN_TIMELINE_TIMELINEID = "timelineid";
  static final String KUDU_CLIENT_WORKER_COUNT_KEY = "kudu.client.worker.count";
  static final String KUDU_CLIENT_BOSS_COUNT_KEY = "kudu.client.boss.count";
  static final String KUDU_CLIENT_STATISTICS_ENABLED_KEY = "kudu.client.statistics.enabled";
  static final String KUDU_CLIENT_TIMEOUT_ADMIN_OPERATION_KEY = "kudu.client.timeout.admin.operation";
  static final String KUDU_CLIENT_TIMEOUT_OPERATION_KEY = "kudu.client.timeout.operation";
  static final String KUDU_CLIENT_TIMEOUT_SOCKET_READ_KEY = "kudu.client.timeout.socket.read";

}



