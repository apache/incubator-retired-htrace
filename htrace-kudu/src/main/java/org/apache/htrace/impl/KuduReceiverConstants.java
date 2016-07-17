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

  public static final String KUDU_MASTER_HOST_KEY = "htrace.kudu.master.host";
  public static final String DEFAULT_KUDU_MASTER_HOST = "127.0.0.1";
  public static final String KUDU_MASTER_PORT_KEY = "htrace.kudu.master.port";
  public static final String DEFAULT_KUDU_MASTER_PORT = "7051";
  public static final String SPAN_BLOCKING_QUEUE_SIZE_KEY = "htrace.kudu.span.queue.size";
  public static final int DEFAULT_SPAN_BLOCKING_QUEUE_SIZE = 1000;
  public static final String KUDU_TABLE_KEY = "htrace.kudu.table";
  public static final String DEFAULT_KUDU_TABLE = "htrace";
  public static final String MAX_SPAN_BATCH_SIZE_KEY = "htrace.kudu.batch.size";
  public static final int DEFAULT_MAX_SPAN_BATCH_SIZE = 100;
  public static final String NUM_PARALLEL_THREADS_KEY = "htrace.kudu.num.threads";
  public static final int DEFAULT_NUM_PARALLEL_THREADS = 1;
  public static final String KUDU_COLUMN_SPAN_ID_KEY = "htrace.kudu.column.spanid";
  public static final String DEFAULT_KUDU_COLUMN_SPAN_ID = "span_id";
  public static final String KUDU_COLUMN_SPAN_KEY = "htrace.kudu.column.span";
  public static final String DEFAULT_KUDU_COLUMN_SPAN = "span";
  public static final String KUDU_COLUMN_ROOT_SPAN_KEY = "htrace.kudu.column.rootspan";
  public static final String DEFAULT_KUDU_COLUMN_ROOT_SPAN = "root_span";
  public static final String KUDU_COLUMN_ROOT_SPAN_START_TIME_KEY = "htrace.kudu.column.rootspan.starttime";
  public static final String DEFAULT_KUDU_COLUMN_ROOT_SPAN_START_TIME = "root_span_start_time";
  public static final String KUDU_CLIENT_WORKER_COUNT_KEY = "htrace.kudu.client.worker.count";
  public static final String KUDU_CLIENT_BOSS_COUNT_KEY = "htrace.kudu.client.boss.count";
  public static final String KUDU_CLIENT_STATISTICS_ENABLED_KEY = "htrace.kudu.client.statistics.enabled";
  public static final String KUDU_CLIENT_TIMEOUT_ADMIN_OPERATION_KEY = "htrace.kudu.client.timeout.admin.operation";
  public static final String KUDU_CLIENT_TIMEOUT_OPERATION_KEY = "htrace.kudu.client.timeout.operation";
  public static final String KUDU_CLIENT_TIMEOUT_SOCKET_READ_KEY = "htrace.kudu.client.timeout.socket.read";

}
