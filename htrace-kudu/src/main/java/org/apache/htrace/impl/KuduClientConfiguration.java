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

import org.kududb.client.KuduClient;
import org.kududb.client.KuduClient.KuduClientBuilder;

public class KuduClientConfiguration {

  private final String host;
  private final Integer port;
  private final Integer workerCount;
  private final Integer bossCount;
  private final Boolean isStatisticsEnabled;
  private final Long adminOperationTimeout;
  private final Long operationTimeout;
  private final Long socketReadTimeout;

  public KuduClientConfiguration(String host,
                                 Integer port,
                                 Integer workerCount,
                                 Integer bossCount,
                                 Boolean isStatisticsEnabled,
                                 Long adminOperationTimeout,
                                 Long operationTimeout,
                                 Long socketReadTimeout) {

    this.host = host;
    this.port = port;
    this.workerCount = workerCount;
    this.bossCount = bossCount;
    this.isStatisticsEnabled = isStatisticsEnabled;
    this.adminOperationTimeout = adminOperationTimeout;
    this.operationTimeout = operationTimeout;
    this.socketReadTimeout = socketReadTimeout;
  }

  public KuduClient buildClient() {
    KuduClientBuilder builder = new KuduClient
            .KuduClientBuilder(host.concat(":").concat(port.toString()));
    if (workerCount != null) {
      builder.workerCount(workerCount);
    }
    if (bossCount != null) {
      builder.bossCount(bossCount);
    }
    if (isStatisticsEnabled != null && isStatisticsEnabled == false) {
      builder.disableStatistics();
    }
    if (adminOperationTimeout != null) {
      builder.defaultAdminOperationTimeoutMs(adminOperationTimeout);
    }
    if (operationTimeout != null) {
      builder.defaultOperationTimeoutMs(operationTimeout);
    }
    if (socketReadTimeout != null) {
      builder.defaultSocketReadTimeoutMs(socketReadTimeout);
    }
    return builder.build();
  }

}
