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

  private String host;
  private String port;
  private Integer workerCount;
  private Integer bossCount;
  private Boolean isStatisticsEnabled;
  private Long adminOperationTimeout;
  private Long operationTimeout;
  private Long socketReadTimeout;

  public KuduClientConfiguration(String host, String port) {
    this.host = host;
    this.port = port;
  }

  public void setWorkerCount(int workerCount) {
    this.workerCount = workerCount;
  }

  public void setBossCount(int bossCount) {
    this.bossCount = bossCount;
  }

  public void setIsStatisticsEnabled(boolean isStatisticsEnabled) {
    this.isStatisticsEnabled = isStatisticsEnabled;
  }

  public void setAdminOperationTimeout(long adminOperationTimeout) {
    this.adminOperationTimeout = adminOperationTimeout;
  }

  public void setOperationTimeout(long operationTimeout) {
    this.operationTimeout = operationTimeout;
  }

  public void setSocketReadTimeout(long socketReadTimeout) {
    this.socketReadTimeout = socketReadTimeout;
  }

  public KuduClient buildClient() {
    KuduClientBuilder builder = new KuduClient
            .KuduClientBuilder(host.concat(":").concat(port));
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
