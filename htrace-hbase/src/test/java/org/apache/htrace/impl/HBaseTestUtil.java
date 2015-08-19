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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.htrace.HBaseHTraceConfiguration;
import org.apache.htrace.core.SpanReceiver;
import org.apache.htrace.core.SpanReceiverBuilder;
import org.junit.Assert;


public class HBaseTestUtil {
  public static Configuration configure(Configuration conf) {
    Configuration hconf = HBaseConfiguration.create(conf);
    hconf.set(HBaseHTraceConfiguration.KEY_PREFIX +
              HBaseSpanReceiver.COLLECTOR_QUORUM_KEY,
              conf.get(HConstants.ZOOKEEPER_QUORUM));
    hconf.setInt(HBaseHTraceConfiguration.KEY_PREFIX +
                 HBaseSpanReceiver.ZOOKEEPER_CLIENT_PORT_KEY,
                 conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181));
    hconf.set(HBaseHTraceConfiguration.KEY_PREFIX +
              HBaseSpanReceiver.ZOOKEEPER_ZNODE_PARENT_KEY,
              conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    return hconf;
  }

  public static Table createTable(HBaseTestingUtility util) {
    Table htable = null;
    try {
      htable = util.createTable(Bytes.toBytes(HBaseSpanReceiver.DEFAULT_TABLE),
                                new byte[][]{Bytes.toBytes(HBaseSpanReceiver.DEFAULT_COLUMNFAMILY),
                                             Bytes.toBytes(HBaseSpanReceiver.DEFAULT_INDEXFAMILY)});
    } catch (IOException e) {
      Assert.fail("failed to create htrace table. " + e.getMessage());
    }
    return htable;
  }

  public static SpanReceiver startReceiver(Configuration conf) {
    return new SpanReceiverBuilder(new HBaseHTraceConfiguration(conf)).build();
  }

  public static SpanReceiver startReceiver(HBaseTestingUtility util) {
    return startReceiver(configure(util.getConfiguration()));
  }

  public static void stopReceiver(SpanReceiver receiver) {
    if (receiver != null) {
      try {
        receiver.close();
        receiver = null;
      } catch (IOException e) {
        Assert.fail("failed to close span receiver. " + e.getMessage());
      }
    }
  }
}
