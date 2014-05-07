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

package org.htrace.impl;

import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.trace.HBaseHTraceConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;
import org.htrace.Span;
import org.htrace.SpanReceiver;
import org.htrace.HTraceConfiguration;
import org.htrace.TraceCreator;
import org.htrace.TraceTree;

public class TestHBaseSpanReceiver {
  private static final Log LOG = LogFactory.getLog(TestHBaseSpanReceiver.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private HTableInterface htable;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  Configuration configure(Configuration conf) {
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

  void createTable() {
    try { 
      this.htable = UTIL.createTable(HBaseSpanReceiver.DEFAULT_TABLE,
                                     HBaseSpanReceiver.DEFAULT_COLUMNFAMILY);
    } catch (IOException e) {
      Assert.fail("failed to create htrace table. " + e.getMessage());
    }
  }

  private SpanReceiver startReceiver(Configuration conf) {
    SpanReceiver receiver = new HBaseSpanReceiver();
    receiver.configure(new HBaseHTraceConfiguration(conf));
    return receiver;
  }

  private void stopReceiver(SpanReceiver receiver) {
    if (receiver != null) {
      try {
        receiver.close();
        receiver = null;
      } catch (IOException e) {
        Assert.fail("failed to close span receiver. " + e.getMessage());
      }
    }
  }

  @Test
  public void testHBaseSpanReceiver() {
    createTable();
    Configuration conf = configure(UTIL.getConfiguration());
    SpanReceiver receiver = startReceiver(conf);
    TraceCreator tc = new TraceCreator(receiver);
    tc.createThreadedTrace();
    tc.createSimpleTrace();
    tc.createSampleRpcTrace();
    stopReceiver(receiver);
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes(HBaseSpanReceiver.DEFAULT_COLUMNFAMILY));
    scan.setMaxVersions(1);
    ArrayList<Span> spans = new ArrayList<Span>();
    try {
      ResultScanner scanner = this.htable.getScanner(scan);
      Result result = null;
      while ((result = scanner.next()) != null) {
        for (Cell cell : result.rawCells()) {
          Span span = HBaseSpanInfo.fromBytes(cell.getQualifierArray(),
                                              cell.getQualifierOffset(),
                                              cell.getQualifierLength());
          spans.add(span);
        }
      }
    } catch (IOException e) {
      Assert.fail("failed to get spans from HBase. " + e.getMessage());
    }

    TraceTree traceTree = new TraceTree(spans);
    Collection<Span> roots = traceTree.getRoots();
    Assert.assertEquals(3, roots.size());

    Map<String, Span> descs = new HashMap<String, Span>();
    for (Span root : roots) {
      descs.put(root.getDescription(), root);
    }
    Assert.assertTrue(descs.keySet().contains(TraceCreator.RPC_TRACE_ROOT));
    Assert.assertTrue(descs.keySet().contains(TraceCreator.SIMPLE_TRACE_ROOT));
    Assert.assertTrue(descs.keySet().contains(TraceCreator.THREADED_TRACE_ROOT));

    Multimap<Long, Span> spansByParentId = traceTree.getSpansByParentIdMap();
    Span rpcRoot = descs.get(TraceCreator.RPC_TRACE_ROOT);
    Assert.assertEquals(1, spansByParentId.get(rpcRoot.getSpanId()).size());
    Span rpcChild1 = spansByParentId.get(rpcRoot.getSpanId()).iterator().next();
    Assert.assertEquals(1, spansByParentId.get(rpcChild1.getSpanId()).size());
    Span rpcChild2 = spansByParentId.get(rpcChild1.getSpanId()).iterator().next();
    Assert.assertEquals(1, spansByParentId.get(rpcChild2.getSpanId()).size());
    Span rpcChild3 = spansByParentId.get(rpcChild2.getSpanId()).iterator().next();
    Assert.assertEquals(0, spansByParentId.get(rpcChild3.getSpanId()).size());
  }
}
