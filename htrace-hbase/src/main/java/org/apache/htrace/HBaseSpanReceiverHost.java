/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.htrace;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.Trace;

/**
 * This class provides functions for reading the names of SpanReceivers from
 * hbase-site.xml, adding those SpanReceivers to the Tracer, and closing those
 * SpanReceivers when appropriate.
 */
public class HBaseSpanReceiverHost {
  public static final String SPAN_RECEIVERS_CONF_KEY = "hbase.trace.spanreceiver.classes";
  private static final Log LOG = LogFactory.getLog(HBaseSpanReceiverHost.class);
  private Collection<SpanReceiver> receivers;
  private Configuration conf;
  private boolean closed = false;

  private static enum SingletonHolder {
    INSTANCE;
    Object lock = new Object();
    HBaseSpanReceiverHost host = null;
  }

  public static HBaseSpanReceiverHost getInstance(Configuration conf) {
    synchronized (SingletonHolder.INSTANCE.lock) {
      if (SingletonHolder.INSTANCE.host != null) {
        return SingletonHolder.INSTANCE.host;
      }

      HBaseSpanReceiverHost host = new HBaseSpanReceiverHost(conf);
      host.loadSpanReceivers();
      SingletonHolder.INSTANCE.host = host;
      return SingletonHolder.INSTANCE.host;
    }

  }

  HBaseSpanReceiverHost(Configuration conf) {
    receivers = new HashSet<SpanReceiver>();
    this.conf = conf;
  }

  /**
   * Reads the names of classes specified in the
   * "hbase.trace.spanreceiver.classes" property and instantiates and registers
   * them with the Tracer as SpanReceiver's.
   *
   */
  public void loadSpanReceivers() {
    String[] receiverNames = conf.getStrings(SPAN_RECEIVERS_CONF_KEY);
    if (receiverNames == null || receiverNames.length == 0) {
      return;
    }
    SpanReceiverBuilder builder = new SpanReceiverBuilder(new HBaseHTraceConfiguration(this.conf));
    for (String className : receiverNames) {
      SpanReceiver receiver = builder.spanReceiverClass(className.trim()).build();
      if (receiver != null) {
        receivers.add(receiver);
        LOG.info("SpanReceiver " + className + " was loaded successfully.");
      }
    }
    for (SpanReceiver rcvr : receivers) {
      Trace.addReceiver(rcvr);
    }
  }

  /**
   * Calls close() on all SpanReceivers created by this HBaseSpanReceiverHost.
   */
  public synchronized void closeReceivers() {
    if (closed) return;
    closed = true;
    for (SpanReceiver rcvr : receivers) {
      try {
        rcvr.close();
      } catch (IOException e) {
        LOG.warn("Unable to close SpanReceiver correctly: " + e.getMessage(), e);
      }
    }
  }
}