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

import com.twitter.zipkin.gen.LogEntry;
import com.twitter.zipkin.gen.Scribe;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.Transport;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ScribeTransport implements Transport {

  /**
   * this is used to tell scribe that the entries are for zipkin..
   */
  public static final String CATEGORY = "zipkin";


  private static final Log LOG = LogFactory.getLog(ScribeTransport.class);
  /**
   * Default hostname to fall back on.
   */
  private static final String DEFAULT_COLLECTOR_HOSTNAME = "localhost";
  public static final String DEPRECATED_HOSTNAME_KEY = "zipkin.collector-hostname";
  public static final String HOSTNAME_KEY = "zipkin.scribe.hostname";

  /**
   * Default collector port.
   */
  private static final int DEFAULT_COLLECTOR_PORT = 9410; // trace collector default port.
  public static final String DEPRECATED_PORT_KEY = "zipkin.collector-port";
  public static final String PORT_KEY = "zipkin.scribe.port";

  private Scribe.Iface scribe = null;

  @Override
  public void open(HTraceConfiguration conf) throws IOException {
    if (!isOpen()) {
      checkDeprecation(conf, DEPRECATED_HOSTNAME_KEY, HOSTNAME_KEY);
      checkDeprecation(conf, DEPRECATED_PORT_KEY, PORT_KEY);

      String collectorHostname = conf.get(HOSTNAME_KEY,
                                          conf.get(DEPRECATED_HOSTNAME_KEY,
                                                   DEFAULT_COLLECTOR_HOSTNAME));
      int collectorPort = conf.getInt(PORT_KEY,
                                      conf.getInt(DEPRECATED_PORT_KEY,
                                                  DEFAULT_COLLECTOR_PORT));
      scribe = newScribe(collectorHostname, collectorPort);
      LOG.info("Opened transport " + collectorHostname + ":" + collectorPort);
    } else {
      LOG.warn("Attempted to open an already opened transport");
    }
  }

  private void checkDeprecation(HTraceConfiguration conf, String deprecatedKey,
                                String newKey) {
    if (conf.get(deprecatedKey) != null) {
      LOG.warn("Configuration \"" + deprecatedKey + "\" is deprecated. Use \"" +
               newKey + "\" instead.");
    }
  }

  @Override
  public boolean isOpen() {
    return scribe != null
           && ((Scribe.Client) scribe).getInputProtocol().getTransport().isOpen();
  }

  /**
   * The Scribe client which is used for rpc writes a list of
   * LogEntry objects, so the span objects are first transformed into LogEntry objects before
   * sending to the zipkin-collector.
   *
   * Here is a little ascii art which shows the above transformation:
   * <pre>
   *  +------------+   +------------+   +------------+              +-----------------+
   *  | HTrace Span|-->|Zipkin Span |-->| (LogEntry) | ===========> | Zipkin Collector|
   *  +------------+   +------------+   +------------+ (Scribe RPC) +-----------------+
   *  </pre>
   * @param spans to be sent. The raw bytes are being sent.
   * @throws IOException
   */
  @Override
  public void send(List<byte[]> spans) throws IOException {

    ArrayList<LogEntry> entries = new ArrayList<LogEntry>(spans.size());
    for (byte[] span : spans) {
      entries.add(new LogEntry(CATEGORY, Base64.encodeBase64String(span)));
    }

    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("sending " + entries.size() + " entries");
      }
      scribe.Log(entries); // TODO (clehene) should we instead interpret the return?
    } catch (TException e) {
      throw new IOException(e);
    }

  }

  @Override
  public void close() throws IOException {
    if (scribe != null) {
      ((Scribe.Client) scribe).getInputProtocol().getTransport().close();
      scribe = null;
      LOG.info("Closed transport");
    } else {
      LOG.warn("Attempted to close an already closed transport");
    }
  }

  private Scribe.Iface newScribe(String collectorHostname,
                                 int collectorPort)
      throws IOException {

    TTransport transport = new TFramedTransport(
        new TSocket(collectorHostname, collectorPort));
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new IOException(e);
    }
    TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
    TProtocol protocol = factory.getProtocol(transport);
    return new Scribe.Client(protocol);
  }

}
