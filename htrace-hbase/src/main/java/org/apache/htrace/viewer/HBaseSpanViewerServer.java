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

package org.apache.htrace.viewer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseSpanViewerServer implements Tool {
  private static final Log LOG = LogFactory.getLog(HBaseSpanViewerServer.class);
  public static final String HTRACE_VIEWER_HTTP_ADDRESS_KEY = "htrace.viewer.http.address";
  public static final String HTRACE_VIEWER_HTTP_ADDRESS_DEFAULT = "0.0.0.0:16900";
  public static final String HTRACE_CONF_ATTR = "htrace.conf";
  public static final String HTRACE_APPDIR = "webapps";
  public static final String NAME = "htrace";

  private Configuration conf;
  private HttpServer2 httpServer;
  private InetSocketAddress httpAddress;

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

  void start() throws IOException {
    httpAddress = NetUtils.createSocketAddr(
        conf.get(HTRACE_VIEWER_HTTP_ADDRESS_KEY, HTRACE_VIEWER_HTTP_ADDRESS_DEFAULT));
    conf.set(HTRACE_VIEWER_HTTP_ADDRESS_KEY, NetUtils.getHostPortString(httpAddress));
    HttpServer2.Builder builder = new HttpServer2.Builder();
    builder.setName(NAME).setConf(conf);
    if (httpAddress.getPort() == 0) {
      builder.setFindPort(true);
    }
    URI uri = URI.create("http://" + NetUtils.getHostPortString(httpAddress));
    builder.addEndpoint(uri);
    LOG.info("Starting Web-server for " + NAME + " at: " + uri);
    httpServer = builder.build();
    httpServer.setAttribute(HTRACE_CONF_ATTR, conf);
    httpServer.addServlet("gettraces",
                          HBaseSpanViewerTracesServlet.PREFIX,
                          HBaseSpanViewerTracesServlet.class);
    httpServer.addServlet("getspans",
                          HBaseSpanViewerSpansServlet.PREFIX + "/*",
                          HBaseSpanViewerSpansServlet.class);

    // for webapps/htrace bundled in jar.
    String rb = httpServer.getClass()
                          .getClassLoader()
                          .getResource("webapps/" + NAME)
                          .toString();
    httpServer.getWebAppContext().setResourceBase(rb);

    httpServer.start();
    httpAddress = httpServer.getConnectorAddress(0);
  }

  void join() throws Exception {
    if (httpServer != null) {
      httpServer.join();
    }
  }

  void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
  }

  InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  public int run(String[] args) throws Exception {
    start();
    join();
    stop();
    return 0;
  }

  /**
   * @throws IOException
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(HBaseConfiguration.create(), new HBaseSpanViewerServer(), args);
  }
}
