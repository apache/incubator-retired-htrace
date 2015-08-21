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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class HBaseSpanViewerServer implements Tool {
  private static final Log LOG = LogFactory.getLog(HBaseSpanViewerServer.class);
  public static final String HTRACE_VIEWER_HTTP_ADDRESS_KEY = "htrace.viewer.http.address";
  public static final String HTRACE_VIEWER_HTTP_ADDRESS_DEFAULT = "0.0.0.0:16900";
  private Configuration conf;
  private Server server;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  public void stop() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  public int run(String[] args) throws Exception {
    URI uri = new URI("http://" + conf.get(HTRACE_VIEWER_HTTP_ADDRESS_KEY,
                                           HTRACE_VIEWER_HTTP_ADDRESS_DEFAULT));
    InetSocketAddress addr = new InetSocketAddress(uri.getHost(), uri.getPort());
    server = new Server(addr);
    ServletContextHandler root =
      new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
    server.setHandler(root);

    String resourceBase = server.getClass()
                                .getClassLoader()
                                .getResource("webapps/htrace")
                                .toExternalForm();
    root.setResourceBase(resourceBase);
    root.setWelcomeFiles(new String[]{"index.html"});
    root.addServlet(new ServletHolder(new DefaultServlet()),
                    "/");
    root.addServlet(new ServletHolder(new HBaseSpanViewerTracesServlet(conf)),
                    "/gettraces");
    root.addServlet(new ServletHolder(new HBaseSpanViewerSpansServlet(conf)),
                    "/getspans/*");

    server.start();
    server.join();
    return 0;
  }

  /**
   * @throws IOException
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(HBaseConfiguration.create(), new HBaseSpanViewerServer(), args);
  }
}
