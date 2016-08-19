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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.HTraceConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.net.InetSocketAddress;
import java.net.URI;

public class KuduSpanViewerServer {

  private static final Log LOG = LogFactory.getLog(KuduSpanViewerServer.class);
  public static final String HTRACE_VIEWER_HTTP_ADDRESS_DEFAULT = "0.0.0.0:17000";
  private Server server;

  public void stop() throws Exception {
    if (server != null) {
      server.stop();
    }
    LOG.info("Embedded jetty server stopped successfully.");
  }

  public int run(HTraceConfiguration conf) throws Exception {
    URI uri = new URI("http://" + HTRACE_VIEWER_HTTP_ADDRESS_DEFAULT);
    InetSocketAddress addr = new InetSocketAddress(uri.getHost(), uri.getPort());
    server = new Server(addr);
    ServletContextHandler root =
            new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
    server.setHandler(root);
    root.addServlet(new ServletHolder(new DefaultServlet()),
            "/");
    root.addServlet(new ServletHolder(new KuduSpanViewerTracesServlet(conf)),
            "/gettraces");
    root.addServlet(new ServletHolder(new KuduSpanViewerSpansServlet(conf)),
            "/getspans/*");

    server.start();
    server.join();
    return 0;
  }

}
