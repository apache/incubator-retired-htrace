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
import org.apache.htrace.core.Span;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class KuduSpanViewerSpansServlet extends HttpServlet {

  private static final Log LOG = LogFactory.getLog(KuduSpanViewerSpansServlet.class);
  public static final String PREFIX = "/getspans";
  private static final ThreadLocal<KuduSpanViewer> kuduSpanViewer =
          new ThreadLocal<KuduSpanViewer>() {
            @Override
            protected KuduSpanViewer initialValue() {
              return null;
            }
          };
  private HTraceConfiguration conf;

  public KuduSpanViewerSpansServlet(HTraceConfiguration conf) {
    this.conf = conf;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void doGet(HttpServletRequest request, HttpServletResponse response)
          throws ServletException, IOException {
    final String path = request.getRequestURI().substring(PREFIX.length());
    if (path == null || path.length() == 0) {
      response.setContentType("text/plain");
      response.getWriter().print("Invalid input");
      return;
    }
    KuduSpanViewer viewer = kuduSpanViewer.get();

    if (viewer == null) {
      viewer = new KuduSpanViewer(conf);
      kuduSpanViewer.set(viewer);
    }
    Long traceid = Long.parseLong(path.substring(1));
    response.setContentType("application/javascript");
    PrintWriter out = response.getWriter();
    out.print("[");
    boolean first = true;
    try {
      for (Span span : viewer.getSpans(traceid)) {
        if (first) {
          first = false;
        } else {
          out.print(",");
        }
        out.print(KuduSpanViewer.toJsonString(span));
      }
    } catch (java.lang.Exception ex) {
      LOG.error("Exception occured while retrieving spans from Kudu Backend.");
    }
    out.print("]");
  }

  @Override
  public void init() throws ServletException {
  }

  @Override
  public void destroy() {
    KuduSpanViewer viewer = kuduSpanViewer.get();
    if (viewer != null) {
      viewer.close();
    }
  }
}
