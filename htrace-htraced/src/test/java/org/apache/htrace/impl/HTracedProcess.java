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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Paths;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.MilliSpan;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Assert;

/**
 * To get instance of HTraced up and running, create an instance of this class.
 * Upon successful construction, htraced is running using <code>dataDir</code> as directory to
 * host data (leveldbs and logs).
 */
class HTracedProcess extends Process {
  private static final Log LOG = LogFactory.getLog(HTracedProcess.class);

  static class Builder {
    String host = "localhost";

    Builder() {
    }

    Builder host(String host) {
      this.host = host;
      return this;
    }

    HTracedProcess build() throws Exception {
      return new HTracedProcess(this);
    }
  }

  /**
   * Path to the htraced binary.
   */
  private final File htracedPath;

  /**
   * Temporary directory for test files.
   */
  private final DataDir dataDir;

  /**
   * The Java Process object for htraced.
   */
  private final Process delegate;

  /**
   * The HTTP host:port returned from htraced.
   */
  private final String httpAddr;

  /**
   * The HRPC host:port returned from htraced.
   */
  private final String hrpcAddr;

  /**
   * REST client to use to talk to htraced.
   */
  private final HttpClient httpClient;

  /**
   * Data send back from the HTraced process on the notification port.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class StartupNotificationData {
    /**
     * The hostname:port pair which the HTraced process uses for HTTP requests.
     */
    @JsonProperty("HttpAddr")
    String httpAddr;

    /**
     * The hostname:port pair which the HTraced process uses for HRPC requests.
     */
    @JsonProperty("HrpcAddr")
    String hrpcAddr;

    /**
     * The process ID of the HTraced process.
     */
    @JsonProperty("ProcessId")
    long processId;
  }

  private HTracedProcess(Builder builder) throws Exception {
    this.htracedPath = Paths.get(
        "target", "..", "go", "build", "htraced").toFile();
    if (!this.htracedPath.exists()) {
      throw new RuntimeException("No htraced binary exists at " +
          this.htracedPath);
    }
    this.dataDir = new DataDir();
    // Create a notifier socket bound to a random port.
    ServerSocket listener = new ServerSocket(0);
    boolean success = false;
    Process process = null;
    HttpClient http = null;
    try {
      // Use a random port for the web address.  No 'scheme' yet.
      String random = builder.host + ":0";
      String logPath = new File(dataDir.get(), "log.txt").getAbsolutePath();
      // Pass cmdline args to htraced to it uses our test dir for data.
      ProcessBuilder pb = new ProcessBuilder(htracedPath.getAbsolutePath(),
        "-Dlog.level=TRACE",
        "-Dlog.path=" + logPath,
        "-Dweb.address=" + random,
        "-Dhrpc.address=" + random,
        "-Ddata.store.clear=true",
        "-Dstartup.notification.address=localhost:" + listener.getLocalPort(),
        "-Ddata.store.directories=" + dataDir.get().getAbsolutePath());
      pb.redirectErrorStream(true);
      // Inherit STDERR/STDOUT i/o; dumps on console for now.  Can add logs later.
      pb.inheritIO();
      pb.directory(dataDir.get());
      //assert pb.redirectInput() == Redirect.PIPE;
      //assert pb.redirectOutput().file() == dataDir;
      process = pb.start();
      assert process.getInputStream().read() == -1;
      StartupNotificationData data = readStartupNotification(listener);
      httpAddr = data.httpAddr;
      hrpcAddr = data.hrpcAddr;
      LOG.info("Started htraced process " + data.processId + " with http " +
               "address " + data.httpAddr + ", logging to " + logPath);
      http = RestBufferManager.createHttpClient(60000L, 60000L);
      http.start();
      success = true;
    } finally {
      if (!success) {
        // Clean up after failure
        if (process != null) {
          process.destroy();
          process = null;
        }
        if (http != null) {
          http.stop();
        }
      }
      delegate = process;
      listener.close();
      httpClient = http;
    }
  }

  private static StartupNotificationData
      readStartupNotification(ServerSocket listener) throws IOException {
    Socket socket = listener.accept();
    try {
      InputStream in = socket.getInputStream();
      ObjectMapper objectMapper = new ObjectMapper();
      StartupNotificationData data = objectMapper.
          readValue(in, StartupNotificationData.class);
      return data;
    } finally {
      socket.close();
    }
  }

  public int hashCode() {
    return delegate.hashCode();
  }

  public OutputStream getOutputStream() {
    throw new UnsupportedOperationException("Unsupported until complaint; output on STDOUT");
  }

  public InputStream getInputStream() {
    throw new UnsupportedOperationException("Unsupported until complaint; output on STDOUT");
  }

  public boolean equals(Object obj) {
    return delegate.equals(obj);
  }

  public InputStream getErrorStream() {
    throw new UnsupportedOperationException("Unsupported until complaint; output on STDOUT");
  }

  public int waitFor() throws InterruptedException {
    return delegate.waitFor();
  }

  public int exitValue() {
    return delegate.exitValue();
  }

  public void destroy() {
    try {
      httpClient.stop();
    } catch (Exception e) {
      LOG.error("Error stopping httpClient", e);
    }
    delegate.destroy();
    try {
      dataDir.close();
    } catch (Exception e) {
      LOG.error("Error closing " + dataDir, e);
    }
    LOG.trace("Destroyed htraced process.");
  }

  public String toString() {
    return delegate.toString();
  }

  public String getHttpAddr() {
    return httpAddr;
  }

  public String getHrpcAddr() {
    return hrpcAddr;
  }

  /**
   * Ugly but how else to do file-math?
   * @param topLevel Presumes top-level of the htrace checkout.
   * @return Path to the htraced binary.
   */
  public static File getPathToHTraceBinaryFromTopLevel(final File topLevel) {
    return new File(new File(new File(new File(topLevel, "htrace-htraced"), "go"), "build"),
      "htraced");
  }

  public String getServerVersionJson() throws Exception {
    ContentResponse response = httpClient.GET(
        new URI(String.format("http://%s/server/version", httpAddr)));
    Assert.assertEquals("application/json", response.getMediaType());
    Assert.assertEquals(HttpStatus.OK_200, response.getStatus());
    return response.getContentAsString();
  }

  public Span getSpan(SpanId spanId) throws Exception {
    ContentResponse response = httpClient.GET(
        new URI(String.format("http://%s/span/%s",
            httpAddr, spanId.toString())));
    Assert.assertEquals("application/json", response.getMediaType());
    String responseJson = response.getContentAsString().trim();
    if (responseJson.isEmpty()) {
      return null;
    }
    return MilliSpan.fromJson(responseJson);
  }
}
