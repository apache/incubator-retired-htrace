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
package org.apache.htrace.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * To get instance of HTraced up and running, create an instance of this class.
 * Upon successful construction, htraced is running using <code>dataDir</code> as directory to
 * host data (leveldbs and logs).
 * TODO: We expect to find the htraced in a very particular place. Fragile. Will break if stuff
 * moves.
 */
public class HTracedProcess extends Process {
  private static final Log LOG = LogFactory.getLog(HTracedProcess.class);
  private final Process delegate;

  private final String httpAddr;

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
     * The process ID of the HTraced process.
     */
    @JsonProperty("ProcessId")
    long processId;
  }

  public HTracedProcess(final File binPath, final File dataDir,
                        final String host) throws IOException {
    // Create a notifier socket bound to a random port.
    ServerSocket listener = new ServerSocket(0);
    boolean success = false;
    Process process = null;
    try {
      // Use a random port for the web address.  No 'scheme' yet.
      String webAddress = host + ":0";
      String logPath = new File(dataDir, "log.txt").getAbsolutePath();
      // Pass cmdline args to htraced to it uses our test dir for data.
      ProcessBuilder pb = new ProcessBuilder(binPath.toString(),
        "-Dlog.level=TRACE",
        "-Dlog.path=" + logPath,
        "-Dweb.address=" + webAddress,
        "-Ddata.store.clear=true",
        "-Dstartup.notification.address=localhost:" + listener.getLocalPort(),
        "-Ddata.store.directories=" + dataDir.toString());
      pb.redirectErrorStream(true);
      // Inherit STDERR/STDOUT i/o; dumps on console for now.  Can add logs later.
      pb.inheritIO();
      pb.directory(dataDir);
      //assert pb.redirectInput() == Redirect.PIPE;
      //assert pb.redirectOutput().file() == dataDir;
      process = pb.start();
      assert process.getInputStream().read() == -1;
      StartupNotificationData data = readStartupNotification(listener);
      httpAddr = data.httpAddr;
      LOG.info("Started htraced process " + data.processId + " with http " +
               "address " + data.httpAddr + ", logging to " + logPath);
      success = true;
    } finally {
      if (!success) {
        // Clean up after failure
        if (process != null) {
          process.destroy();
          process = null;
        }
      }
      delegate = process;
      listener.close();
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
    delegate.destroy();
  }

  public String toString() {
    return delegate.toString();
  }

  public String getHttpAddr() {
    return httpAddr;
  }

  /**
   * Ugly but how else to do file-math?
   * @param topLevel Presumes top-level of the htrace checkout.
   * @return Path to the htraced binary.
   */
  public static File getPathToHTraceBinaryFromTopLevel(final File topLevel) {
    return new File(new File(new File(new File(new File(topLevel, "htrace-core"), "src"), "go"),
      "build"), "htraced");
  }
}
