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
package org.apache.htrace.impl;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.MilliSpan;
import org.apache.htrace.core.TracerId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDroppedSpans {
  private static final Log LOG = LogFactory.getLog(TestDroppedSpans.class);

  private static Path tempDir;

  @BeforeClass
  public static void beforeClass() throws IOException {
    // Allow setting really small buffer sizes for testing purposes.
    // We do not allow setting such small sizes in production.
    Conf.BUFFER_SIZE_MIN = 0;

    // Create a temporary directory to hold the dropped spans logs.
    String tmp = System.getProperty("java.io.tmpdir", "/tmp");
    File dir = new File(tmp,
        "TestDroppedSpans." + UUID.randomUUID().toString());
    Files.createDirectory(dir.toPath());
    tempDir = dir.toPath();
  }

  @BeforeClass
  public static void afterClass() throws IOException {
    if (tempDir != null) {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(tempDir)) {
        for (final Iterator<Path> it = stream.iterator(); it.hasNext();) {
          Files.delete(it.next());
        }
      }
      Files.delete(tempDir);
      tempDir = null;
    }
  }

  /**
   * Test that we can disable the dropped spans log.
   */
  @Test(timeout = 60000)
  public void testDisableDroppedSpansLog() throws Exception {
    HTraceConfiguration conf = HTraceConfiguration.fromMap(
        new HashMap<String, String>() {{
          put(Conf.ADDRESS_KEY, "127.0.0.1:8080");
          put(TracerId.TRACER_ID_KEY, "testAppendToDroppedSpansLog");
          put(Conf.DROPPED_SPANS_LOG_PATH_KEY, "/");
          put(Conf.DROPPED_SPANS_LOG_MAX_SIZE_KEY, "0");
        }});
    HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf);
    try {
      rcvr.appendToDroppedSpansLog("this won't get written");
    } finally {
      rcvr.close();
    }
  }

  /**
   * Test that we can write to the dropped spans log.
   */
  @Test(timeout = 60000)
  public void testWriteToDroppedSpansLog() throws Exception {
    final String logPath = new File(
        tempDir.toFile(), "testWriteToDroppedSpansLog").getAbsolutePath();
    HTraceConfiguration conf = HTraceConfiguration.fromMap(
        new HashMap<String, String>() {{
          put(Conf.ADDRESS_KEY, "127.0.0.1:8080");
          put(TracerId.TRACER_ID_KEY, "testWriteToDroppedSpansLog");
          put(Conf.DROPPED_SPANS_LOG_PATH_KEY, logPath);
          put(Conf.DROPPED_SPANS_LOG_MAX_SIZE_KEY, "128");
        }});
    HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf);
    try {
      final String LINE1 = "This is a test of the dropped spans log.";
      rcvr.appendToDroppedSpansLog(LINE1 + "\n");
      final String LINE2 = "These lines should appear in the log.";
      rcvr.appendToDroppedSpansLog(LINE2 + "\n");
      try {
        rcvr.appendToDroppedSpansLog("This line won't be written because we're " +
            "out of space.");
        Assert.fail("expected append to fail because of lack of space");
      } catch (IOException e) {
        // ignore
      }
      List<String> lines =
          Files.readAllLines(Paths.get(logPath), StandardCharsets.UTF_8);
      Assert.assertEquals(2, lines.size());
      Assert.assertEquals(LINE1, lines.get(0).substring(25));
      Assert.assertEquals(LINE2, lines.get(1).substring(25));
    } finally {
      rcvr.close();
    }
  }

  /**
   * Test that we write to the dropped spans log when htraced is unreachable.
   */
  @Test(timeout = 60000)
  public void testSpansDroppedBecauseOfUnreachableHTraced() throws Exception {
    final String logPath = new File(tempDir.toFile(),
        "testSpansDroppedBecauseOfUnreachableHTraced").getAbsolutePath();
    // Open a local socket.  We know that nobody is listening on this socket, so
    // all attempts to send to it will fail.
    final ServerSocket serverSocket = new ServerSocket(0);
    HTracedSpanReceiver rcvr = null;
    try {
      HTraceConfiguration conf = HTraceConfiguration.fromMap(
          new HashMap<String, String>() {{
            put(Conf.ADDRESS_KEY, "127.0.0.1:" + serverSocket.getLocalPort());
            put(TracerId.TRACER_ID_KEY,
                "testSpansDroppedBecauseOfUnreachableHTraced");
            put(Conf.DROPPED_SPANS_LOG_PATH_KEY, logPath);
            put(Conf.DROPPED_SPANS_LOG_MAX_SIZE_KEY, "78");
            put(Conf.CONNECT_TIMEOUT_MS_KEY, "1");
            put(Conf.IO_TIMEOUT_MS_KEY, "1");
            put(Conf.FLUSH_RETRY_DELAYS_KEY, "1,1");
          }});
      rcvr = new HTracedSpanReceiver(conf);
      rcvr.receiveSpan(new MilliSpan.Builder().
          begin(123).end(456).description("FooBar").build());
      HTracedSpanReceiver tmpRcvr = rcvr;
      rcvr = null;
      tmpRcvr.close();
      List<String> lines =
          Files.readAllLines(Paths.get(logPath), StandardCharsets.UTF_8);
      Assert.assertTrue(lines.size() >= 1);
      Assert.assertTrue(lines.get(0).contains("Failed to flush "));
    } finally {
      serverSocket.close();
      if (rcvr != null) {
        rcvr.close();
      }
    }
  }
}
