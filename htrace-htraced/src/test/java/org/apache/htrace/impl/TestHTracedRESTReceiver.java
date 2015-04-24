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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Span;
import org.apache.htrace.util.DataDir;
import org.apache.htrace.util.HTracedProcess;
import org.apache.htrace.util.TestUtil;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHTracedRESTReceiver {
  private static final Log LOG =
      LogFactory.getLog(TestHTracedRESTReceiver.class);
  private URL restServerUrl;
  private DataDir dataDir;
  HTracedProcess htraced;

  @Before
  public void setUp() throws Exception {
    this.dataDir = new DataDir();
    File tlDir = DataDir.getTopLevelOfCheckout(this.dataDir.getDataDir());
    File pathToHTracedBinary = HTracedProcess.getPathToHTraceBinaryFromTopLevel(tlDir);
    this.htraced = new HTracedProcess(pathToHTracedBinary,
        dataDir.getDataDir(), "localhost");
    this.restServerUrl = new URL("http://" + htraced.getHttpAddr() + "/");
  }

  @After
  public void tearDown() throws Exception {
    if (this.htraced != null) this.htraced.destroy();
  }

  /**
   * Our simple version of htrace configuration for testing.
   */
  private final class TestHTraceConfiguration extends HTraceConfiguration {
    private final URL restServerUrl;
    final static String PROCESS_ID = "TestHTracedRESTReceiver";

    public TestHTraceConfiguration(final URL restServerUrl) {
      this.restServerUrl = restServerUrl;
    }

    @Override
    public String get(String key) {
      return null;
    }

    @Override
    public String get(String key, String defaultValue) {
      if (key.equals(HTracedRESTReceiver.HTRACED_REST_URL_KEY)) {
        return this.restServerUrl.toString();
      } else if (key.equals(ProcessId.PROCESS_ID_KEY)) {
        return PROCESS_ID;
      }
      return defaultValue;
    }
  }

  /**
   * Make sure the REST server basically works.
   * @throws Exception
   */
  @Test (timeout = 10000)
  public void testBasicGet() throws Exception {
    HTracedRESTReceiver receiver =
      new HTracedRESTReceiver(new TestHTraceConfiguration(this.restServerUrl));
    HttpClient http = receiver.createHttpClient(60000L, 60000L);
    http.start();
    try {
      // Do basic a GET /server/info against htraced
      ContentResponse response =
        http.GET(restServerUrl + "server/info");
      assertEquals("application/json", response.getMediaType());
      String content = processGET(response);
      assertTrue(content.contains("ReleaseVersion"));
      System.out.println(content);
    } finally {
      http.stop();
      receiver.close();
    }
  }

  private String processGET(final ContentResponse response) {
    assertTrue("" + response.getStatus(), HttpStatus.OK_200 <= response.getStatus() &&
      response.getStatus() <= HttpStatus.NO_CONTENT_204);
    return response.getContentAsString();
  }

  private void testSendingSpansImpl(boolean testClose) throws Exception {
    final HTracedRESTReceiver receiver =
      new HTracedRESTReceiver(new TestHTraceConfiguration(this.restServerUrl));
    final int NUM_SPANS = 3;
    final HttpClient http = receiver.createHttpClient(60000, 60000);
    http.start();
    Span spans[] = new Span[NUM_SPANS];
    for (int i = 0; i < NUM_SPANS; i++) {
      MilliSpan.Builder builder = new MilliSpan.Builder().
          parents(new long[]{1L}).
          spanId(i);
      if (i == NUM_SPANS - 1) {
        builder.processId("specialPid");
      }
      spans[i] = builder.build();
    }
    try {
      for (int i = 0; i < NUM_SPANS; i++) {
        LOG.info("receiving " + spans[i].toString());
        receiver.receiveSpan(spans[i]);
      }
      if (testClose) {
        receiver.close();
      } else {
        receiver.startFlushing();
      }
      TestUtil.waitFor(new TestUtil.Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            for (int i = 0; i < NUM_SPANS; i++) {
              // This is what the REST server expects when querying for a
              // span id.
              String findSpan = String.format("span/%016x", i);
              ContentResponse response =
                  http.GET(restServerUrl + findSpan);
              String content = processGET(response);
              if ((content == null) || (content.length() == 0)) {
                LOG.info("Failed to find span " + i);
                return false;
              }
              LOG.info("Got " + content + " for span " + i);
              MilliSpan dspan = MilliSpan.fromJson(content);
              assertEquals((long)i, dspan.getSpanId());
              // Every span should have the process ID we set in the
              // configuration... except for the last span, which had
              // a custom value set.
              if (i == NUM_SPANS - 1) {
                assertEquals("specialPid", dspan.getProcessId());
              } else {
                assertEquals(TestHTraceConfiguration.PROCESS_ID,
                    dspan.getProcessId());
              }
            }
            return true;
          } catch (Throwable t) {
            LOG.error("Got exception", t);
            return false;
          }
        }
      }, 10, 20000);
    } finally {
      http.stop();
      if (!testClose) {
        receiver.close();
      }
    }
  }

  /**
   * Send 100 spans then confirm they made it in.
   * @throws Exception
   */
  @Test (timeout = 60000)
  public void testSendingSpans() throws Exception {
    testSendingSpansImpl(false);
  }

  /**
   * Test that the REST receiver blocks during shutdown until all spans are sent
   * (or a long timeout elapses).  Otherwise, short-lived client processes will
   * never have a chance to send all their spans and we will have incomplete
   * information.
   */
  @Test (timeout = 60000)
  public void testShutdownBlocksUntilSpanAreSent() throws Exception {
    testSendingSpansImpl(true);
  }
}
