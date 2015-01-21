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

import static org.junit.Assert.*;

import java.io.File;
import java.net.URL;

import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Span;
import org.apache.htrace.util.DataDir;
import org.apache.htrace.util.HTracedProcess;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TestHTracedRESTReceiver {
  private URL restServerUrl;;
  private DataDir dataDir;
  HTracedProcess htraced;
  private final ObjectMapper jsonMapper = new ObjectMapper();

  @Before
  public void setUp() throws Exception {
    this.dataDir = new DataDir();
    this.restServerUrl = new URL("http://localhost:9097/");
    File tlDir = DataDir.getTopLevelOfCheckout(this.dataDir.getDataDir());
    File pathToHTracedBinary = HTracedProcess.getPathToHTraceBinaryFromTopLevel(tlDir);
    this.htraced =
      new HTracedProcess(pathToHTracedBinary, dataDir.getDataDir(), restServerUrl);
  }

  @After
  public void tearDown() throws Exception {
    if (this.htraced != null) this.htraced.destroy();
  }

  /**
   * Our simple version of htrace configuration for testing.
   */
  private final class TestHTraceConfiguration extends HTraceConfiguration {
    @Override
    public String get(String key) {
      return null;
    }

    @Override
    public String get(String key, String defaultValue) {
      return defaultValue;
    }
  }

  @Test (timeout = 10000)
  public void testBasicGet() throws Exception {
    HTracedRESTReceiver receiver = new HTracedRESTReceiver(new TestHTraceConfiguration());
    try {
      // Do basic a GET /server/info against localhost:9095 htraced
      ContentResponse response = receiver.httpClient.GET(restServerUrl + "server/info");
      assertEquals("application/json", response.getMediaType());
      String content = processGET(response);
      assertTrue(content.contains("ReleaseVersion"));
      System.out.println(content);
    } finally {
      receiver.close();
    }
  }

  private String processGET(final ContentResponse response) {
    assertTrue("" + response.getStatus(), HttpStatus.OK_200 <= response.getStatus() &&
      response.getStatus() <= HttpStatus.NO_CONTENT_204);
    return response.getContentAsString();
  }

  @Test (timeout = 10000)
  public void testSendingSpans() throws Exception {
    HTracedRESTReceiver receiver = new HTracedRESTReceiver(new TestHTraceConfiguration());
    try {
      // TODO: Fix MilliSpan. Requires a parentid.  Shouldn't have to have one else be explicit it
      // is required.
      for (int i = 0; i < 100; i++) {
        Span span = new MilliSpan.Builder().parents(new long [] {1L}).spanId(i).build();
        receiver.receiveSpan(span);
      }
      // Read them all back.
      for (int i = 0; i < 100; i++) {
        // This is what the REST server expends when querying for a span id.
        String findSpan = String.format("span/%016x", 10);
        ContentResponse response = receiver.httpClient.GET(restServerUrl + findSpan);
        String content = processGET(response);
        Span span = this.jsonMapper.readValue(content, Span.class);
        assertEquals(i, span.getSpanId());
      }
    } finally {
      receiver.close();
    }
  }
}