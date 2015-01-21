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

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.junit.Before;
import org.junit.Test;

/**
 * Test putting up an htraced and making sure it basically works.
 * Makes presumption about paths; where data is relative to the htraced binary, etc., encoded
 * in methods in the below.
 */
public class TestHTracedProcess {
  private DataDir testDir = null;
  private final int TIMEOUT = 10000;

  @Before
  public void setupTest() {
    this.testDir = new DataDir();
  }

  /*
   * Do a basic GET of the server info from the running htraced instance.
   */
  private String doGet(final URL url) throws IOException {
    URLConnection connection = url.openConnection();
    connection.setConnectTimeout(TIMEOUT);
    connection.setReadTimeout(TIMEOUT);
    connection.connect();
    StringBuffer sb = new StringBuffer();
    BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
    try {
      String line = null;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        sb.append(line);
      }
    } finally {
      reader.close();
    }
    return sb.toString();
  }

  /**
   * Put up an htraced instance and do a Get against /server/info.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=100000)
  public void testStartStopHTraced() throws IOException, InterruptedException {
    // TODO: Make the test port random so no classes if concurrent test runs. Anything better
    // I can do here?  Pass a zero and have the daemon tell me where it is successfully listening?
    String restURL = "http://localhost:9096/";
    URL restServerURL = new URL(restURL);
    HTracedProcess htraced = null;
    File dataDir = this.testDir.getDataDir();
    File topLevel = DataDir.getTopLevelOfCheckout(dataDir);
    try {
      htraced = new HTracedProcess(HTracedProcess.getPathToHTraceBinaryFromTopLevel(topLevel),
        dataDir, restServerURL);
      String str = doGet(new URL(restServerURL + "server/info"));
      // Assert we go something back.
      assertTrue(str.contains("ReleaseVersion"));
      // Assert that the datadir is not empty.
    } finally {
      if (htraced != null) htraced.destroy();
      System.out.println("ExitValue=" + htraced.exitValue());
    }
  }
}