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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.HTraceConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestHTracedReceiverConf {
  private static final Log LOG =
      LogFactory.getLog(TestHTracedReceiverConf.class);

  @Test(timeout = 60000)
  public void testParseHostPort() throws Exception {
    InetSocketAddress addr = new Conf(
        HTraceConfiguration.fromKeyValuePairs(
          Conf.ADDRESS_KEY, "example.com:8080")).endpoint;
    Assert.assertEquals("example.com", addr.getHostName());
    Assert.assertEquals(8080, addr.getPort());

    addr = new Conf(
        HTraceConfiguration.fromKeyValuePairs(
          Conf.ADDRESS_KEY, "127.0.0.1:8081")).endpoint;
    Assert.assertEquals("127.0.0.1", addr.getHostName());
    Assert.assertEquals(8081, addr.getPort());

    addr = new Conf(
        HTraceConfiguration.fromKeyValuePairs(
          Conf.ADDRESS_KEY, "[ff02:0:0:0:0:0:0:12]:9096")).endpoint;
    Assert.assertEquals("ff02:0:0:0:0:0:0:12", addr.getHostName());
    Assert.assertEquals(9096, addr.getPort());
  }

  private static void verifyFail(String hostPort) {
    try {
      new Conf(HTraceConfiguration.fromKeyValuePairs(
            Conf.ADDRESS_KEY, hostPort));
      Assert.fail("Expected bad host:port configuration " + hostPort +
          " to fail, but it succeeded.");
    } catch (IOException e) {
      // expected
    }
  }

  @Test(timeout = 60000)
  public void testFailToParseHostPort() throws Exception {
    verifyFail("localhost"); // no port
    verifyFail("127.0.0.1"); // no port
    verifyFail(":8080"); // no hostname
    verifyFail("bob[ff02:0:0:0:0:0:0:12]:9096"); // bracket at incorrect place
  }

  @Test(timeout = 60000)
  public void testGetIntArray() throws Exception {
    int[] arr = Conf.getIntArray("");
    Assert.assertEquals(0, arr.length);
    arr = Conf.getIntArray("123");
    Assert.assertEquals(1, arr.length);
    Assert.assertEquals(123, arr[0]);
    arr = Conf.getIntArray("1,2,3");
    Assert.assertEquals(3, arr.length);
    Assert.assertEquals(1, arr[0]);
    Assert.assertEquals(2, arr[1]);
    Assert.assertEquals(3, arr[2]);
    arr = Conf.getIntArray(",-4,5,66,");
    Assert.assertEquals(3, arr.length);
    Assert.assertEquals(-4, arr[0]);
    Assert.assertEquals(5, arr[1]);
    Assert.assertEquals(66, arr[2]);
  }
}
