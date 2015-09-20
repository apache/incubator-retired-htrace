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
package org.apache.htrace.core;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

public class TestTracerId {
  private void testTracerIdImpl(String expected, String fmt) {
    assertEquals(expected, new TracerId(
        HTraceConfiguration.fromKeyValuePairs(TracerId.TRACER_ID_KEY, fmt),
        "TracerName").get());
  }

  @Test
  public void testSimpleTracerIds() {
    testTracerIdImpl("abc", "abc");
    testTracerIdImpl("abc", "a\\bc");
    testTracerIdImpl("abc", "ab\\c");
    testTracerIdImpl("abc", "\\a\\b\\c");
    testTracerIdImpl("a\\bc", "a\\\\bc");
  }

  @Test
  public void testSubstitutionVariables() throws IOException {
    testTracerIdImpl("myTracerName", "my%{tname}");
    testTracerIdImpl(TracerId.getProcessName(), "%{pname}");
    testTracerIdImpl("my." + TracerId.getProcessName(), "my.%{pname}");
    testTracerIdImpl(TracerId.getBestIpString() + ".str", "%{ip}.str");
    testTracerIdImpl("%{pname}", "\\%{pname}");
    testTracerIdImpl("%cash%money{}", "%cash%money{}");
    testTracerIdImpl("Foo." + Long.valueOf(TracerId.getOsPid()).toString(),
        "Foo.%{pid}");
  }
}
