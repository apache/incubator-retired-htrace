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

import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestProcessId {
  private void testProcessIdImpl(String expected, String fmt) {
    assertEquals(expected, new ProcessId(fmt).get());
  }

  @Test
  public void testSimpleProcessIds() {
    testProcessIdImpl("abc", "abc");
    testProcessIdImpl("abc", "a\\bc");
    testProcessIdImpl("abc", "ab\\c");
    testProcessIdImpl("abc", "\\a\\b\\c");
    testProcessIdImpl("a\\bc", "a\\\\bc");
  }

  @Test
  public void testSubstitutionVariables() throws IOException {
    testProcessIdImpl(ProcessId.getProcessName(), "${pname}");
    testProcessIdImpl("my." + ProcessId.getProcessName(), "my.${pname}");
    testProcessIdImpl(ProcessId.getBestIpString() + ".str", "${ip}.str");
    testProcessIdImpl("${pname}", "\\${pname}");
    testProcessIdImpl("$cash$money{}", "$cash$money{}");
    testProcessIdImpl("Foo." + Long.valueOf(ProcessId.getOsPid()).toString(),
        "Foo.${pid}");
  }
}
