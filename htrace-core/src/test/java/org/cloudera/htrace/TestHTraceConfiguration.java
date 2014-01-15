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

package org.cloudera.htrace;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestHTraceConfiguration {
  @Test
  public void testGetBoolean() throws Exception {

    Map<String, String> m = new HashMap<String, String>();
    m.put("testTrue", " True");
    m.put("testFalse", "falsE ");
    HTraceConfiguration configuration = HTraceConfiguration.fromMap(m);

    // Tests for value being there
    assertTrue(configuration.getBoolean("testTrue", false));
    assertFalse(configuration.getBoolean("testFalse", true));

    // Test for absent
    assertTrue(configuration.getBoolean("absent", true));
    assertFalse(configuration.getBoolean("absent", false));
  }

  @Test
  public void testGetInt() throws Exception {
    Map<String, String> m = new HashMap<String, String>();
    m.put("a", "100");
    m.put("b", "0");
    m.put("c", "-100");
    m.put("d", "5");

    HTraceConfiguration configuration = HTraceConfiguration.fromMap(m);
    assertEquals(100, configuration.getInt("a", -999));
    assertEquals(0, configuration.getInt("b", -999));
    assertEquals(-100, configuration.getInt("c", -999));
    assertEquals(5, configuration.getInt("d", -999));
    assertEquals(-999, configuration.getInt("absent", -999));
  }
}
