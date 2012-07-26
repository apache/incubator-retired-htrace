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
package org.apache.hadoop.hbase.htrace;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.htrace.impl.CountSampler;
import org.junit.Test;

public class TestCountSampler {
  
  @Test
  public void testNext() {
    CountSampler half = new CountSampler(2);
    CountSampler hundred = new CountSampler(100);
    int halfCount = 0;
    int hundredCount = 0;
    for (int i = 0; i < 200; i++) {
      if (half.next(null))
        halfCount++;
      if (hundred.next(null))
        hundredCount++;
    }
    assertEquals(2, hundredCount);
    assertEquals(100, halfCount);
  }
}
