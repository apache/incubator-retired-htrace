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

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class TestTimeUtil {
  /**
   * Test that our deltaMs function can compute the time difference between any
   * two monotonic times in milliseconds.
   */
  @Test(timeout = 60000)
  public void testDeltaMs() throws Exception {
    Assert.assertEquals(0, TimeUtil.deltaMs(0, 0));
    Assert.assertEquals(1, TimeUtil.deltaMs(0, 1));
    Assert.assertEquals(0, TimeUtil.deltaMs(1, 0));
    Assert.assertEquals(10, TimeUtil.deltaMs(1000, 1010));
    long minMs = TimeUnit.MILLISECONDS.convert(Long.MIN_VALUE,
        TimeUnit.NANOSECONDS);
    long maxMs = TimeUnit.MILLISECONDS.convert(Long.MAX_VALUE,
        TimeUnit.NANOSECONDS);
    Assert.assertEquals(10, TimeUtil.deltaMs(minMs, minMs + 10));
    Assert.assertEquals(maxMs, TimeUtil.deltaMs(minMs, maxMs));
    Assert.assertEquals(11, TimeUtil.deltaMs(maxMs - 10, minMs));
  }
}
