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

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class TestSpanId {
  private void testRoundTrip(SpanId id) throws Exception {
    String str = id.toString();
    SpanId id2 = SpanId.fromString(str);
    Assert.assertEquals(id, id2);
  }

  @Test
  public void testToStringAndFromString() throws Exception {
    testRoundTrip(SpanId.INVALID);
    testRoundTrip(new SpanId(0x1234567812345678L, 0x1234567812345678L));
    testRoundTrip(new SpanId(0xf234567812345678L, 0xf234567812345678L));
    testRoundTrip(new SpanId(0xffffffffffffffffL, 0xffffffffffffffffL));
    Random rand = new Random(12345);
    for (int i = 0; i < 100; i++) {
      testRoundTrip(new SpanId(rand.nextLong(), rand.nextLong()));
    }
  }

  @Test
  public void testValidAndInvalidIds() throws Exception {
    Assert.assertFalse(SpanId.INVALID.isValid());
    Assert.assertTrue(
        new SpanId(0x1234567812345678L, 0x1234567812345678L).isValid());
    Assert.assertTrue(
        new SpanId(0xf234567812345678L, 0xf234567812345678L).isValid());
  }

  private void expectLessThan(SpanId a, SpanId b) throws Exception {
    int cmp = a.compareTo(b);
    Assert.assertTrue("Expected " + a + " to be less than " + b,
        (cmp < 0));
    int cmp2 = b.compareTo(a);
    Assert.assertTrue("Expected " + b + " to be greater than " + a,
        (cmp2 > 0));
  }

  @Test
  public void testIdComparisons() throws Exception {
    expectLessThan(new SpanId(0x0000000000000001L, 0x0000000000000001L),
                   new SpanId(0x0000000000000001L, 0x0000000000000002L));
    expectLessThan(new SpanId(0x0000000000000001L, 0x0000000000000001L),
                   new SpanId(0x0000000000000002L, 0x0000000000000000L));
    expectLessThan(SpanId.INVALID,
                   new SpanId(0xffffffffffffffffL, 0xffffffffffffffffL));
    expectLessThan(new SpanId(0x1234567812345678L, 0x1234567812345678L),
                   new SpanId(0x1234567812345678L, 0xf234567812345678L));
  }
}
