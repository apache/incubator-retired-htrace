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

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.Span;
import org.apache.htrace.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

public class TestPackedBuffer {
  private static final Log LOG = LogFactory.getLog(TestPackedBuffer.class);

  @Test(timeout = 60000)
  public void testWriteReqFrame() throws Exception {
    byte[] arr = new byte[PackedBuffer.HRPC_REQ_FRAME_LENGTH];
    ByteBuffer bb = ByteBuffer.wrap(arr);
    PackedBuffer buf = new PackedBuffer(bb);
    PackedBuffer.writeReqFrame(bb, 1, 123, 456);
    Assert.assertEquals(PackedBuffer.HRPC_REQ_FRAME_LENGTH, bb.position());
    Assert.assertEquals("48 54 52 43 " +
        "01 00 00 00 " +
        "7b 00 00 00 00 00 00 00 " +
        "c8 01 00 00",
        buf.toHexString());
  }

  @Test(timeout = 60000)
  public void testPackSpans() throws Exception {
    Random rand = new Random(123);
    byte[] arr = new byte[16384];
    ByteBuffer bb = ByteBuffer.wrap(arr);
    bb.limit(bb.capacity());
    PackedBuffer buf = new PackedBuffer(bb);
    final int NUM_TEST_SPANS = 5;
    Span[] spans = new Span[NUM_TEST_SPANS];
    for (int i = 0; i < NUM_TEST_SPANS; i++) {
      spans[i] = TestUtil.randomSpan(rand);
    }
    for (int i = 0; i < NUM_TEST_SPANS; i++) {
      buf.writeSpan(spans[i]);
    }
    LOG.info("wrote " + buf.toHexString());
    MessagePack msgpack = new MessagePack(PackedBuffer.MSGPACK_CONF);
    MessageUnpacker unpacker = msgpack.newUnpacker(arr, 0, bb.position());
    Span[] respans = new Span[NUM_TEST_SPANS];
    for (int i = 0; i < NUM_TEST_SPANS; i++) {
      respans[i] = PackedBuffer.readSpan(unpacker);
    }
    for (int i = 0; i < NUM_TEST_SPANS; i++) {
      Assert.assertEquals("Failed to read back span " + i,
          spans[i].toJson(), respans[i].toJson());
    }
  }
}
