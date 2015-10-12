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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.MilliSpan;
import org.apache.htrace.core.TimelineAnnotation;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.core.buffer.MessageBufferOutput;

import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;

/**
 * A ByteBuffer which we are writing msgpack data to.
 */
class PackedBuffer {
  /**
   * A MessageBufferOutput that simply outputs to a ByteBuffer.
   */
  private class PackedBufferOutput implements MessageBufferOutput {
    private MessageBuffer savedBuffer;

    PackedBufferOutput() {
    }

    @Override
    public MessageBuffer next(int bufferSize) throws IOException {
      if (savedBuffer == null || savedBuffer.size() != bufferSize) {
        savedBuffer = MessageBuffer.newBuffer(bufferSize);
      }
      MessageBuffer buffer = savedBuffer;
      savedBuffer = null;
      return buffer;
    }

    @Override
    public void flush(MessageBuffer buffer) throws IOException {
      ByteBuffer b = buffer.toByteBuffer();
      bb.put(b);
      savedBuffer = buffer;
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

  private static final Log LOG = LogFactory.getLog(PackedBuffer.class);
  private static final Charset UTF8 = StandardCharsets.UTF_8;
  private static final byte SPANS[] = "Spans".getBytes(UTF8);
  private static final byte DEFAULT_PID[] = "DefaultPid".getBytes(UTF8);
  private static final byte A[] = "a".getBytes(UTF8);
  private static final byte B[] = "b".getBytes(UTF8);
  private static final byte E[] = "e".getBytes(UTF8);
  private static final byte D[] = "d".getBytes(UTF8);
  private static final byte R[] = "r".getBytes(UTF8);
  private static final byte P[] = "p".getBytes(UTF8);
  private static final byte N[] = "n".getBytes(UTF8);
  private static final byte T[] = "t".getBytes(UTF8);
  private static final byte M[] = "m".getBytes(UTF8);
  private static final int HRPC_MAGIC = 0x43525448;
  static final int HRPC_REQ_FRAME_LENGTH = 20;
  static final int HRPC_RESP_FRAME_LENGTH = 20;
  static final int MAX_HRPC_ERROR_LENGTH = 4 * 1024 * 1024;
  static final int MAX_HRPC_BODY_LENGTH = 64 * 1024 * 1024;
  private static final int SPAN_ID_BYTE_LENGTH = 16;
  static final MessagePack.Config MSGPACK_CONF =
      new MessagePack.ConfigBuilder()
        .readBinaryAsString(false)
        .readStringAsBinary(false)
        .build();
  /**
   * The array which we are filling.
   */
  final ByteBuffer bb;

  /**
   * Used to tell the MessagePacker to output to our array.
   */
  final PackedBufferOutput out;

  /**
   * A temporary buffer for serializing span ids and other things.
   */
  final byte[] temp;

  /**
   * Generates msgpack output.
   */
  final MessagePacker packer;

  /**
   * Create a new PackedBuffer.
   *
   * @param bb        The ByteBuffer to use to create the packed buffer.
   */
  PackedBuffer(ByteBuffer bb) {
    this.bb = bb;
    this.out = new PackedBufferOutput();
    this.temp = new byte[SPAN_ID_BYTE_LENGTH];
    this.packer = new MessagePacker(out, MSGPACK_CONF);
  }

  /**
   * Write the fixed-length request frame which starts packed RPC messages.
   */
  static void writeReqFrame(ByteBuffer bb, int methodId, long seq, int length)
      throws IOException {
    int oldPos = bb.position();
    boolean success = false;
    try {
      bb.order(ByteOrder.LITTLE_ENDIAN);
      bb.putInt(HRPC_MAGIC);
      bb.putInt(methodId);
      bb.putLong(seq);
      bb.putInt(length);
      success = true;
    } finally {
      if (!success) {
        bb.position(oldPos);
      }
    }
  }

  /**
   * Write an 8-byte value to a byte array as little-endian.
   */
  private static void longToBigEndian(byte b[], int pos, long val) {
    b[pos + 0] =(byte) ((val >> 56) & 0xff);
    b[pos + 1] =(byte) ((val >> 48) & 0xff);
    b[pos + 2] =(byte) ((val >> 40) & 0xff);
    b[pos + 3] =(byte) ((val >> 32) & 0xff);
    b[pos + 4] =(byte) ((val >> 24) & 0xff);
    b[pos + 5] =(byte) ((val >> 16) & 0xff);
    b[pos + 6] =(byte) ((val >>  8) & 0xff);
    b[pos + 7] =(byte) ((val >>  0) & 0xff);
  }

  private void writeSpanId(SpanId spanId) throws IOException {
    longToBigEndian(temp, 0, spanId.getHigh());
    longToBigEndian(temp, 8, spanId.getLow());
    packer.packBinaryHeader(SPAN_ID_BYTE_LENGTH);
    packer.writePayload(temp, 0, SPAN_ID_BYTE_LENGTH);
  }

  /**
   * Serialize a span to the given OutputStream.
   */
  void writeSpan(Span span) throws IOException {
    boolean success = false;
    int oldPos = bb.position();
    try {
      int mapSize = 0;
      if (span.getSpanId().isValid()) {
        mapSize++;
      }
      if (span.getStartTimeMillis() != 0) {
        mapSize++;
      }
      if (span.getStopTimeMillis() != 0) {
        mapSize++;
      }
      if (!span.getDescription().isEmpty()) {
        mapSize++;
      }
      if (!span.getTracerId().isEmpty()) {
        mapSize++;
      }
      if (span.getParents().length > 0) {
        mapSize++;
      }
      if (!span.getKVAnnotations().isEmpty()) {
        mapSize++;
      }
      if (!span.getTimelineAnnotations().isEmpty()) {
        mapSize++;
      }
      packer.packMapHeader(mapSize);
      if (span.getSpanId().isValid()) {
        packer.packRawStringHeader(1);
        packer.writePayload(A);
        writeSpanId(span.getSpanId());
      }
      if (span.getStartTimeMillis() != 0) {
        packer.packRawStringHeader(1);
        packer.writePayload(B);
        packer.packLong(span.getStartTimeMillis());
      }
      if (span.getStopTimeMillis() != 0) {
        packer.packRawStringHeader(1);
        packer.writePayload(E);
        packer.packLong(span.getStopTimeMillis());
      }
      if (!span.getDescription().isEmpty()) {
        packer.packRawStringHeader(1);
        packer.writePayload(D);
        packer.packString(span.getDescription());
      }
      if (!span.getTracerId().isEmpty()) {
        packer.packRawStringHeader(1);
        packer.writePayload(R);
        packer.packString(span.getTracerId());
      }
      if (span.getParents().length > 0) {
        packer.packRawStringHeader(1);
        packer.writePayload(P);
        packer.packArrayHeader(span.getParents().length);
        for (int i = 0; i < span.getParents().length; i++) {
          writeSpanId(span.getParents()[i]);
        }
      }
      if (!span.getKVAnnotations().isEmpty()) {
        packer.packRawStringHeader(1);
        packer.writePayload(N);
        Map<String, String> map = span.getKVAnnotations();
        packer.packMapHeader(map.size());
        for (Map.Entry<String, String> entry : map.entrySet()) {
          packer.packString(entry.getKey());
          packer.packString(entry.getValue());
        }
      }
      if (!span.getTimelineAnnotations().isEmpty()) {
        packer.packRawStringHeader(1);
        packer.writePayload(T);
        List<TimelineAnnotation> list = span.getTimelineAnnotations();
        packer.packArrayHeader(list.size());
        for (TimelineAnnotation annotation : list) {
          packer.packMapHeader(2);
          packer.packRawStringHeader(1);
          packer.writePayload(T);
          packer.packLong(annotation.getTime());
          packer.packRawStringHeader(1);
          packer.writePayload(M);
          packer.packString(annotation.getMessage());
        }
      }
      packer.flush();
      success = true;
    } finally {
      if (!success) {
        // If we failed earlier, restore the old position.
        // This is so that if we run out of space, we don't add a
        // partial span to the buffer.
        bb.position(oldPos);
      }
    }
  }

  static SpanId readSpanId(MessageUnpacker unpacker) throws IOException {
    int alen = unpacker.unpackBinaryHeader();
    if (alen != SPAN_ID_BYTE_LENGTH) {
      throw new IOException("Invalid length given for spanID array.  " +
          "Expected " + SPAN_ID_BYTE_LENGTH + "; got " + alen);
    }
    byte[] payload = new byte[SPAN_ID_BYTE_LENGTH];
    unpacker.readPayload(payload);
    return new SpanId(
        ((payload[ 7] & 0xffL) <<  0) |
        ((payload[ 6] & 0xffL) <<  8) |
        ((payload[ 5] & 0xffL) << 16) |
        ((payload[ 4] & 0xffL) << 24) |
        ((payload[ 3] & 0xffL) << 32) |
        ((payload[ 2] & 0xffL) << 40) |
        ((payload[ 1] & 0xffL) << 48) |
        ((payload[ 0] & 0xffL) << 56),
        ((payload[15] & 0xffL) <<  0) |
        ((payload[14] & 0xffL) <<  8) |
        ((payload[13] & 0xffL) << 16) |
        ((payload[12] & 0xffL) << 24) |
        ((payload[11] & 0xffL) << 32) |
        ((payload[10] & 0xffL) << 40) |
        ((payload[ 9] & 0xffL) << 48) |
        ((payload[ 8] & 0xffL) << 56)
      );
  }

  /**
   * Read a span.  Used in unit tests.  Not optimized.
   */
  static Span readSpan(MessageUnpacker unpacker) throws IOException {
    int numEntries = unpacker.unpackMapHeader();
    MilliSpan.Builder builder = new MilliSpan.Builder();
    while (--numEntries >= 0) {
      String key = unpacker.unpackString();
      if (key.length() != 1) {
        throw new IOException("Unknown key " + key);
      }
      switch (key.charAt(0)) {
        case 'a':
          builder.spanId(readSpanId(unpacker));
          break;
        case 'b':
          builder.begin(unpacker.unpackLong());
          break;
        case 'e':
          builder.end(unpacker.unpackLong());
          break;
        case 'd':
          builder.description(unpacker.unpackString());
          break;
        case 'r':
          builder.tracerId(unpacker.unpackString());
          break;
        case 'p':
          int numParents = unpacker.unpackArrayHeader();
          SpanId[] parents = new SpanId[numParents];
          for (int i = 0; i < numParents; i++) {
            parents[i] = readSpanId(unpacker);
          }
          builder.parents(parents);
          break;
        case 'n':
          int mapEntries = unpacker.unpackMapHeader();
          HashMap<String, String> entries =
              new HashMap<String, String>(mapEntries);
          for (int i = 0; i < mapEntries; i++) {
            String k = unpacker.unpackString();
            String v = unpacker.unpackString();
            entries.put(k, v);
          }
          builder.traceInfo(entries);
          break;
        case 't':
          int listEntries = unpacker.unpackArrayHeader();
          ArrayList<TimelineAnnotation> list =
              new ArrayList<TimelineAnnotation>(listEntries);
          for (int i = 0; i < listEntries; i++) {
            int timelineObjectSize = unpacker.unpackMapHeader();
            long time = 0;
            String msg = "";
            for (int j = 0; j < timelineObjectSize; j++) {
              String tlKey = unpacker.unpackString();
              if (tlKey.length() != 1) {
                throw new IOException("Unknown timeline map key " + tlKey);
              }
              switch (tlKey.charAt(0)) {
                case 't':
                  time = unpacker.unpackLong();
                  break;
                case 'm':
                  msg = unpacker.unpackString();
                  break;
                default:
                  throw new IOException("Unknown timeline map key " + tlKey);
              }
            }
            list.add(new TimelineAnnotation(time, msg));
          }
          builder.timeline(list);
          break;
        default:
          throw new IOException("Unknown key " + key);
      }
    }
    return builder.build();
  }

  void beginWriteSpansRequest(String defaultPid, int numSpans)
      throws IOException {
    boolean success = false;
    int oldPos = bb.position();
    try {
      int mapSize = 1;
      if (defaultPid != null) {
        mapSize++;
      }
      packer.packMapHeader(mapSize);
      if (defaultPid != null) {
        packer.packRawStringHeader(DEFAULT_PID.length);
        packer.writePayload(DEFAULT_PID);
        packer.packString(defaultPid);
      }
      packer.packRawStringHeader(SPANS.length);
      packer.writePayload(SPANS);
      packer.packArrayHeader(numSpans);
      packer.flush();
      success = true;
    } finally {
      if (!success) {
        bb.position(oldPos);
      }
    }
  }

  /**
   * Get the underlying ByteBuffer.
   */
  ByteBuffer getBuffer() {
    return bb;
  }

  /**
   * Reset our position in the array.
   */
  void reset() throws IOException {
    packer.reset(out);
  }

  void close() {
    try {
      packer.close();
    } catch (IOException e) {
      LOG.error("Error closing MessagePacker", e);
    }
  }

  public String toHexString() {
    String prefix = "";
    StringBuilder bld = new StringBuilder();
    ByteBuffer b = bb.duplicate();
    b.flip();
    while (b.hasRemaining()) {
      bld.append(String.format("%s%02x", prefix, b.get()));
      prefix = " ";
    }
    return bld.toString();
  }
}
