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
package org.htrace.impl;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.hbase.util.Bytes;
import org.htrace.Span;
import org.htrace.TimelineAnnotation;
import org.htrace.Tracer;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HBaseSpanInfo implements Span {
  private final long traceId;
  private final long parentSpanId;
  private long start;
  private long stop;
  private final long spanId;
  private final String processId;
  private final String description;
  private List<TimelineAnnotation> timeline = null;
  private Map<byte[], byte[]> traceInfo = null;

  public HBaseSpanInfo(long traceId, long parentSpanId, long start, long stop,
                       long spanId, String processId, String description,
                       List<TimelineAnnotation> timeline,
                       Map<byte[], byte[]> traceInfo) {
    this.traceId = traceId;
    this.parentSpanId = parentSpanId;
    this.start = start;
    this.stop = stop;
    this.spanId = spanId;
    this.processId = processId;
    this.description = description;
    this.timeline = timeline;
    this.traceInfo = traceInfo;
  }

  /*
   * Convert span to byte array.
   */
  public static byte[] toBytes(Span span) throws IOException {
    return toBytes(span, 0);
  }

  /*
   * Convert span to byte array.
   */
  public static byte[] toBytes(Span span, int offset) throws IOException {
    final DataOutputBuffer out = toDataOutputBuffer(span);
    return Bytes.copy(out.getData(), offset, out.getLength() - offset);
  }

  /*
   * Convert span to byte array as DataOutputBuffer.
   * byte array retured by DataOutputBuffer#getData()
   * is only valid to DataOutputBuffer#getLength()
   */
  public static DataOutputBuffer toDataOutputBuffer(Span span) throws IOException {
    final DataOutputBuffer out = new DataOutputBuffer(128);
    out.write(Bytes.toBytes(span.getTraceId()));
    out.write(Bytes.toBytes(span.getParentId()));
    out.write(Bytes.toBytes(span.getStartTimeMillis()));
    out.write(Bytes.toBytes(span.getStopTimeMillis()));
    out.write(Bytes.toBytes(span.getSpanId()));
    Bytes.writeByteArray(out, Bytes.toBytes(span.getProcessId()));
    Bytes.writeByteArray(out, Bytes.toBytes(span.getDescription()));
    List<TimelineAnnotation> timeline = span.getTimelineAnnotations();
    int size = timeline.size();
    out.write(Bytes.toBytes(size));
    for (int i = 0; i < size; i++) {
      TimelineAnnotation tl = timeline.get(i);
      out.write(Bytes.toBytes(tl.getTime()));
      Bytes.writeByteArray(out, Bytes.toBytes(tl.getMessage()));
    }
    // todo: support traceInfo
    out.close();
    return out;
  }

  /*
   * Convert byte array created by toBytes() to HBaseSpanInfo instance.
   */
  public static HBaseSpanInfo fromBytes(byte[] bytes) throws IOException {
    return fromBytes(bytes, 0, bytes.length);
  }

  /*
   * Convert byte array created by toBytes() to HBaseSpanInfo instance.
   */
  public static HBaseSpanInfo fromBytes(byte[] bytes, int offset, int length)
      throws IOException {
    DataInputStream in =
      new DataInputStream(new ByteArrayInputStream(bytes, offset, length));
    List<TimelineAnnotation> timeline = new ArrayList<TimelineAnnotation>();
    byte[] buf = new byte[Bytes.SIZEOF_LONG];
    in.readFully(buf);
    long traceId = Bytes.toLong(buf);
    in.readFully(buf);
    long parentSpanId  = Bytes.toLong(buf);
    in.readFully(buf);
    long start = Bytes.toLong(buf);
    in.readFully(buf);
    long stop = Bytes.toLong(buf);
    in.readFully(buf);
    long spanId  = Bytes.toLong(buf);
    String processId = Bytes.toString(Bytes.readByteArray(in));
    String description = Bytes.toString(Bytes.readByteArray(in));
    byte[] size = new byte[Bytes.SIZEOF_INT];
    in.readFully(size);
    byte[] msg = null;
    for (int i = Bytes.toInt(size); i > 0; i--) {
      in.readFully(buf);
      msg = Bytes.readByteArray(in);
      timeline.add(new TimelineAnnotation(Bytes.toLong(buf),
                                          Bytes.toString(msg)));
    }
    return new HBaseSpanInfo(traceId, parentSpanId, start, stop,
                             spanId, processId, description,
                             timeline,
                             null);
  }

  @Override
  public long getTraceId() {
    return traceId;
  }

  @Override
  public long getParentId() {
    return parentSpanId;
  }

  @Override
  public long getStartTimeMillis() {
    return start;
  }

  @Override
  public long getStopTimeMillis() {
    return stop;
  }

  @Override
  public long getSpanId() {
    return spanId;
  }

  @Override
  public String getProcessId() {
    return processId;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String toString() {
    return String.format("Span{Id:0x%16x,parentId:0x%16x,desc:%s,pid:%s}",
                         spanId, parentSpanId, description, processId);
  }

  @Override
  public Map<byte[], byte[]> getKVAnnotations() {
    if (traceInfo == null)
      return Collections.emptyMap();
    return Collections.unmodifiableMap(traceInfo);
  }

  @Override
  public List<TimelineAnnotation> getTimelineAnnotations() {
    if (timeline == null) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(timeline);
  }

  @Override
  public void addKVAnnotation(byte[] key, byte[] value) {
  }

  @Override
  public void addTimelineAnnotation(String msg) {
  }

  @Override
  public synchronized void stop() {}

  @Override
  public synchronized boolean isRunning() {
    return false;
  }
  
  @Override
  public synchronized long getAccumulatedMillis() {
    if (start == 0)
      return 0;
    if (stop > 0)
      return stop - start;
    return System.currentTimeMillis() - start;
  }

  @Override
  public Span child(String description) {
    return null;
  }
}
