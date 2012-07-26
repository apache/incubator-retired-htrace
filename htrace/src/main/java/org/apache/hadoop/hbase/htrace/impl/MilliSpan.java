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
package org.apache.hadoop.hbase.htrace.impl;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.htrace.Span;
import org.apache.hadoop.hbase.htrace.Trace;

/**
 * A Span implementation that stores its information in milliseconds since the
 * epoch.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MilliSpan implements Span {

  private static final Random next = new SecureRandom();
  private long start;
  private long stop;
  final private Span parent;
  final private String description;
  final private long spanId;
  private Map<byte[], byte[]> traceInfo = null;
  private String processId;

  public Span child(String description) {
    return new MilliSpan(description, next.nextLong(), this, processId);
  }

  public MilliSpan(String description, long id, Span parent, String processId) {
    this.description = description;
    this.spanId = id;
    this.parent = parent;
    this.start = 0;
    this.stop = 0;
    this.processId = processId;
  }

  public synchronized void start() {
    if (start > 0)
      throw new IllegalStateException("Span for " + description
          + " has already been started");
    start = System.currentTimeMillis();
  }

  public synchronized void stop() {
    if (start == 0)
      throw new IllegalStateException("Span for " + description
          + " has not been started");
    stop = System.currentTimeMillis();
    Trace.pop(this);
  }

  protected long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  public synchronized boolean isRunning() {
    return start != 0 && stop == 0;
  }

  public synchronized long getAccumulatedMillis() {
    if (start == 0)
      return 0;
    if (stop > 0)
      return stop - start;
    return currentTimeMillis() - start;
  }

  public String toString() {
    long parentId = getParentId();
    return ("\"" + getDescription() + "\" trace:" + Long.toHexString(getTraceId())
        + " span:" + spanId + (parentId > 0 ? " parent:" + parentId : "")
        + " start:" + start + " ms: " + Long.toString(getAccumulatedMillis()) + (isRunning() ? "..."
          : ""));

  }

  public String getDescription() {
    return description;
  }

  @Override
  public long getSpanId() {
    return spanId;
  }

  @Override
  public Span getParent() {
    return parent;
  }

  @Override
  public long getParentId() {
    if (parent == null)
      return -1;
    return parent.getSpanId();
  }

  @Override
  public long getTraceId() {
    return parent.getTraceId();
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
  public void addAnnotation(byte[] key, byte[] value) {
    if (traceInfo == null)
      traceInfo = new HashMap<byte[], byte[]>();
    traceInfo.put(key, value);
  }

  @Override
  public Map<byte[], byte[]> getAnnotations() {
    if (traceInfo == null)
      return Collections.emptyMap();
    return Collections.unmodifiableMap(traceInfo);
  }

  @Override
  public String getProcessId() {
    return processId;
  }
}