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

import org.apache.htrace.Span;
import org.apache.htrace.TimelineAnnotation;
import org.apache.htrace.Tracer;
import org.mortbay.util.ajax.JSON;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A Span implementation that stores its information in milliseconds since the
 * epoch.
 */
public class MilliSpan implements Span {

  private static Random rand = new Random();

  private long start;
  private long stop;
  private final String description;
  private final long traceId;
  private final long parents[];
  private final long spanId;
  private Map<byte[], byte[]> traceInfo = null;
  private final String processId;
  private List<TimelineAnnotation> timeline = null;

  @Override
  public Span child(String description) {
    return new MilliSpan(description, traceId, spanId, rand.nextLong(), processId);
  }

  public MilliSpan(String description, long traceId, long parentSpanId, long spanId, String processId) {
    this.description = description;
    this.traceId = traceId;
    if (parentSpanId == Span.ROOT_SPAN_ID) {
      this.parents = new long[0];
    } else {
      this.parents = new long[] { parentSpanId };
    } 
    this.spanId = spanId;
    this.start = System.currentTimeMillis();
    this.stop = 0;
    this.processId = processId;
  }

  @Override
  public synchronized void stop() {
    if (stop == 0) {
      if (start == 0)
        throw new IllegalStateException("Span for " + description
            + " has not been started");
      stop = System.currentTimeMillis();
      Tracer.getInstance().deliver(this);
    }
  }

  protected long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  @Override
  public synchronized boolean isRunning() {
    return start != 0 && stop == 0;
  }

  @Override
  public synchronized long getAccumulatedMillis() {
    if (start == 0)
      return 0;
    if (stop > 0)
      return stop - start;
    return currentTimeMillis() - start;
  }

  @Override
  public String toString() {
    return toJson();
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public long getSpanId() {
    return spanId;
  }

  // TODO: Fix API callers to deal with multiple parents, and get rid of
  // Span.ROOT_SPAN_ID.
  @Override
  public long getParentId() {
    if (parents.length == 0) {
      return Span.ROOT_SPAN_ID;
    }
    return parents[0];
  }

  @Override
  public long getTraceId() {
    return traceId;
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
  public void addKVAnnotation(byte[] key, byte[] value) {
    if (traceInfo == null)
      traceInfo = new HashMap<byte[], byte[]>();
    traceInfo.put(key, value);
  }

  @Override
  public void addTimelineAnnotation(String msg) {
    if (timeline == null) {
      timeline = new ArrayList<TimelineAnnotation>();
    }
    timeline.add(new TimelineAnnotation(System.currentTimeMillis(), msg));
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
  public String getProcessId() {
    return processId;
  }

  @Override
  public String toJson() {
    Map<String, Object> values = new LinkedHashMap<String, Object>();
    values.put("i", String.format("%016x", traceId));
    values.put("s", String.format("%016x", spanId));
    String parentStrs[] = new String[parents.length];
    for (int parentIdx = 0; parentIdx < parents.length; parentIdx++) {
      parentStrs[parentIdx] = String.format("%016x", parents[parentIdx]);
    }
    values.put("p", parentStrs);
    values.put("r", processId);
    values.put("b", Long.toString(start));
    values.put("e", Long.toString(stop));
    values.put("d", description);
    if (timeline != null) {
      values.put("t", timeline);
    }
    if (traceInfo != null){
      values.put("n", traceInfo);
    }
    return JSON.toString(values);
  }
}
