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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.htrace.Span;
import org.apache.htrace.TimelineAnnotation;
import org.apache.htrace.Tracer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A Span implementation that stores its information in milliseconds since the
 * epoch.
 */
@JsonDeserialize(using = MilliSpan.MilliSpanDeserializer.class)
public class MilliSpan implements Span {

  private static Random rand = new Random();
  private static ObjectWriter JSON_WRITER = new ObjectMapper().writer();
  private static final long EMPTY_PARENT_ARRAY[] = new long[0];

  private long begin;
  private long end;
  private final String description;
  private final long traceId;
  private final long parents[];
  private final long spanId;
  private Map<String, String> traceInfo = null;
  private final String processId;
  private List<TimelineAnnotation> timeline = null;

  @Override
  public Span child(String description) {
    return new MilliSpan(description, traceId, spanId, rand.nextLong(), processId);
  }

  /**
   * The public interface for constructing a MilliSpan.
   */
  public static class Builder {
    private long begin;
    private long end;
    private String description;
    private long traceId;
    private long parents[] = EMPTY_PARENT_ARRAY;
    private long spanId;
    private Map<String, String> traceInfo = null;
    private String processId;
    private List<TimelineAnnotation> timeline = null;

    public Builder() {
    }

    public Builder begin(long begin) {
      this.begin = begin;
      return this;
    }

    public Builder end(long end) {
      this.end = end;
      return this;
    }

    public Builder description(String description) {
      this.description = description;
      return this;
    }

    public Builder traceId(long traceId) {
      this.traceId = traceId;
      return this;
    }

    public Builder parents(long parents[]) {
      this.parents = parents;
      return this;
    }

    public Builder parents(List<Long> parentList) {
      long[] parents = new long[parentList.size()];
      for (int i = 0; i < parentList.size(); i++) {
        parents[i] = parentList.get(i).longValue();
      }
      this.parents = parents;
      return this;
    }

    public Builder spanId(long spanId) {
      this.spanId = spanId;
      return this;
    }

    public Builder traceInfo(Map<String, String> traceInfo) {
      this.traceInfo = traceInfo.isEmpty() ? null : traceInfo;
      return this;
    }

    public Builder processId(String processId) {
      this.processId = processId;
      return this;
    }

    public Builder timeline(List<TimelineAnnotation> timeline) {
      this.timeline = timeline.isEmpty() ? null : timeline;
      return this;
    }

    public MilliSpan build() {
      return new MilliSpan(this);
    }
  }

  private MilliSpan(Builder builder) {
    this.begin = builder.begin;
    this.end = builder.end;
    this.description = builder.description;
    this.traceId = builder.traceId;
    this.parents = builder.parents;
    this.spanId = builder.spanId;
    this.traceInfo = builder.traceInfo;
    this.processId = builder.processId;
    this.timeline = builder.timeline;
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
    this.begin = System.currentTimeMillis();
    this.end = 0;
    this.processId = processId;
  }

  @Override
  public synchronized void stop() {
    if (end == 0) {
      if (begin == 0)
        throw new IllegalStateException("Span for " + description
            + " has not been started");
      end = System.currentTimeMillis();
      Tracer.getInstance().deliver(this);
    }
  }

  protected long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  @Override
  public synchronized boolean isRunning() {
    return begin != 0 && end == 0;
  }

  @Override
  public synchronized long getAccumulatedMillis() {
    if (begin == 0)
      return 0;
    if (end > 0)
      return end - begin;
    return currentTimeMillis() - begin;
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
    return begin;
  }

  @Override
  public long getStopTimeMillis() {
    return end;
  }

  @Override
  public void addKVAnnotation(byte[] key, byte[] value)  {
    // TODO: remove this method
    try {
      addKVAnnotation(new String(key, "UTF-8"), new String(value, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addKVAnnotation(String key, String value) {
    if (traceInfo == null)
      traceInfo = new HashMap<String, String>();
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
  public Map<String, String> getKVAnnotations() {
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
    StringWriter writer = new StringWriter();
    try {
      JSON_WRITER.writeValue(writer, this);
    } catch (IOException e) {
      // An IOException should not be possible when writing to a string.
      throw new RuntimeException(e);
    }
    return writer.toString();
  }

  private static long parseUnsignedHexLong(String s) {
    return new BigInteger(s, 16).longValue();
  }

  public static class MilliSpanDeserializer
        extends JsonDeserializer<MilliSpan> {
    @Override
    public MilliSpan deserialize(JsonParser jp, DeserializationContext ctxt)
          throws IOException, JsonProcessingException {
      JsonNode root = jp.getCodec().readTree(jp);
      Builder builder = new Builder();
      builder.begin(root.get("b").asLong());
      builder.end(root.get("e").asLong());
      builder.description(root.get("d").asText());
      builder.traceId(parseUnsignedHexLong(root.get("i").asText()));
      builder.spanId(parseUnsignedHexLong(root.get("s").asText()));
      builder.processId(root.get("r").asText());
      JsonNode parentsNode = root.get("p");
      LinkedList<Long> parents = new LinkedList<Long>();
      for (Iterator<JsonNode> iter = parentsNode.elements();
           iter.hasNext(); ) {
        JsonNode parentIdNode = iter.next();
        parents.add(parseUnsignedHexLong(parentIdNode.asText()));
      }
      builder.parents(parents);
      JsonNode traceInfoNode = root.get("n");
      if (traceInfoNode != null) {
        HashMap<String, String> traceInfo = new HashMap<String, String>();
        for (Iterator<String> iter = traceInfoNode.fieldNames();
             iter.hasNext(); ) {
          String field = iter.next();
          traceInfo.put(field, traceInfoNode.get(field).asText());
        }
        builder.traceInfo(traceInfo);
      }
      JsonNode timelineNode = root.get("t");
      if (timelineNode != null) {
        LinkedList<TimelineAnnotation> timeline =
            new LinkedList<TimelineAnnotation>();
        for (Iterator<JsonNode> iter = timelineNode.elements();
             iter.hasNext(); ) {
          JsonNode ann = iter.next();
          timeline.add(new TimelineAnnotation(ann.get("t").asLong(),
              ann.get("m").asText()));
        }
        builder.timeline(timeline);
      }
      return builder.build();
    }
  }
}
