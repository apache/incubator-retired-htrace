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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.htrace.Span;
import org.apache.htrace.TimelineAnnotation;
import org.apache.htrace.Tracer;

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
import java.util.concurrent.ThreadLocalRandom;

/**
 * A Span implementation that stores its information in milliseconds since the
 * epoch.
 */
@JsonDeserialize(using = MilliSpan.MilliSpanDeserializer.class)
public class MilliSpan implements Span {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static ObjectReader JSON_READER = OBJECT_MAPPER.reader(MilliSpan.class);
  private static ObjectWriter JSON_WRITER = OBJECT_MAPPER.writer();
  private static final long EMPTY_PARENT_ARRAY[] = new long[0];
  private static final String EMPTY_STRING = "";

  private long begin;
  private long end;
  private final String description;
  private final long traceId;
  private long parents[];
  private final long spanId;
  private Map<String, String> traceInfo = null;
  private String processId;
  private List<TimelineAnnotation> timeline = null;

  private static long nonZeroRandom64() {
    long id;
    Random random = ThreadLocalRandom.current();
    do {
      id = random.nextLong();
    } while (id == 0);
    return id;
  }

  @Override
  public Span child(String childDescription) {
    return new MilliSpan.Builder().
      begin(System.currentTimeMillis()).
      end(0).
      description(childDescription).
      traceId(traceId).
      parents(new long[] {spanId}).
      spanId(nonZeroRandom64()).
      processId(processId).
      build();
  }

  /**
   * The public interface for constructing a MilliSpan.
   */
  public static class Builder {
    private long begin;
    private long end;
    private String description = EMPTY_STRING;
    private long traceId;
    private long parents[] = EMPTY_PARENT_ARRAY;
    private long spanId;
    private Map<String, String> traceInfo = null;
    private String processId = EMPTY_STRING;
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

  public MilliSpan() {
    this.begin = 0;
    this.end = 0;
    this.description = EMPTY_STRING;
    this.traceId = 0;
    this.parents = EMPTY_PARENT_ARRAY;
    this.spanId = 0;
    this.traceInfo = null;
    this.processId = EMPTY_STRING;
    this.timeline = null;
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

  @Override
  public long[] getParents() {
    return parents;
  }

  @Override
  public void setParents(long[] parents) {
    this.parents = parents;
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
  public void setProcessId(String processId) {
    this.processId = processId;
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
      JsonNode bNode = root.get("b");
      if (bNode != null) {
        builder.begin(bNode.asLong());
      }
      JsonNode eNode = root.get("e");
      if (eNode != null) {
        builder.end(eNode.asLong());
      }
      JsonNode dNode = root.get("d");
      if (dNode != null) {
        builder.description(dNode.asText());
      }
      JsonNode iNode = root.get("i");
      if (iNode != null) {
        builder.traceId(parseUnsignedHexLong(iNode.asText()));
      }
      JsonNode sNode = root.get("s");
      if (sNode != null) {
        builder.spanId(parseUnsignedHexLong(sNode.asText()));
      }
      JsonNode rNode = root.get("r");
      if (rNode != null) {
        builder.processId(rNode.asText());
      }
      JsonNode parentsNode = root.get("p");
      LinkedList<Long> parents = new LinkedList<Long>();
      if (parentsNode != null) {
        for (Iterator<JsonNode> iter = parentsNode.elements();
             iter.hasNext(); ) {
          JsonNode parentIdNode = iter.next();
          parents.add(parseUnsignedHexLong(parentIdNode.asText()));
        }
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

  static MilliSpan fromJson(String json) throws IOException {
    return JSON_READER.readValue(json);
  }
}
