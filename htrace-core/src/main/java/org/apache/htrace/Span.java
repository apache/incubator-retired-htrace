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
package org.apache.htrace;

import java.util.List;
import java.util.Map;


/**
 * Base interface for gathering and reporting statistics about a block of
 * execution.
 * <p/>
 * Spans form a tree structure with the parent relationship. The first span in a
 * trace has no parent span.
 */
public interface Span {
  public static final long ROOT_SPAN_ID = 0x74ace;

  /**
   * The block has completed, stop the clock
   */
  void stop();

  /**
   * Get the start time, in milliseconds
   */
  long getStartTimeMillis();

  /**
   * Get the stop time, in milliseconds
   */
  long getStopTimeMillis();

  /**
   * Return the total amount of time elapsed since start was called, if running,
   * or difference between stop and start
   */
  long getAccumulatedMillis();

  /**
   * Has the span been started and not yet stopped?
   */
  boolean isRunning();

  /**
   * Return a textual description of this span
   */
  String getDescription();

  /**
   * A pseudo-unique (random) number assigned to this span instance
   */
  long getSpanId();

  /**
   * A pseudo-unique (random) number assigned to the trace associated with this
   * span
   */
  long getTraceId();

  /**
   * Create a child span of this span with the given description
   */
  Span child(String description);

  @Override
  String toString();

  /**
   * Return the pseudo-unique (random) number of the parent span, returns
   * ROOT_SPAN_ID if this is the root span
   */
  long getParentId();

  /**
   * Add a data annotation associated with this span
   */
  void addKVAnnotation(byte[] key, byte[] value);

  /**
   * Add a timeline annotation associated with this span
   */
  void addTimelineAnnotation(String msg);

  /**
   * Get data associated with this span (read only)
   */
  Map<byte[], byte[]> getKVAnnotations();

  /**
   * Get any timeline annotations (read only)
   */
  List<TimelineAnnotation> getTimelineAnnotations();

  /**
   * Return a unique id for the node or process from which this Span originated.
   * IP address is a reasonable choice.
   *
   * @return
   */
  String getProcessId();

  /**
   * Serialize to Json
   */
  String toJson();
}
