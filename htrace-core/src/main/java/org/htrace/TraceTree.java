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
package org.htrace;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;

/**
 * Used to create the graph formed by spans.
 */
public class TraceTree {
  public static final Log LOG = LogFactory.getLog(Tracer.class);
  private Multimap<Long, Span> spansByParentID;
  private Collection<Span> spans;
  private Multimap<String, Span> spansByPid;

  /**
   * Create a new TraceTree
   *
   * @param spans The collection of spans to use to create this TraceTree. Should
   *              have at least one root span (span with parentId =
   *              Span.ROOT_SPAN_ID
   */
  public TraceTree(Collection<Span> spans) {
    this.spans = ImmutableList.copyOf(spans);
    this.spansByParentID = HashMultimap.<Long, Span>create();
    this.spansByPid = HashMultimap.<String, Span>create();

    for (Span s : this.spans) {
      if (s.getProcessId() != null) {
        spansByPid.put(s.getProcessId(), s);
      } else {
        LOG.warn("Encountered span with null processId. This should not happen. Span: "
            + s);
      }
      spansByParentID.put(s.getParentId(), s);
    }
  }

  /**
   * @return The collection of spans given to this TraceTree at construction.
   */
  public Collection<Span> getSpans() {
    return spans;
  }

  /**
   * @return A copy of the MultiMap from parent span ID -> children of span with
   *         that ID.
   */
  public Multimap<Long, Span> getSpansByParentIdMap() {
    return HashMultimap.<Long, Span>create(spansByParentID);
  }

  /**
   * @return A collection of the root spans (spans with parent ID =
   *         Span.ROOT_SPAN_ID) in this tree.
   */
  public Collection<Span> getRoots() {
    Collection<Span> roots = spansByParentID.get(Span.ROOT_SPAN_ID);
    if (roots != null) {
      return roots;
    }
    throw new IllegalStateException(
        "TraceTree is not correctly formed - there are no root spans in the collection provided at construction.");
  }

  /**
   * @return A copy of the Multimap from String process ID -> spans with that
   *         process ID. If process ID was not set in Trace.java, all spans will
   *         have empty string process IDs.
   */
  public Multimap<String, Span> getSpansByPidMap() {
    return HashMultimap.<String, Span>create(spansByPid);
  }
}