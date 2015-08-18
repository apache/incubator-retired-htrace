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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

/**
 * Used to create the graph formed by spans.
 */
public class TraceGraph {
  private static final Log LOG = LogFactory.getLog(Tracer.class);


  public static class SpansByParent {
    /**
     * Compare two spans by span ID.
     */
    private static Comparator<Span> COMPARATOR =
        new Comparator<Span>() {
          @Override
          public int compare(Span a, Span b) {
            return a.getSpanId().compareTo(b.getSpanId());
          }
        };

    private final TreeSet<Span> treeSet;

    private final HashMap<SpanId, LinkedList<Span>> parentToSpans;

    SpansByParent(Collection<Span> spans) {
      TreeSet<Span> treeSet = new TreeSet<Span>(COMPARATOR);
      parentToSpans = new HashMap<SpanId, LinkedList<Span>>();
      for (Span span : spans) {
        treeSet.add(span);
        for (SpanId parent : span.getParents()) {
          LinkedList<Span> list = parentToSpans.get(parent);
          if (list == null) {
            list = new LinkedList<Span>();
            parentToSpans.put(parent, list);
          }
          list.add(span);
        }
        if (span.getParents().length == 0) {
          LinkedList<Span> list = parentToSpans.get(SpanId.INVALID);
          if (list == null) {
            list = new LinkedList<Span>();
            parentToSpans.put(SpanId.INVALID, list);
          }
          list.add(span);
        }
      }
      this.treeSet = treeSet;
    }

    public List<Span> find(SpanId parentId) {
      LinkedList<Span> spans = parentToSpans.get(parentId);
      if (spans == null) {
        return new LinkedList<Span>();
      }
      return spans;
    }

    public Iterator<Span> iterator() {
      return Collections.unmodifiableSortedSet(treeSet).iterator();
    }
  }

  public static class SpansByTracerId {
    /**
     * Compare two spans by process ID, and then by span ID.
     */
    private static Comparator<Span> COMPARATOR =
        new Comparator<Span>() {
          @Override
          public int compare(Span a, Span b) {
            int cmp = a.getTracerId().compareTo(b.getTracerId());
            if (cmp != 0) {
              return cmp;
            }
            return a.getSpanId().compareTo(b.getSpanId());
          }
        };

    private final TreeSet<Span> treeSet;

    SpansByTracerId(Collection<Span> spans) {
      TreeSet<Span> treeSet = new TreeSet<Span>(COMPARATOR);
      for (Span span : spans) {
        treeSet.add(span);
      }
      this.treeSet = treeSet;
    }

    public List<Span> find(String tracerId) {
      List<Span> spans = new ArrayList<Span>();
      Span span = new MilliSpan.Builder().
                    spanId(SpanId.INVALID).
                    tracerId(tracerId).
                    build();
      while (true) {
        span = treeSet.higher(span);
        if (span == null) {
          break;
        }
        if (span.getTracerId().equals(tracerId)) {
          break;
        }
        spans.add(span);
      }
      return spans;
    }

    public Iterator<Span> iterator() {
      return Collections.unmodifiableSortedSet(treeSet).iterator();
    }
  }

  private final SpansByParent spansByParent;
  private final SpansByTracerId spansByTracerId;

  /**
   * Create a new TraceGraph
   *
   * @param spans The collection of spans to use to create this TraceGraph. Should
   *              have at least one root span.
   */
  public TraceGraph(Collection<Span> spans) {
    this.spansByParent = new SpansByParent(spans);
    this.spansByTracerId = new SpansByTracerId(spans);
  }

  public SpansByParent getSpansByParent() {
    return spansByParent;
  }

  public SpansByTracerId getSpansByTracerId() {
    return spansByTracerId;
  }

  @Override
  public String toString() {
    StringBuilder bld = new StringBuilder();
    String prefix = "";
    for (Iterator<Span> iter = spansByParent.iterator(); iter.hasNext();) {
      Span span = iter.next();
      bld.append(prefix).append(span.toString());
      prefix = "\n";
    }
    return bld.toString();
  }
}
