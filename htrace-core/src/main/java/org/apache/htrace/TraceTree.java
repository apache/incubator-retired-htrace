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

import org.apache.htrace.impl.MilliSpan;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * Used to create the graph formed by spans.
 */
public class TraceTree {
  public static final Log LOG = LogFactory.getLog(Tracer.class);

  public static class SpansByParent {
    private static Comparator<Span> COMPARATOR =
        new Comparator<Span>() {
          @Override
          public int compare(Span a, Span b) {
            if (a.getParentId() < b.getParentId()) {
              return -1;
            } else if (a.getParentId() > b.getParentId()) {
              return 1;
            } else if (a.getSpanId() < b.getSpanId()) {
              return -1;
            } else if (a.getSpanId() > b.getSpanId()) {
              return 1;
            } else {
              return 0;
            }
          }
        };

    private final TreeSet<Span> treeSet;

    SpansByParent(Collection<Span> spans) {
      TreeSet<Span> treeSet = new TreeSet<Span>(COMPARATOR);
      for (Span span : spans) {
        treeSet.add(span);
      }
      this.treeSet = treeSet;
    }

    public List<Span> find(long parentId) {
      List<Span> spans = new ArrayList<Span>();
      Span span = new MilliSpan("", Long.MIN_VALUE,
          parentId, Long.MIN_VALUE, "");
      while (true) {
        span = treeSet.higher(span);
        if (span == null) {
          break;
        }
        if (span.getParentId() != parentId) {
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

  public static class SpansByProcessId {
    private static Comparator<Span> COMPARATOR =
        new Comparator<Span>() {
          @Override
          public int compare(Span a, Span b) {
            int cmp = a.getProcessId().compareTo(b.getProcessId());
            if (cmp != 0) {
              return cmp;
            } else if (a.getSpanId() < b.getSpanId()) {
              return -1;
            } else if (a.getSpanId() > b.getSpanId()) {
              return 1;
            } else {
              return 0;
            }
          }
        };

    private final TreeSet<Span> treeSet;

    SpansByProcessId(Collection<Span> spans) {
      TreeSet<Span> treeSet = new TreeSet<Span>(COMPARATOR);
      for (Span span : spans) {
        treeSet.add(span);
      }
      this.treeSet = treeSet;
    }

    public List<Span> find(String processId) {
      List<Span> spans = new ArrayList<Span>();
      Span span = new MilliSpan("", Long.MIN_VALUE,
          Long.MIN_VALUE, Long.MIN_VALUE, processId);
      while (true) {
        span = treeSet.higher(span);
        if (span == null) {
          break;
        }
        if (span.getProcessId().equals(processId)) {
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
  private final SpansByProcessId spansByProcessId;

  /**
   * Create a new TraceTree
   *
   * @param spans The collection of spans to use to create this TraceTree. Should
   *              have at least one root span (span with parentId =
   *              Span.ROOT_SPAN_ID
   */
  public TraceTree(Collection<Span> spans) {
    this.spansByParent = new SpansByParent(spans);
    this.spansByProcessId = new SpansByProcessId(spans);
  }

  public SpansByParent getSpansByParent() {
    return spansByParent;
  }

  public SpansByProcessId getSpansByProcessId() {
    return spansByProcessId;
  }
}
