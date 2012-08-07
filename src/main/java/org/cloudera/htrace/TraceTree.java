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
package org.cloudera.htrace;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
/**
 * NOTE: Experimental class, not recommended for use in production.
 */
public class TraceTree {
  private Map<Long, Collection<Span>> pc; // parent->children map
  private Collection<Span> spans;
  private Map<String, Collection<Span>> processIdMap;

  public Collection<Span> getSpans() {
    return spans;
  }

  public TraceTree(Collection<Span> spans) {
    this.spans = spans;
    this.pc = new HashMap<Long, Collection<Span>>();
    this.processIdMap = new HashMap<String, Collection<Span>>();

    for (Span s : spans) {
      if (!s.getProcessId().equals("")) {
        if (!processIdMap.containsKey(s.getProcessId())) {
          processIdMap.put(s.getProcessId(), new HashSet<Span>());
        }
        processIdMap.get(s.getProcessId()).add(s);
      }

      if (!pc.containsKey(s.getSpanId())) {
        pc.put(s.getSpanId(), new HashSet<Span>());
      }

      if (!pc.containsKey(s.getParentId())) {
        pc.put(s.getParentId(), new HashSet<Span>());
      }

      pc.get(s.getParentId()).add(s);
    }
  }

  public Span getRoot() {
    if (pc.get(0L) != null) {
      Iterator<Span> iter = pc.get(0L).iterator();
      if (iter.hasNext()) {
        return iter.next();
      }
    } 
    throw new IllegalStateException(
        "TraceTree is not correctly formed - there is no root trace in this collection.");
  }

  public Map<Long, Collection<Span>> getPc() {
    return pc;
  }

  public Map<String, Collection<Span>> getProcessIdMap() {
    return processIdMap;
  }
}