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
package org.apache.hadoop.hbase.htrace;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TraceTree {
  private Map<Long, Collection<SpanStruct>> pc; // parent->children map
  private Collection<SpanStruct> spans;
  private Map<String, Collection<SpanStruct>> processIdMap;

  public Collection<SpanStruct> getSpans() {
    return spans;
  }

  public TraceTree(Collection<SpanStruct> spans) {
    this.spans = spans;
    this.pc = new HashMap<Long, Collection<SpanStruct>>();
    this.processIdMap = new HashMap<String, Collection<SpanStruct>>();

    for (SpanStruct s : spans) {
      if (!s.getProcessId().equals("")) {
        if (!processIdMap.containsKey(s.getProcessId())) {
          processIdMap.put(s.getProcessId(), new HashSet<SpanStruct>());
        }
        processIdMap.get(s.getProcessId()).add(s);
      }

      if (!pc.containsKey(s.spanId())) {
        pc.put(s.spanId(), new HashSet<SpanStruct>());
      }

      if (!pc.containsKey(s.parentId())) {
        pc.put(s.parentId(), new HashSet<SpanStruct>());
      }

      pc.get(s.parentId()).add(s);
    }
  }

  public SpanStruct getRoot() {
    return pc.get(0L).iterator().next();
  }

  public Map<Long, Collection<SpanStruct>> getPc() {
    return pc;
  }

  public Map<String, Collection<SpanStruct>> getProcessIdMap() {
    return processIdMap;
  }
}