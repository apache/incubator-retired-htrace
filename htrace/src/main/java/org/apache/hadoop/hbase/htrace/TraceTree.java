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

public class TraceTree {
  private Map<Long, Collection<SpanStruct>> _pc; // parent->children map
  private Collection<SpanStruct> _spans;
  private Map<String, Collection<SpanStruct>> _processIdMap;

  public Collection<SpanStruct> getSpans() {
    return _spans;
  }

  public TraceTree(Collection<SpanStruct> spans) {
    _spans = spans;
    _pc = new HashMap<Long, Collection<SpanStruct>>();
    _processIdMap = new HashMap<String, Collection<SpanStruct>>();

    for (SpanStruct s : _spans) {
      if (s.getProcessId() != null) {
        if (!_processIdMap.containsKey(s.getProcessId())) {
          _processIdMap.put(s.getProcessId(), new HashSet<SpanStruct>());
        }
        _processIdMap.get(s.getProcessId()).add(s);
      }

      if (!_pc.containsKey(s.spanId())) {
        _pc.put(s.spanId(), new HashSet<SpanStruct>());
      }

      if (!_pc.containsKey(s.parentId())) {
        _pc.put(s.parentId(), new HashSet<SpanStruct>());
      }

      _pc.get(s.parentId()).add(s);
    }
  }

  public SpanStruct getRoot() {
    return _pc.get(0L).iterator().next();
  }

  public Map<Long, Collection<SpanStruct>> getPc() {
    return _pc;
  }

  public Map<String, Collection<SpanStruct>> getProcessIdMap() {
    return _processIdMap;
  }
}