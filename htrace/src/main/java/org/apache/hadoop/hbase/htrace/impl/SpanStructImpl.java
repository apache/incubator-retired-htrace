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
package org.apache.hadoop.hbase.htrace.impl;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.htrace.SpanStruct;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SpanStructImpl implements SpanStruct {
  private long _spanId;
  private long _traceId;
  private long _parentId;
  private String _description;
  private Map<String, String> _data;
  private long _start;
  private long _stop;
  private String _processId;

  public SpanStructImpl(long spanId, long traceId, long parentid,
      String description, Map<String, String> data, long start, long stop,
      String processId) {
    _spanId = spanId;
    _traceId = traceId;
    _parentId = parentid;
    _description = description;
    _data = data;
    _start = start;
    _stop = stop;
    _processId = processId;
  }

  @Override
  public String description() {
    return _description;
  }

  @Override
  public long spanId() {
    return _spanId;
  }

  @Override
  public long parentId() {
    return _parentId;
  }

  @Override
  public long traceId() {
    return _traceId;
  }

  @Override
  public Map<String, String> getData() {
    return _data;
  }

  @Override
  public long start() {
    return _start;
  }

  @Override
  public long stop() {
    return _stop;
  }

  @Override
  public String getProcessId() {
    return _processId;
  }

  @Override
  public String toString() {
    return String
        .format(
            "Span: %d, trace: %d, parent: %d , start: %d , stop: %d, \nellapsed: %d, description: %s",
            _spanId, _traceId, _parentId, _start, _stop, _stop - _start,
            _description);
  }
}
