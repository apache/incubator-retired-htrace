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


public class TraceInfo {
  public final long traceId;
  public final long spanId;

  public TraceInfo(long traceId, long spanId) {
    this.traceId = traceId;
    this.spanId = spanId;
  }

  @Override
  public String toString() {
    return "TraceInfo(traceId=" + traceId + ", spanId=" + spanId + ")";
  }

  public static TraceInfo fromSpan(Span s) {
    if (s == null) return null;
    return new TraceInfo(s.getTraceId(), s.getSpanId());
  }
}
