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
package org.cloudera.htrace.impl;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.SpanReceiver;
import org.mortbay.util.ajax.JSON;

/**
 * Writes the spans it receives to a local file. For now I am ignoring the data
 * (annotations) portion of the spans. A production LocalFileSpanReceiver should
 * use a real CSV format.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LocalFileSpanReceiver implements SpanReceiver {
  public static final Log LOG = LogFactory
.getLog(LocalFileSpanReceiver.class);
  private String file;
  private FileWriter fwriter;
  private BufferedWriter bwriter;
  private Map<String, Object> values;

  public LocalFileSpanReceiver(String file) throws IOException {
    this.file = file;
    this.fwriter = new FileWriter(this.file, true);
    this.bwriter = new BufferedWriter(fwriter);
    values = new HashMap<String, Object>();
  }

  @Override
  public void receiveSpan(Span span) {
    try {
      values.put("SpanID", span.getSpanId());
      values.put("TraceID", span.getTraceId());
      values.put("ParentID", span.getParentId());
      values.put("Start", span.getStartTimeMillis());
      values.put("Stop", span.getStopTimeMillis());
      values.put("Description", span.getDescription());
      values.put("Annotations", span.getAnnotations());
      bwriter.write(JSON.toString(values));
      bwriter.flush();
    } catch (IOException e) {
      LOG.error("Error when writing to file: " + file, e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      fwriter.close();
    } catch (IOException e) {
      LOG.error("Error closing filewriter for file: " + file, e);
    }
    try {
      bwriter.close();
    } catch (IOException e) {
      LOG.error("Error closing bufferedwriter for file: " + file, e);
    }
  }
}
