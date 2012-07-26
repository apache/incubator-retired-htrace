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

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.htrace.Span;
import org.apache.hadoop.hbase.htrace.SpanReceiver;

/**
 * Writes the spans it receives to a local file. For now I am ignoring the data
 * (annotations) portion of the spans. A production LocalFileSpanReceiver should
 * use a real CSV format.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LocalFileSpanReceiver implements SpanReceiver, Closeable {
  public static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.hbase.LocalFileSpanReceiver");
  private String _file;
  private FileWriter _fwriter;
  private BufferedWriter _bwriter;

  public LocalFileSpanReceiver(String file) throws IOException {
    this._file = file;
    this._fwriter = new FileWriter(_file, true);
    this._bwriter = new BufferedWriter(_fwriter);
  }

  @Override
  public void receiveSpan(Span span) {
    try {
      // writes in this weird delimited format, mainly for demonstration
      // purposes.
      // doesn't write out data (annotations) to the file
      _bwriter.write(String.format("%d/<,%d/<,%d/<,%d/<,%d/<,%s\\\\;;;;",
          span.getTraceId(), span.getSpanId(), span.getParentId(),
          span.getStartTimeMillis(), span.getStopTimeMillis(),
          span.getDescription()));
      _bwriter.flush();
    } catch (IOException e) {
      LOG.error("Error when writing to file: " + _file, e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      _fwriter.close();
    } catch (IOException e) {
      LOG.error("Error closing filewriter for file: " + _file, e);
    }
    try {
      _bwriter.close();
    } catch (IOException e) {
      LOG.error("Error closing bufferedwriter for file: " + _file, e);
    }
  }
}