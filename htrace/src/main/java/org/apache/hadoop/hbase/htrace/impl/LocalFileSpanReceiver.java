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
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.htrace.SpanReceiver;

/**
 * Writes the spans it receives to a local file. For now I am ignoring the data
 * (annotations) portion of the spans. A production LocalFileSpanReceiver should
 * use a real CSV format.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class LocalFileSpanReceiver implements SpanReceiver, Closeable {
  private String _file;
  private FileWriter _fwriter;
  private BufferedWriter _bwriter;

  public LocalFileSpanReceiver(String file) throws IOException {
    this._file = file;
    this._fwriter = new FileWriter(_file, true);
    this._bwriter = new BufferedWriter(_fwriter);
  }

  @Override
  public void span(long traceId, long spanId, long parentId, long start,
      long stop, String description, Map<byte[], byte[]> annotations,
      String processId) {
    try {
      // writes in this weird delimited format, mainly for demonstration
      // purposes.
      // doesn't write out data (annotations) to the file
      _bwriter.write(String.format("%d/<,%d/<,%d/<,%d/<,%d/<,%s\\\\;;;;",
          traceId, spanId, parentId, start, stop, description));
      _bwriter.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      _fwriter.close();
    } catch (IOException e) {
      // TODO: log the exception
    }
    try {
      _bwriter.close();
    } catch (IOException e) {
      // TODO: log the exception
    }
  }
}