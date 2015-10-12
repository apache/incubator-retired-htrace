/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.htrace.impl;

import java.io.IOException;

import org.apache.htrace.core.Span;

/**
 * A buffer which contains span data and is able to send it over the network.
 *
 * BufferManager functions are not thread-safe.  You must rely on external
 * synchronization to protect buffers from concurrent operations.
 */
interface BufferManager {
  /**
   * Write a span to this buffer.
   *
   * @param span            The span to write.
   *
   * @throws IOException    If the buffer doesn't have enough space to hold the
   *                          new span.  We will not write a partial span to the
   *                          buffer in this case.
   */
  void writeSpan(Span span) throws IOException;

  /**
   * Get the amount of content currently in the buffer.
   */
  int contentLength();

  /**
   * Get the number of spans currently buffered.
   */
  int getNumberOfSpans();

  /**
   * Prepare the buffers to be flushed.
   */
  void prepare() throws IOException;

  /**
   * Flush this buffer to htraced.
   *
   * This is a blocking operation which will not return until the buffer is
   * completely flushed.
   */
  void flush() throws IOException;

  /**
   * Clear the data in this buffer.
   */
  void clear();

  /**
   * Closes the buffer manager and frees all resources.
   */
  void close();
}
