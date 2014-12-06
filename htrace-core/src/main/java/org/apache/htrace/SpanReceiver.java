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


import java.io.Closeable;


/**
 * The collector within a process that is the destination of Spans when a trace
 * is running.
 */
public interface SpanReceiver extends Closeable {

  /**
   * Hosts of SpanReceivers should call this method to provide
   * configuration to SpanReceivers after creating them.
   */
  public void configure(HTraceConfiguration conf);

  /**
   * Called when a Span is stopped and can now be stored.
   *
   * @param span
   */
  public void receiveSpan(Span span);

}
