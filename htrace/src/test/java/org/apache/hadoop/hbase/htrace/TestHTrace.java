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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hbase.htrace.impl.LocalFileSpanReceiver;
import org.junit.Test;

public class TestHTrace {

  @Test
  public void testHtrace() {
    LocalFileSpanReceiver rec = null;

    try {
      File f = new File("test-output-spans.txt");
      f.delete();
      rec = new LocalFileSpanReceiver("test-output-spans.txt");
    } catch (IOException e1) {
      System.out.println("Error constructing LocalFileSpanReceiver: "
          + e1.getMessage());
      System.exit(1);
    }

    try {
      TraceCreator tc = new TraceCreator(rec);
      tc.createDemoTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        rec.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
