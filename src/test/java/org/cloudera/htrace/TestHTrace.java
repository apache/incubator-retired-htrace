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
package org.cloudera.htrace;

import java.io.File;
import java.io.IOException;

import org.cloudera.htrace.impl.LocalFileSpanReceiver;
import org.cloudera.htrace.impl.StandardOutSpanReceiver;
import org.junit.Test;

public class TestHTrace {

  @Test
  public void testHtrace() throws Exception {
    String fileName = System.getProperty("spanFile");
    LocalFileSpanReceiver lfsr = null;
    StandardOutSpanReceiver sosr = null;

    // writes spans to a file if one is provided to maven with
    // -DspanFile="FILENAME", otherwise writes to standard out.
    if (fileName != null) {
      try {
        File f = new File(fileName);
        File parent = f.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
          throw new IllegalArgumentException("Couldn't create file: "
              + fileName);
        }
        lfsr = new LocalFileSpanReceiver(fileName);
      } catch (IOException e1) {
        System.out.println("Error constructing LocalFileSpanReceiver: "
            + e1.getMessage());
        throw e1;
      }
      TraceCreator tc = new TraceCreator(lfsr);
      tc.createDemoTrace();
      lfsr.close();
    } else {
      sosr = new StandardOutSpanReceiver();
      TraceCreator tc = new TraceCreator(sosr);
      tc.createDemoTrace();
    }
  }
}
