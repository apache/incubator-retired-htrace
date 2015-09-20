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
package org.apache.htrace.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class TestLocalFileSpanReceiver {
  @Test
  public void testUniqueLocalTraceFileName() {
    String filename1 = LocalFileSpanReceiver.getUniqueLocalTraceFileName();
    String filename2 = LocalFileSpanReceiver.getUniqueLocalTraceFileName();
    boolean eq = filename1.equals(filename2);
    if (System.getProperty("os.name").startsWith("Linux")) {
      // ${java.io.tmpdir}/[pid]
      assertTrue(eq);
    } else {
      // ${java.io.tmpdir}/[random UUID]
      assertFalse(eq);
    }
  }

  @Test
  public void testWriteToLocalFile() throws IOException {
    String traceFileName = LocalFileSpanReceiver.getUniqueLocalTraceFileName();
    Tracer tracer = new Tracer.Builder().
        name("testWriteToLocalFileTracer").
        tracerPool(new TracerPool("testWriteToLocalFile")).
        conf(HTraceConfiguration.fromKeyValuePairs(
            "sampler.classes", "AlwaysSampler",
            "span.receiver.classes", LocalFileSpanReceiver.class.getName(),
            "local.file.span.receiver.path", traceFileName,
            "tracer.id", "%{tname}")).
        build();
    TraceScope scope = tracer.newScope("testWriteToLocalFile");
    scope.close();
    tracer.close();

    ObjectMapper mapper = new ObjectMapper();
    MilliSpan span = mapper.readValue(new File(traceFileName), MilliSpan.class);
    assertEquals("testWriteToLocalFile", span.getDescription());
    assertEquals("testWriteToLocalFileTracer", span.getTracerId());
  }
}
