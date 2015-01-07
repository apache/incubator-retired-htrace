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
package org.apache.htrace.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Sampler;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.SpanReceiverBuilder;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

@Ignore
public class TestLocalFileSpanReceiver {
  @Test
  public void testUniqueLocalTraceFileName() {
    String filename1 = LocalFileSpanReceiver.getUniqueLocalTraceFileName();
    System.out.println("##### :" + filename1);
    String filename2 = LocalFileSpanReceiver.getUniqueLocalTraceFileName();
    System.out.println("##### :" + filename2);
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
    HashMap<String, String> confMap = new HashMap<String, String>();
    confMap.put(LocalFileSpanReceiver.PATH_KEY, traceFileName);
    confMap.put(SpanReceiverBuilder.SPAN_RECEIVER_CONF_KEY,
                LocalFileSpanReceiver.class.getName());
    SpanReceiver rcvr =
        new SpanReceiverBuilder(HTraceConfiguration.fromMap(confMap))
            .logErrors(false).build();
    Trace.addReceiver(rcvr);
    TraceScope ts = Trace.startSpan("testWriteToLocalFile", Sampler.ALWAYS);
    ts.close();
    Trace.removeReceiver(rcvr);
    rcvr.close();

    ObjectMapper mapper = new ObjectMapper();
    MilliSpan span = mapper.readValue(new File(traceFileName), MilliSpan.class);
    assertEquals("testWriteToLocalFile", span.getDescription());
  }
}
