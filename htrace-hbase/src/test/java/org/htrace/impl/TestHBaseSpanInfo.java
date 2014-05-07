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

package org.htrace.impl;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.htrace.Span;
import org.htrace.TimelineAnnotation;
import org.junit.Assert;
import org.junit.Test;

public class TestHBaseSpanInfo {
  private static final Log LOG = LogFactory.getLog(TestHBaseSpanInfo.class);
  private static Random rand = new Random();

  @Test
  public void testToBytesAndFromBytes() {
    List<TimelineAnnotation> t1 = new ArrayList<TimelineAnnotation>();
    t1.add(new TimelineAnnotation(1, "timeline annotation 1"));
    t1.add(new TimelineAnnotation(2, "timeline annotation 2"));
    t1.add(new TimelineAnnotation(3, "timeline annotation 3"));
    Span s1 = new HBaseSpanInfo(rand.nextLong(), rand.nextLong(), rand.nextLong(),
                                rand.nextLong(), rand.nextLong(),
                                "process id", "span description",
                                t1, null);
    byte[] bytes = null;
    try {
      bytes = HBaseSpanInfo.toBytes(s1);
      Span s2 = HBaseSpanInfo.fromBytes(bytes);
      Assert.assertEquals(s1.getTraceId(), s2.getTraceId());
      Assert.assertEquals(s1.getSpanId(), s2.getSpanId());
      Assert.assertEquals(s1.getParentId(), s2.getParentId());
      Assert.assertEquals(s1.getStartTimeMillis(), s2.getStartTimeMillis());
      Assert.assertEquals(s1.getStopTimeMillis(), s2.getStopTimeMillis());
      Assert.assertEquals(s1.getDescription(), s2.getDescription());
      Assert.assertEquals(s1.getProcessId(), s2.getProcessId());
      List<TimelineAnnotation> t2 = s2.getTimelineAnnotations();
      for (int i = 0; i < 3; i++) {
        Assert.assertEquals(t1.get(i).getTime(), t2.get(i).getTime());
        Assert.assertEquals(t1.get(i).getMessage(), t2.get(i).getMessage());
      }
    } catch (IOException e) {
      Assert.fail("failed to convert byte array. " + e.getMessage());
    }
  }
}
