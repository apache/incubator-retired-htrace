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

import java.util.HashMap;
import java.util.Map;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.htrace.impl.AlwaysSampler;
import org.apache.htrace.impl.NeverSampler;
import org.junit.Assert;
import org.junit.Test;

public class TestSampler {
  @Test
  public void testSamplerBuilder() {
    Sampler alwaysSampler = new SamplerBuilder(
        HTraceConfiguration.fromKeyValuePairs("sampler", "AlwaysSampler")).
        build();
    Assert.assertEquals(AlwaysSampler.class, alwaysSampler.getClass());

    Sampler neverSampler = new SamplerBuilder(
        HTraceConfiguration.fromKeyValuePairs("sampler", "NeverSampler")).
        build();
    Assert.assertEquals(NeverSampler.class, neverSampler.getClass());

    Sampler neverSampler2 = new SamplerBuilder(HTraceConfiguration.
        fromKeyValuePairs("sampler", "NonExistentSampler")).
        build();
    Assert.assertEquals(NeverSampler.class, neverSampler2.getClass());

    Sampler neverSampler3 = new SamplerBuilder(HTraceConfiguration.
        fromKeyValuePairs("sampler.is.not.defined", "NonExistentSampler")).
        build();
    Assert.assertEquals(NeverSampler.class, neverSampler3.getClass());
  }

  @Test
  public void testParamterizedSampler() {
    TestParamSampler sampler = new TestParamSampler();
    TraceScope s = Trace.startSpan("test", sampler, 1);
    Assert.assertNotNull(s.getSpan());
    s.close();
    s = Trace.startSpan("test", sampler, -1);
    Assert.assertNull(s.getSpan());
    s.close();
  }

  @Test
  public void testAlwaysSampler() {
    TraceScope cur = Trace.startSpan("test");
    Assert.assertNotNull(cur);
    cur.close();
  }

  private class TestParamSampler implements Sampler<Integer> {

    @Override
    public boolean next(Integer info) {
      return info > 0;
    }

  }
}
