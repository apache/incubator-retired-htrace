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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TestSampler {
  private Sampler[] getSamplersFromConf(HTraceConfiguration conf) {
    Tracer tracer = new TracerBuilder().
        name("MyTracer").
        tracerPool(new TracerPool("getSamplersFromConf")).
        conf(conf).
        build();
    Sampler[] samplers = tracer.getSamplers();
    tracer.close();
    return samplers;
  }

  private void checkArrayContains(List<Class<? extends Sampler>> expected,
                                  Sampler[] samplers) {
    for (Iterator<Class<? extends Sampler>> iter = expected.iterator();
         iter.hasNext(); ) {
      Class<? extends Sampler> samplerClass = iter.next();
      boolean found = false;
      for (int i = 0; i < samplers.length; i++) {
        if (samplers[i] != null) {
          if (samplers[i].getClass().equals(samplerClass)) {
            samplers[i] = null;
            found = true;
            break;
          }
        }
      }
      Assert.assertTrue("Failed to find sampler class " +
          samplerClass.getName(), found);
    }
    for (int i = 0; i < samplers.length; i++) {
      if (samplers[i] != null) {
        Assert.fail("Got extra sampler of type " +
            samplers.getClass().getName());
      }
    }
  }

  private void checkArrayContains(Class<? extends Sampler> expected, Sampler[] samplers) {
    LinkedList<Class<? extends Sampler>> expectedList =
        new LinkedList<Class<? extends Sampler>>();
    expectedList.add(expected);
    checkArrayContains(expectedList, samplers);
  }

  @Test
  public void testTracerBuilderCreatesCorrectSamplers() {
    Sampler[] samplers = getSamplersFromConf(HTraceConfiguration.
        fromKeyValuePairs("sampler.classes", "AlwaysSampler"));
    checkArrayContains(AlwaysSampler.class, samplers);

    samplers = getSamplersFromConf(HTraceConfiguration.
        fromKeyValuePairs("sampler.classes", "NeverSampler"));
    checkArrayContains(NeverSampler.class, samplers);

    samplers = getSamplersFromConf(HTraceConfiguration.
        fromKeyValuePairs("sampler.classes", "NonExistentSampler"));
    Assert.assertEquals(0, samplers.length);

    samplers = getSamplersFromConf(HTraceConfiguration.EMPTY);
    Assert.assertEquals(0, samplers.length);
  }

  @Test
  public void testAlwaysSampler() {
    AlwaysSampler sampler = new AlwaysSampler(HTraceConfiguration.EMPTY);
    Assert.assertTrue(sampler.next());
  }

  @Test
  public void testNeverSampler() {
    NeverSampler sampler = new NeverSampler(HTraceConfiguration.EMPTY);
    Assert.assertTrue(!sampler.next());
  }
}
