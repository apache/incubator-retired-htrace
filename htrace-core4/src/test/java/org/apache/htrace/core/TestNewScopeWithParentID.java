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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class TestNewScopeWithParentID {

  @Test
  public void testNewScopeWithParentID() throws Exception {

    Tracer tracer = new Tracer.Builder().
              name("testNewScopeWithParentID").
              tracerPool(new TracerPool("testNewScopeWithParentID")).
              conf(HTraceConfiguration.fromKeyValuePairs(
                      "sampler.classes", "AlwaysSampler")).build();
    TraceScope activeScope = tracer.newScope("CurrentActiveScope");
    HashMap<Integer,SpanId> spanIdHashMap = new HashMap<>();
    SpanId parentID = new SpanId(100L, 200L);
    spanIdHashMap.put(activeScope.getSpanId().hashCode(),activeScope.getSpanId());
    spanIdHashMap.put(parentID.hashCode(),parentID);
    TraceScope childScope = tracer.
              newScope("ChildScope", parentID);
    Assert.assertNotNull(childScope);
    Assert.assertEquals(childScope.getSpan().getParents().length, 2);
    //parent on index 0
    Assert.assertNotNull(spanIdHashMap.get(childScope.getSpan().getParents()[0].hashCode()));
    Assert.assertEquals(childScope.getSpan().getParents()[0].getHigh(),
            spanIdHashMap.get(childScope.getSpan().getParents()[0].hashCode()).getHigh());
    Assert.assertEquals(childScope.getSpan().getParents()[0].getLow(),
            spanIdHashMap.get(childScope.getSpan().getParents()[0].hashCode()).getLow());
    //parent on index 1
    Assert.assertNotNull(spanIdHashMap.get(childScope.getSpan().getParents()[1].hashCode()));
    Assert.assertEquals(childScope.getSpan().getParents()[1].getHigh(),
            spanIdHashMap.get(childScope.getSpan().getParents()[1].hashCode()).getHigh());
    Assert.assertEquals(childScope.getSpan().getParents()[1].getLow(),
            spanIdHashMap.get(childScope.getSpan().getParents()[1].hashCode()).getLow());
    childScope.close();
    activeScope.close();

  }
}
