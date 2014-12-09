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

import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Sampler;

import java.util.Random;

public class ProbabilitySampler implements Sampler<Object> {
  public final double threshold;
  private Random random = new Random();
  private final static String SAMPLER_FRACTION_CONF_KEY = "sampler.fraction";

  public ProbabilitySampler(HTraceConfiguration conf) {
    this.threshold = Double.parseDouble(conf.get(SAMPLER_FRACTION_CONF_KEY));
  }

  @Override
  public boolean next(Object info) {
    return random.nextDouble() < threshold;
  }
}
