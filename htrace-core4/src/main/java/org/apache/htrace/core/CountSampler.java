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

import java.util.concurrent.ThreadLocalRandom;

/**
 * Sampler that returns true every N calls. Specify the frequency interval by configuring a
 * {@code long} value for {@link #SAMPLER_FREQUENCY_CONF_KEY}.
 */
public class CountSampler extends Sampler {
  public final static String SAMPLER_FREQUENCY_CONF_KEY = "sampler.frequency";

  final long frequency;
  long count = ThreadLocalRandom.current().nextLong();

  public CountSampler(HTraceConfiguration conf) {
    this.frequency = Long.parseLong(conf.get(SAMPLER_FREQUENCY_CONF_KEY), 10);
  }

  @Override
  public boolean next() {
    return (count++ % frequency) == 0;
  }
}
