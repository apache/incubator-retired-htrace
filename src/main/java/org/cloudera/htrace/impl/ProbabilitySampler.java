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
package org.cloudera.htrace.impl;

import java.util.Random;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.cloudera.htrace.Sampler;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ProbabilitySampler implements Sampler<Object> {
  public double _threshold;
  private Random _random;

  public ProbabilitySampler(double threshold) {
    _threshold = threshold;
    _random = new Random();
  }

  @Override
  public boolean next(Object info) {
    return _random.nextDouble() > _threshold;
  }

}
