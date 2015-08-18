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

/**
 * Extremely simple callback to determine the frequency that an action should be
 * performed.
 * <p/>
 * For example, the next() function may look like this:
 * <p/>
 * <pre>
 * <code>
 * public boolean next() {
 *   return Math.random() &gt; 0.5;
 * }
 * </code>
 * </pre>
 * This would trace 50% of all gets, 75% of all puts and would not trace any other requests.
 */
public interface Sampler {
  public static final Sampler ALWAYS = AlwaysSampler.INSTANCE;
  public static final Sampler NEVER = NeverSampler.INSTANCE;

  public boolean next();
}
