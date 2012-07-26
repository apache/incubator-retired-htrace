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
package org.apache.hadoop.hbase.htrace;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Extremely simple callback to determine the frequency that an action should be
 * performed.
 * 
 * 'T' is the object type you require to create a more advanced sampling
 * function. For example if there is some RPC information in a 'Call' object,
 * you might implement Sampler<Call>. Then when the RPC is received you can call
 * one of the Trace.java functions that takes the extra 'info' parameter, which
 * will be passed into the next function you implemented.
 * 
 * For the example above, the next(T info) function may look like this
 * 
 * public boolean next(Call info){
 * 
 *    if (info.getName().equals("get")) {
 *        return Math.random() > 0.5;
 *    } else if (info.getName().equals("put")) {
 *        return Math.random() > 0.25;
 *    } else {
 *      return false;
 *    }
 * }
 * This would trace half of all gets, 75% of all puts and would not trace any other requests.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Sampler<T> {

  public boolean next();

  public boolean next(T info);

}
