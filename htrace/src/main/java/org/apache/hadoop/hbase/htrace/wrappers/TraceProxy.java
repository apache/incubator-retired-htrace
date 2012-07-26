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
package org.apache.hadoop.hbase.htrace.wrappers;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.htrace.Sampler;
import org.apache.hadoop.hbase.htrace.Span;
import org.apache.hadoop.hbase.htrace.Trace;
import org.apache.hadoop.hbase.htrace.impl.AlwaysSampler;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TraceProxy {

  @SuppressWarnings("rawtypes")
  static final Sampler ALWAYS = AlwaysSampler.getInstance();

  /**
   * Returns an object that will trace all calls to itself.
   * 
   * @param instance
   * @return
   */
  public static <T> T trace(T instance) {
    return trace(instance, ALWAYS);
  }

  /**
   * Returns an object that will trace all calls to itself.
   * 
   * @param instance
   * @param sampler
   * @return
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <T> T trace(final T instance, final Sampler sampler) {
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object obj, Method method, Object[] args)
          throws Throwable {
        if (!sampler.next(null)) {
          return method.invoke(instance, args);
        }
        Span span = Trace.startTraceIfNotStarted(method.getName());
        try {
          return method.invoke(instance, args);
        } catch (Throwable ex) {
          ex.printStackTrace();
          throw ex;
        } finally {
          span.stop();
        }
      }
    };
    return (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(),
        instance.getClass().getInterfaces(), handler);
  }
}
