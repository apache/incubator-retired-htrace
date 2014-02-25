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
package org.htrace.wrappers;

import org.htrace.Sampler;
import org.htrace.Trace;
import org.htrace.TraceScope;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class TraceProxy {
  /**
   * Returns an object that will trace all calls to itself.
   *
   * @param instance
   * @return
   */
  public static <T> T trace(T instance) {
    return trace(instance, Sampler.ALWAYS);
  }

  /**
   * Returns an object that will trace all calls to itself.
   *
   * @param <V>
   * @param instance
   * @param sampler
   * @return
   */
  @SuppressWarnings("unchecked")
  public static <T, V> T trace(final T instance, final Sampler<V> sampler) {
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object obj, Method method, Object[] args)
          throws Throwable {
        if (!sampler.next(null)) {
          return method.invoke(instance, args);
        }

        TraceScope scope = Trace.startSpan(method.getName(), Sampler.ALWAYS);
        try {
          return method.invoke(instance, args);
        } catch (Throwable ex) {
          ex.printStackTrace();
          throw ex;
        } finally {
          scope.close();
        }
      }
    };
    return (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(),
        instance.getClass().getInterfaces(), handler);
  }
}
