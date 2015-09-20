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

import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
public abstract class Sampler {
  /**
   * A {@link Sampler} builder. It takes a {@link Sampler} class name and
   * constructs an instance of that class, with the provided configuration.
   */
  public static class Builder {
    private static final Log LOG = LogFactory.getLog(Builder.class);

    private final static String DEFAULT_PACKAGE = "org.apache.htrace.core";
    private final HTraceConfiguration conf;
    private String className;
    private ClassLoader classLoader = Builder.class.getClassLoader();

    public Builder(HTraceConfiguration conf) {
      this.conf = conf;
      reset();
    }

    public Builder reset() {
      this.className = null;
      return this;
    }

    public Builder className(String className) {
      this.className = className;
      return this;
    }

    public Builder classLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
    }

    private void throwError(String errorStr) {
      LOG.error(errorStr);
      throw new RuntimeException(errorStr);
    }

    private void throwError(String errorStr, Throwable e) {
      LOG.error(errorStr, e);
      throw new RuntimeException(errorStr, e);
    }

    public Sampler build() {
      Sampler sampler = newSampler();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Created new sampler of type " +
            sampler.getClass().getName(), new Exception());
      }
      return sampler;
    }

    private Sampler newSampler() {
      if (className == null || className.isEmpty()) {
        throwError("No sampler class specified.");
      }
      String str = className;
      if (!str.contains(".")) {
        str = DEFAULT_PACKAGE + "." + str;
      }
      Class cls = null;
      try {
        cls = classLoader.loadClass(str);
      } catch (ClassNotFoundException e) {
        throwError("Cannot find Sampler class " + str);
      }
      Constructor<Sampler> ctor = null;
      try {
        ctor = cls.getConstructor(HTraceConfiguration.class);
      } catch (NoSuchMethodException e) {
        throwError("Cannot find a constructor for class " +
            str + "which takes an HTraceConfiguration.");
      }
      Sampler sampler = null;
      try {
        LOG.debug("Creating new instance of " + str + "...");
        sampler = ctor.newInstance(conf);
      } catch (ReflectiveOperationException e) {
        throwError("Reflection error when constructing " +
            str + ".", e);
      } catch (Throwable t) {
        throwError("NewInstance error when constructing " +
            str + ".", t);
      }
      return sampler;
    }
  }

  public static final Sampler ALWAYS = AlwaysSampler.INSTANCE;
  public static final Sampler NEVER = NeverSampler.INSTANCE;

  public abstract boolean next();
}
