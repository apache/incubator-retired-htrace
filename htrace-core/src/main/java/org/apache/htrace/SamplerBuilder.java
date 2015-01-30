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
package org.apache.htrace;

import java.lang.reflect.Constructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.impl.AlwaysSampler;
import org.apache.htrace.impl.NeverSampler;

/**
 * A {@link Sampler} builder. It reads a {@link Sampler} class name from the provided
 * configuration using the {@link #SAMPLER_CONF_KEY} key. Unqualified class names
 * are interpreted as members of the {@code org.apache.htrace.impl} package. The {@link #build()}
 * method constructs an instance of that class, initialized with the same configuration.
 */
public class SamplerBuilder {

  // TODO: should follow the same API as SpanReceiverBuilder

  public final static String SAMPLER_CONF_KEY = "sampler";
  private final static String DEFAULT_PACKAGE = "org.apache.htrace.impl";
  private final static ClassLoader classLoader =
      SamplerBuilder.class.getClassLoader();
  private final HTraceConfiguration conf;
  private static final Log LOG = LogFactory.getLog(SamplerBuilder.class);

  public SamplerBuilder(HTraceConfiguration conf) {
    this.conf = conf;
  }

  public Sampler build() {
    String str = conf.get(SAMPLER_CONF_KEY);
    if (str == null || str.isEmpty()) {
      return NeverSampler.INSTANCE;
    }
    if (!str.contains(".")) {
      str = DEFAULT_PACKAGE + "." + str;
    }
    Class cls = null;
    try {
      cls = classLoader.loadClass(str);
    } catch (ClassNotFoundException e) {
      LOG.error("SamplerBuilder cannot find sampler class " + str +
          ": falling back on NeverSampler.");
      return NeverSampler.INSTANCE;
    }
    Constructor<Sampler> ctor = null;
    try {
      ctor = cls.getConstructor(HTraceConfiguration.class);
    } catch (NoSuchMethodException e) {
      LOG.error("SamplerBuilder cannot find a constructor for class " + str +
          "which takes an HTraceConfiguration.  Falling back on " +
          "NeverSampler.");
      return NeverSampler.INSTANCE;
    }
    try {
      return ctor.newInstance(conf);
    } catch (ReflectiveOperationException e) {
      LOG.error("SamplerBuilder reflection error when constructing " + str +
          ".  Falling back on NeverSampler.", e);
      return NeverSampler.INSTANCE;
    } catch (Throwable e) {
      LOG.error("SamplerBuilder constructor error when constructing " + str +
          ".  Falling back on NeverSampler.", e);
      return NeverSampler.INSTANCE;
    }
  }
}
