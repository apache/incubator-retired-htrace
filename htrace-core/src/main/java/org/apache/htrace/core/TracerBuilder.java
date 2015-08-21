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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.List;

/**
 * Builds a new Tracer object.
 */
public class TracerBuilder {
  public final static String SPAN_RECEIVER_CLASSES_KEY =
      "span.receiver.classes";
  public final static String SAMPLER_CLASSES_KEY =
      "sampler.classes";

  private static final Log LOG = LogFactory.getLog(TracerBuilder.class);

  private String name;
  private HTraceConfiguration conf = HTraceConfiguration.EMPTY;
  private ClassLoader classLoader =
      TracerBuilder.class.getClassLoader();
  private TracerPool tracerPool = TracerPool.GLOBAL;

  public TracerBuilder() {
  }

  public TracerBuilder name(String name) {
    this.name = name;
    return this;
  }

  public TracerBuilder conf(HTraceConfiguration conf) {
    this.conf = conf;
    return this;
  }

  public TracerBuilder tracerPool(TracerPool tracerPool) {
    this.tracerPool = tracerPool;
    return this;
  }

  private void loadSamplers(List<Sampler> samplers) {
    String classNamesStr = conf.get(SAMPLER_CLASSES_KEY, "");
    List<String> classNames = getClassNamesFromConf(classNamesStr);
    StringBuilder bld = new StringBuilder();
    String prefix = "";
    for (String className : classNames) {
      try {
        Sampler sampler = new Sampler.Builder(conf).
          className(className).
          classLoader(classLoader).
          build();
        samplers.add(sampler);
        bld.append(prefix).append(className);
        prefix = ", ";
      } catch (Throwable e) {
        LOG.error("Failed to create SpanReceiver of type " + className, e);
      }
    }
    String resultString = bld.toString();
    if (resultString.isEmpty()) {
      resultString = "no samplers";
    }
    LOG.info(SAMPLER_CLASSES_KEY + " = " + classNamesStr +
        "; loaded " + resultString);
  }

  private void loadSpanReceivers() {
    String classNamesStr = conf.get(SPAN_RECEIVER_CLASSES_KEY, "");
    List<String> classNames = getClassNamesFromConf(classNamesStr);
    StringBuilder bld = new StringBuilder();
    String prefix = "";
    for (String className : classNames) {
      try {
        tracerPool.loadReceiverType(className, conf, classLoader);
        bld.append(prefix).append(className);
        prefix = ", ";
      } catch (Throwable e) {
        LOG.error("Failed to create SpanReceiver of type " + className, e);
      }
    }
    String resultString = bld.toString();
    if (resultString.isEmpty()) {
      resultString = "no span receivers";
    }
    LOG.info(SPAN_RECEIVER_CLASSES_KEY + " = " + classNamesStr +
        "; loaded " + resultString);
  }

  /**
   * Get a list of class names from the HTrace configuration.
   * Entries which are empty will be removed.  Entries which lack a package will
   * be given the default package.
   *
   * @param classNamesStr     A semicolon-separated string containing a list
   *                            of class names.
   * @return                  A list of class names.
   */
  private List<String> getClassNamesFromConf(String classNamesStr) {
    String classNames[] = classNamesStr.split(";");
    LinkedList<String> cleanedClassNames = new LinkedList<String>();
    for (String className : classNames) {
      String cleanedClassName = className.trim();
      if (!cleanedClassName.isEmpty()) {
        cleanedClassNames.add(cleanedClassName);
      }
    }
    return cleanedClassNames;
  }

  public Tracer build() {
    if (name == null) {
      throw new RuntimeException("You must specify a name for this Tracer.");
    }
    LinkedList<SpanReceiver> spanReceivers = new LinkedList<SpanReceiver>();
    LinkedList<Sampler> samplers = new LinkedList<Sampler>();
    loadSamplers(samplers);
    String tracerId = new TracerId(conf, name).get();
    Tracer tracer = new Tracer(tracerId, tracerPool,
        samplers.toArray(new Sampler[samplers.size()]));
    tracerPool.addTracer(tracer);
    loadSpanReceivers();
    return tracer;
  }
}
