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

public class SpanReceiverFactory {
  static final Log LOG = LogFactory.getLog(SpanReceiverFactory.class);

  public final static String SPAN_RECEIVER_CONF_KEY = "span.receiver";
  private final static ClassLoader classLoader =
      SpanReceiverFactory.class.getClassLoader();
  private final HTraceConfiguration conf;
  private boolean logErrors = true;

  public SpanReceiverFactory(HTraceConfiguration conf) {
    this.conf = conf;
  }

  /**
   * Configure whether we should log errors during build().
   */
  public SpanReceiverFactory logErrors(boolean logErrors) {
    this.logErrors = logErrors;
    return this;
  }

  private void logError(String errorStr) {
    if (!logErrors) {
      return;
    }
    LOG.error(errorStr);
  }

  private void logError(String errorStr, Throwable e) {
    if (!logErrors) {
      return;
    }
    LOG.error(errorStr, e);
  }

  public SpanReceiver build() {
    String str = conf.get(SPAN_RECEIVER_CONF_KEY);
    if ((str == null) || str.isEmpty()) {
      return null;
    }
    if (!str.contains(".")) {
      str = "org.apache.htrace.impl." + str;
    }
    Class cls = null;
    try {
      cls = classLoader.loadClass(str);
    } catch (ClassNotFoundException e) {
      logError("SpanReceiverFactory cannot find SpanReceiver class " + str +
          ": disabling span receiver.");
      return null;
    }
    Constructor<SpanReceiver> ctor = null;
    try {
      ctor = cls.getConstructor(HTraceConfiguration.class);
    } catch (NoSuchMethodException e) {
      logError("SpanReceiverFactory cannot find a constructor for class " +
          str + "which takes an HTraceConfiguration.  Disabling span " +
          "receiver.");
      return null;
    }
    try {
      return ctor.newInstance(conf);
    } catch (ReflectiveOperationException e) {
      logError("SpanReceiverFactory reflection error when constructing " + str +
          ".  Disabling span receiver.", e);
      return null;
    } catch (Throwable e) {
      logError("SpanReceiverFactory constructor error when constructing " + str +
          ".  Disabling span receiver.", e);
      return null;
    }
  }
}
