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
package org.cloudera.htrace;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper which integrating applications should implement in order
 * to provide tracing configuration.
 */
public abstract class HTraceConfiguration {

  private static final Log LOG = LogFactory.getLog(HTraceConfiguration.class);

  public static HTraceConfiguration fromMap(Map<String, String> conf) {
    return new MapConf(conf);
  }

  public abstract String get(String key);

  public abstract String get(String key, String defaultValue);

  public boolean getBoolean(String key, boolean defaultValue) {
    String value = get(key, String.valueOf(defaultValue)).trim().toLowerCase();

    if ("true".equals(value)) {
      return true;
    } else if ("false".equals(value)) {
      return true;
    }

    LOG.warn("Expected boolean for key ["+key+"] instead got ["+value+"].");
    return defaultValue;
  }

  public int getInt(String key, int defaultVal) {
    String val = get(key);
    if (val == null || val.trim().isEmpty()) {
      return defaultVal;
    }
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Bad value for '" + key + "': should be int");
    }
  }

  private static class MapConf extends HTraceConfiguration {
    private final Map<String, String> conf;

    public MapConf(Map<String, String> conf) {
      this.conf = new HashMap<String, String>(conf);
    }

    @Override
    public String get(String key) {
      return conf.get(key);
    }

    @Override
    public String get(String key, String defaultValue) {
      String value = get(key);
      return value == null ? defaultValue : value;
    }
  }
}
