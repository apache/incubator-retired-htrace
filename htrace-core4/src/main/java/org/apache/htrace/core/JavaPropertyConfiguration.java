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

import java.util.Iterator;
import java.util.LinkedList;

/**
 * This is an implementation of HTraceConfiguration which draws its properties
 * from global Java Properties.
 */
public final class JavaPropertyConfiguration extends HTraceConfiguration {
  private static final Log LOG =
      LogFactory.getLog(JavaPropertyConfiguration.class);

  public static class Builder {
    final LinkedList<String> prefixes;

    public Builder() {
      prefixes = new LinkedList<String>();
      prefixes.add("htrace.");
    }

    public Builder clearPrefixes() {
      prefixes.clear();
      return this;
    }

    public Builder addPrefix(String prefix) {
      prefixes.add(prefix);
      return this;
    }

    JavaPropertyConfiguration build() {
      return new JavaPropertyConfiguration(prefixes);
    }
  }

  private final String[] prefixes;

  private JavaPropertyConfiguration(LinkedList<String> prefixes) {
    this.prefixes = new String[prefixes.size()];
    int i = 0;
    for (Iterator<String> it = prefixes.descendingIterator(); it.hasNext(); ) {
      this.prefixes[i++] = it.next();
    }
  }

  @Override
  public String get(String key) {
    for (String prefix : prefixes) {
      String val = System.getProperty(prefix + key);
      if (val != null) {
        return val;
      }
    }
    return null;
  }

  @Override
  public String get(String key, String defaultValue) {
    String val = get(key);
    return (val != null) ? val : defaultValue;
  }
}
