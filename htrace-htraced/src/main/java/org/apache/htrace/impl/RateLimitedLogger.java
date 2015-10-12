/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.htrace.impl;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;

/**
 * A logger which rate-limits its logging to a configurable level.
 */
class RateLimitedLogger {
  private final Log log;
  private final long timeoutMs;
  private long lastLogTimeMs;

  public RateLimitedLogger(Log log, long timeoutMs) {
    this.log = log;
    this.timeoutMs = timeoutMs;
    synchronized (this) {
      this.lastLogTimeMs = 0L;
    }
  }

  public void warn(String what) {
    long now = TimeUnit.MILLISECONDS.convert(System.nanoTime(),
        TimeUnit.NANOSECONDS);
    synchronized (this) {
      if (now >= lastLogTimeMs + timeoutMs) {
        log.warn(what);
        lastLogTimeMs = now;
      }
    }
  }

  public void error(String what) {
    long now = TimeUnit.MILLISECONDS.convert(System.nanoTime(),
        TimeUnit.NANOSECONDS);
    synchronized (this) {
      if (now >= lastLogTimeMs + timeoutMs) {
        log.error(what);
        lastLogTimeMs = now;
      }
    }
  }

  public void error(String what, Throwable e) {
    long now = TimeUnit.MILLISECONDS.convert(System.nanoTime(),
        TimeUnit.NANOSECONDS);
    synchronized (this) {
      if (now >= lastLogTimeMs + timeoutMs) {
        log.error(what, e);
        lastLogTimeMs = now;
      }
    }
  }
}
