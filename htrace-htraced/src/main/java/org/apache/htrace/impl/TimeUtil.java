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

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

/**
 * Utilities for dealing with monotonic time.
 */
class TimeUtil {
  /**
   * Returns the current monotonic time in milliseconds.
   */
  static long nowMs() {
    return TimeUnit.MILLISECONDS.convert(
        System.nanoTime(), TimeUnit.NANOSECONDS);
  }

  /**
   * Get the approximate delta between two monotonic times.
   *
   * This function makes the following assumptions:
   * 1. We read startMs from the monotonic clock prior to endMs.
   * 2. The two times are not more than 100 years or so apart.
   *
   * With these two assumptions in hand, we can smooth over some of the
   * unpleasant features of the monotonic clock:
   * 1. It can return either positive or negative values.
   * 2. When the number of nanoseconds reaches Long.MAX_VALUE it wraps around
   * to Long.MIN_VALUE.
   * 3. On some messed up systems it has been known to jump backwards every
   * now and then.  Oops.  CPU core synchronization mumble mumble.
   *
   * @param startMs  The start time.
   * @param endMs    The end time.
   * @return         The delta between the two times.
   */
  static long deltaMs(long startMs, long endMs) {
    BigInteger startNs = BigInteger.valueOf(TimeUnit.NANOSECONDS.
        convert(startMs, TimeUnit.MILLISECONDS));
    BigInteger endNs = BigInteger.valueOf(TimeUnit.NANOSECONDS.
        convert(endMs, TimeUnit.MILLISECONDS));
    BigInteger deltaNs = endNs.subtract(startNs);
    if (deltaNs.signum() >= 0) {
      return TimeUnit.MILLISECONDS.convert(deltaNs.min(
          BigInteger.valueOf(Long.MAX_VALUE)).longValue(), TimeUnit.NANOSECONDS);
    }
    deltaNs = deltaNs.negate();
    if (deltaNs.compareTo(BigInteger.valueOf(Long.MAX_VALUE / 2)) < 0) {
      // If the 'startNs' is numerically less than the 'endNs', and the
      // difference between the two is less than 100 years, it's probably
      // just clock jitter.  Certain old OSes and CPUs had monotonic clocks
      // that could go backwards under certain conditions (ironic, given
      // the name).
      return 0L;
    }
    // Handle rollover.
    BigInteger revDeltaNs = BigInteger.ONE.shiftLeft(64).subtract(deltaNs);
    return TimeUnit.MILLISECONDS.convert(revDeltaNs.min(
        BigInteger.valueOf(Long.MAX_VALUE)).longValue(), TimeUnit.NANOSECONDS);
  }
}
