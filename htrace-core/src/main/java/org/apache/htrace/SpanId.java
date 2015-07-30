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

import java.math.BigInteger;
import java.lang.Void;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Random;

/**
 * Uniquely identifies an HTrace span.
 *
 * Span IDs are 128 bits in total.  The upper 64 bits of a span ID is the same
 * as the upper 64 bits of the parent span, if there is one.  The lower 64 bits
 * are always random.
 */
public final class SpanId implements Comparable<SpanId> {
  private static final int SPAN_ID_STRING_LENGTH = 32;
  private final long high;
  private final long low;

  /**
   * The invalid span ID, which is all zeroes.
   *
   * It is also the "least" span ID in the sense that it is considered
   * smaller than any other span ID.
   */
  public static SpanId INVALID = new SpanId(0, 0);

  private static long nonZeroRand64() {
    while (true) {
      long r = ThreadLocalRandom.current().nextLong();
      if (r != 0) {
        return r;
      }
    }
  }

  public static SpanId fromRandom() {
    return new SpanId(nonZeroRand64(), nonZeroRand64());
  }

  public static SpanId fromString(String str) {
    if (str.length() != SPAN_ID_STRING_LENGTH) {
      throw new RuntimeException("Invalid SpanID string: length was not " +
          SPAN_ID_STRING_LENGTH);
    }
    long high =
      ((Long.parseLong(str.substring(0, 8), 16)) << 32) |
      (Long.parseLong(str.substring(8, 16), 16));
    long low =
      ((Long.parseLong(str.substring(16, 24), 16)) << 32) |
      (Long.parseLong(str.substring(24, 32), 16));
    return new SpanId(high, low);
  }

  public SpanId(long high, long low) {
    this.high = high;
    this.low = low;
  }

  public long getHigh() {
    return high;
  }

  public long getLow() {
    return low;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SpanId)) {
      return false;
    }
    SpanId other = (SpanId)o;
    return ((other.high == high) && (other.low == low));
  }

  @Override
  public int compareTo(SpanId other) {
    int cmp = compareAsUnsigned(high, other.high);
    if (cmp != 0) {
      return cmp;
    }
    return compareAsUnsigned(low, other.low);
  }

  private static int compareAsUnsigned(long a, long b) {
    boolean aSign = a < 0;
    boolean bSign = b < 0;
    if (aSign != bSign) {
      if (aSign) {
        return 1;
      } else {
        return -1;
      }
    }
    if (aSign) {
      a = -a;
      b = -b;
    }
    if (a < b) {
      return -1;
    } else if (a > b) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public int hashCode() {
    return (int)((0xffffffff & (high >> 32))) ^
           (int)((0xffffffff & (high >> 0))) ^
           (int)((0xffffffff & (low >> 32))) ^
           (int)((0xffffffff & (low >> 0)));
  }

  @Override
  public String toString() {
    return String.format("%08x%08x%08x%08x",
        (0x00000000ffffffffL & (high >> 32)),
        (0x00000000ffffffffL & high),
        (0x00000000ffffffffL & (low >> 32)),
        (0x00000000ffffffffL & low));
  }

  public boolean isValid() {
    return (high != 0)  || (low != 0);
  }

  public SpanId newChildId() {
    return new SpanId(high, nonZeroRand64());
  }
}
