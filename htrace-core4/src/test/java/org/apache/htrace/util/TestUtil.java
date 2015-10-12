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
package org.apache.htrace.util;

import org.apache.htrace.core.MilliSpan;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TimelineAnnotation;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utilities for writing unit tests.
 */
public class TestUtil {
  /**
   * Get a dump of the stack traces of all threads.
   */
  public static String threadDump() {
    StringBuilder dump = new StringBuilder();
    Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
    for (Map.Entry<Thread, StackTraceElement[]> e : stackTraces.entrySet()) {
      Thread thread = e.getKey();
      dump.append(String.format(
          "\"%s\" %s prio=%d tid=%d %s\njava.lang.Thread.State: %s",
          thread.getName(),
          (thread.isDaemon() ? "daemon" : ""),
          thread.getPriority(),
          thread.getId(),
          Thread.State.WAITING.equals(thread.getState()) ?
              "in Object.wait()" : thread.getState().name().toLowerCase(),
          Thread.State.WAITING.equals(thread.getState()) ?
              "WAITING (on object monitor)" : thread.getState()));
      for (StackTraceElement stackTraceElement : e.getValue()) {
        dump.append("\n        at ");
        dump.append(stackTraceElement);
      }
      dump.append("\n");
    }
    return dump.toString();
  }

  /**
   * A callback which returns a value of type T.
   *
   * TODO: remove this when we're on Java 8, in favor of
   * java.util.function.Supplier.
   */
  public interface Supplier<T> {
    T get();
  }

  /**
   * Wait for a condition to become true for a configurable amount of time.
   *
   * @param check           The condition to wait for.
   * @param periodMs        How often to check the condition, in milliseconds.
   * @param timeoutMs       How long to wait in total, in milliseconds.
   */
  public static void waitFor(Supplier<Boolean> check, 
      long periodMs, long timeoutMs)
          throws TimeoutException, InterruptedException
  {
    long endNs = System.nanoTime() +
        TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);
    while (true) {
      boolean result = check.get();
      if (result) {
        return;
      }
      long nowNs = System.nanoTime();
      if (nowNs >= endNs) {
        throw new TimeoutException("Timed out waiting for test condition. " +
            "Thread dump:\n" + threadDump());
      }
      Thread.sleep(periodMs);
    }
  }

  private static long nonZeroRandomLong(Random rand) {
    long r = 0;
    do {
      r = rand.nextLong();
    } while (r == 0);
    return r;
  }

  private static long positiveRandomLong(Random rand) {
    long r = rand.nextLong();
    if (r == Long.MIN_VALUE) {
      // Math.abs can't handle this case
      return Long.MAX_VALUE;
    } else if (r > 0) {
      return r;
    } else {
      return -r;
    }
  }

  private static String randomString(Random rand) {
    return new UUID(positiveRandomLong(rand),
          positiveRandomLong(rand)).toString();
  }

  public static Span randomSpan(Random rand) {
    MilliSpan.Builder builder = new MilliSpan.Builder();
    builder.spanId(
          new SpanId(nonZeroRandomLong(rand), nonZeroRandomLong(rand)));
    builder.begin(positiveRandomLong(rand));
    builder.end(positiveRandomLong(rand));
    builder.description(randomString(rand));
    builder.tracerId(randomString(rand));
    int numParents = rand.nextInt(4);
    SpanId[] parents = new SpanId[numParents];
    for (int i = 0; i < numParents; i++) {
      parents[i] =
          new SpanId(nonZeroRandomLong(rand), nonZeroRandomLong(rand));
    }
    builder.parents(parents);
    int numTraceInfos = rand.nextInt(4);
    Map<String, String> traceInfo = new HashMap<String, String>(numTraceInfos);
    for (int i = 0; i < numTraceInfos; i++) {
      traceInfo.put(randomString(rand), randomString(rand));
    }
    builder.traceInfo(traceInfo);
    int numTimelineAnnotations = rand.nextInt(4);
    List<TimelineAnnotation> timeline =
        new LinkedList<TimelineAnnotation>();
    for (int i = 0; i < numTimelineAnnotations; i++) {
      timeline.add(new TimelineAnnotation(positiveRandomLong(rand),
            randomString(rand)));
    }
    builder.timeline(timeline);
    return builder.build();
  }

  public static Span[] randomSpans(Random rand, int numSpans) {
    Span[] spans = new Span[numSpans];
    for (int i = 0; i < spans.length; i++) {
      spans[i] = randomSpan(rand);
    }
    return spans;
  }
}
