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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TracerId;
import org.apache.htrace.impl.HTracedSpanReceiver.FaultInjector;
import org.apache.htrace.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestHTracedReceiver {
  private static final Log LOG = LogFactory.getLog(TestHTracedReceiver.class);

  @BeforeClass
  public static void beforeClass() {
    // Allow setting really small buffer sizes for testing purposes.
    // We do not allow setting such small sizes in production.
    Conf.BUFFER_SIZE_MIN = 0;
  }

  @Rule
  public TestRule watcher = new TestWatcher() {
    protected void starting(Description description) {
      LOG.info("*** Starting junit test: " + description.getMethodName());
    }

    protected void finished(Description description) {
      LOG.info("*** Finished junit test: " + description.getMethodName());
    }
  };

  @Test(timeout = 60000)
  public void testGetServerInfoJson() throws Exception {
    HTracedProcess ht = new HTracedProcess.Builder().build();
    try {
      String response = ht.getServerInfoJson();
      assertTrue(response.contains("ReleaseVersion"));
    } finally {
      ht.destroy();
    }
  }

  private void waitForSpans(final HTracedProcess ht, Span[] spans)
      throws Exception {
    waitForSpans(ht, spans, spans.length);
  }

  private void waitForSpans(final HTracedProcess ht, Span[] spans,
      int numSpans) throws Exception {
    final LinkedList<SpanId> spanIds = new LinkedList<SpanId>();
    for (int i = 0; i < numSpans; i++) {
      spanIds.add(spans[i].getSpanId());
    }
    boolean success = false;
    try {
      TestUtil.waitFor(new TestUtil.Supplier<Boolean>() {
        @Override
        public Boolean get() {
          for (Iterator<SpanId> iter = spanIds.iterator();
               iter.hasNext(); ) {
            SpanId spanId = iter.next();
            try {
              if (ht.getSpan(spanId) == null) {
                return false;
              }
            } catch (InterruptedException e) {
              LOG.error("Got InterruptedException while looking for " +
                  "span ID " + spanId, e);
              Thread.currentThread().interrupt();
            } catch (Exception e) {
              LOG.error("Got error looking for span ID " + spanId, e);
              return false;
            }
            iter.remove();
          }
          return true;
        }
      }, 10, 30000);
      success = true;
    } finally {
      if (!success) {
        String prefix = "";
        StringBuilder idStringBld = new StringBuilder();
        for (Iterator<SpanId> iter = spanIds.iterator();
             iter.hasNext(); ) {
          idStringBld.append(prefix);
          idStringBld.append(iter.next());
          prefix = ",";
        }
        LOG.error("Unable to find span IDs " + idStringBld.toString());
      }
    }
  }

  /**
   * Test that we can send spans via the HRPC interface.
   */
  @Test(timeout = 10000) //60000)
  public void testSendSpansViaPacked() throws Exception {
    final Random rand = new Random(123);
    final HTracedProcess ht = new HTracedProcess.Builder().build();
    try {
      HTraceConfiguration conf = HTraceConfiguration.fromMap(
          new HashMap<String, String>() {{
            put(TracerId.TRACER_ID_KEY, "testSendSpansViaPacked");
            put(Conf.ADDRESS_KEY, ht.getHrpcAddr());
            put(Conf.PACKED_KEY, "true");
            put(Conf.MAX_FLUSH_INTERVAL_MS_KEY, "100");
            put(Conf.ERROR_LOG_PERIOD_MS_KEY, "0");
          }});
      HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf);
      Span[] spans = TestUtil.randomSpans(rand, 10);
      for (Span span : spans) {
        rcvr.receiveSpan(span);
      }
      waitForSpans(ht, spans);
      rcvr.close();
    } finally {
      ht.destroy();
    }
  }

  /**
   * Test that when the SpanReceiver is closed, we send any spans we have
   * buffered via the HRPC interface.
   */
  @Test(timeout = 60000)
  public void testSendSpansViaPackedAndClose() throws Exception {
    final Random rand = new Random(456);
    final HTracedProcess ht = new HTracedProcess.Builder().build();
    try {
      HTraceConfiguration conf = HTraceConfiguration.fromMap(
          new HashMap<String, String>() {{
            put(TracerId.TRACER_ID_KEY, "testSendSpansViaPackedAndClose");
            put(Conf.ADDRESS_KEY, ht.getHrpcAddr());
            put(Conf.PACKED_KEY, "true");
            put(Conf.MAX_FLUSH_INTERVAL_MS_KEY, "60000");
          }});
      HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf);
      Span[] spans = TestUtil.randomSpans(rand, 10);
      for (Span span : spans) {
        rcvr.receiveSpan(span);
      }
      rcvr.close();
      waitForSpans(ht, spans);
    } finally {
      ht.destroy();
    }
  }

  /**
   * Test that we can send spans via the REST interface.
   */
  @Test(timeout = 60000)
  public void testSendSpansViaRest() throws Exception {
    final Random rand = new Random(789);
    final HTracedProcess ht = new HTracedProcess.Builder().build();
    try {
      HTraceConfiguration conf = HTraceConfiguration.fromMap(
          new HashMap<String, String>() {{
            put(TracerId.TRACER_ID_KEY, "testSendSpansViaRest");
            put(Conf.ADDRESS_KEY, ht.getHttpAddr());
            put(Conf.PACKED_KEY, "false");
            put(Conf.MAX_FLUSH_INTERVAL_MS_KEY, "100");
          }});
      HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf);
      Span[] spans = TestUtil.randomSpans(rand, 10);
      for (Span span : spans) {
        rcvr.receiveSpan(span);
      }
      waitForSpans(ht, spans);
      rcvr.close();
    } finally {
      ht.destroy();
    }
  }

  /**
   * Test that when the SpanReceiver is closed, we send any spans we have
   * buffered via the REST interface.
   */
  @Test(timeout = 60000)
  public void testSendSpansViaRestAndClose() throws Exception {
    final Random rand = new Random(321);
    final HTracedProcess ht = new HTracedProcess.Builder().build();
    try {
      HTraceConfiguration conf = HTraceConfiguration.fromMap(
          new HashMap<String, String>() {{
            put(TracerId.TRACER_ID_KEY, "testSendSpansViaRestAndClose");
            put(Conf.ADDRESS_KEY, ht.getHttpAddr());
            put(Conf.PACKED_KEY, "false");
            put(Conf.MAX_FLUSH_INTERVAL_MS_KEY, "60000");
          }});
      HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf);
      Span[] spans = TestUtil.randomSpans(rand, 10);
      for (Span span : spans) {
        rcvr.receiveSpan(span);
      }
      rcvr.close();
      waitForSpans(ht, spans);
    } finally {
      ht.destroy();
    }
  }

  private static class Mutable<T> {
    private T t;

    Mutable(T t) {
      this.t = t;
    }

    void set(T t) {
      this.t = t;
    }

    T get() {
      return this.t;
    }
  }

  private static class TestHandleContentLengthTriggerInjector
      extends HTracedSpanReceiver.FaultInjector {
    final Semaphore threadStartSem = new Semaphore(0);
    int contentLengthOnTrigger = 0;

    @Override
    public synchronized void handleContentLengthTrigger(int len) {
      contentLengthOnTrigger = len;
    }
    @Override
    public void handleThreadStart() throws Exception {
      threadStartSem.acquire();
    }

    public synchronized int getContentLengthOnTrigger() {
      return contentLengthOnTrigger;
    }
  }

  /**
   * Test that filling up one of the buffers causes us to trigger a flush and
   * start using the other buffer, when using PackedBufferManager.
   * This also tests that PackedBufferManager can correctly handle a buffer
   * getting full.
   */
  @Test(timeout = 60000)
  public void testFullBufferCausesPackedThreadTrigger() throws Exception {
    final Random rand = new Random(321);
    final HTracedProcess ht = new HTracedProcess.Builder().build();
    try {
      HTraceConfiguration conf = HTraceConfiguration.fromMap(
          new HashMap<String, String>() {{
            put(TracerId.TRACER_ID_KEY,
                "testFullBufferCausesPackedThreadTrigger");
            put(Conf.ADDRESS_KEY, ht.getHrpcAddr());
            put(Conf.PACKED_KEY, "true");
            put(Conf.BUFFER_SIZE_KEY, "16384");
            put(Conf.BUFFER_SEND_TRIGGER_FRACTION_KEY, "0.95");
          }});
      TestHandleContentLengthTriggerInjector injector =
          new TestHandleContentLengthTriggerInjector();
      HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf, injector);
      Span[] spans = TestUtil.randomSpans(rand, 47);
      for (Span span : spans) {
        rcvr.receiveSpan(span);
      }
      Assert.assertTrue("The wakePostSpansThread should have been " +
          "triggered by the spans added so far.  " +
          "contentLengthOnTrigger = " + injector.getContentLengthOnTrigger(),
          injector.getContentLengthOnTrigger() > 16000);
      injector.threadStartSem.release();
      rcvr.close();
      waitForSpans(ht, spans, 45);
    } finally {
      ht.destroy();
    }
  }

  /**
   * Test that filling up one of the buffers causes us to trigger a flush and
   * start using the other buffer, when using RestBufferManager.
   * This also tests that RestBufferManager can correctly handle a buffer
   * getting full.
   */
  @Test(timeout = 60000)
  public void testFullBufferCausesRestThreadTrigger() throws Exception {
    final Random rand = new Random(321);
    final HTracedProcess ht = new HTracedProcess.Builder().build();
    try {
      HTraceConfiguration conf = HTraceConfiguration.fromMap(
          new HashMap<String, String>() {{
            put(TracerId.TRACER_ID_KEY,
                "testFullBufferCausesRestThreadTrigger");
            put(Conf.ADDRESS_KEY, ht.getHttpAddr());
            put(Conf.PACKED_KEY, "false");
            put(Conf.BUFFER_SIZE_KEY, "16384");
            put(Conf.BUFFER_SEND_TRIGGER_FRACTION_KEY, "0.95");
          }});
      TestHandleContentLengthTriggerInjector injector =
          new TestHandleContentLengthTriggerInjector();
      HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf, injector);
      Span[] spans = TestUtil.randomSpans(rand, 34);
      for (Span span : spans) {
        rcvr.receiveSpan(span);
      }
      Assert.assertTrue("The wakePostSpansThread should have been " +
              "triggered by the spans added so far.  " +
              "contentLengthOnTrigger = " + injector.getContentLengthOnTrigger(),
          injector.getContentLengthOnTrigger() > 16000);
      injector.threadStartSem.release();
      rcvr.close();
      waitForSpans(ht, spans, 33);
    } finally {
      ht.destroy();
    }
  }

  /**
   * A FaultInjector that causes all flushes to fail until a specified
   * number of milliseconds have passed.
   */
  private static class TestInjectFlushFaults
      extends HTracedSpanReceiver.FaultInjector {
    private long remainingFaults;

    TestInjectFlushFaults(long remainingFaults) {
      this.remainingFaults = remainingFaults;
    }

    @Override
    public synchronized void handleFlush() throws IOException {
      if (remainingFaults > 0) {
        remainingFaults--;
        throw new IOException("Injected IOException into flush " +
            "code path.");
      }
    }
  }

  /**
   * Test that even if the flush fails, the system stays stable and we can
   * still close the span receiver.
   */
  @Test(timeout = 60000)
  public void testPackedThreadHandlesFlushFailure() throws Exception {
    final Random rand = new Random(321);
    final HTracedProcess ht = new HTracedProcess.Builder().build();
    try {
      HTraceConfiguration conf = HTraceConfiguration.fromMap(
          new HashMap<String, String>() {{
            put(TracerId.TRACER_ID_KEY, "testPackedThreadHandlesFlushFailure");
            put(Conf.ADDRESS_KEY, ht.getHrpcAddr());
            put(Conf.PACKED_KEY, "true");
          }});
      TestInjectFlushFaults injector = new TestInjectFlushFaults(Long.MAX_VALUE);
      HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf, injector);
      Span[] spans = TestUtil.randomSpans(rand, 15);
      for (Span span : spans) {
        rcvr.receiveSpan(span);
      }
      rcvr.close();
    } finally {
      ht.destroy();
    }
  }

  /**
   * Test that even if the flush fails, the system stays stable and we can
   * still close the span receiver.
   */
  @Test(timeout = 60000)
  public void testRestThreadHandlesFlushFailure() throws Exception {
    final Random rand = new Random(321);
    final HTracedProcess ht = new HTracedProcess.Builder().build();
    try {
      HTraceConfiguration conf = HTraceConfiguration.fromMap(
          new HashMap<String, String>() {{
            put(TracerId.TRACER_ID_KEY, "testRestThreadHandlesFlushFailure");
            put(Conf.ADDRESS_KEY, ht.getHttpAddr());
            put(Conf.PACKED_KEY, "false");
          }});
      TestInjectFlushFaults injector = new TestInjectFlushFaults(Long.MAX_VALUE);
      HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf, injector);
      Span[] spans = TestUtil.randomSpans(rand, 15);
      for (Span span : spans) {
        rcvr.receiveSpan(span);
      }
      rcvr.close();
    } finally {
      ht.destroy();
    }
  }

  /**
   * A FaultInjector that causes all flushes to fail until a specified
   * number of milliseconds have passed.
   */
  private static class WaitForFlushes
      extends HTracedSpanReceiver.FaultInjector {
    final Semaphore flushSem;

    WaitForFlushes(int numFlushes) {
      this.flushSem = new Semaphore(-numFlushes);
    }

    @Override
    public void handleFlush() throws IOException {
      flushSem.release();
    }
  }

  /**
   * Test that the packed code works when performing multiple flushes.
   */
  @Test(timeout = 60000)
  public void testMultiplePackedFlushes() throws Exception {
    final Random rand = new Random(123);
    final HTracedProcess ht = new HTracedProcess.Builder().build();
    try {
      HTraceConfiguration conf = HTraceConfiguration.fromMap(
          new HashMap<String, String>() {{
            put(TracerId.TRACER_ID_KEY, "testMultiplePackedFlushes");
            put(Conf.ADDRESS_KEY, ht.getHrpcAddr());
            put(Conf.PACKED_KEY, "true");
            put(Conf.MAX_FLUSH_INTERVAL_MS_KEY, "1");
          }});
      WaitForFlushes injector = new WaitForFlushes(5);
      HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf, injector);
      Span[] spans = TestUtil.randomSpans(rand, 3);
      while (true) {
        for (Span span : spans) {
          rcvr.receiveSpan(span);
        }
        if (injector.flushSem.availablePermits() >= 0) {
          break;
        }
        Thread.sleep(1);
      }
      waitForSpans(ht, spans, 3);
      rcvr.close();
    } finally {
      ht.destroy();
    }
  }

  /**
   * Test that the REST code works when performing multiple flushes.
   */
  @Test(timeout = 60000)
  public void testMultipleRestFlushes() throws Exception {
    final Random rand = new Random(123);
    final HTracedProcess ht = new HTracedProcess.Builder().build();
    try {
      HTraceConfiguration conf = HTraceConfiguration.fromMap(
          new HashMap<String, String>() {{
            put(TracerId.TRACER_ID_KEY, "testMultipleRestFlushes");
            put(Conf.ADDRESS_KEY, ht.getHttpAddr());
            put(Conf.PACKED_KEY, "false");
            put(Conf.MAX_FLUSH_INTERVAL_MS_KEY, "1");
          }});
      WaitForFlushes injector = new WaitForFlushes(5);
      HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf, injector);
      Span[] spans = TestUtil.randomSpans(rand, 3);
      while (true) {
        for (Span span : spans) {
          rcvr.receiveSpan(span);
        }
        if (injector.flushSem.availablePermits() >= 0) {
          break;
        }
        Thread.sleep(1);
      }
      waitForSpans(ht, spans, 3);
      rcvr.close();
    } finally {
      ht.destroy();
    }
  }

  /**
   * Test that the packed code works when performing multiple flushes.
   */
  @Test(timeout = 60000)
  public void testPackedRetryAfterFlushError() throws Exception {
    final Random rand = new Random(123);
    final HTracedProcess ht = new HTracedProcess.Builder().build();
    try {
      HTraceConfiguration conf = HTraceConfiguration.fromMap(
          new HashMap<String, String>() {{
            put(TracerId.TRACER_ID_KEY, "testPackedRetryAfterFlushError");
            put(Conf.ADDRESS_KEY, ht.getHrpcAddr());
            put(Conf.PACKED_KEY, "true");
            put(Conf.MAX_FLUSH_INTERVAL_MS_KEY, "1000");
            put(Conf.FLUSH_RETRY_DELAYS_KEY, "100,100,100,100,100,100,100");
          }});
      TestInjectFlushFaults injector = new TestInjectFlushFaults(5);
      HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf, injector);
      Span[] spans = TestUtil.randomSpans(rand, 3);
      for (Span span : spans) {
        rcvr.receiveSpan(span);
      }
      waitForSpans(ht, spans);
      rcvr.close();
    } finally {
      ht.destroy();
    }
  }

  /**
   * Test that the REST code works when performing multiple flushes.
   */
  @Test(timeout = 60000)
  public void testRestRetryAfterFlushError() throws Exception {
    final Random rand = new Random(123);
    final HTracedProcess ht = new HTracedProcess.Builder().build();
    try {
      HTraceConfiguration conf = HTraceConfiguration.fromMap(
          new HashMap<String, String>() {{
            put(TracerId.TRACER_ID_KEY, "testRestRetryAfterFlushError");
            put(Conf.ADDRESS_KEY, ht.getHttpAddr());
            put(Conf.PACKED_KEY, "false");
            put(Conf.MAX_FLUSH_INTERVAL_MS_KEY, "1000");
            put(Conf.FLUSH_RETRY_DELAYS_KEY, "100,100,100,100,100,100,100");
          }});
      TestInjectFlushFaults injector = new TestInjectFlushFaults(5);
      HTracedSpanReceiver rcvr = new HTracedSpanReceiver(conf, injector);
      Span[] spans = TestUtil.randomSpans(rand, 3);
      for (Span span : spans) {
        rcvr.receiveSpan(span);
      }
      waitForSpans(ht, spans);
      rcvr.close();
    } finally {
      ht.destroy();
    }
  }
}
