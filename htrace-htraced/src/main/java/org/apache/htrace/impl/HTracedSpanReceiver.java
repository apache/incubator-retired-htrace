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

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
//import java.nio.file.attribute.FileAttribute;
//import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.htrace.core.HTraceConfiguration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanReceiver;

/**
 * The SpanReceiver which sends spans to htraced.
 *
 * HTracedSpanReceiver sends trace spans out to the htraced daemon, where they
 * are stored and indexed.  It supports two forms of RPC: a JSON/HTTP form, and
 * an HRPC/msgpack form.  We will use the msgpack form when
 * htraced.receiver.packed is set to true.
 *
 * HTraced buffers are several megabytes in size, and we reuse them to avoid
 * creating extra garbage on the heap.  They are flushed whenever a timeout
 * elapses, or when they get more than a configurable percent full.  We allocate
 * two buffers so that we can continue filling one buffer while the other is
 * being sent over the wire.  The buffers store serialized spans.  This is
 * better than storing references to span objects because it minimzes the amount
 * of pointers we have to follow during a GC.  Buffers are managed by instances
 * of BufferManager.
 */
public class HTracedSpanReceiver extends SpanReceiver {
  private static final Log LOG = LogFactory.getLog(HTracedSpanReceiver.class);

  private final static int MAX_CLOSING_WAIT_MS = 120000;

  private final FaultInjector faultInjector;

  private final Conf conf;

  private final ReentrantLock lock = new ReentrantLock();

  private final Condition wakePostSpansThread = lock.newCondition();

  private final BufferManager bufferManager[] = new BufferManager[2];

  private final RateLimitedLogger flushErrorLog;

  private final RateLimitedLogger spanDropLog;

  private final PostSpansThread thread;

  private boolean shutdown = false;

  private int activeBuf = 0;

  private int flushingBuf = -1;

  private long lastBufferClearedTimeMs = 0;

  private long unbufferableSpans = 0;

  static class FaultInjector {
    static FaultInjector NO_OP = new FaultInjector();
    public void handleContentLengthTrigger(int len) { }
    public void handleThreadStart() throws Exception { }
    public void handleFlush() throws IOException { }
  }

  public HTracedSpanReceiver(HTraceConfiguration c) throws Exception {
    this(c, FaultInjector.NO_OP);
  }

  HTracedSpanReceiver(HTraceConfiguration c,
      FaultInjector faultInjector) throws Exception {
    this.faultInjector = faultInjector;
    this.conf = new Conf(c);
    if (this.conf.packed) {
      for (int i = 0; i < bufferManager.length; i++) {
        bufferManager[i] = new PackedBufferManager(conf);
      }
    } else {
      for (int i = 0; i < bufferManager.length; i++) {
        bufferManager[i] = new RestBufferManager(conf);
      }
    }
    this.flushErrorLog = new RateLimitedLogger(LOG, conf.errorLogPeriodMs);
    this.spanDropLog = new RateLimitedLogger(LOG, conf.errorLogPeriodMs);
    this.thread = new PostSpansThread();
    LOG.debug("Created new HTracedSpanReceiver with " + conf.toString());
  }

  @Override
  public void receiveSpan(Span span) {
    long startTimeMs = 0;
    int numTries = 1;
    while (true) {
      lock.lock();
      try {
        if (shutdown) {
          LOG.info("Unable to add span because HTracedSpanReceiver is shutting down.");
          return;
        }
        Throwable exc = null;
        try {
          bufferManager[activeBuf].writeSpan(span);
          int contentLength = bufferManager[activeBuf].contentLength();
          if (contentLength > conf.triggerSize) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Triggering buffer #" + activeBuf + " flush because" +
                  " buffer contains " + contentLength + " bytes, and " +
                  "triggerSize is " + conf.triggerSize);
            }
            faultInjector.handleContentLengthTrigger(contentLength);
            wakePostSpansThread.signal();
          }
          return;
        } catch (Exception e) {
          exc = e;
        } catch (Error e) {
          exc = e;
        }
        if (startTimeMs == 0) {
          startTimeMs = TimeUtil.nowMs();
        }
        long deltaMs = TimeUtil.deltaMs(startTimeMs, TimeUtil.nowMs());
        if (deltaMs > conf.spanDropTimeoutMs) {
          StringBuilder bld = new StringBuilder();
          spanDropLog.error("Dropping a span after unsuccessfully " +
              "attempting to add it for " + deltaMs + " ms.  There is not " +
              "enough buffer space. Please increase " + Conf.BUFFER_SIZE_KEY +
              " or decrease the rate of spans being generated.");
          unbufferableSpans++;
          return;
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("Unable to write span to buffer #" + activeBuf +
              " after " + numTries + " attempt(s) and " + deltaMs + " ms" +
              ".  Buffer already has " +
                  bufferManager[activeBuf].getNumberOfSpans() + " spans.",
              exc);
        }
        numTries++;
      } finally {
        lock.unlock();
      }
      try {
        Thread.sleep(conf.spanDropTimeoutMs / 3);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void close() {
    lock.lock();
    try {
      shutdown = true;
      wakePostSpansThread.signal();
    } finally {
      lock.unlock();
    }
    try {
      thread.join(MAX_CLOSING_WAIT_MS);
    } catch (InterruptedException e) {
      LOG.error("HTracedSpanReceiver#close was interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  private class PostSpansThread extends Thread {
    PostSpansThread() {
      this.setDaemon(true);
      this.setName("PostSpans");
      this.start();
    }

    private boolean shouldWaitForCond(long timeSinceLastClearedMs) {
      if (shutdown) {
        // If we're shutting down, don't wait around.
        LOG.trace("Should not wait for cond because we're shutting down.");
        return false;
      }
      int contentLength = bufferManager[activeBuf].contentLength();
      if (contentLength == 0) {
        // If there is nothing in the buffer, there is nothing to do.
        if (LOG.isTraceEnabled()) {
          LOG.trace("Should wait for cond because we have no data buffered " +
              "in bufferManager " + activeBuf);
        }
        lastBufferClearedTimeMs = TimeUtil.nowMs();
        return true;
      } else if (contentLength >= conf.triggerSize) {
        // If the active buffer is filling up, start flushing.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Should not wait for cond because we have more than " +
              conf.triggerSize + " bytes buffered in bufferManager " +
              activeBuf);
        }
        return false;
      }
      if (timeSinceLastClearedMs > conf.maxFlushIntervalMs) {
        // If we have let the spans sit in the buffer for too long,
        // start flushing.
        if (LOG.isTraceEnabled()) {
          LOG.trace("Should not wait for cond because it has been " +
              timeSinceLastClearedMs + " ms since our last flush, and we " +
              "are overdue for another because maxFlushIntervalMs is " +
              conf.maxFlushIntervalMs);
        }
        return false;
      }
      LOG.trace("Should wait for cond.");
      return true;
    }

    @Override
    public void run() {
      try {
        faultInjector.handleThreadStart();
        LOG.debug("Starting HTracedSpanReceiver thread for " +
            conf.endpointStr);
        BufferManager flushBufManager = null;
        while (true) {
          lock.lock();
          flushingBuf = -1;
          try {
            while (true) {
              long timeSinceLastClearedMs = TimeUtil.
                deltaMs(lastBufferClearedTimeMs, TimeUtil.nowMs());
              if (!shouldWaitForCond(timeSinceLastClearedMs)) {
                break;
              }
              long waitMs = conf.maxFlushIntervalMs -
                  Math.min(conf.maxFlushIntervalMs, TimeUtil.
                      deltaMs(TimeUtil.nowMs(), lastBufferClearedTimeMs));
              if (LOG.isTraceEnabled()) {
                LOG.trace("Waiting on wakePostSpansThread for " + waitMs +
                    " ms.");
              }
              try {
                wakePostSpansThread.await(waitMs, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                LOG.info("HTraceSpanReceiver thread was interrupted.", e);
                throw e;
              }
            }
            if (shutdown && (bufferManager[activeBuf].contentLength() == 0)) {
              LOG.debug("PostSpansThread shutting down.");
              return;
            }
            flushingBuf = activeBuf;
            flushBufManager = bufferManager[flushingBuf];
            activeBuf = (activeBuf == 1) ? 0 : 1;
          } finally {
            lock.unlock();
          }
          doFlush(flushBufManager);
          flushBufManager.clear();
          lastBufferClearedTimeMs = TimeUtil.nowMs();
          if (LOG.isTraceEnabled()) {
            LOG.trace("setting lastBufferClearedTimeMs to " + lastBufferClearedTimeMs);
          }
        }
      } catch (Throwable e) {
        LOG.error("PostSpansThread exiting on unexpected exception", e);
      } finally {
        for (int i = 0; i < bufferManager.length; i++) {
          bufferManager[i].close();
        }
      }
    }

    private void doFlush(BufferManager flushBufManager)
        throws InterruptedException {
      try {
        flushBufManager.prepare();
      } catch (IOException e) {
        LOG.error("Failed to prepare buffer containing " +
            flushBufManager.getNumberOfSpans() + " spans for " +
            "sending to " + conf.endpointStr + " Discarding " +
            "all spans.", e);
        return;
      }
      int flushTries = 0;
      if (unbufferableSpans > 0) {
        try {
          appendToDroppedSpansLog("Dropped " + unbufferableSpans +
              " spans because of lack of local buffer space.\n");
        } catch (IOException e) {
          // Ignore.  We already logged a message about the dropped spans
          // earlier.
        }
        unbufferableSpans = 0;
      }
      while (true) {
        Throwable exc;
        try {
          faultInjector.handleFlush();
          flushBufManager.flush();
          exc = null;
        } catch (RuntimeException e) {
          exc = e;
        } catch (Exception e) {
          exc = e;
        }
        if (exc == null) {
          return;
        }
        int numSpans = flushBufManager.getNumberOfSpans();
        flushErrorLog.error("Failed to flush " + numSpans  + " htrace " +
            "spans to " + conf.endpointStr + " on try " + (flushTries + 1),
            exc);
        if (flushTries >= conf.flushRetryDelays.length) {
          StringBuilder bld = new StringBuilder();
          bld.append("Failed to flush ").append(numSpans).
            append(" spans to htraced at").append(conf.endpointStr).
            append(" after ").append(flushTries).append(" tries: ").
            append(exc.getMessage());
          try {
            appendToDroppedSpansLog(bld.toString());
          } catch (IOException e) {
            bld.append(".  Failed to write to dropped spans log: ").
              append(e.getMessage());
          }
          spanDropLog.error(bld.toString());
          return;
        }
        int delayMs = conf.flushRetryDelays[flushTries];
        Thread.sleep(delayMs);
        flushTries++;
      }
    }
  }

  void appendToDroppedSpansLog(String text) throws IOException {
    // Is the dropped spans log is disabled?
    if (conf.droppedSpansLogPath.isEmpty() ||
        (conf.droppedSpansLogMaxSize == 0)) {
      return;
    }
    FileLock lock = null;
    ByteBuffer bb = ByteBuffer.wrap(
        text.getBytes(StandardCharsets.UTF_8));
    // FileChannel locking corresponds to advisory locking on UNIX.  It will
    // protect multiple processes from attempting to write to the same dropped
    // spans log at once.  However, within a single process, we need this
    // synchronized block to ensure that multiple HTracedSpanReceiver objects
    // don't try to write to the same log at once.  (It is unusal to configure
    // multiple HTracedSpanReceiver objects, but possible.)
    synchronized(HTracedSpanReceiver.class) {
      FileChannel channel = FileChannel.open(
          Paths.get(conf.droppedSpansLogPath), APPEND, CREATE, WRITE);
      try {
        lock = channel.lock();
        long size = channel.size();
        if (size > conf.droppedSpansLogMaxSize) {
          throw new IOException("Dropped spans log " +
              conf.droppedSpansLogPath + " is already " + size +
              " bytes; will not add to it.");
        }
        channel.write(bb);
      } finally {
        try {
          if (lock != null) {
            lock.release();
          }
        } finally {
          channel.close();
        }
      }
    }
  }
}
