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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Writes the spans it receives to a local file.
 */
public class LocalFileSpanReceiver implements SpanReceiver {
  private static final Log LOG = LogFactory.getLog(LocalFileSpanReceiver.class);
  public static final String PATH_KEY = "local-file-span-receiver.path";
  public static final String CAPACITY_KEY = "local-file-span-receiver.capacity";
  public static final int CAPACITY_DEFAULT = 5000;
  private static ObjectWriter JSON_WRITER = new ObjectMapper().writer();
  private final String path;

  private byte[][] bufferedSpans;
  private int bufferedSpansIndex;
  private final ReentrantLock bufferLock = new ReentrantLock();

  private final FileOutputStream stream;
  private final FileChannel channel;
  private final ReentrantLock channelLock = new ReentrantLock();
  private final TracerId tracerId;

  public LocalFileSpanReceiver(HTraceConfiguration conf) {
    int capacity = conf.getInt(CAPACITY_KEY, CAPACITY_DEFAULT);
    if (capacity < 1) {
      throw new IllegalArgumentException(CAPACITY_KEY + " must not be " +
          "less than 1.");
    }
    this.path = conf.get(PATH_KEY);
    if (path == null || path.isEmpty()) {
      throw new IllegalArgumentException("must configure " + PATH_KEY);
    }
    boolean success = false;
    try {
      this.stream = new FileOutputStream(path, true);
    } catch (IOException ioe) {
      LOG.error("Error opening " + path + ": " + ioe.getMessage());
      throw new RuntimeException(ioe);
    }
    this.channel = stream.getChannel();
    if (this.channel == null) {
      try {
        this.stream.close();
      } catch (IOException e) {
        LOG.error("Error closing " + path, e);
      }
      LOG.error("Failed to get channel for " + path);
      throw new RuntimeException("Failed to get channel for " + path);
    }
    this.bufferedSpans = new byte[capacity][];
    this.bufferedSpansIndex = 0;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created new LocalFileSpanReceiver with path = " + path +
                ", capacity = " + capacity);
    }
    this.tracerId = new TracerId(conf);
  }

  /**
   * Number of buffers to use in FileChannel#write.
   *
   * On UNIX, FileChannel#write uses writev-- a kernel interface that allows
   * us to send multiple buffers at once.  This is more efficient than making a
   * separate write call for each buffer, since it minimizes the number of
   * transitions from userspace to kernel space.
   */
  private final int WRITEV_SIZE = 20;

  private final static ByteBuffer newlineBuf = 
      ByteBuffer.wrap(new byte[] { (byte)0xa });

  /**
   * Flushes a bufferedSpans array.
   */
  private void doFlush(byte[][] toFlush, int len) throws IOException {
    int bidx = 0, widx = 0;
    ByteBuffer writevBufs[] = new ByteBuffer[2 * WRITEV_SIZE];

    while (true) {
      if (widx == writevBufs.length) {
        channel.write(writevBufs);
        widx = 0;
      }
      if (bidx == len) {
        break;
      }
      writevBufs[widx] = ByteBuffer.wrap(toFlush[bidx]);
      writevBufs[widx + 1] = newlineBuf;
      bidx++;
      widx+=2;
    }
    if (widx > 0) {
      channel.write(writevBufs, 0, widx);
    }
  }

  @Override
  public void receiveSpan(Span span) {
    if (span.getTracerId().isEmpty()) {
      span.setTracerId(tracerId.get());
    }

    // Serialize the span data into a byte[].  Note that we're not holding the
    // lock here, to improve concurrency.
    byte jsonBuf[] = null;
    try {
      jsonBuf = JSON_WRITER.writeValueAsBytes(span);
    } catch (JsonProcessingException e) {
        LOG.error("receiveSpan(path=" + path + ", span=" + span + "): " +
                  "Json processing error: " + e.getMessage());
      return;
    }

    // Grab the bufferLock and put our jsonBuf into the list of buffers to
    // flush. 
    byte toFlush[][] = null;
    bufferLock.lock();
    try {
      if (bufferedSpans == null) {
        LOG.debug("receiveSpan(path=" + path + ", span=" + span + "): " +
                  "LocalFileSpanReceiver for " + path + " is closed.");
        return;
      }
      bufferedSpans[bufferedSpansIndex] = jsonBuf;
      bufferedSpansIndex++;
      if (bufferedSpansIndex == bufferedSpans.length) {
        // If we've hit the limit for the number of buffers to flush, 
        // swap out the existing bufferedSpans array for a new array, and
        // prepare to flush those spans to disk.
        toFlush = bufferedSpans;
        bufferedSpansIndex = 0;
        bufferedSpans = new byte[bufferedSpans.length][];
      }
    } finally {
      bufferLock.unlock();
    }
    if (toFlush != null) {
      // We released the bufferLock above, to avoid blocking concurrent
      // receiveSpan calls.  But now, we must take the channelLock, to make
      // sure that we have sole access to the output channel.  If we did not do
      // this, we might get interleaved output.
      //
      // There is a small chance that another thread doing a flush of more
      // recent spans could get ahead of us here, and take the lock before we
      // do.  This is ok, since spans don't have to be written out in order.
      channelLock.lock();
      try {
        doFlush(toFlush, toFlush.length);
      } catch (IOException ioe) {
        LOG.error("Error flushing buffers to " + path + ": " +
            ioe.getMessage());
      } finally {
        channelLock.unlock();
      }
    }
  }

  @Override
  public void close() throws IOException {
    byte toFlush[][] = null;
    int numToFlush = 0;
    bufferLock.lock();
    try {
      if (bufferedSpans == null) {
        LOG.info("LocalFileSpanReceiver for " + path + " was already closed.");
        return;
      }
      numToFlush = bufferedSpansIndex;
      bufferedSpansIndex = 0;
      toFlush = bufferedSpans;
      bufferedSpans = null;
    } finally {
      bufferLock.unlock();
    }
    channelLock.lock();
    try {
      doFlush(toFlush, numToFlush);
    } catch (IOException ioe) {
      LOG.error("Error flushing buffers to " + path + ": " +
          ioe.getMessage());
    } finally {
      try {
        stream.close();
      } catch (IOException e) {
        LOG.error("Error closing stream for " + path, e);
      }
      channelLock.unlock();
    }
  }

  public static String getUniqueLocalTraceFileName() {
    String tmp = System.getProperty("java.io.tmpdir", "/tmp");
    String nonce = null;
    BufferedReader reader = null;
    try {
      // On Linux we can get a unique local file name by reading the process id
      // out of /proc/self/stat.  (There isn't any portable way to get the
      // process ID from Java.)
      reader = new BufferedReader(
          new InputStreamReader(new FileInputStream("/proc/self/stat"),
                                "UTF-8"));
      String line = reader.readLine();
      if (line == null) {
        throw new EOFException();
      }
      nonce = line.split(" ")[0];
    } catch (IOException e) {
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch(IOException e) {
          LOG.warn("Exception in closing " + reader, e);
        }
      }
    }
    if (nonce == null) {
      // If we can't use the process ID, use a random nonce.
      nonce = UUID.randomUUID().toString();
    }
    return new File(tmp, nonce).getAbsolutePath();
  }
}
