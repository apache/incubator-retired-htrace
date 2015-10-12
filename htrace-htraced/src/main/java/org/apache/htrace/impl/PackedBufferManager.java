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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.nio.charset.StandardCharsets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.htrace.core.Span;

class PackedBufferManager implements BufferManager {
  private static final Log LOG = LogFactory.getLog(PackedBuffer.class);
  private static final int MAX_PREQUEL_LENGTH = 2048;
  private static final int METHOD_ID_WRITE_SPANS = 0x1;
  private final Conf conf;
  private final ByteBuffer frameBuffer;
  private final PackedBuffer prequel;
  private final PackedBuffer spans;
  private final Selector selector;
  private int numSpans;

  PackedBufferManager(Conf conf) throws IOException {
    this.conf = conf;
    this.frameBuffer = ByteBuffer.allocate(PackedBuffer.HRPC_REQ_FRAME_LENGTH);
    this.prequel = new PackedBuffer(ByteBuffer.allocate(MAX_PREQUEL_LENGTH));
    this.spans = new PackedBuffer(ByteBuffer.allocate(conf.bufferSize));
    this.selector = SelectorProvider.provider().openSelector();
    clear();
  }

  @Override
  public void writeSpan(Span span) throws IOException {
    spans.writeSpan(span);
    numSpans++;
    if (LOG.isTraceEnabled()) {
      LOG.trace("wrote " + span.toJson() + " to PackedBuffer for " +
          conf.endpointStr + ". numSpans = " + numSpans +
          ", buffer position = " + spans.getBuffer().position());
    }
  }

  @Override
  public int contentLength() {
    return spans.getBuffer().position();
  }

  @Override
  public int getNumberOfSpans() {
    return numSpans;
  }

  @Override
  public void prepare() throws IOException {
    prequel.beginWriteSpansRequest(null, numSpans);
    long totalLength =
        prequel.getBuffer().position() + spans.getBuffer().position();
    if (totalLength > PackedBuffer.MAX_HRPC_BODY_LENGTH) {
      throw new IOException("Can't send RPC of " + totalLength + " bytes " +
          "because it is longer than " + PackedBuffer.MAX_HRPC_BODY_LENGTH);
    }
    PackedBuffer.writeReqFrame(frameBuffer,
        METHOD_ID_WRITE_SPANS, 1, (int)totalLength);
    frameBuffer.flip();
    prequel.getBuffer().flip();
    spans.getBuffer().flip();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Preparing to send RPC of length " +
          (totalLength + PackedBuffer.HRPC_REQ_FRAME_LENGTH) + " to " +
          conf.endpointStr + ", containing " + numSpans + " spans.");
    }
  }

  @Override
  public void flush() throws IOException {
    SelectionKey sockKey = null;
    IOException ioe = null;
    frameBuffer.position(0);
    prequel.getBuffer().position(0);
    spans.getBuffer().position(0);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Preparing to flush " + numSpans + " spans to " +
          conf.endpointStr);
    }
    try {
      sockKey = doConnect();
      doSend(sockKey, new ByteBuffer[] {
          frameBuffer, prequel.getBuffer(), spans.getBuffer() });
      ByteBuffer response = prequel.getBuffer();
      readAndValidateResponseFrame(sockKey, response,
          1, METHOD_ID_WRITE_SPANS);
    } catch (IOException e) {
      // This LOG message is only at debug level because we also log these
      // exceptions at error level inside HTracedReceiver.  The logging in
      // HTracedReceiver is rate-limited to avoid overwhelming the client log
      // if htraced goes down.  The debug and trace logging is not
      // rate-limited.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got exception during flush", e);
      }
      ioe = e;
    } finally {
      if (sockKey != null) {
        sockKey.cancel();
        try {
          SocketChannel sock = (SocketChannel)sockKey.attachment();
          sock.close();
        } catch (IOException e) {
          if (ioe != null) {
            ioe.addSuppressed(e);
          }
        }
      }
    }
    if (ioe != null) {
      throw ioe;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Successfully flushed " + numSpans + " spans to " +
          conf.endpointStr);
    }
  }

  private long updateRemainingMs(long startMs, long timeoutMs) {
    long deltaMs = TimeUtil.deltaMs(startMs, TimeUtil.nowMs());
    if (deltaMs > timeoutMs) {
      return 0;
    }
    return timeoutMs - deltaMs;
  }

  private SelectionKey doConnect() throws IOException {
    SocketChannel sock = SocketChannel.open();
    SelectionKey sockKey = null;
    boolean success = false;
    try {
      if (sock.isBlocking()) {
        sock.configureBlocking(false);
      }
      InetSocketAddress resolvedEndpoint =
          new InetSocketAddress(conf.endpoint.getHostString(),
              conf.endpoint.getPort());
      resolvedEndpoint.getHostName(); // trigger DNS resolution
      sock.connect(resolvedEndpoint);
      sockKey = sock.register(selector, SelectionKey.OP_CONNECT, sock);
      long startMs = TimeUtil.nowMs();
      long remainingMs = conf.connectTimeoutMs;
      while (true) {
        selector.select(remainingMs);
        for (SelectionKey key : selector.keys()) {
          if (key.isConnectable()) {
            SocketChannel s = (SocketChannel)key.attachment();
            s.finishConnect();
            if (LOG.isTraceEnabled()) {
              LOG.trace("Successfully connected to " + conf.endpointStr + ".");
            }
            success = true;
            return sockKey;
          }
        }
        remainingMs = updateRemainingMs(startMs, conf.connectTimeoutMs);
        if (remainingMs == 0) {
          throw new IOException("Attempt to connect to " + conf.endpointStr +
              " timed out after " +  TimeUtil.deltaMs(startMs, TimeUtil.nowMs()) +
              " ms.");
        }
      }
    } finally {
      if (!success) {
        if (sockKey != null) {
          sockKey.cancel();
        }
        sock.close();
      }
    }
  }

  /**
   * Send the provided ByteBuffer objects.
   *
   * We use non-blocking I/O because Java does not provide write timeouts.
   * Without a write timeout, the socket could get hung and we'd never recover.
   * We also use the GatheringByteChannel#write method which calls the pread()
   * system call under the covers.  This ensures that even if TCP_NODELAY is on,
   * we send the minimal number of packets.
   */
  private void doSend(SelectionKey sockKey, ByteBuffer[] bufs)
        throws IOException {
    long totalWritten = 0;
    sockKey.interestOps(SelectionKey.OP_WRITE);
    SocketChannel sock = (SocketChannel)sockKey.attachment();
    long startMs = TimeUtil.nowMs();
    long remainingMs = conf.ioTimeoutMs;
    while (true) {
      selector.select(remainingMs);
      int firstBuf = 0;
      for (SelectionKey key : selector.selectedKeys()) {
        if (key.isWritable()) {
          long written = sock.write(bufs, firstBuf, bufs.length - firstBuf);
          if (LOG.isTraceEnabled()) {
            LOG.trace("Sent " + written + " bytes to " + conf.endpointStr);
          }
          totalWritten += written;
        }
      }
      while (true) {
        if (firstBuf == bufs.length) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Finished sending " + totalWritten + " bytes to " +
                conf.endpointStr);
          }
          return;
        }
        if (bufs[firstBuf].remaining() > 0) {
          break;
        }
        firstBuf++;
      }
      remainingMs = updateRemainingMs(startMs, conf.ioTimeoutMs);
      if (remainingMs == 0) {
        throw new IOException("Attempt to write to " + conf.endpointStr +
            " timed out after " + TimeUtil.deltaMs(startMs, TimeUtil.nowMs()) +
            " ms.");
      }
    }
  }

  private void doRecv(SelectionKey sockKey, ByteBuffer response)
      throws IOException {
    sockKey.interestOps(SelectionKey.OP_READ);
    SocketChannel sock = (SocketChannel)sockKey.attachment();
    int totalRead = response.remaining();
    long startMs = TimeUtil.nowMs();
    long remainingMs = conf.ioTimeoutMs;
    while (remainingMs > 0) {
      selector.select(remainingMs);
      for (SelectionKey key : selector.selectedKeys()) {
        if (key.isReadable()) {
          sock.read(response);
        }
      }
      if (response.remaining() == 0) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Received all " + totalRead + " bytes from " +
              conf.endpointStr);
        }
        return;
      }
      remainingMs = updateRemainingMs(startMs, conf.ioTimeoutMs);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Received " + (totalRead - response.remaining()) +
                " out of " + totalRead + " bytes from " + conf.endpointStr);
      }
      if (remainingMs == 0) {
        throw new IOException("Attempt to write to " + conf.endpointStr +
            " timed out after " + TimeUtil.deltaMs(startMs, TimeUtil.nowMs()) +
            " ms.");
      }
    }
  }

  private void readAndValidateResponseFrame(SelectionKey sockKey,
        ByteBuffer buf, long expectedSeq, int expectedMethodId)
          throws IOException {
    buf.clear();
    buf.limit(PackedBuffer.HRPC_RESP_FRAME_LENGTH);
    doRecv(sockKey, buf);
    buf.flip();
    buf.order(ByteOrder.LITTLE_ENDIAN);
    long seq = buf.getLong();
    if (seq != expectedSeq) {
      throw new IOException("Expected sequence number " + expectedSeq +
          ", but got sequence number " + seq);
    }
    int methodId = buf.getInt();
    if (expectedMethodId != methodId) {
      throw new IOException("Expected method id " + expectedMethodId +
          ", but got " + methodId);
    }
    int errorLength = buf.getInt();
    buf.getInt();
    if ((errorLength < 0) ||
        (errorLength > PackedBuffer.MAX_HRPC_ERROR_LENGTH)) {
      throw new IOException("Got server error with invalid length " +
          errorLength);
    } else if (errorLength > 0) {
      buf.clear();
      buf.limit(errorLength);
      doRecv(sockKey, buf);
      buf.flip();
      CharBuffer charBuf = StandardCharsets.UTF_8.decode(buf);
      String serverErrorStr = charBuf.toString();
      throw new IOException("Got server error " + serverErrorStr);
    }
  }

  @Override
  public void clear() {
    frameBuffer.clear();
    prequel.getBuffer().clear();
    spans.getBuffer().clear();
    numSpans = 0;
  }

  @Override
  public void close() {
    clear();
    prequel.close();
    spans.close();
    try {
      selector.close();
    } catch (IOException e) {
      LOG.warn("Error closing selector", e);
    }
  }
}
