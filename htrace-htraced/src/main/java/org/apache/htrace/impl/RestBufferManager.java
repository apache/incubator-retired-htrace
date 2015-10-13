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
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.htrace.core.Span;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;

class RestBufferManager implements BufferManager {
  private static final Log LOG = LogFactory.getLog(RestBufferManager.class);
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final byte COMMA_BYTE = (byte)0x2c;
  private static final int MAX_PREQUEL_LENGTH = 512;
  private static final int MAX_EPILOGUE_LENGTH = 32;
  private final Conf conf;
  private final HttpClient httpClient;
  private final String urlString;
  private final ByteBuffer prequel;
  private final ByteBuffer spans;
  private final ByteBuffer epilogue;
  private int numSpans;

  private static class RestBufferManagerContentProvider
      implements ContentProvider {
    private final ByteBuffer[] bufs;

    private class ByteBufferIterator implements Iterator<ByteBuffer> {
      private int bufIdx = -1;

      @Override
      public boolean hasNext() {
        return (bufIdx + 1) < bufs.length;
      }

      @Override
      public ByteBuffer next() {
        if ((bufIdx + 1) >= bufs.length) {
          throw new NoSuchElementException();
        }
        bufIdx++;
        return bufs[bufIdx];
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }

    RestBufferManagerContentProvider(ByteBuffer[] bufs) {
      this.bufs = bufs;
    }

    @Override
    public long getLength() {
      long total = 0;
      for (int i = 0; i < bufs.length; i++) {
        total += bufs[i].remaining();
      }
      return total;
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
      return new ByteBufferIterator();
    }
  }

  /**
   * Create an HttpClient instance.
   *
   * @param connTimeout         The timeout to use for connecting.
   * @param idleTimeout         The idle timeout to use.
   */
  static HttpClient createHttpClient(long connTimeout, long idleTimeout) {
    HttpClient httpClient = new HttpClient();
    httpClient.setUserAgentField(
        new HttpField(HttpHeader.USER_AGENT, "HTracedSpanReceiver"));
    httpClient.setConnectTimeout(connTimeout);
    httpClient.setIdleTimeout(idleTimeout);
    return httpClient;
  }

  RestBufferManager(Conf conf) throws Exception {
    this.conf = conf;
    this.httpClient =
        createHttpClient(conf.connectTimeoutMs, conf.idleTimeoutMs);
    this.urlString = new URL("http", conf.endpoint.getHostName(),
        conf.endpoint.getPort(), "/writeSpans").toString();
    this.prequel = ByteBuffer.allocate(MAX_PREQUEL_LENGTH);
    this.spans = ByteBuffer.allocate(conf.bufferSize);
    this.epilogue = ByteBuffer.allocate(MAX_EPILOGUE_LENGTH);
    clear();
    this.httpClient.start();
  }

  @Override
  public void writeSpan(Span span) throws IOException {
    byte[] spanJsonBytes = span.toString().getBytes(UTF8);
    if ((spans.capacity() - spans.position()) < (spanJsonBytes.length + 1)) {
      // Make sure we have enough space for the span JSON and a comma.
      throw new IOException("Not enough space remaining in span buffer.");
    }
    spans.put(COMMA_BYTE);
    spans.put(spanJsonBytes);
    numSpans++;
  }

  @Override
  public int contentLength() {
    return Math.max(spans.position() - 1, 0);
  }

  @Override
  public int getNumberOfSpans() {
    return numSpans;
  }

  @Override
  public void prepare() throws IOException {
    String prequelString = "{\"Spans\":[";
    prequel.put(prequelString.getBytes(UTF8));
    prequel.flip();

    spans.flip();

    String epilogueString = "]}";
    epilogue.put(epilogueString.toString().getBytes(UTF8));
    epilogue.flip();

    if (LOG.isTraceEnabled()) {
      LOG.trace("Preparing to send " + contentLength() + " bytes of span " +
          "data to " + conf.endpointStr + ", containing " + numSpans +
          " spans.");
    }
  }

  @Override
  public void flush() throws IOException {
    // Position the buffers at the beginning.
    prequel.position(0);
    spans.position(spans.limit() == 0 ? 0 : 1); // Skip the first comma
    epilogue.position(0);

    RestBufferManagerContentProvider contentProvider =
        new RestBufferManagerContentProvider(
            new ByteBuffer[] { prequel, spans, epilogue });
    long rpcLength = contentProvider.getLength();
    try {
      Request request = httpClient.
          newRequest(urlString).method(HttpMethod.POST);
      request.header(HttpHeader.CONTENT_TYPE, "application/json");
      request.content(contentProvider);
      ContentResponse response = request.send();
      if (response.getStatus() != HttpStatus.OK_200) {
        throw new IOException("Got back error response " +
            response.getStatus() + " from " + conf.endpointStr + "; " +
            response.getContentAsString());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sent WriteSpansReq of length " + rpcLength + " to " + conf.endpointStr);
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while sending spans via REST", e);
    } catch (TimeoutException e) {
      throw new IOException("Timed out sending spans via REST", e);
    } catch (ExecutionException e) {
      throw new IOException("Execution exception sending spans via REST", e);
    }
  }

  @Override
  public void clear() {
    prequel.clear();
    spans.clear();
    epilogue.clear();
    numSpans = 0;
  }

  @Override
  public void close() {
    try {
      httpClient.stop();
    } catch (Exception e) {
      LOG.error("Error stopping HTracedReceiver httpClient", e);
    }
  }
}
