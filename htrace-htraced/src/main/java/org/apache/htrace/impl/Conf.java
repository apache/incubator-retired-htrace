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
import java.io.File;
import java.net.InetSocketAddress;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.htrace.core.HTraceConfiguration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The configuration of the HTracedSpanReceiver.
 *
 * This class extracts all the relevant configuration information for
 * HTracedSpanReceiver from the HTraceConfiguration.  It performs parsing and
 * bounds-checking for the configuration keys.
 *
 * It is more efficient to store the configuration as final values in this
 * structure than to access the HTraceConfiguration object directly.  This is
 * especially true when the HTraceConfiguration object is a thin shim around a
 * Hadoop Configuration object, which requires synchronization to access.
 */
class Conf {
  private static final Log LOG = LogFactory.getLog(Conf.class);

  /**
   * Address of the htraced server.
   */
  final static String ADDRESS_KEY =
      "htraced.receiver.address";

  /**
   * The minimum number of milliseconds to wait for a read or write
   * operation on the network.
   */
  final static String IO_TIMEOUT_MS_KEY =
      "htraced.receiver.io.timeout.ms";
  final static int IO_TIMEOUT_MS_DEFAULT = 60000;
  final static int IO_TIMEOUT_MS_MIN = 50;

  /**
   * The minimum number of milliseconds to wait for a network
   * connection attempt.
   */
  final static String CONNECT_TIMEOUT_MS_KEY =
      "htraced.receiver.connect.timeout.ms";
  final static int CONNECT_TIMEOUT_MS_DEFAULT = 60000;
  final static int CONNECT_TIMEOUT_MS_MIN = 50;

  /**
   * The minimum number of milliseconds to keep alive a connection when it's
   * not in use.
   */
  final static String IDLE_TIMEOUT_MS_KEY =
      "htraced.receiver.idle.timeout.ms";
  final static int IDLE_TIMEOUT_MS_DEFAULT = 60000;
  final static int IDLE_TIMEOUT_MS_MIN = 0;

  /**
   * Configure the retry times to use when an attempt to flush spans to
   * htraced fails.  This is configured as a comma-separated list of delay
   * times in milliseconds.  If the configured value is empty, no retries
   * will be made.
   */
  final static String FLUSH_RETRY_DELAYS_KEY =
      "htraced.flush.retry.delays.key";
  final static String FLUSH_RETRY_DELAYS_DEFAULT =
      "1000,30000";

  /**
   * The maximum length of time to go in between flush attempts.
   * Once this time elapses, a flush will be triggered even if we don't
   * have that many spans buffered.
   */
  final static String MAX_FLUSH_INTERVAL_MS_KEY =
      "htraced.receiver.max.flush.interval.ms";
  final static int MAX_FLUSH_INTERVAL_MS_DEFAULT = 60000;
  final static int MAX_FLUSH_INTERVAL_MS_MIN = 10;

  /**
   * Whether or not to use msgpack for span serialization.
   * If this key is false, JSON over REST will be used.
   * If this key is true, msgpack over custom RPC will be used.
   */
  final static String PACKED_KEY =
      "htraced.receiver.packed";
  final static boolean PACKED_DEFAULT = true;

  /**
   * The size of the span buffers.
   */
  final static String BUFFER_SIZE_KEY =
      "htraced.receiver.buffer.size";
  final static int BUFFER_SIZE_DEFAULT = 48 * 1024 * 1024;
  static int BUFFER_SIZE_MIN = 4 * 1024 * 1024;
  // The maximum buffer size should not be longer than
  // PackedBuffer.MAX_HRPC_BODY_LENGTH.
  final static int BUFFER_SIZE_MAX = 63 * 1024 * 1024;

  /**
   * Set the fraction of the span buffer which needs to fill up before we
   * will automatically trigger a flush.  This is a fraction, not a percentage.
   * It is between 0 and 1.
   */
  final static String BUFFER_SEND_TRIGGER_FRACTION_KEY =
      "htraced.receiver.buffer.send.trigger.fraction";
  final static double BUFFER_SEND_TRIGGER_FRACTION_DEFAULT = 0.5;
  final static double BUFFER_SEND_TRIGGER_FRACTION_MIN = 0.1;

  /**
   * The length of time which receiveSpan should wait for a free spot in a
   * span buffer before giving up and dropping the span
   */
  final static String SPAN_DROP_TIMEOUT_MS_KEY =
      "htraced.max.buffer.full.retry.ms.key";
  final static int SPAN_DROP_TIMEOUT_MS_DEFAULT = 5000;

  /**
   * The length of time we should wait between displaying log messages on the
   * rate-limited loggers.
   */
  final static String ERROR_LOG_PERIOD_MS_KEY =
      "htraced.error.log.period.ms";
  final static long ERROR_LOG_PERIOD_MS_DEFAULT = 30000L;

  final static String DROPPED_SPANS_LOG_PATH_KEY =
      "htraced.dropped.spans.log.path";

  final static String DROPPED_SPANS_LOG_PATH_DEFAULT =
      new File(System.getProperty("java.io.tmpdir", "/tmp"), "htraceDropped").
        getAbsolutePath();

  final static String DROPPED_SPANS_LOG_MAX_SIZE_KEY =
      "htraced.dropped.spans.log.max.size";

  final static long DROPPED_SPANS_LOG_MAX_SIZE_DEFAULT = 1024L * 1024L;

  @JsonProperty("ioTimeoutMs")
  final int ioTimeoutMs;

  @JsonProperty("connectTimeoutMs")
  final int connectTimeoutMs;

  @JsonProperty("idleTimeoutMs")
  final int idleTimeoutMs;

  @JsonProperty("flushRetryDelays")
  final int[] flushRetryDelays;

  @JsonProperty("maxFlushIntervalMs")
  final int maxFlushIntervalMs;

  @JsonProperty("packed")
  final boolean packed;

  @JsonProperty("bufferSize")
  final int bufferSize;

  @JsonProperty("spanDropTimeoutMs")
  final int spanDropTimeoutMs;

  @JsonProperty("errorLogPeriodMs")
  final long errorLogPeriodMs;

  @JsonProperty("triggerSize")
  final int triggerSize;

  @JsonProperty("endpointStr")
  final String endpointStr;

  @JsonProperty("endpoint")
  final InetSocketAddress endpoint;

  @JsonProperty("droppedSpansLogPath")
  final String droppedSpansLogPath;

  @JsonProperty("droppedSpansLogMaxSize")
  final long droppedSpansLogMaxSize;

  private static int getBoundedInt(final HTraceConfiguration conf,
        String key, int defaultValue, int minValue, int maxValue) {
    int val = conf.getInt(key, defaultValue);
    if (val < minValue) {
      LOG.warn("Can't set " + key + " to " + val + ".  Using minimum value " +
          "of " + minValue + " instead.");
      return minValue;
    } else if (val > maxValue) {
      LOG.warn("Can't set " + key + " to " + val + ".  Using maximum value " +
          "of " + maxValue + " instead.");
      return maxValue;
    }
    return val;
  }

  private static long getBoundedLong(final HTraceConfiguration conf,
        String key, long defaultValue, long minValue, long maxValue) {
    String strVal = conf.get(key, Long.toString(defaultValue));
    long val = 0;
    try {
      val = Long.parseLong(strVal);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Bad value for '" + key +
        "': should be long");
    }
    if (val < minValue) {
      LOG.warn("Can't set " + key + " to " + val + ".  Using minimum value " +
          "of " + minValue + " instead.");
      return minValue;
    } else if (val > maxValue) {
      LOG.warn("Can't set " + key + " to " + val + ".  Using maximum value " +
          "of " + maxValue + " instead.");
      return maxValue;
    }
    return val;
  }

  private static double getBoundedDouble(final HTraceConfiguration conf,
        String key, double defaultValue, double minValue, double maxValue) {
    String strVal = conf.get(key, Double.toString(defaultValue));
    double val = 0;
    try {
      val = Double.parseDouble(strVal);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Bad value for '" + key +
        "': should be double");
    }
    if (val < minValue) {
      LOG.warn("Can't set " + key + " to " + val + ".  Using minimum value " +
          "of " + minValue + " instead.");
      return minValue;
    }
    if (val > maxValue) {
      LOG.warn("Can't set " + key + " to " + val + ".  Using maximum value " +
          "of " + maxValue + " instead.");
      return maxValue;
    }
    return val;
  }

  private static int parseColonPort(String portStr) throws IOException {
    int colonPosition = portStr.indexOf(':');
    if (colonPosition != 0) {
      throw new IOException("Invalid port string " + portStr);
    }
    int port = Integer.parseInt(portStr.substring(1), 10);
    if ((port < 0) || (port > 65535)) {
      throw new IOException("Invalid port number " + port);
    }
    return port;
  }

  /**
   * Parse a hostname:port or ip:port pair.
   *
   * @param str       The string to parse.
   * @return          The socket address.
   */
  InetSocketAddress parseHostPortPair(String str) throws IOException {
    str = str.trim();
    if (str.isEmpty()) {
      throw new IOException("No hostname:port pair given.");
    }
    int bracketBegin = str.indexOf('[');
    if (bracketBegin == 0) {
      // Parse an ipv6-style address enclosed in square brackets.
      int bracketEnd = str.indexOf(']');
      if (bracketEnd < 0) {
        throw new IOException("Found left bracket, but no corresponding " +
            "right bracket, in " + str);
      }
      String host = str.substring(bracketBegin + 1, bracketEnd);
      int port = parseColonPort(str.substring(bracketEnd + 1));
      return InetSocketAddress.createUnresolved(host, port);
    } else if (bracketBegin > 0) {
        throw new IOException("Found a left bracket that wasn't at the " +
          "start of the host:port pair in " + str);
    } else {
      int colon = str.indexOf(':');
      if (colon <= 0) {
        throw new IOException("No port component found in " + str);
      }
      String host = str.substring(0, colon);
      int port = parseColonPort(str.substring(colon));
      return InetSocketAddress.createUnresolved(host, port);
    }
  }

  static int[] getIntArray(String arrayStr) {
    String[] array = arrayStr.split(",");
    int nonEmptyEntries = 0;
    for (String str : array) {
      if (!str.trim().isEmpty()) {
        nonEmptyEntries++;
      }
    }
    int[] ret = new int[nonEmptyEntries];
    int i = 0;
    for (String str : array) {
      if (!str.trim().isEmpty()) {
        ret[i++] = Integer.parseInt(str);
      }
    }
    return ret;
  }

  Conf(HTraceConfiguration conf) throws IOException {
    this.ioTimeoutMs = getBoundedInt(conf, IO_TIMEOUT_MS_KEY,
              IO_TIMEOUT_MS_DEFAULT,
              IO_TIMEOUT_MS_MIN, Integer.MAX_VALUE);
    this.connectTimeoutMs = getBoundedInt(conf, CONNECT_TIMEOUT_MS_KEY,
              CONNECT_TIMEOUT_MS_DEFAULT,
              CONNECT_TIMEOUT_MS_MIN, Integer.MAX_VALUE);
    this.idleTimeoutMs = getBoundedInt(conf, IDLE_TIMEOUT_MS_KEY,
              IDLE_TIMEOUT_MS_DEFAULT,
              IDLE_TIMEOUT_MS_MIN, Integer.MAX_VALUE);
    this.flushRetryDelays = getIntArray(conf.get(FLUSH_RETRY_DELAYS_KEY,
              FLUSH_RETRY_DELAYS_DEFAULT));
    this.maxFlushIntervalMs = getBoundedInt(conf, MAX_FLUSH_INTERVAL_MS_KEY,
              MAX_FLUSH_INTERVAL_MS_DEFAULT,
              MAX_FLUSH_INTERVAL_MS_MIN, Integer.MAX_VALUE);
    this.packed = conf.getBoolean(PACKED_KEY, PACKED_DEFAULT);
    this.bufferSize = getBoundedInt(conf, BUFFER_SIZE_KEY,
              BUFFER_SIZE_DEFAULT,
              BUFFER_SIZE_MIN, BUFFER_SIZE_MAX);
    double triggerFraction = getBoundedDouble(conf,
              BUFFER_SEND_TRIGGER_FRACTION_KEY,
              BUFFER_SEND_TRIGGER_FRACTION_DEFAULT,
              BUFFER_SEND_TRIGGER_FRACTION_MIN, 1.0);
    this.spanDropTimeoutMs = conf.getInt(SPAN_DROP_TIMEOUT_MS_KEY,
        SPAN_DROP_TIMEOUT_MS_DEFAULT);
    this.errorLogPeriodMs = getBoundedLong(conf, ERROR_LOG_PERIOD_MS_KEY,
        ERROR_LOG_PERIOD_MS_DEFAULT, 0, Long.MAX_VALUE);
    this.triggerSize = (int)(this.bufferSize * triggerFraction);
    try {
      this.endpointStr = conf.get(ADDRESS_KEY, "");
      this.endpoint = parseHostPortPair(endpointStr);
    } catch (IOException e) {
      throw new IOException("Error reading " + ADDRESS_KEY + ": " +
          e.getMessage());
    }
    this.droppedSpansLogPath = conf.get(
        DROPPED_SPANS_LOG_PATH_KEY, DROPPED_SPANS_LOG_PATH_DEFAULT);
    this.droppedSpansLogMaxSize = getBoundedLong(conf,
        DROPPED_SPANS_LOG_MAX_SIZE_KEY, DROPPED_SPANS_LOG_MAX_SIZE_DEFAULT,
        0, Long.MAX_VALUE);
  }

  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
