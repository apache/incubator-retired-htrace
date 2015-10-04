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

package org.apache.htrace.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.Transport;
import org.apache.htrace.core.HTraceConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaTransport implements Transport {

  private static final Log LOG = LogFactory.getLog(KafkaTransport.class);
  private static final String DEFAULT_TOPIC = "zipkin";
  public static final String TOPIC_KEY = "zipkin.kafka.topic";

  Producer<byte[], byte[]> producer;
  private boolean isOpen = false;
  private String topic;

  /**
   * Opens a new Kafka transport
   * @param conf Transport configuration. Some Kafka producer configurations
   *             can be passed by prefixing the config key with zipkin.kafka
   *             (e.g. zipkin.kafka.producer.type for producer.type)
   * @throws IOException if an I/O error occurs
   * @throws IllegalStateException if transport is already open
   */
  @Override
  public void open(HTraceConfiguration conf) throws IOException,
                                                    IllegalStateException {
    if (!isOpen()) {
      topic = conf.get(TOPIC_KEY, DEFAULT_TOPIC);
      producer = newProducer(conf);
      isOpen = true;
    } else {
      LOG.warn("Attempted to open an already opened transport");
    }
  }

  @Override
  public boolean isOpen() {
    return isOpen;
  }

  @Override
  public void send(List<byte[]> spans) throws IOException {

    List<KeyedMessage<byte[], byte[]>> entries = new ArrayList<>(spans.size());

    for (byte[] span : spans) {
      entries.add(new KeyedMessage<byte[], byte[]>(topic, span));
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("sending " + entries.size() + " entries");
    }
    producer.send(entries);
  }

  @Override
  public void close() throws IOException {
    if (isOpen) {
      producer.close();
      isOpen = false;
    } else {
      LOG.warn("Attempted to close already closed transport");
    }
  }

  public Producer<byte[], byte[]> newProducer(HTraceConfiguration conf) {
    // https://kafka.apache.org/083/configuration.html
    Properties producerProps = new Properties();
    // Essential producer configurations
    producerProps.put("metadata.broker.list",
                      conf.get("zipkin.kafka.metadata.broker.list", "localhost:9092"));
    producerProps.put("request.required.acks",
                      conf.get("zipkin.kafka.request.required.acks", "0"));
    producerProps.put("producer.type",
                      conf.get("zipkin.kafka.producer.type", "async"));
    producerProps.put("serializer.class",
                      conf.get("zipkin.kafka.serializer.class", "kafka.serializer.DefaultEncoder"));
    producerProps.put("compression.codec",
                      conf.get("zipkin.kafka.compression.codec", "1"));

    Producer<byte[], byte[]> producer = new Producer<>(new ProducerConfig(producerProps));
    LOG.info("Connected to Kafka transport \n" +  producerProps);
    return producer;
  }

}
