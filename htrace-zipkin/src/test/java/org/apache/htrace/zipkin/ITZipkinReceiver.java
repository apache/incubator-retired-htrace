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

package org.apache.htrace.zipkin;


import com.twitter.zipkin.gen.Span;

import org.I0Itec.zkclient.ZkClient;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.apache.htrace.core.TracerPool;
import org.apache.htrace.impl.KafkaTransport;
import org.apache.htrace.impl.ZipkinSpanReceiver;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;

public class ITZipkinReceiver {

  @Test
  public void testKafkaTransport() throws Exception {

    String topic = "zipkin";
    // Kafka setup
    EmbeddedZookeeper zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
    ZkClient zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
    Properties props = TestUtils.createBrokerConfig(0, TestUtils.choosePort(), false);
    KafkaConfig config = new KafkaConfig(props);
    KafkaServer kafkaServer = TestUtils.createServer(config, new MockTime());

    Buffer<KafkaServer> servers = JavaConversions.asScalaBuffer(Collections.singletonList(kafkaServer));
    TestUtils.createTopic(zkClient, topic, 1, 1, servers, new Properties());
    zkClient.close();
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0, 5000);

    // HTrace
    HTraceConfiguration hTraceConfiguration = HTraceConfiguration.fromKeyValuePairs(
        "sampler.classes", "AlwaysSampler",
        "span.receiver.classes", ZipkinSpanReceiver.class.getName(),
        "zipkin.kafka.metadata.broker.list", config.advertisedHostName() + ":" + config.advertisedPort(),
        "zipkin.kafka.topic", topic,
        ZipkinSpanReceiver.TRANSPORT_CLASS_KEY, KafkaTransport.class.getName()
    );

    final Tracer tracer = new Tracer.Builder("test-tracer")
        .tracerPool(new TracerPool("test-tracer-pool"))
        .conf(hTraceConfiguration)
        .build();

    String scopeName = "test-kafka-transport-scope";
    TraceScope traceScope = tracer.newScope(scopeName);
    traceScope.close();
    tracer.close();

    // Kafka consumer
    Properties consumerProps = new Properties();
    consumerProps.put("zookeeper.connect", props.getProperty("zookeeper.connect"));
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testing.group");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");
    ConsumerConnector connector =
        kafka.consumer.Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(consumerProps));
    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(topic, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topicCountMap);
    ConsumerIterator<byte[], byte[]> it = streams.get(topic).get(0).iterator();

    // Test
    Assert.assertTrue("We should have one message in Kafka", it.hasNext());
    Span span = new Span();
    new TDeserializer(new TBinaryProtocol.Factory()).deserialize(span, it.next().message());
    Assert.assertEquals("The span name should match our scope description", span.getName(), scopeName);

    kafkaServer.shutdown();

  }

}
