<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# HTrace Configuration

Clearly, HTrace requires configuration.  We need to control which SpanReceiver
is used, what the sampling rate is, and many other things besides.  Luckily, as
we discussed earlier, the Tracer objects maintain this configuration
information for us.  When we ask for a new trace scope, the Tracer knows what
configuration to use.

This configuration comes from the HTraceConfiguration object that we supplied
to the Tracer#Builder earlier.  In general, we want to configure HTrace the
same way we configure anything else in our distributed system.  So we normally
create a subclass of HTraceConfiguration that accesses the appropriate
information in our existing configuration system.

To make this a little more concrete, let`s suppose we are writing Bob`s
Distributed System.  Being a pragmatic (not to mention lazy) guy, Bob has
decided to just use Java configuration properties for configuration.
So our Tracer#Builder invoation would look something like this:

    this.tracer = new Tracer.Builder("Bob").
        conf(new HTraceConfiguration() {
            @Override
            public String get(String key) {
              return System.getProperty("htrace." + key);
            }

            @Override
            public String get(String key, String defaultValue) {
              String ret = get(key);
              return (ret != null) ? ret : defaultValue;
            }
        }).
        build();

You can see that this configuration object maps every property starting in
"htrace." to an htrace property.  So, for example, you would set the Java
system property value "htrace.span.receiver.classes" in order to control the
HTrace configuration key "span.receiver.classes".

Of course, Bob probably should have been less lazy, and used a real
configuration system instead of using Java system properties.  This is just a
toy example to illustrate how to integrate with an existing configuration
system.

Bob might also have wanted to use different prefixes for different Tracer
objects.  For example, in Hadoop you can configure the FsShell Tracer
separately from the NameNode Tracer, by setting
"fs.shell.htrace.span.receiver.classes".  This is easy to control by changing
the HTraceConfiguration object that you pass to Tracer#Builder.

Note that in C and C++, this system is a little different, based on explicitly
creating a configuration object prior to creating a tracer, rather than using a
callback-based system.

##Configuration Keys
In the tables below, programmatic Htrace Java configuration is split by module.

###htrace-core4 configuration

| Key                   | Default Value | Description  | Mandatory | Possible Values |
| --------------------- | :-----------: | :--------- : | :-------: | :-------------: |
| span.receiver.classes | | A [SpanReceiver](https://github.com/apache/incubator-htrace/blob/master/htrace-core4/src/main/java/org/apache/htrace/core/SpanReceiver.java) is a collector within a process that is the destination of Spans when a trace is running. The value should be a comma separated list of classes which extend the abstract SpanReceiver class | yes | [org.apache.htrace.core.StandardOutSpanReceiver](https://github.com/apache/incubator-htrace/blob/master/htrace-core4/src/main/java/org/apache/htrace/core/StandardOutSpanReceiver.java), [org.apache.htrace.core.LocalFileSpanReceiver](https://github.com/apache/incubator-htrace/blob/master/htrace-core4/src/main/java/org/apache/htrace/core/LocalFileSpanReceiver.java), [org.apache.htrace.core.POJOSpanReceiver](https://github.com/apache/incubator-htrace/blob/master/htrace-core4/src/main/java/org/apache/htrace/core/POJOSpanReceiver.java), [org.apache.htrace.impl.FlumeSpanReceiver](https://github.com/apache/incubator-htrace/blob/master/htrace-flume/src/main/java/org/apache/htrace/impl/FlumeSpanReceiver.java), [org.apache.htrace.impl.HBaseSpanReceiver](https://github.com/apache/incubator-htrace/blob/master/htrace-hbase/src/main/java/org/apache/htrace/impl/HBaseSpanReceiver.java), [org.apache.htrace.impl.HTracedSpanReceiver](https://github.com/apache/incubator-htrace/blob/master/htrace-htraced/src/main/java/org/apache/htrace/impl/HTracedSpanReceiver.java), [org.apache.htrace.impl.ZipkinSpanReceiver](https://github.com/apache/incubator-htrace/blob/master/htrace-zipkin/src/main/java/org/apache/htrace/impl/ZipkinSpanReceiver.java)|
| sampler.classes       | | Samplers which extend the [Sampler](https://github.com/apache/incubator-htrace/blob/master/htrace-core4/src/main/java/org/apache/htrace/core/Sampler.java) class determine the frequency that an action should be performed.| yes | [org.apache.htrace.core.AlwaysSampler](https://github.com/apache/incubator-htrace/blob/master/htrace-core4/src/main/java/org/apache/htrace/core/AlwaysSampler.java), [org.apache.htrace.core.CountSampler](https://github.com/apache/incubator-htrace/blob/master/htrace-core4/src/main/java/org/apache/htrace/core/CountSampler.java), [org.apache.htrace.core.NeverSampler](https://github.com/apache/incubator-htrace/blob/master/htrace-core4/src/main/java/org/apache/htrace/core/NeverSampler.java), [org.apache.htrace.core.ProbabilitySampler](https://github.com/apache/incubator-htrace/blob/master/htrace-core4/src/main/java/org/apache/htrace/core/ProbabilitySampler.java) |

###htrace-htraced configuration

Configuration for the [org.apache.htrace.impl.HTracedSpanReceiver](https://github.com/apache/incubator-htrace/blob/master/htrace-htraced/src/main/java/org/apache/htrace/impl/HTracedSpanReceiver.java)

| Key        | Default Value | Description | Mandatory | Possible Values |
| ---------- |:-------------:| :---------: | :-------: | :-------------: |
| htraced.receiver.address |  | Address of the htraced server | yes | an established server and port address |
| htraced.receiver.io.timeout.ms | 60000 | The minimum number of milliseconds to wait for a read or write operation on the network. | no | single integer |
| htraced.receiver.connect.timeout.ms | 60000 | The minimum number of milliseconds to wait for a network connection attempt. | no | single integer |
| htraced.receiver.idle.timeout.ms | 60000 | The minimum number of milliseconds to keep alive a connection when it's not in use.| no | single integer |
| htraced.flush.retry.delays.key | 1000,30000 | Configure the retry times to use when an attempt to flush spans to htraced fails.  This is configured as a comma-separated list of delay times in milliseconds. If the configured value is empty, no retries will be made.| no | two comma separated integers |
| htraced.receiver.max.flush.interval.ms | 60000 | The maximum length of time to go in between flush attempts. Once this time elapses, a flush will be triggered even if we don't have that many spans buffered. | no | single integer |
| htraced.receiver.packed | true | Whether or not to use msgpack for span serialization. If this key is false, JSON over REST will be used. If this key is true, msgpack over custom RPC will be used.| no | true/false |
| htraced.receiver.buffer.size | 16 * 1024 * 1024 | The size of the span buffers. | no | single integer no larger than 32 * 1024 * 1024 |
| htraced.receiver.buffer.send.trigger.fraction | 0.5 | Set the fraction of the span buffer which needs to fill up before we will automatically trigger a flush.  This is a fraction, not a percentage. It is between 0 and 1. | no | single double |
| htraced.max.buffer.full.retry.ms.key | 5000 | The length of time which receiveSpan should wait for a free spot in a span buffer before giving up and dropping the span | no | single integer | 
| htraced.error.log.period.ms | 30000L | The length of time we should wait between displaying log messages on the rate-limited loggers. | no | single integer |
| htraced.dropped.spans.log.path | Absolute path of System.getProperty("java.io.tmpdir", "/tmp") | Path to local disk at which spans should be writtent o disk | no | string path to local disk |
| htraced.dropped.spans.log.max.size | 1024L * 1024L | The maximum size in bytes of a span log file on disk | no | single integer |

###htrace-flume configuration

Configuration for the [org.apache.htrace.impl.FlumeSpanReceiver](https://github.com/apache/incubator-htrace/blob/master/htrace-flume/src/main/java/org/apache/htrace/impl/FlumeSpanReceiver.java)

| Key                   | Default Value | Description  | Mandatory | Possible Values |
| --------------------- | :-----------: | :--------- : | :-------: | :-------------: |
| hadoop.htrace.flume.hostname | localhost | HTTP accessible host at which Flume is available | yes | single string |
| hadoop.htrace.flume.port | 0 | Port on the host at which Flume is available | yes | single integer |
| htrace.flume.num-threads | 1 | The number of threads used to write data from HTrace into Flume | no | single integer |
| htrace.flume.batchsize | 100 | Number of HTrace spans to include in every batch sent to Flume | no | single integer |

In addition, please also see the [htrace-flume documentation](https://github.com/apache/incubator-htrace/tree/master/htrace-flume)

###htrace-hbase configuration

Configuration for the [org.apache.htrace.impl.HBaseSpanReceiver](https://github.com/apache/incubator-htrace/blob/master/htrace-hbase/src/main/java/org/apache/htrace/impl/HBaseSpanReceiver.java)

| Key                   | Default Value | Description  | Mandatory | Possible Values |
| --------------------- | :-----------: | :--------- : | :-------: | :-------------: |
| hbase.htrace.hbase.collector-quorum | 127.0.0.1 | Host at which HBase Zookeeper server is running | no | string |
| hbase.htrace.hbase.zookeeper.property.clientPort | 2181 | Port at which HBase Zookeeper server is running | no | single integer |
| hbase.htrace.hbase.zookeeper.znode.parent | /hbase | The HBase root znode path | no | string |
| htrace.hbase.num-threads | 1 | The number of threads used to write data from HTrace into HBase | no | single integer |
| htrace.hbase.batch.size | 100 | Number of HTrace spans to include in every batch sent to HBase | no | single integer |
| hbase.htrace.hbase.table | htrace | The HBase Table name | no | string |
| hbase.htrace.hbase.columnfamily | s | The HBase column family name | no | string |
| hbase.htrace.hbase.indexfamily | i | The Hbase index family name | no | string |

In addition, please also see the [htrace-hbase documentation](https://github.com/apache/incubator-htrace/tree/master/htrace-hbase)

###htrace-zipkin configuration

Configuration for the [org.apache.htrace.impl.ZipkinSpanReceiver](https://github.com/apache/incubator-htrace/blob/master/htrace-zipkin/src/main/java/org/apache/htrace/impl/ZipkinSpanReceiver.java)

| Key                   | Default Value | Description  | Mandatory | Possible Values |
| --------------------- | :-----------: | :--------- : | :-------: | :-------------: |
| zipkin.transport.class | [org.apache.htrace.impl.ScribeTransport](https://github.com/apache/incubator-htrace/blob/master/htrace-zipkin/src/main/java/org/apache/htrace/impl/ScribeTransport.java) | Implementation of [Transport](https://github.com/apache/incubator-htrace/blob/master/htrace-zipkin/src/main/java/org/apache/htrace/Transport.java) to be used. | no | [org.apache.htrace.impl.ScribeTransport](https://github.com/apache/incubator-htrace/blob/master/htrace-zipkin/src/main/java/org/apache/htrace/impl/ScribeTransport.java), [org.apache.htrace.impl.KafkaTransport](https://github.com/apache/incubator-htrace/blob/master/htrace-zipkin/src/main/java/org/apache/htrace/impl/KafkaTransport.java) |
| zipkin.num-threads | 1 | The number of threads used to write data from HTrace into Zipkin | no | single integer |
| zipkin.traced-service-hostname (Deprecated) | InetAddress.getLocalHost().getHostAddress() | the host on which Zipkin resides | no | string |
| zipkin.traced-service-port (Deprecated) | -1 | The port on which the zipkin service is running | no | single integer |

####Scribe (Thrift) Transport
The following configuration relate to the [org.apache.htrace.impl.ScribeTransport](https://github.com/apache/incubator-htrace/blob/master/htrace-zipkin/src/main/java/org/apache/htrace/impl/ScribeTransport.java)

| Key                   | Default Value | Description  | Mandatory | Possible Values |
| --------------------- | :-----------: | :--------- : | :-------: | :-------------: |
| zipkin.scribe.hostname | localhost | Host at which Scribe server is running | no | string |
| zipkin.scribe.port | 9410 | Port at which Scribe server is running | no | single integer |

####Kafka Transport
The following configuration relate to the [org.apache.htrace.impl.KafkaTransport](https://github.com/apache/incubator-htrace/blob/master/htrace-zipkin/src/main/java/org/apache/htrace/impl/KafkaTransport.java)

| Key                   | Default Value | Description  | Mandatory | Possible Values |
| --------------------- | :-----------: | :--------- : | :-------: | :-------------: |
| zipkin.kafka.topic | zipkin | The Kafka [Topic](http://kafka.apache.org/documentation.html#intro_topics) to write HTrace data to | no | string |
| zipkin.kafka.metadata.broker.list| localhost:9092 | Host and Port where the Kafka broker is running | no | Colon seperated Host:Port |
| zipkin.kafka.request.required.acks | 0 | Number of acknowledgements required before a message has been written into Kafka | no | single integer |
| zipkin.kafka.producer.type | async | Whether the Kafka [Producer](http://kafka.apache.org/documentation.html#theproducer) will send data in a [synchronous or asynchronous](http://kafka.apache.org/documentation.html#design_asyncsend) manner | no | async/sync |
| zipkin.kafka.serializer.class | kafka.serializer.DefaultEncoder | Type of [Encoder](https://github.com/apache/kafka/blob/6eacc0de303e4d29e083b89c1f53615c1dfa291e/core/src/main/scala/kafka/serializer/Encoder.scala) to be used within the Producer | no | kafka.serializer.DefaultEncoder, kafka.serializer.NullEncoder, kafka.serializer.StringEncoder |
| zipkin.kafka.compression.codec | 1 | Controls the [compression codec](https://cwiki.apache.org/confluence/display/KAFKA/Compression) to be used by the producer | no | O means no compression. 1 means GZIP compression. 2 means Snappy compression |
