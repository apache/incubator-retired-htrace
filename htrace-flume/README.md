<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

htrace-flume
============

htrace-flume provides the span receiver which sends tracing spans to Flume collector.

Tutorial
--------

1) build and deploy

	$ cd htrace/htrace-flume
	$ mvn compile assembly:single
	$ cp target/htrace-flume-*-jar-with-dependencies.jar $HADOOP_HOME/share/hadoop/hdfs/lib/

2) Edit hdfs-site.xml to include the following:

	<property>
		<name>hadoop.htrace.span.receiver.classes</name>
		<value>org.apache.htrace.impl.FlumeSpanReceiver</value>
	</property>
	<property>
		<name>hadoop.htrace.flume.hostname</name>
		<value>127.0.0.1</value>
	</property>
	<property>
		<name>hadoop.htrace.flume.port</name>
		<value>60000</value>
	</property>

3) Setup flume collector

Create flume-conf.properties file. Below is a sample that sets up an hdfs sink.

	agent.sources = avro-collection-source
	agent.channels = memoryChannel
	agent.sinks = loggerSink hdfs-sink

	# avro source - should match the configurations in hdfs-site.xml
	agent.sources.avro-collection-source.type = avro
	agent.sources.avro-collection-source.bind = 127.0.0.1
	agent.sources.avro-collection-source.port = 60000
	agent.sources.avro-collection-source.channels = memoryChannel

	#sample hdfs-sink, change to any sink that flume supports
	agent.sinks.hdfs-sink.type = hdfs
	agent.sinks.hdfs-sink.hdfs.path = hdfs://127.0.0.1:9000/flume
	agent.sinks.hdfs-sink.channel = memoryChannel
	agent.sinks.hdfs-sink.hdfs.fileType = DataStream
	agent.sinks.hdfs-sink.hdfs.writeFormat = Text
	agent.sinks.hdfs-sink.hdfs.rollSize = 0
	agent.sinks.hdfs-sink.hdfs.rollCount = 10000
	agent.sinks.hdfs-sink.hdfs.batchSize = 100

	# memory channel
	agent.channels.memoryChannel.capacity = 10000
	agent.channels.memoryChannel.transactionCapacity = 1000

Run flume agent using command "flume-ng agent -c ./conf/ -f ./conf/flume-conf.properties -n agent"

