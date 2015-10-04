HTrace Zipkin Receiver
======================

Use the HTrace Java library with [Zipkin](https://github.com/openzipkin/zipkin).

To use, set `"span.receiver.classes", "org.apache.htrace.impl.ZipkinSpanReceiver"`
in the HTraceConfiguration.  


Transports
----------

The Zipkin receiver supports both the Zipkin Scribe (default) and Kafka transports,
controlled through the `zipkin.transport.class` configuration.  

Scribe (Thrift) Transport
-------------------------

### Configuration

Configurations are prefixed with `zipkin.scribe`.

* `zipkin.scribe.hostname`, `localhost`
* `zipkin.scribe.port`, `9410`

Deprecated (backwards compatibility):

* `zipkin.collector-hostname`, `localhost`
* `zipkin.collector-port`, `9410`

Kafka Transport
---------------

To use the Kafka transport, add 
`"zipkin.transport.class", "org.apache.htrace.impl.KafkaTransport"`
to the configuration.

### Configuration

Configurations are prefixed with `zipkin.kafka`.  

* `zipkin.kafka.topic`, `zipkin`

Producer specific configurations  

* `zipkin.kafka.metadata.broker.list`, `localhost:9092`
* `zipkin.kafka.request.required.acks`, `0`
* `zipkin.kafka.producer.type`, `async`
* `zipkin.kafka.serializer.class`, `kafka.serializer.DefaultEncoder`
* `zipkin.kafka.compression.codec`, `1`
