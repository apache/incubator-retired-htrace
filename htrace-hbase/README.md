hbase-htrace
============

htrace-hbase provides the span receiver which sends tracing spans to HBase
and the viewer which retrieves spans from HBase and show them graphically.


Tutorial
--------

We are using the same HBase instance running in standalone-mode as a tracee
and storage of tracing spans at the same time in this example.

At first, get HBase and build it.:

    $ git clone https://github.com/apache/hbase
    $ cd hbase
    $ mvn package -DskipTests

Putting jar of hbase-htrace on the classpath of HBase.:

    $ mkdir lib
    $ cp path/to/htrace/htrace-hbase/target/htrace-hbase-3.0.4.jar lib/

Adding configuration for span receiver to hbase-site.xml.:

    <property>
      <name>hbase.trace.spanreceiver.classes</name>
      <value>org.htrace.impl.HBaseSpanReceiver</value>
    </property>

Starting HBase server in standalone-mode.:

    $ bin/hbase master start

Running HBase shell from another terminal,
adding the table in which tracing spans stored.:

    hbase(main):001:0> create 'htrace', 's', 'i'

Run some tracing from hbase shell.:

    hbase(main):002:0> trace 'start'; create 't1', 'f'; trace 'stop'
    ...
    hbase(main):003:0> trace 'start'; put 't1', 'r1', 'f:c1', 'value'; trace 'stop'
    ...

Running the main class of receiver also generate simple trace for test.:

    $ bin/hbase org.htrace.impl.HBaseSpanReceiver

Strating viewer process which listens on 16900 by default.:

    $ bin/hbase org.htrace.viewer.HBaseSpanViewerServer

Accessing http://host:16900/ with Web browser shows you list of traces like below.:

![list of traces](traces.png "traces list")

Clicking the trace in the list shows you the spans.:

![visualization of spans](spans.png "spans view")


Configuration
-------------

Configurations for span receiver running in HBase
to connect to the HBase to which spands are sent.
These are diffrent from the properties of usual HBase client.:

    <property>
      <name>hbase.htrace.hbase.collector-quorum</name>
      <value>127.0.0.1</value>
    </property>
    <property>
      <name>hbase.htrace.hbase.zookeeper.property.clientPort</name>
      <value>2181</value>
    </property>
    <property>
      <name>hbase.htrace.hbase.zookeeper.znode.parent</name>
      <value>/hbase</value>
    </property>

You can set listen address of span viewer server by `htrace.viewer.http.address`.
In addition, span viewer server uses usual HBase client configuration
to connect to HBase.:

    $ bin/hbase org.htrace.viewer.HBaseSpanViewerServer \
        -Dhtrace.viewer.http.address=0.0.0.0:16900 \
        -Dhbase.zookeeper.quorum=127.0.0.1


Todo
----

- showing timeline annotation in spans view.
- showing parent-child relationships in spans view.
- enabling to focus in/out specific spans in trace.
- limiting the traces shown in list based on time period.
- adding tests.
