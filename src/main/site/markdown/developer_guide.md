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

# HTrace Developer Guide

## Introduction
Apache HTrace is an open source framework for distributed tracing.  It can be
used with both standalone applications and libraries.

By adding HTrace support to your project, you will allow end-users to trace
their requests.  In addition, any other project that uses HTrace can follow the
requests it makes to your project.  That`s why we say HTrace is "end-to-end."

HTrace was designed for use in big distributed systems such as the Apache
Hadoop Distributed Filesystem and the Apache HBase storage engine.  However,
there is nothing Hadoop-specific about HTrace.  It has no dependencies on
Hadoop, and is a useful building block for many distributed systems.

## The HTrace Core Library
In order to use HTrace, your application must link against the appropriate core
library.  HTrace`s core libraries have been carefully designed to minimize the
number of dependencies that each one pulls in.  HTrace currently has Java, C,
and C++ support.

HTrace guarantees that the API of core libraries will not change in an
incompatible way during a minor release.  So if your application worked with
HTrace 4.1, it should continue working with HTrace 4.2 with no code changes.
(However HTrace 5 may change things, since it is a major release.)

### Java
The Java library for HTrace is named htrace-core4.jar.  This jar must appear on
your CLASSPATH  in order to use tracing in Java.  If you are using Maven, add
the following to your dependencyManagement section:

    <dependencyManagement>
      <dependencies>
        <dependency>
          <groupId>org.apache.htrace</groupId>
          <artifactId>htrace-core4</artifactId>
          <version>4.1.0-incubating</version>
        </dependency>
        ...
      </dependencies>
      ...
    </dependencyManagement>

If you are using an alternate build system, use the appropriate configuration
for your build system.  Note that it is not a good idea to shade htrace-core4,
because some parts of the code use reflection to load classes by name.

### C
The C library for HTrace is named libhtrace.so.  The interface for libhtrace.so
is described in [htrace.h](https://github.com/apache/incubator-htrace/blob/rel/4.1/htrace-c/src/core/htrace.h)

As with all dynamically loaded native libraries, your application or library
must be able to locate libhtrace.so in order to use it.  There are many ways to
accomplish this.  The easiest way is to put libhtrace.so in one of the system
shared library paths.  You can also use RPATH or LD\_LIBRARY\_PATH to alter the
search paths which the operating system uses.

### C++
The C++ API for HTrace is a wrapper around the C API.  This approach makes it
easy to use HTrace with any dialect of C++ without recompiling the core
library.  It also makes it easier for us to avoid making incompatible ABI
changes.

The interface is described in
[htrace.hpp](https://github.com/apache/incubator-htrace/blob/rel/4.1/htrace-c/src/core/htrace.hpp)
the same as using the C API, except that you use htrace.hpp instead of
htrace.h.

## Core Concepts
HTrace is based on a few core concepts.

### Spans
[Spans](https://github.com/apache/incubator-htrace/blob/rel/4.1/htrace-core4/src/main/java/org/apache/htrace/core/Span.java)
in HTrace are lengths of time.  A span has a beginning time in milliseconds, an
end time, a description, and many other fields besides.

Spans have parents and children.  The parent-child relationship between spans
is a little bit like a stack trace.  For example, the span graph of an HDFS
"ls" request might look like this:

    ls
    +--- FileSystem#createFileSystem
    +--- Globber#glob
    |  +---- GetFileInfo
    |      +---- ClientNamenodeProtocol#GetFileInfo
    |          +---- ClientProtocol#GetFileInfo
    +--- listPaths
       +---- ClientNamenodeProtocol#getListing
           +---- ClientProtocol#getListing

"ls" has several children, "FileSystem#createFileSystem", "Globber#glob", and
"listPaths".  Those spans, in turn, have their own children.

Unlike in a traditional stack trace, the spans in HTrace may be in different
processes or even on different computers.  For example,
ClientProtocol#getListing is done on the NameNode, whereas
ClientNamenodeProtocol#getListing happens inside the HDFS client.  These are
usually on different computers.

Each span has a unique 128-bit ID.  Because the space of 128-bit numbers is so
large, HTrace can use random generation to avoid collisions.

### Scopes
[TraceScope](https://github.com/apache/incubator-htrace/blob/rel/4.1/htrace-core4/src/main/java/org/apache/htrace/core/TraceScope.java)
objects manage the lifespan of Span objects.  When a TraceScope is created, it
often comes with an associated Span object.  When this scope is closed, the
Span will be closed as well.  "Closing" the scope means that the span is sent
to a SpanReceiver for processing.  We will talk more about what that means
later.  For now, just think of closing a TraceScope as similar to closing a
file descriptor-- the natural thing to do when the TraceScope is done.

HTrace tracks whether a trace scope is active in the current thread by using
thread-local data.  This approach makes it easier to add HTrace to existing
code, by avoiding the need to pass around context objects.

TraceScopes lend themselves to the try... finally pattern of management:

    TraceScope computationScope = tracer.newScope("CalculateFoo");
    try {
        calculateFoo();
    } finally {
        computationScope.close();
    }

Any trace spans created inside calculateFoo will automatically have the
CalculateFoo trace span we have created here as their parents.  We don`t have
to do any additional work to set up the parent/child relationship because the
thread-local data takes care of it.

In Java7, the
[try-with-resources](https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html)
idiom may be used to accomplish the same thing without a finally block:

    try (TraceScope computationScope = tracer.newScope("CalculateFoo")) {
        calculateFoo();
    }

The important thing to remember is to close the scope when you are done with it.

Note that in the C++ API, the destructor of the htrace::Scope object will
automatically close the span.

    htrace::Scope(tracer_, "CalculateFoo");
    calculateFoo();

TraceScope are associatewith particular threads.  If you want to pass a trace
scope to another thread, you must detach it from the current one first.  We
will talk more about that later in this guide.

### Tracers
[Tracers](https://github.com/apache/incubator-htrace/blob/rel/4.1/htrace-core4/src/main/java/org/apache/htrace/core/Tracer.java)
are the API for creating trace scope objects.  You can see that in the example
above, we called the Tracer#newScope function to create a scope.

It is difficult to trace every operation.  The volume of trace span data that
would be generated would be extremely large!  So we rely on sampling a subset
of all possible traces.  Tracer objects contain Samplers.  When you call
Tracer#newScope, the Tracer will consult that Sampler to determine if a new
span should be created, or if an empty scope which contains no span should be
returned.  Note that if there is already a currently active span, the Tracer
will always create a child span, regardless of what the sampler says.  This is
because we want to see the complete graph of every operation, not just "bits
and pieces." Tracer objects also manage the SpanReceiver objects which control
where spans are sent.

A single process or library can have many Tracer objects.  Each Tracer object
has its own configuration.  One way of thinking of Tracer objects is that they
are similar to Log objects in log4j.  Just as you might create a Log object for
the NameNode and one for the DataNode, we create a Tracer for the NameNode and
another Tracer for the DataNode.  This allows users to control the sampling
rate for the DataNode and the NameNode separately.

Unlike TraceScope and Span, Tracer objects are thread-safe.  It is perfectly
acceptable (and even recommended) to have multiple threads calling
Tracer#newScope at once on the same Tracer object.

The number of Tracer objects you should create in your project depends on the
structure of your project.  Many applications end up creating a small number of
global Tracer objects.  Libraries usually should not use globals, but associate
the Tracer with the current library context.

### Wrappers
HTrace contains many helpful wrapper objects like
[TraceRunnable](https://github.com/apache/incubator-htrace/blob/rel/4.1/htrace-core4/src/main/java/org/apache/htrace/core/TraceRunnable.java)
[TraceCallable](https://github.com/apache/incubator-htrace/blob/rel/4.1/htrace-core4/src/main/java/org/apache/htrace/core/TraceCallable.java)
and
[TraceExecutorService](https://github.com/apache/incubator-htrace/blob/rel/4.1/htrace-core4/src/main/java/org/apache/htrace/core/TraceExecutorService.java)
These helper classes make it easier for you to create trace spans.  Basically,
they act as wrappers around Tracer#newScope.

## SpanReceivers
HTrace is a pluggable framework.  We can configure where trace spans are sent
at runtime, by selecting the appropriate SpanReceiver.

    FoobarApplication
          |
          V
    htrace-core4
          |
          V
    HTracedSpanReceiver
         OR
    LocalFileSpanReceiver
         OR
    StandardOutSpanReceiver
         OR
    ZipkinSpanReceiver
         OR
         ...

As a developer integrating HTrace into your application, you do not need to
know what each and every SpanReceiver does-- only the ones you actually intend
to use.   The nice thing is that users can use any span receiver with your
project, without any additional effort on your part.  The span receivers are
decoupled from the core library.

When using Java, you will need to add the jar file for whichever span receiver
you want to use to your CLASSPATH.  (These span receivers are not added to the
CLASSPATH by default because they may have additional dependencies that not
every user wants.) For C and C++, a more limited set of span receivers is
available, but they are all integrated into libhtrace.so, so no additional
libraries are needed.

## Configuration
Please see the [configuration documentation](configuration.html), 
which also includes an entire  table of key, value configuration options.

## TracerPool
SpanReceiver objects often need to make a network connection to a remote
serveice or daemon.  Usually, we don`t want to create more than one
SpanReceiver of each type in a particular process, so that we can optimize the
number of these connections that we have open.  TracerPool objects allow us to
acheieve this.

Each Tracer object belongs to a TracerPool.  When a call to Tracer#Builder is
made which requests a specific SpanReceiver, we check the TracerPool to see if
there is already an instance of that SpanReceiver.  If so, we simply re-use the
existing one rather than creating a new one.

Normally, you don`t need to worry about TracerPools.  However, if you have an
explicit need to create multiple SpanReceivers of the same type, you can do it
by using a TracerPool other than the default one, or by explicitly adding the
SpanReceiver to your Tracer once it has been created.  This is not a very
common need.

When the application terminates, we will attempt to close all currently open
SpanReceivers.  You can attempt to close the SpanReceivers earlier than that by
calling tracer.getTracerPool().removeAndCloseAllSpanReceivers().

## Passing Span IDs over RPC
So far, we have described how to use HTrace inside a single process.  However,
since we are dealing with distributed systems, a single process is not enough.
We need a way to send HTrace information across the network.

Unlike some other tracing systems, HTrace works with many different RPC
systems.  You do not need to change the RPC framework you are using in order to
use HTrace.  You simply need to find a way to pass HTrace information using the
RPC framework that you`re already using.  In most cases, what this boils down
to is figuring out a way to send the 128-bit parent ID of an operation over the
network as an optional field.

Let`s say that Bob is writing the server side of his system.  If the client
sent its parent ID over the wire, Bob might write code like this:

    BobRequestProto bp = ...;
    SpanId spanId = (bp.hasSpanId()) ? bp.getSpanId() : SpanId.INVALID;
    try (TraceScope scope = tracer.newScope("bobRequest", spanId)) {
        doBobRequest(bp);
    }

By passing the spanId to Tracer#newScope, we ensure that any new span we create
will have a record of its parent.

## Handling work done in multiple threads
Sometimes, you end up performing work for a single request in multiple threads.
How can we handle this?  Certainly, we can use the same approach as we did in
the RPC case above.  We can have the child threads create trace scopes which
use our parent ID object.  SpanId objects are immtuable and easy to share
between threads.

    try (TraceScope bigScope = tracer.newScope("bigComputation")) {
        SpanId bigScopeId = bigScope.getCurrentSpanId();
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                TraceScope scope = (bigScopeId.isValid()) ?
                    tracer.newScope("bigComputationWorker", bigScopeId) :
                    tracer.newNullScope();
                try {
                    doWorkerStuff();
                } finally {
                    scope.close();
                }
            }
        }, "myWorker");
        t1.start();
        t1.join();
    }

Note that in this case, the two threads are not sharing trace scopes.  Instead,
we are setting up a new trace scope, which may have its own span, which has the
outer scope as a parent.  Note that HTrace will be well-behaved even if the
outer scope may be closed before the inner one.  The SpanId object of a
TraceScope continues to be valid even after a scope is closed.  It`s just a
number, essentially-- and that number will not be reused by any other scope.

Why do we make the worker Thread call newNullScope in the case where the outer
scope`s span id is invalid?  Well, we don`t want to ever create the inner span
if the outer one does not exist.  Calling newNullScope ensures that we get a
scope with no span, no matter what samplers are configured.

What if we don`t want to create more than one span here?  In that case, we need
to have some way of detaching the TraceScope from the parent thread, and
re-attaching it to the worker thread.  Luckily, HTrace has an API for that.

    final TraceScope bigScope = tracer.newScope("bigComputation");
    bigScope.detach();
    Thread t1 = new Thread(new Runnable() {
        @Override
        public void run() {
            bigScope.reattach();
            try {
                doWorkerStuff();
            } finally {
                bigScope.close();
            }
        }
    }, "myWorker");
    t1.start();
    t1.join();

Note that in this case, we don`t need to close the TraceScope in the containing
thread.  It has already been closed by the worker thread.
