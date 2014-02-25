HTrace
======
HTrace is a tracing framework intended for use with distributed systems written in java.  

The project is hosted at http://github.com/cloudera/htrace.  
The project is available in Maven Central with groupId: org.htrace, and name: htrace.  
(It was formally at groupId: org.cloudera.htrace, and name: htrace).  

API
---
Using HTrace requires some instrumentation to your application.  
Before we get into that we have to review our terminology.  HTrace
borrows [Dapper's](http://research.google.com/pubs/pub36356.html)
terminology.  
  
<b>Span:</b> The basic unit of work. For example, sending an RPC is a
new span, as is sending a response to an RPC.  
Span's are identified by a unique 64-bit ID for the span and another
64-bit ID for the trace the span is a part of.  Spans also have other
data, such as descriptions, key-value annotations, the ID of the span
that caused them, and process ID's (normally IP address).  
<br>
Spans are started and stopped, and they keep track of their timing
information.  Once you create a span, you must stop it at some point
in the future.  
  
<b>Trace:</b> A set of spans forming a tree-like structure.  For
example, if you are running a distributed big-data store, a trace
might be formed by a put request. 

To instrument your system you must:  
<br>
<b>1. Attach additional information to your RPC's.</b>  
In order to create the causal links necessary for a trace, HTrace
needs to know about the causal
relationships between spans.  The only information you need to add to
your RPC's is two 64-bit longs.  If tracing is enabled (Trace.isTracing()
returns true) when you send an RPC, attach the ID of the current span
and the ID of the current trace to the message.  
On the receiving end of the RPC, check to see if the message has the
additional tracing information above.  If it does, start a new span
with the information given (more on that in a bit).  
<br>
<b>2. Wrap your thread changes.</b>  
HTrace stores span information in java's ThreadLocals, which causes
the trace to be "lost" on thread changes. The only way to prevent
this is to "wrap" your thread changes. For example, if your code looks
like this:

````java
    Thread t1 = new Thread(new MyRunnable());
    ...  
````

Just change it to look this:  

````java
    Thread t1 = new Thread(Trace.wrap(new MyRunnable()));
````

That's it! `Trace.wrap()` takes a single argument (a runnable or a
callable) and if the current thread is a part of a trace, returns a
wrapped version of the argument.  The wrapped version of a callable
and runnable just knows about the span that created it and will start
a new span in the new thread that is the child of the span that
created the runnable/callable.  There may be situations in which a
simple `Trace.wrap()` does not suffice.  In these cases all you need
to do is keep a reference to the "parent span" (the span before the
thread change) and once you're in the new thread start a new span that
is the "child" of the parent span you stored.  
<br>
For example:  
<br>
Say you have some object representing a "put" operation.  When the
client does a "put," the put is first added to a list so another
thread can batch together the puts. In this situation, you
might want to add another field to the Put class that could store the
current span at the time the put was created.  Then when the put is
pulled out of the list to be processed, you can start a new span as
the child of the span stored in the Put.  
<br>
<b>3. Add custom spans and annotations.</b>  
Once you've augmented your RPC's and wrapped the necessary thread
changes, you can add more spans and annotations wherever you want.  
For example, you might do some expensive computation that you want to
see on your traces.  In this case, you could start a new span before
the computation that you then stop after the computation has
finished. It might look like this:  

````java
    Span computationSpan = Trace.startSpan("Expensive computation.");  
    try {  
        //expensive computation here  
    } finally {  
        computationSpan.stop();  
    }  
````

HTrace also supports key-value annotations on a per-trace basis.  
<br>
Example:

````java
    Trace.currentTrace().addAnnotation("faultyRecordCounter".getBytes(), "1".getBytes());
````

`Trace.currentTrace()` will not return `null` if the current thread is
not tracing, but instead it will return a `NullSpan`, which does
nothing on any of its method calls. The takeaway here is you can call
methods on the `currentTrace()` without fear of NullPointerExceptions.

###Samplers  
`Sampler` is an interface that defines one function:  

````java
    boolean next(T info);
````

All of the `Trace.startSpan()` methods can take an optional sampler.  
A new span is only created if the sampler's next function returns
true.  If the Sampler returns false, the `NullSpan` is returned from
`startSpan()`, so it's safe to call `stop()` or `addAnnotation()` on it.
As you may have noticed from the `next()` method signature, Sampler is
parameterized.  The argument to `next()` is whatever piece of
information you might need for sampling.  See `Sampler.java` for an
example of this.  If you do not require any additional information,
then just ignore the parameter.  
HTrace includes  a sampler that always returns true, a
sampler that always returns false and a sampler returns true some
percentage of the time (you pass in the percentage as a decimal at construction). 

###`Trace.startSpan()` 
There is a single method to create and start spans: `startSpan()`.  
For the `startSpan()` methods that do not take an explicit Sampler, the
default Sampler is used.  The default sampler returns true if and only
if tracing is already on in the current thread.  That means that
calling `startSpan()` with no explicit Sampler is a good idea when you
have information that you would like to add to a trace if it's already
occurring, but is not something you would want to start a whole new
trace for.  
<br>
If you are using a sampler that makes use of the `T info` parameter to
`next()`, just pass in the object as the last argument.  If you leave it
out, HTrace will pass `null` for you (so make sure your Samplers can
handle `null`).  
<br>
Aside from whether or not you pass in an explicit `Sampler`, there are
other options you have when calling `startSpan()`.  
For the next section I am assuming you are familiar with the options
for passing in `Samplers` and `info` parameters, so when I say "no
arguments," I mean no additional arguments other than whatever
`Sampler`/`info` parameters you deem necessary.  
<br>
You can call `startSpan()` with no additional arguments.
In this case, `Trace.java` will start a span if the sampler (explicit
or default) returns true. If the current span is not the `NullSpan`, the span
returned will be a child of the current span, otherwise it will start
a new trace in the current thread (it will be a
`ProcessRootMilliSpan`). All of the other `startSpan()` methods take some
parameter describing the parent span of the span to be created. The
versions that take a `TraceInfo` or a `long traceId` and `long
parentId` will mostly be used when continuing a trace over RPC. The
receiver of the RPC will check the message for the additional two
`longs` and will call `startSpan()` if they are attached.  The last
`startSpan()` takes a `Span parent`.  The result of `parent.child()`
will be used for the new span.  `Span.child()` simply returns a span
that is a child of `this`. 

Testing Information
-------------------------------

The test that creates a sample trace (TestHTrace) takes a command line argument telling it where to write span information. Run mvn test -DspanFile="FILE\_PATH" to write span information to FILE_PATH. If no file is specified, span information will be written to standard out. If span information is written to a file, you can use the included graphDrawer python script in tools/ to create a simple visualization of the trace. Or you could write some javascript to make a better visualization, and send a pull request if you do :). 
