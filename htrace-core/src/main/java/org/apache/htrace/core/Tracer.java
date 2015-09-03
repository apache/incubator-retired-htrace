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
package org.apache.htrace.core;

import java.io.Closeable;
import java.lang.System;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A Tracer provides the implementation for collecting and distributing Spans
 * within a process.
 */
public class Tracer implements Closeable {
  private static final Log LOG = LogFactory.getLog(Tracer.class);

  public final static String SPAN_RECEIVER_CLASSES_KEY =
      "span.receiver.classes";
  public final static String SAMPLER_CLASSES_KEY =
      "sampler.classes";

  public static class Builder {
    private String name;
    private HTraceConfiguration conf = HTraceConfiguration.EMPTY;
    private ClassLoader classLoader =
        Builder.class.getClassLoader();
    private TracerPool tracerPool = TracerPool.GLOBAL;

    public Builder() {
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder conf(HTraceConfiguration conf) {
      this.conf = conf;
      return this;
    }

    public Builder tracerPool(TracerPool tracerPool) {
      this.tracerPool = tracerPool;
      return this;
    }

    private void loadSamplers(List<Sampler> samplers) {
      String classNamesStr = conf.get(SAMPLER_CLASSES_KEY, "");
      List<String> classNames = getClassNamesFromConf(classNamesStr);
      StringBuilder bld = new StringBuilder();
      String prefix = "";
      for (String className : classNames) {
        try {
          Sampler sampler = new Sampler.Builder(conf).
            className(className).
            classLoader(classLoader).
            build();
          samplers.add(sampler);
          bld.append(prefix).append(className);
          prefix = ", ";
        } catch (Throwable e) {
          LOG.error("Failed to create SpanReceiver of type " + className, e);
        }
      }
      String resultString = bld.toString();
      if (resultString.isEmpty()) {
        resultString = "no samplers";
      }
      LOG.info(SAMPLER_CLASSES_KEY + " = " + classNamesStr +
          "; loaded " + resultString);
    }

    private void loadSpanReceivers() {
      String classNamesStr = conf.get(SPAN_RECEIVER_CLASSES_KEY, "");
      List<String> classNames = getClassNamesFromConf(classNamesStr);
      StringBuilder bld = new StringBuilder();
      String prefix = "";
      for (String className : classNames) {
        try {
          tracerPool.loadReceiverType(className, conf, classLoader);
          bld.append(prefix).append(className);
          prefix = ", ";
        } catch (Throwable e) {
          LOG.error("Failed to create SpanReceiver of type " + className, e);
        }
      }
      String resultString = bld.toString();
      if (resultString.isEmpty()) {
        resultString = "no span receivers";
      }
      LOG.info(SPAN_RECEIVER_CLASSES_KEY + " = " + classNamesStr +
          "; loaded " + resultString);
    }

    /**
     * Get a list of class names from the HTrace configuration.
     * Entries which are empty will be removed.  Entries which lack a package will
     * be given the default package.
     *
     * @param classNamesStr     A semicolon-separated string containing a list
     *                            of class names.
     * @return                  A list of class names.
     */
    private List<String> getClassNamesFromConf(String classNamesStr) {
      String classNames[] = classNamesStr.split(";");
      LinkedList<String> cleanedClassNames = new LinkedList<String>();
      for (String className : classNames) {
        String cleanedClassName = className.trim();
        if (!cleanedClassName.isEmpty()) {
          cleanedClassNames.add(cleanedClassName);
        }
      }
      return cleanedClassNames;
    }

    public Tracer build() {
      if (name == null) {
        throw new RuntimeException("You must specify a name for this Tracer.");
      }
      LinkedList<SpanReceiver> spanReceivers = new LinkedList<SpanReceiver>();
      LinkedList<Sampler> samplers = new LinkedList<Sampler>();
      loadSamplers(samplers);
      String tracerId = new TracerId(conf, name).get();
      Tracer tracer = new Tracer(tracerId, tracerPool,
          samplers.toArray(new Sampler[samplers.size()]));
      tracerPool.addTracer(tracer);
      loadSpanReceivers();
      return tracer;
    }
  }

  /**
   * The thread-specific context for this Tracer.
   *
   * This tracks the current number of trace scopes in a particular thread
   * created by this tracer.  We use this to apply our samplers only for the
   * "top-level" spans.
   *
   * Note that we can't put the TraceScope objects themselves in this context,
   * since we need to be able to use TraceScopes created by other Tracers, and
   * this context is per-Tracer.
   */
  private static class ThreadContext {
    private long depth;

    ThreadContext() {
      this.depth = 0;
    }

    boolean isTopLevel() {
      return (depth == 0);
    }

    void pushScope() {
      depth++;
    }

    TraceScope pushNewScope(Tracer tracer, Span span, TraceScope parentScope) {
      TraceScope scope = new TraceScope(tracer, span, parentScope);
      threadLocalScope.set(scope);
      depth++;
      return scope;
    }

    void popScope() {
      if (depth <= 0) {
        throwClientError("There were more trace scopes closed than " +
            "were opened.");
      }
      depth--;
    }
  };

  /**
   * A subclass of ThreadLocal that starts off with a non-null initial value in
   * each thread.
   */
  private static class ThreadLocalContext extends ThreadLocal<ThreadContext> {
    @Override
    protected ThreadContext initialValue() {
      return new ThreadContext();
    }
  };

  /**
   * The current trace scope.  This is global, so it is shared amongst all
   * libraries using HTrace.
   */
  final static ThreadLocal<TraceScope> threadLocalScope =
      new ThreadLocal<TraceScope>();

  /**
   * An empty array of SpanId objects.  Can be used rather than constructing a
   * new object whenever we need an empty array.
   */
  private static final SpanId EMPTY_PARENT_ARRAY[] = new SpanId[0];

  /**
   * The tracerId.
   */
  private final String tracerId;

  /**
   * The TracerPool which this Tracer belongs to.
   *
   * This gets set to null after the Tracer is closed in order to catch some
   * use-after-close errors.  Note that we do not synchronize access on this
   * field, since it only changes when the Tracer is closed, and the Tracer
   * should not be used after that.
   */
  private TracerPool tracerPool;

  /**
   * The current thread-local context for this particualr Tracer.
   */
  private final ThreadLocalContext threadContext;

  /**
   * The NullScope instance for this Tracer.
   */
  private final NullScope nullScope;

  /**
   * The currently active Samplers.
   *
   * Arrays are immutable once set.  You must take the Tracer lock in order to
   * set this to a new array.  If this is null, the Tracer is closed.
   */
  private volatile Sampler[] curSamplers;

  /**
   * Log a client error, and throw an exception.
   *
   * @param str     The message to use in the log and the exception.
   */
  static void throwClientError(String str) {
    LOG.error(str);
    throw new RuntimeException(str);
  }

  /**
   * If the current thread is tracing, this function returns the Tracer that is
   * being used; otherwise, it returns null.
   */
  public static Tracer curThreadTracer() {
    TraceScope traceScope = threadLocalScope.get();
    if (traceScope == null) {
      return null;
    }
    return traceScope.tracer;
  }

  Tracer(String tracerId, TracerPool tracerPool, Sampler[] curSamplers) {
    this.tracerId = tracerId;
    this.tracerPool = tracerPool;
    this.threadContext = new ThreadLocalContext();
    this.nullScope = new NullScope(this);
    this.curSamplers = curSamplers;
  }

  public String getTracerId() {
    return tracerId;
  }

  private TraceScope newScopeImpl(ThreadContext context, String description) {
    Span span = new MilliSpan.Builder().
        tracerId(tracerId).
        begin(System.currentTimeMillis()).
        description(description).
        parents(EMPTY_PARENT_ARRAY).
        spanId(SpanId.fromRandom()).
        build();
    return context.pushNewScope(this, span, null);
  }

  private TraceScope newScopeImpl(ThreadContext context, String description,
        TraceScope parentScope) {
    SpanId parentId = parentScope.getSpan().getSpanId();
    Span span = new MilliSpan.Builder().
        tracerId(tracerId).
        begin(System.currentTimeMillis()).
        description(description).
        parents(new SpanId[] { parentId }).
        spanId(parentId.newChildId()).
        build();
    return context.pushNewScope(this, span, parentScope);
  }

  private TraceScope newScopeImpl(ThreadContext context, String description,
        SpanId parentId) {
    Span span = new MilliSpan.Builder().
        tracerId(tracerId).
        begin(System.currentTimeMillis()).
        description(description).
        parents(new SpanId[] { parentId }).
        spanId(parentId.newChildId()).
        build();
    return context.pushNewScope(this, span, null);
  }

  private TraceScope newScopeImpl(ThreadContext context, String description,
        TraceScope parentScope, SpanId secondParentId) {
    SpanId parentId = parentScope.getSpan().getSpanId();
    Span span = new MilliSpan.Builder().
        tracerId(tracerId).
        begin(System.currentTimeMillis()).
        description(description).
        parents(new SpanId[] { parentId, secondParentId }).
        spanId(parentId.newChildId()).
        build();
    return context.pushNewScope(this, span, parentScope);
  }

  /**
   * Create a new trace scope.
   *
   * If there are no scopes above the current scope, we will apply our
   * configured samplers.  Otherwise, we will create a span only if this thread
   * is already tracing, or if the passed parentID was valid.
   *
   * @param description         The description of the new span to create.
   * @param parentId            If this is a valid span ID, it will be added to
   *                              the parents of the new span we create.
   * @return                    The new trace scope.
   */
  public TraceScope newScope(String description, SpanId parentId) {
    TraceScope parentScope = threadLocalScope.get();
    ThreadContext context = threadContext.get();
    if (parentScope != null) {
      if (parentId.isValid() &&
          (!parentId.equals(parentScope.getSpan().getSpanId()))) {
        return newScopeImpl(context, description, parentScope, parentId);
      } else {
        return newScopeImpl(context, description, parentScope);
      }
    } else if (parentId.isValid()) {
      return newScopeImpl(context, description, parentId);
    }
    if (!context.isTopLevel()) {
      context.pushScope();
      return nullScope;
    }
    if (!sample()) {
      context.pushScope();
      return nullScope;
    }
    return newScopeImpl(context, description);
  }

  /**
   * Create a new trace scope.
   *
   * If there are no scopes above the current scope, we will apply our
   * configured samplers.  Otherwise, we will create a span only if this thread
   * is already tracing.
   */
  public TraceScope newScope(String description) {
    TraceScope parentScope = threadLocalScope.get();
    ThreadContext context = threadContext.get();
    if (parentScope != null) {
      return newScopeImpl(context, description, parentScope);
    }
    if (!context.isTopLevel()) {
      context.pushScope();
      return nullScope;
    }
    if (!sample()) {
      context.pushScope();
      return nullScope;
    }
    return newScopeImpl(context, description);
  }

  /**
   * Return a null trace scope.
   */
  public TraceScope newNullScope() {
    ThreadContext context = threadContext.get();
    context.pushScope();
    return nullScope;
  }

  /**
   * Wrap the callable in a TraceCallable, if tracing.
   *
   * @return The callable provided, wrapped if tracing, 'callable' if not.
   */
  public <V> Callable<V> wrap(Callable<V> callable, String description) {
    TraceScope parentScope = threadLocalScope.get();
    if (parentScope == null) {
      return callable;
    }
    return new TraceCallable<V>(this, parentScope, callable, description);
  }

  /**
   * Wrap the runnable in a TraceRunnable, if tracing
   *
   * @return The runnable provided, wrapped if tracing, 'runnable' if not.
   */
  public Runnable wrap(Runnable runnable, String description) {
    TraceScope parentScope = threadLocalScope.get();
    if (parentScope == null) {
      return runnable;
    }
    return new TraceRunnable(this, parentScope, runnable, description);
  }

  public TraceExecutorService newTraceExecutorService(ExecutorService impl,
                                                      String scopeName) {
    return new TraceExecutorService(this, scopeName, impl);
  }

  public TracerPool getTracerPool() {
    if (tracerPool == null) {
      throwClientError(toString() + " is closed.");
    }
    return tracerPool;
  }

  /**
   * Returns an object that will trace all calls to itself.
   */
  @SuppressWarnings("unchecked")
  <T, V> T createProxy(final T instance) {
    final Tracer tracer = this;
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object obj, Method method, Object[] args)
          throws Throwable {
        TraceScope scope = tracer.newScope(method.getName());
        try {
          return method.invoke(instance, args);
        } catch (Throwable ex) {
          ex.printStackTrace();
          throw ex;
        } finally {
          scope.close();
        }
      }
    };
    return (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(),
        instance.getClass().getInterfaces(), handler);
  }

  /**
   * Return true if we should create a new top-level span.
   *
   * We will create the span if any configured sampler returns true.
   */
  private boolean sample() {
    Sampler[] samplers = curSamplers;
    for (Sampler sampler : samplers) {
      if (sampler.next()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns an array of all the current Samplers.
   *
   * Note that if the current Samplers change, those changes will not be
   * reflected in this array.  In other words, this array may be stale.
   */
  public Sampler[] getSamplers() {
    return curSamplers;
  }

  /**
   * Add a new Sampler.
   *
   * @param sampler       The new sampler to add.
   *                      You cannot add a particular Sampler object more
   *                        than once.  You may add multiple Sampler objects
   *                        of the same type, although this is not recommended.
   *
   * @return              True if the sampler was added; false if it already had
   *                        been added earlier.
   */
  public synchronized boolean addSampler(Sampler sampler) {
    if (tracerPool == null) {
      throwClientError(toString() + " is closed.");
    }
    Sampler[] samplers = curSamplers;
    int j = 0;
    for (int i = 0; i < samplers.length; i++) {
      if (samplers[i] == sampler) {
        return false;
      }
    }
    Sampler[] newSamplers =
        Arrays.copyOf(samplers, samplers.length + 1);
    newSamplers[samplers.length] = sampler;
    curSamplers = newSamplers;
    return true;
  }

  /**
   * Remove a SpanReceiver.
   *
   * @param sampler       The sampler to remove.
   */
  public synchronized boolean removeSampler(Sampler sampler) {
    if (tracerPool == null) {
      throwClientError(toString() + " is closed.");
    }
    Sampler[] samplers = curSamplers;
    int j = 0;
    for (int i = 0; i < samplers.length; i++) {
      if (samplers[i] == sampler) {
        Sampler[] newSamplers = new Sampler[samplers.length - 1];
        System.arraycopy(samplers, 0, newSamplers, 0, i);
        System.arraycopy(samplers, i + 1, newSamplers, i,
            samplers.length - i - 1);
        curSamplers = newSamplers;
        return true;
      }
    }
    return false;
  }

  void detachScope(TraceScope scope) {
    TraceScope curScope = threadLocalScope.get();
    if (curScope != scope) {
      throwClientError("Can't detach TraceScope for " +
          scope.getSpan().toJson() + " because it is not the current " +
          "TraceScope in thread " + Thread.currentThread().getName());
    }
    ThreadContext context = threadContext.get();
    context.popScope();
    threadLocalScope.set(scope.getParent());
  }

  void reattachScope(TraceScope scope) {
    TraceScope parent = threadLocalScope.get();
    Tracer.threadLocalScope.set(scope);
    ThreadContext context = threadContext.get();
    context.pushScope();
    scope.setParent(parent);
  }

  void closeScope(TraceScope scope) {
    TraceScope curScope = threadLocalScope.get();
    if (curScope != scope) {
      throwClientError("Can't close TraceScope for " +
          scope.getSpan().toJson() + " because it is not the current " +
          "TraceScope in thread " + Thread.currentThread().getName());
    }
    if (tracerPool == null) {
      throwClientError(toString() + " is closed.");
    }
    SpanReceiver[] receivers = tracerPool.getReceivers();
    if (receivers == null) {
      throwClientError(toString() + " is closed.");
    }
    ThreadContext context = threadContext.get();
    context.popScope();
    threadLocalScope.set(scope.getParent());
    scope.setParent(null);
    Span span = scope.getSpan();
    span.stop();
    for (SpanReceiver receiver : receivers) {
      receiver.receiveSpan(span);
    }
  }

  void popNullScope() {
    TraceScope curScope = threadLocalScope.get();
    if (curScope != null) {
      throwClientError("Attempted to close an empty scope, but it was not " +
          "the current thread scope in thread " +
          Thread.currentThread().getName());
    }
    ThreadContext context = threadContext.get();
    context.popScope();
  }

  public static Span getCurrentSpan() {
    TraceScope curScope = threadLocalScope.get();
    if (curScope == null) {
      return null;
    } else {
      return curScope.getSpan();
    }
  }

  public static SpanId getCurrentSpanId() {
    TraceScope curScope = threadLocalScope.get();
    if (curScope == null) {
      return SpanId.INVALID;
    } else {
      return curScope.getSpan().getSpanId();
    }
  }

  @Override
  public synchronized void close() {
    if (tracerPool == null) {
      return;
    }
    curSamplers = new Sampler[0];
    tracerPool.removeTracer(this);
  }

  /**
   * Get the hash code of a Tracer object.
   *
   * This hash code is based on object identity.
   * This is used in TracerPool to create a hash table of Tracers.
   */
  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  /**
   * Compare two tracer objects.
   *
   * Tracer objects are always compared by object equality.
   * This is used in TracerPool to create a hash table of Tracers.
   */
  @Override
  public boolean equals(Object other) {
    return (this == other);
  }

  @Override
  public String toString() {
    return "Tracer(" + tracerId + ")";
  }

}
