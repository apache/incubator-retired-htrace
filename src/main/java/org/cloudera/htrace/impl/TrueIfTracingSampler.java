package org.cloudera.htrace.impl;

import org.cloudera.htrace.Sampler;
import org.cloudera.htrace.Trace;

/**
 * A Sampler that returns true if and only if tracing is on the current thread.
 */
public class TrueIfTracingSampler implements Sampler<Object> {

  public static final TrueIfTracingSampler INSTANCE = new TrueIfTracingSampler();

  private TrueIfTracingSampler() {
  }

  @Override
  public boolean next(Object info) {
    return Trace.isTracing();
  }

}
