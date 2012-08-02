package org.cloudera.htrace.impl;

import org.cloudera.htrace.Sampler;
import org.cloudera.htrace.Trace;

public class TraceExistsSampler implements Sampler {

  @Override
  public boolean next(Object info) {
    return Trace.isTracing();
  }

}
