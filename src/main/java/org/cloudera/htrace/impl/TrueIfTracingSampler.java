package org.cloudera.htrace.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.cloudera.htrace.Sampler;
import org.cloudera.htrace.Trace;

@SuppressWarnings("rawtypes")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TrueIfTracingSampler implements Sampler {

  private static TrueIfTracingSampler instance;

  public static TrueIfTracingSampler getInstance() {
    if (instance == null) {
      instance = new TrueIfTracingSampler();
    }
    return instance;
  }

  private TrueIfTracingSampler() {
  }

  @Override
  public boolean next(Object info) {
    return Trace.isTracing();
  }

}
