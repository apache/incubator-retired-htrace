package org.cloudera.htrace;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestSampler {
  @Test
  public void testParamterizedSampler() {
    TestParamSampler sampler = new TestParamSampler();
    TraceScope s = Trace.startSpan("test", sampler, 1);
    assertNotNull(s.getSpan());
    s.close();
    s = Trace.startSpan("test", sampler, -1);
    assertNull(s.getSpan());
    s.close();
  }

  @Test
  public void testAlwaysSampler() {
    TraceScope cur = Trace.startSpan("test", new TraceInfo(0, 0));
    assertNotNull(cur);
    cur.close();
  }

  private class TestParamSampler implements Sampler<Integer> {

    @Override
    public boolean next(Integer info) {
      return info > 0;
    }

  }
}
