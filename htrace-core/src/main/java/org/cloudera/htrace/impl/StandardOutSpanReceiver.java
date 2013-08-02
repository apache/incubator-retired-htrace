package org.cloudera.htrace.impl;

import org.cloudera.htrace.HTraceConfiguration;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.SpanReceiver;

import java.io.IOException;

/**
 * Used for testing. Simply prints to standard out any spans it receives.
 */
public class StandardOutSpanReceiver implements SpanReceiver {

  @Override
  public void configure(HTraceConfiguration conf) {    
  }

  @Override
  public void receiveSpan(Span span) {
    System.out.println(span);
  }

  @Override
  public void close() throws IOException {
  }
}
