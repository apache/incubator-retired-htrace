package org.cloudera.htrace.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.cloudera.htrace.Span;
import org.cloudera.htrace.SpanReceiver;

/**
 * SpanReceiver for testing only that just collects the Span objects it
 * receives. The spans it receives can be accessed with getSpans();
 */
public class POJOSpanReceiver implements SpanReceiver {
  private final Collection<Span> spans;

  public Collection<Span> getSpans() {
    return spans;
  }

  public POJOSpanReceiver() {
    this.spans = new HashSet<Span>();
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void receiveSpan(Span span) {
    spans.add(span);
  }
}
