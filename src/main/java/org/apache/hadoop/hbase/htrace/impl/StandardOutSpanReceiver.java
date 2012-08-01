package org.apache.hadoop.hbase.htrace.impl;

import org.apache.hadoop.hbase.htrace.Span;
import org.apache.hadoop.hbase.htrace.SpanReceiver;

/**
 * Used for testing. Simply prints to standard out any spans it receives.
 */
public class StandardOutSpanReceiver implements SpanReceiver {

  @Override
  public void receiveSpan(Span span) {
    System.out.println(span);
  }
}
