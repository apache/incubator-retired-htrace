package org.cloudera.htrace;

import java.io.Closeable;

public class TraceScope implements Closeable {

  /** the span for this scope */
  private final Span span;

  /** the span that was "current" before this scope was entered */
  private final Span savedSpan;

  private boolean detached = false;
  
  TraceScope(Span span, Span saved) {
    this.span = span;
    this.savedSpan = saved;
  }
  
  public Span getSpan() {
    return span;
  }
  
  /**
   * Remove this span as the current thread, but don't stop it yet or
   * send it for collection. This is useful if the span object is then
   * passed to another thread for use with Trace.continueTrace().
   * 
   * @return the same Span object
   */
  public Span detach() {
    detached = true;

    Span cur = Tracer.getInstance().currentTrace();
    if (cur != span) {
      Tracer.LOG.debug("Closing trace span " + span + " but " +
        cur + " was top-of-stack");
    } else {
      Tracer.getInstance().setCurrentSpan(savedSpan);
    }
    return span;
  }
  
  @Override
  public void close() {
    if (span == null) return;
    
    if (!detached) {
      // The span is done
      span.stop();
      Tracer.getInstance().deliver(span);
      detach();
    }
  }
}
