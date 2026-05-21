package com.linkedin.venice.stats.metrics;

import java.io.Closeable;


/**
 * Base class for stats holders that own a {@link CompositeCloseable} registry. Subclasses inherit
 * the {@code statsCloseables} field and a {@link #close()} that drains it. Override {@code close()}
 * to add extra cleanup; remember to call {@code super.close()} so the registry is drained.
 */
public abstract class AbstractStatsCloseable implements Closeable {
  protected final CompositeCloseable statsCloseables = new CompositeCloseable();

  @Override
  public void close() {
    statsCloseables.close();
  }
}
