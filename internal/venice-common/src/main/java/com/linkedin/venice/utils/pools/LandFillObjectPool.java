package com.linkedin.venice.utils.pools;

import java.util.function.Supplier;


/**
 * A dummy implementation which simply allocates a new object on every {@link #get()} and lets the garbage collector
 * dispose of it rather than reusing.
 */
public class LandFillObjectPool<O> extends AbstractObjectPool<O> {
  public LandFillObjectPool(Supplier<O> objectSupplier) {
    super(objectSupplier);
  }

  @Override
  public void dispose(O object) {
    // No-op
  }
}
