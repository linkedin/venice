package com.linkedin.venice.utils.pools;

import java.util.function.Supplier;


public class LandFillObjectPool<O> extends AbstractObjectPool<O> {
  public LandFillObjectPool(Supplier<O> objectSupplier) {
    super(objectSupplier);
  }

  @Override
  public void dispose(O object) {
    // No-op
  }
}
