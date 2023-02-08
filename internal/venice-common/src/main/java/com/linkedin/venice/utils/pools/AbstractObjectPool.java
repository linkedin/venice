package com.linkedin.venice.utils.pools;

import java.util.function.Supplier;


abstract class AbstractObjectPool<O> implements ObjectPool<O> {
  private final Supplier<O> objectSupplier;

  AbstractObjectPool(Supplier<O> objectSupplier) {
    this.objectSupplier = objectSupplier;
  }

  @Override
  public O get() {
    return objectSupplier.get();
  }
}
