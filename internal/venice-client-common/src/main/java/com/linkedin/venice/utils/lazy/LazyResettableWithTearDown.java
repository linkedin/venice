package com.linkedin.venice.utils.lazy;

import java.util.function.Consumer;
import java.util.function.Supplier;


public class LazyResettableWithTearDown<C> extends LazyResettableImpl<C> {
  private final Consumer<C> tearDown;

  public LazyResettableWithTearDown(Supplier<C> supplier, Consumer<C> tearDown) {
    super(supplier);
    this.tearDown = tearDown;
  }

  @Override
  public void reset() {
    Lazy<C> old = lazy;
    super.reset();
    old.ifPresent(tearDown);
  }
}
