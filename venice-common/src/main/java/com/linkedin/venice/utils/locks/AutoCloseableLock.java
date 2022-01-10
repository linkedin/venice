package com.linkedin.venice.utils.locks;

import com.linkedin.venice.utils.CollectionUtils;

import org.apache.commons.lang.Validate;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;


public final class AutoCloseableLock implements AutoCloseable {
  private final List<Lock> locks;
  private final AtomicBoolean closing = new AtomicBoolean();

  public AutoCloseableLock(Lock... locks) {
    Validate.notEmpty(locks);
    Validate.noNullElements(locks);
    this.locks = Arrays.asList(locks);
    this.locks.forEach(Lock::lock);
  }

  @Override
  public void close() {
    if (closing.getAndSet(true)) {
      throw new IllegalStateException("Duplicate unlock attempt");
    }
    CollectionUtils.reversed(locks).forEach(Lock::unlock);
  }
}
