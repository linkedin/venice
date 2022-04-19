package com.linkedin.venice.utils.locks;

import java.util.concurrent.locks.Lock;
import org.apache.commons.lang.Validate;


public final class AutoCloseableMultiLock extends AutoCloseableLock {
  private final Lock[] locks;

  /**
   * Package private constructor. Use {@link AutoCloseableLock#ofMany(Lock...)} to get an instance of this class.
   */
  AutoCloseableMultiLock(Lock... locks) {
    Validate.notEmpty(locks);
    Validate.noNullElements(locks);
    this.locks = locks;
    for (int i = 0; i < this.locks.length; i++) {
      locks[i].lock();
    }
  }

  @Override
  protected void unlock() {
    for (int i = this.locks.length - 1; i >= 0; i--) {
      this.locks[i].unlock();
    }
  }
}
