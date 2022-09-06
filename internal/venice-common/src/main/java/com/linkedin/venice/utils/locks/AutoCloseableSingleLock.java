package com.linkedin.venice.utils.locks;

import java.util.concurrent.locks.Lock;


public class AutoCloseableSingleLock extends AutoCloseableLock {
  private final Lock lock;

  /**
   * Package private constructor. Use {@link AutoCloseableLock#of(Lock)} to get an instance of this class.
   */
  AutoCloseableSingleLock(Lock lock) {
    this.lock = lock;
    this.lock.lock();
  }

  @Override
  protected void unlock() {
    this.lock.unlock();
  }
}
