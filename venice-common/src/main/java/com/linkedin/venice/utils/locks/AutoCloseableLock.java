package com.linkedin.venice.utils.locks;

import java.util.List;
import java.util.concurrent.locks.Lock;


public class AutoCloseableLock implements AutoCloseable {
  private final List<Lock> locks;

  public AutoCloseableLock(List<Lock> locks) {
    for (Lock lock : locks) {
      lock.lock();
    }

    this.locks = locks;
  }

  @Override
  public void close() {
    try {
      unlock();
    } catch (Exception e) {
      throw new IllegalStateException("while invoking close action", e);
    }
  }

  private void unlock() {
    for (Lock lock : locks) {
      lock.unlock();
    }
  }
}
