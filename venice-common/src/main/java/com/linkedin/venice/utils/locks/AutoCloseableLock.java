package com.linkedin.venice.utils.locks;

import com.linkedin.venice.utils.CollectionUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;


public class AutoCloseableLock implements AutoCloseable {
  private final List<Lock> locks;

  public AutoCloseableLock(List<Lock> locks) {
    CollectionUtils.assertCollectionsNotEmpty(locks);
    for (Lock lock : locks) {
      lock.lock();
    }

    this.locks = locks;
  }

  public AutoCloseableLock(Lock lock) {
    this(Collections.singletonList(Utils.notNull(lock)));
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
