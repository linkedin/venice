package com.linkedin.venice.utils.locks;

import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestResourceAutoClosableLockManager {
  private ResourceAutoClosableLockManager resourceAutoClosableLockManager;

  @BeforeClass
  public void setUp() {
    resourceAutoClosableLockManager = new ResourceAutoClosableLockManager(() -> new ReentrantLock());
  }

  /**
   * N.B. Invocation count > 1 ensures that the locks are reusable, and thus closed properly after their first usage.
   */
  @Test(invocationCount = 5, timeOut = 5 * Time.MS_PER_SECOND)
  public void testStoreWriteLock() throws Exception {
    final String store1 = "store1";
    final AtomicInteger value1 = new AtomicInteger(0);
    final String store2 = "store2";
    final AtomicInteger value2 = new AtomicInteger(0);
    final int VALUE_WRITTEN_BY_THREAD_1 = 1;
    final int VALUE_WRITTEN_BY_THREAD_2 = 2;
    final int VALUE_WRITTEN_BY_THREAD_3 = 3;

    Thread thread2 = new Thread(() -> {
      try (AutoCloseableLock ignore2 = resourceAutoClosableLockManager.getLockForResource(store1)) {
        value1.set(VALUE_WRITTEN_BY_THREAD_2);
      }
    });

    Thread thread3 = new Thread(() -> {
      try (AutoCloseableLock ignore2 = resourceAutoClosableLockManager.getLockForResource(store2)) {
        value2.set(VALUE_WRITTEN_BY_THREAD_3);
      }
    });

    try (AutoCloseableLock ignore1 = resourceAutoClosableLockManager.getLockForResource(store1)) {
      value1.set(VALUE_WRITTEN_BY_THREAD_1);
      thread2.start();
      Assert.assertEquals(
          value1.get(),
          VALUE_WRITTEN_BY_THREAD_1,
          "The write lock for store1 is acquired by a thread1. Value should not be updated by thread2.");

      value2.set(VALUE_WRITTEN_BY_THREAD_1);
      thread3.start();
      thread3.join();
      TestUtils.waitForNonDeterministicAssertion(
          1,
          TimeUnit.SECONDS,
          () -> Assert.assertEquals(
              value2.get(),
              VALUE_WRITTEN_BY_THREAD_3,
              "The write lock for store2 is not acquired by thread1. Value should be updated by thread3."));
    } finally {
      thread2.join();
    }

    TestUtils.waitForNonDeterministicAssertion(
        1,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(
            value1.get(),
            VALUE_WRITTEN_BY_THREAD_2,
            "Thread2 should already acquire the lock and modify the value1."));
  }
}
