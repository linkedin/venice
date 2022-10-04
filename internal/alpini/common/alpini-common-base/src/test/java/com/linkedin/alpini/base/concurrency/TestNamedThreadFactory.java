/*
 * $Id$
 */
package com.linkedin.alpini.base.concurrency;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Jemiah Westerman &lt;jwesterman@linkedin.com&gt;
 */
public class TestNamedThreadFactory {
  @Test(groups = "unit")
  public void testThreadNames() throws Exception {
    NamedThreadFactory poolA = new NamedThreadFactory("testThreadNames-poolA");
    ExecutorService exec1 = Executors.newCachedThreadPool(poolA);
    final ArrayList<String> threadNames = new ArrayList<String>(5);
    final CountDownLatch startLatch = new CountDownLatch(5);
    final CountDownLatch endLatch = new CountDownLatch(5);
    for (int i = 0; i < 5; i++) {
      exec1.execute(new Runnable() {
        @Override
        public void run() {
          // Make sure we start all 5 threads by blocking on the startlatch.
          String threadName = Thread.currentThread().getName();
          startLatch.countDown();
          try {
            startLatch.await();
          } catch (InterruptedException ex) {
          }

          // Remember the thread name
          synchronized (threadNames) {
            threadNames.add(threadName);
          }
          endLatch.countDown();
        }
      });

    }

    endLatch.await();

    // Pool and thread names start at 1, not 0
    for (int i = 1; i < 6; i++) {
      Assert.assertTrue(
          threadNames.contains("testThreadNames-poolA-1-thread-" + i),
          "Expected a thread named poolA-0-thread-" + i + ", but it did not exist. Thread names are " + threadNames
              + ".");
    }

    exec1.shutdown();
  }

  @Test(groups = "unit")
  public void testPoolNames() throws Exception {
    // Create a pool named "poolA". Since this is the first "poolA" we have created,
    // it should get the name "poolA-1".
    NamedThreadFactory poolA_1 = new NamedThreadFactory("testPoolNames-poolA");
    ExecutorService execA_1 = Executors.newCachedThreadPool(poolA_1);
    final AtomicReference<String> _ref = new AtomicReference<String>(null);
    execA_1.execute(new Runnable() {
      @Override
      public void run() {
        _ref.set(Thread.currentThread().getName());
      }
    });
    while (_ref.get() == null)
      Thread.sleep(100);
    Assert.assertEquals(_ref.get(), "testPoolNames-poolA-1-thread-1", "Wrong name for first instance of poolA.");

    // Create a second pool named "poolA". Since this is the second "poolA" we have created,
    // it should get the name "poolA-2".
    NamedThreadFactory poolA_2 = new NamedThreadFactory("testPoolNames-poolA");
    ExecutorService execA_2 = Executors.newCachedThreadPool(poolA_2);
    _ref.set(null);
    execA_2.execute(new Runnable() {
      @Override
      public void run() {
        _ref.set(Thread.currentThread().getName());
      }
    });
    while (_ref.get() == null)
      Thread.sleep(100);
    Assert.assertEquals(_ref.get(), "testPoolNames-poolA-2-thread-1", "Wrong name for second instance of poolA.");

    // Create a pool named "poolB". Since this is the first "poolB" we have created,
    // it should get the name "poolB-1". Make sure it didn't collide with "poolA" and get
    // a name like "poolB-3".
    NamedThreadFactory poolB_1 = new NamedThreadFactory("testPoolNames-poolB");
    ExecutorService execB_1 = Executors.newCachedThreadPool(poolB_1);
    _ref.set(null);
    execB_1.execute(new Runnable() {
      @Override
      public void run() {
        _ref.set(Thread.currentThread().getName());
      }
    });
    while (_ref.get() == null)
      Thread.sleep(100);
    Assert.assertEquals(_ref.get(), "testPoolNames-poolB-1-thread-1", "Wrong name for first instance of poolB.");

    execA_1.shutdown();
    execA_2.shutdown();
    execB_1.shutdown();
  }

  @Test(groups = "unit")
  public void testThreadGroup() throws InterruptedException, ExecutionException {
    ThreadGroup groupA = new ThreadGroup(getClass() + "-A");
    ThreadGroup groupB = new ThreadGroup(getClass() + "-B");
    // Create a pool named "poolA". Since this is the first "poolA" we have created,
    // it should get the name "poolA-1".
    NamedThreadFactory poolA_1 = new NamedThreadFactory(groupA, "testPoolNames-poolA");
    ExecutorService execA_1 = Executors.newCachedThreadPool(poolA_1);
    final AtomicReference<String> _ref = new AtomicReference<String>(null);
    execA_1.submit(new Runnable() {
      @Override
      public void run() {
        _ref.set(Thread.currentThread().getThreadGroup().getName());
      }
    }).get();
    Assert.assertEquals(_ref.get(), groupA.getName(), "Wrong name for first instance of poolA.");

    // Create a second pool named "poolA". Since this is the second "poolA" we have created,
    // it should get the name "poolA-2".
    NamedThreadFactory poolA_2 = new NamedThreadFactory(groupA, "testPoolNames-poolA");
    ExecutorService execA_2 = Executors.newCachedThreadPool(poolA_2);
    _ref.set(null);
    execA_2.submit(new Runnable() {
      @Override
      public void run() {
        _ref.set(Thread.currentThread().getThreadGroup().getName());
      }
    }).get();
    Assert.assertEquals(_ref.get(), groupA.getName(), "Wrong name for second instance of poolA.");

    // Create a pool named "poolB". Since this is the first "poolB" we have created,
    // it should get the name "poolB-1". Make sure it didn't collide with "poolA" and get
    // a name like "poolB-3".
    NamedThreadFactory poolB_1 = new NamedThreadFactory(groupB, "testPoolNames-poolB");
    ExecutorService execB_1 = Executors.newCachedThreadPool(poolB_1);
    _ref.set(null);
    execB_1.submit(new Runnable() {
      @Override
      public void run() {
        _ref.set(Thread.currentThread().getThreadGroup().getName());
      }
    }).get();
    Assert.assertEquals(_ref.get(), groupB.getName(), "Wrong name for first instance of poolB.");

    execA_1.shutdown();
    execA_2.shutdown();
    execB_1.shutdown();

    execA_1.awaitTermination(1, TimeUnit.SECONDS);
    execA_2.awaitTermination(1, TimeUnit.SECONDS);
    execB_1.awaitTermination(1, TimeUnit.SECONDS);

    // Wait for threads to finish exiting.
    while (groupA.activeCount() > 0 || groupB.activeCount() > 0)
      Thread.sleep(100);

    groupA.destroy();
    groupB.destroy();
  }
}
