package com.linkedin.alpini.base.concurrency;

import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 10/16/17.
 */
public class TestBlockingLatch {
  @Test(groups = "unit")
  public void testLatchUninterruptibly() throws InterruptedException {

    BlockingLatch latch = new BlockingLatch();

    Assert.assertFalse(latch.isBlocking());
    latch.setBlock(true);
    Assert.assertTrue(latch.isBlocking());

    AsyncPromise<Void> started = AsyncFuture.deferred(false);
    AsyncPromise<Void> completed = AsyncFuture.deferred(false);

    Thread thd = new Thread(() -> {
      started.setSuccess(null);
      latch.awaitUninterruptibly();
      completed.setSuccess(null);
    });

    thd.start();
    started.await();
    Thread.sleep(1);
    Assert.assertFalse(completed.isSuccess());
    latch.setBlock(false);
    completed.await();
    thd.join();
  }

  @Test(groups = "unit")
  public void testLatch() throws InterruptedException {

    BlockingLatch latch = new BlockingLatch();

    latch.setBlock(true);

    AsyncPromise<Void> started = AsyncFuture.deferred(false);
    AsyncPromise<Void> completed = AsyncFuture.deferred(false);

    Thread thd = new Thread(() -> {
      started.setSuccess(null);
      try {
        latch.await();
        completed.setSuccess(null);
      } catch (InterruptedException e) {
        completed.setFailure(e);
      }
    });

    thd.start();
    started.await();
    Thread.sleep(1);
    Assert.assertFalse(completed.isSuccess());
    latch.setBlock(false);
    completed.await();
    thd.join();
  }

  @Test(groups = "unit")
  public void testLatchUninterruptiblyNoTimeout() throws InterruptedException {

    BlockingLatch latch = new BlockingLatch();

    latch.setBlock(true);

    AsyncPromise<Void> started = AsyncFuture.deferred(false);
    AsyncPromise<Void> completed = AsyncFuture.deferred(false);

    Thread thd = new Thread(() -> {
      started.setSuccess(null);
      latch.awaitUninterruptibly(1, TimeUnit.SECONDS);
      completed.setSuccess(null);
    });

    thd.start();
    started.await();
    Thread.sleep(1);
    Assert.assertFalse(completed.isSuccess());
    latch.setBlock(false);
    completed.await();
    thd.join();
  }

  @Test(groups = "unit")
  public void testLatchNoTimeout() throws InterruptedException {

    BlockingLatch latch = new BlockingLatch();

    latch.setBlock(true);

    AsyncPromise<Void> started = AsyncFuture.deferred(false);
    AsyncPromise<Void> completed = AsyncFuture.deferred(false);

    Thread thd = new Thread(() -> {
      started.setSuccess(null);
      try {
        latch.await(1, TimeUnit.SECONDS);
        completed.setSuccess(null);
      } catch (InterruptedException e) {
        completed.setFailure(e);
      }
    });

    thd.start();
    started.await();
    Thread.sleep(1);
    Assert.assertFalse(completed.isSuccess());
    latch.setBlock(false);
    completed.await();
    thd.join();
  }
}
