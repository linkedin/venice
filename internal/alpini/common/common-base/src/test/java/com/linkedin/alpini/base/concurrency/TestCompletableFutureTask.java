package com.linkedin.alpini.base.concurrency;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 1/17/18.
 */
public class TestCompletableFutureTask {
  private final ExecutorService _executorService = Executors.newSingleThreadExecutor();

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    _executorService.shutdownNow();
  }

  @Test(groups = "unit")
  public void testException1() throws InterruptedException {
    class TestException1 extends Exception {
    }

    CompletableFutureTask<?> future = new CompletableFutureTask<>(() -> {
      throw new TestException1();
    });

    Assert.assertFalse(future.isDone());
    _executorService.execute(future);

    try {
      future.get();
      Assert.fail();
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof TestException1);
    }
    Assert.assertTrue(future.isDone());
  }

  @Test(groups = "unit")
  public void testException2() throws InterruptedException {
    class TestException2 extends Exception {
    }
    class DoomedException extends Exception {
    }

    CountDownLatch runLatch = new CountDownLatch(1);
    CountDownLatch holdLatch = new CountDownLatch(1);
    CompletableFutureTask<?> future = new CompletableFutureTask<>(() -> {
      runLatch.countDown();
      holdLatch.await();
      throw new DoomedException();
    });

    _executorService.execute(future);
    runLatch.await(1000, TimeUnit.MILLISECONDS);
    Assert.assertFalse(future.isDone());
    Assert.assertTrue(future.toCompletableFuture().completeExceptionally(new TestException2()));
    Assert.assertTrue(future.isDone());
    holdLatch.countDown();

    try {
      future.get();
      Assert.fail();
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof TestException2);
    }
    Assert.assertTrue(future.isDone());
  }

  @Test(groups = "unit")
  public void testCancellation1() throws InterruptedException, ExecutionException {
    CompletableFutureTask<?> future = new CompletableFutureTask<>(() -> {
      throw new Exception();
    });

    Assert.assertTrue(future.cancel(false));

    _executorService.execute(future);

    Assert.assertTrue(future.isCancelled());

    try {
      future.get();
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertTrue(e instanceof CancellationException);
    }
  }

  @Test(groups = "unit")
  public void testCancellation2() throws InterruptedException, ExecutionException {
    class SuccessObject2 {
    }

    CountDownLatch runLatch = new CountDownLatch(1);
    CountDownLatch holdLatch = new CountDownLatch(1);
    CountDownLatch waitedLatch = new CountDownLatch(1);
    CompletableFutureTask<?> future = new CompletableFutureTask<>(() -> {
      runLatch.countDown();
      holdLatch.await();
      waitedLatch.countDown();
      return new SuccessObject2();
    });

    _executorService.execute(future);
    runLatch.await(1000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(future.cancel(false));
    holdLatch.countDown();

    Assert.assertTrue(future.isCancelled());
    Assert.assertTrue(waitedLatch.await(1000, TimeUnit.MILLISECONDS));
  }

  @Test(groups = "unit")
  public void testCancellation3() throws InterruptedException, ExecutionException {
    class SuccessObject3 {
    }

    CountDownLatch runLatch = new CountDownLatch(1);
    CountDownLatch holdLatch = new CountDownLatch(1);
    CountDownLatch waitedLatch = new CountDownLatch(1);
    CompletableFutureTask<?> future = new CompletableFutureTask<>(() -> {
      runLatch.countDown();
      holdLatch.await();
      waitedLatch.countDown();
      return new SuccessObject3();
    });

    _executorService.execute(future);
    runLatch.await(1000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(future.cancel(true));
    holdLatch.countDown();

    Assert.assertTrue(future.isCancelled());
    Assert.assertFalse(waitedLatch.await(1000, TimeUnit.MILLISECONDS));
  }

  @Test(groups = "unit")
  public void testSuccess1() throws InterruptedException, ExecutionException, TimeoutException {
    class SuccessObject1 {
    }

    CountDownLatch runLatch = new CountDownLatch(1);
    CountDownLatch holdLatch = new CountDownLatch(1);
    CountDownLatch waitedLatch = new CountDownLatch(1);
    CompletableFutureTask<?> future = new CompletableFutureTask<>(() -> {
      runLatch.countDown();
      holdLatch.await();
      waitedLatch.countDown();
      return new SuccessObject1();
    });

    _executorService.execute(future);
    runLatch.await(1000, TimeUnit.MILLISECONDS);
    holdLatch.countDown();

    Assert.assertTrue(future.get(1000, TimeUnit.MILLISECONDS) instanceof SuccessObject1);
    Assert.assertTrue(waitedLatch.await(0, TimeUnit.MILLISECONDS));
  }

  @Test(groups = "unit")
  public void testSuccess2() throws InterruptedException, ExecutionException, TimeoutException {
    class SuccessObject {
    }
    class DoomedObject {
    }

    CountDownLatch runLatch = new CountDownLatch(1);
    CountDownLatch holdLatch = new CountDownLatch(1);
    CountDownLatch waitedLatch = new CountDownLatch(1);
    CompletableFutureTask<Object> future = new CompletableFutureTask<>(() -> {
      try {
        runLatch.countDown();
        holdLatch.await();
        waitedLatch.countDown();
      } catch (Exception ex) {
        // ignore
      }
    }, new DoomedObject());

    _executorService.execute(future);
    runLatch.await(1000, TimeUnit.MILLISECONDS);
    future.toCompletableFuture().complete(new SuccessObject());
    holdLatch.countDown();

    Assert.assertTrue(future.get(1000, TimeUnit.MILLISECONDS) instanceof SuccessObject);
    Assert.assertTrue(waitedLatch.await(1000, TimeUnit.MILLISECONDS));
  }
}
