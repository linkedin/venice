package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.misc.Time;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestFutures {
  @Test
  public void testAsNettyFuture() throws InterruptedException {
    CompletableFuture<Integer> future1 = new CompletableFuture<>();

    Future<Integer> future = Futures.asNettyFuture(future1);

    Assert.assertFalse(future.isDone());

    future1.complete(42);
    Assert.assertEquals(future.getNow(), (Integer) 42);

    future1 = new CompletableFuture<>();
    future1.cancel(false);

    future = Futures.asNettyFuture(future1);

    Assert.assertTrue(future.isDone());
    Assert.assertTrue(future.isCancelled());

    future1 = new CompletableFuture<>();

    future = Futures.asNettyFuture(future1);
    future.cancel(false);

    Assert.assertTrue(future1.isDone());
    Assert.assertTrue(future1.isCancelled());

    CompletableFuture<Integer> future2 = new CompletableFuture<>();
    future = Futures.asNettyFuture(future2);

    Executors.newSingleThreadExecutor().submit(() -> {
      Time.sleep(100);
      return future2.complete(42);
    });
    Assert.assertFalse(future.isDone());
    Assert.assertEquals(future.await().getNow(), (Integer) 42);
  }

  @Test
  public void testAllOf1() {
    Promise<Integer> promise1 = ImmediateEventExecutor.INSTANCE.newPromise();
    Promise<Integer> promise2 = ImmediateEventExecutor.INSTANCE.newPromise();

    Future<?> future = Futures.allOf(promise1, promise2);

    Assert.assertFalse(future.isDone());
    promise1.setSuccess(42);
    Assert.assertFalse(future.isDone());
    promise2.setSuccess(34);
    Assert.assertTrue(future.isDone());
  }
}
