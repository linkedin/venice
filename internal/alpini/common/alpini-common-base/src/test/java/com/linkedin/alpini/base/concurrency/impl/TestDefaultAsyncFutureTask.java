package com.linkedin.alpini.base.concurrency.impl;

import com.linkedin.alpini.base.concurrency.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableFuture;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestDefaultAsyncFutureTask {
  @Test(groups = "unit")
  public void testCallable() throws ExecutionException, InterruptedException {

    Executor executor = Executors.newSingleThreadExecutor();

    Callable<Integer> callable = () -> 42;

    RunnableFuture<Integer> future = new DefaultAsyncFutureTask<>(callable, false);

    Assert.assertFalse(future.isDone());

    future.run();

    Assert.assertTrue(future.isDone());

    Assert.assertEquals(future.get(), (Integer) 42);

  }
}
