package com.linkedin.alpini.base.concurrency.impl;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncFutureListener;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public class TestSuccessAsyncFuture {
  @Test(groups = { "unit" })
  public void testIsSuccess() {
    Assert.assertTrue(new SuccessAsyncFuture<Boolean>(true).isSuccess());
  }

  @Test(groups = { "unit" })
  public void testGetCause() {
    Assert.assertNull(new SuccessAsyncFuture<Boolean>(true).getCause());
  }

  @Test(groups = { "unit" })
  public void testSetSuccess() {
    Assert.assertFalse(new SuccessAsyncFuture<Boolean>(true).setSuccess(false));
  }

  @Test(groups = { "unit" })
  public void testSetFailure() {
    Assert.assertFalse(new SuccessAsyncFuture<Boolean>(true).setFailure(new Exception()));
  }

  @Test(groups = { "unit" })
  public void testAddListener1() {
    String string = "Hello World";
    final String[] result = new String[1];
    final Thread[] thd = new Thread[1];

    new SuccessAsyncFuture<String>(string).addListener(new AsyncFutureListener<String>() {
      @Override
      public void operationComplete(AsyncFuture<String> future) throws Exception {
        result[0] = future.get();
        thd[0] = Thread.currentThread();
      }
    });
    Assert.assertSame(thd[0], Thread.currentThread());
    Assert.assertSame(result[0], string);
  }

  @Test(groups = { "unit" })
  public void testAddListener2() throws ExecutionException, InterruptedException {
    String string = "Hello World";
    AsyncPromise<String> future = new DefaultAsyncFuture<String>(false);
    new SuccessAsyncFuture<String>(string).addListener(future);
    Assert.assertSame(future.get(), string);
  }

  @Test(groups = { "unit" })
  public void testAwait() throws InterruptedException {
    AsyncFuture<Boolean> future = new SuccessAsyncFuture<Boolean>(true);
    Assert.assertSame(future.await(), future);
  }

  @Test(groups = { "unit" })
  public void testAwaitUninterruptibly() {
    AsyncFuture<Boolean> future = new SuccessAsyncFuture<Boolean>(true);
    Assert.assertSame(future.awaitUninterruptibly(), future);
  }

  @Test(groups = { "unit" })
  public void testAwait2() throws InterruptedException {
    AsyncFuture<Boolean> future = new SuccessAsyncFuture<Boolean>(true);
    Assert.assertTrue(future.await(0, null));
  }

  @Test(groups = { "unit" })
  public void testAwaitUninterruptibly2() {
    AsyncFuture<Boolean> future = new SuccessAsyncFuture<Boolean>(true);
    Assert.assertTrue(future.awaitUninterruptibly(0, null));
  }

  @Test(groups = { "unit" })
  public void testCancel() {
    Assert.assertFalse(new SuccessAsyncFuture<Boolean>(true).cancel(true));
  }

  @Test(groups = { "unit" })
  public void testIsCancelled() {
    Assert.assertFalse(new SuccessAsyncFuture<Boolean>(true).isCancelled());
  }

  @Test(groups = { "unit" })
  public void testIsDone() {
    Assert.assertTrue(new SuccessAsyncFuture<Boolean>(true).isDone());
  }

  @Test(groups = { "unit" })
  public void testGet() throws ExecutionException, InterruptedException {
    Object test = 1234;
    Assert.assertSame(new SuccessAsyncFuture<Object>(test).get(), test);
  }

  @Test(groups = { "unit" })
  public void testGet2() throws ExecutionException, InterruptedException, TimeoutException {
    Object test = 1234;
    Assert.assertSame(new SuccessAsyncFuture<Object>(test).get(0, null), test);
  }
}
