package com.linkedin.alpini.base.concurrency.impl;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 5/26/15.
 */
public class TestDefaultCollectingAsyncFuture {
  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testFailOnEmptyList() {
    List<AsyncFuture<List<Void>>> emptyList = Collections.emptyList();
    new DefaultCollectingAsyncFuture<>(emptyList, false);
  }

  @Test(groups = "unit")
  public void testSuccessOnSingleSucceededSingleton() throws ExecutionException, InterruptedException {
    List<AsyncFuture<List<Integer>>> list =
        Collections.<AsyncFuture<List<Integer>>>singletonList(new SuccessAsyncFuture<>(Collections.singletonList(42)));
    AsyncFuture<List<Integer>> future = AsyncFuture.collect(list, false);
    Assert.assertTrue(future.isSuccess());
    Assert.assertEquals(future.get().size(), 1);
    Assert.assertEquals(future.get().get(0), (Integer) 42);
  }

  @Test(groups = "unit")
  public void testSuccessOnTwoSucceededSingletons() throws ExecutionException, InterruptedException {
    List<AsyncFuture<List<Integer>>> list = new ArrayList<>();
    list.add(new SuccessAsyncFuture<>(Collections.singletonList(42)));
    list.add(new SuccessAsyncFuture<>(Collections.singletonList(24)));
    AsyncFuture<List<Integer>> future = AsyncFuture.collect(list, false);
    Assert.assertTrue(future.isSuccess());
    Assert.assertEquals(future.get().size(), 2);
    Assert.assertEquals(future.get().get(0), (Integer) 42);
    Assert.assertEquals(future.get().get(1), (Integer) 24);
  }

  @Test(groups = "unit")
  public void testWithTwoFutureSingletons() throws ExecutionException, InterruptedException {
    List<AsyncFuture<List<Integer>>> list = new ArrayList<>();
    AsyncPromise<List<Integer>> input = new DefaultAsyncFuture<>(false);
    list.add(input);
    list.add(new SuccessAsyncFuture<>(Collections.singletonList(24)));
    AsyncFuture<List<Integer>> future = AsyncFuture.collect(list, false);
    Assert.assertFalse(future.isDone());
    input.setSuccess(Collections.singletonList(42));
    Assert.assertTrue(future.isSuccess());
    Assert.assertEquals(future.get().size(), 2);
    Assert.assertTrue(future.get().containsAll(Arrays.asList(24, 42)));
  }

  @Test(groups = "unit")
  public void testWithFailedFuture() throws ExecutionException, InterruptedException {
    List<AsyncFuture<List<Integer>>> list = new ArrayList<>();
    AsyncPromise<List<Integer>> input = new DefaultAsyncFuture<>(false);
    list.add(input);
    AsyncPromise<List<Integer>> notyet = new DefaultAsyncFuture<>(false);
    list.add(notyet);
    AsyncFuture<List<Integer>> future = AsyncFuture.collect(list, false);
    Assert.assertFalse(future.isDone());
    Exception ex;
    input.setFailure(ex = new Exception());
    Assert.assertTrue(future.isDone());
    Assert.assertFalse(future.isSuccess());
    Assert.assertSame(future.getCause(), ex);
  }

  @Test(groups = "unit")
  public void testWithFailedFuture2() throws ExecutionException, InterruptedException {
    List<AsyncFuture<List<Integer>>> list = new ArrayList<>();
    AsyncPromise<List<Integer>> notyet = new DefaultAsyncFuture<>(false);
    list.add(notyet);
    AsyncPromise<List<Integer>> input = new DefaultAsyncFuture<>(false);
    list.add(input);
    AsyncFuture<List<Integer>> future = AsyncFuture.collect(list, false);
    Assert.assertFalse(future.isDone());
    Exception ex;
    input.setFailure(ex = new Exception());
    Assert.assertTrue(future.isDone());
    Assert.assertFalse(future.isSuccess());
    Assert.assertSame(future.getCause(), ex);
  }

  @Test(groups = "unit")
  public void testWithNullList() throws ExecutionException, InterruptedException {
    List<AsyncFuture<List<Integer>>> list = new ArrayList<>();
    AsyncPromise<List<Integer>> input = new DefaultAsyncFuture<>(false);
    list.add(input);
    list.add(new SuccessAsyncFuture<>(Collections.singletonList(24)));
    AsyncFuture<List<Integer>> future = AsyncFuture.collect(list, false);
    Assert.assertFalse(future.isDone());
    input.setSuccess(null);
    Assert.assertTrue(future.isDone());
    Assert.assertFalse(future.isSuccess());
    Assert.assertTrue(future.getCause() instanceof NullPointerException);
  }

  @Test(groups = "unit")
  public void testWithEmptyLists() throws ExecutionException, InterruptedException {
    List<AsyncFuture<List<Integer>>> list = new ArrayList<>();
    AsyncPromise<List<Integer>> input = new DefaultAsyncFuture<>(false);
    list.add(input);
    list.add(new SuccessAsyncFuture<>(Collections.<Integer>emptyList()));
    AsyncFuture<List<Integer>> future = AsyncFuture.collect(list, false);
    Assert.assertFalse(future.isDone());
    input.setSuccess(Collections.<Integer>emptyList());
    Assert.assertTrue(future.isSuccess());
    Assert.assertSame(future.get(), Collections.emptyList());
  }
}
