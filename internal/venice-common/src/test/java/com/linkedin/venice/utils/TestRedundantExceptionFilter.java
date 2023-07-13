package com.linkedin.venice.utils;

import com.linkedin.alpini.router.api.RouterException;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestRedundantExceptionFilter {
  @Test
  public void testIsRedundantException() {
    long duration = 1000;
    RedundantExceptionFilter filter =
        new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, duration);
    String store = "testLogException";
    String store1 = "testLogException1";
    HttpResponseStatus status = HttpResponseStatus.NOT_FOUND;
    HttpResponseStatus status1 = HttpResponseStatus.TOO_MANY_REQUESTS;
    Exception e = new RouterException(HttpResponseStatus.class, status, status.code(), "test", false);
    Assert.assertFalse(filter.isRedundantException(store, e), "This is the first time we see this exception.");
    Assert.assertFalse(filter.isRedundantException(store1, e), "This is the first time we see this exception.");
    Assert.assertFalse(
        filter.isRedundantException(store, String.valueOf(status.code())),
        "This is the first time we see this exception.");
    Assert.assertFalse(
        filter.isRedundantException(store1, String.valueOf(status1.code())),
        "This is the first time we see this exception.");
    Assert.assertTrue(
        filter.isRedundantException(store, String.valueOf(status.code())),
        "This is the second time we see this exception.");
    // After duration the filter's bitset will be cleaned up.
    TestUtils.waitForNonDeterministicCompletion(
        duration * 2,
        TimeUnit.MILLISECONDS,
        () -> !filter.isRedundantException(store, e));
  }

  @Test
  public void testClear() {
    long duration = 10000000;
    RedundantExceptionFilter filter =
        new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, duration);
    String store = "testClear";
    HttpResponseStatus status = HttpResponseStatus.NOT_FOUND;
    Exception e = new RouterException(HttpResponseStatus.class, status, status.code(), "test", false);
    filter.isRedundantException(store, e);
    filter.clearBitSet();
    Assert.assertFalse(filter.isRedundantException(store, e));
  }

  @Test
  public void testGetFilter() {
    RedundantExceptionFilter filter = RedundantExceptionFilter.getRedundantExceptionFilter();
    RedundantExceptionFilter filter1 = RedundantExceptionFilter.getRedundantExceptionFilter();
    Assert.assertTrue(filter == filter1);
  }
}
