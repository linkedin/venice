package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class TestRedundantExceptionFilter {
  @Test
  public void testIsRedundantException() {
    long duration = 1000;
    RedundantExceptionFilter filter =
        new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, duration);
    try {
      String store = "testLogException";
      String store1 = "testLogException1";
      HttpResponseStatus status = HttpResponseStatus.NOT_FOUND;
      HttpResponseStatus status1 = HttpResponseStatus.TOO_MANY_REQUESTS;
      Exception e = new VeniceHttpException(status.code(), "testException");
      assertFalse(filter.isRedundantException(store, e), "This is the first time we see this exception.");
      assertFalse(filter.isRedundantException(store1, e), "This is the first time we see this exception.");
      assertTrue(
          filter.isRedundantException(store, String.valueOf(status.code())),
          "This is the second time we see this exception.");
      assertFalse(
          filter.isRedundantException(store1, String.valueOf(status1.code())),
          "This is the first time we see this exception.");
      assertTrue(
          filter.isRedundantException(store1, String.valueOf(status1.code())),
          "This is the second time we see this exception.");
      // After duration the filter's bitset will be cleaned up.
      TestUtils.waitForNonDeterministicCompletion(
          duration * 2,
          TimeUnit.MILLISECONDS,
          () -> !filter.isRedundantException(store, e));
    } finally {
      filter.shutdown();
    }
  }

  @Test
  public void testIsRedundantExceptionWithUpdateRedundancyFlag() {
    RedundantExceptionFilter filter = new RedundantExceptionFilter();
    try {
      String msg = "testLogException";
      // First time check the msg with updateRedundancy as false
      assertFalse(filter.isRedundantException(msg, false));
      // Second time check the msg: should not be redundant as updateRedundancy was false
      assertFalse(filter.isRedundantException(msg));
      // third time check the msg: should be redundant as updateRedundancy was true
      assertTrue(filter.isRedundantException(msg));

      filter.clearBitSet();
      // Fourth time check the msg: should not be redundant due to clearBitSet
      assertFalse(filter.isRedundantException(msg, false));

      // The below functions sets updateRedundancy as true by default, so the second time is redundant
      String storeName = "testStore";
      HttpResponseStatus status = HttpResponseStatus.NOT_FOUND;
      Exception e = new VeniceHttpException(status.code(), "testException");
      // First time
      assertFalse(filter.isRedundantException(storeName, e));
      // second time
      assertTrue(filter.isRedundantException(storeName, e));
      filter.clearBitSet();
      assertFalse(filter.isRedundantException(storeName, e));

      // Second time: as its same status code
      assertTrue(filter.isRedundantException(storeName, String.valueOf(status.code())));
      filter.clearBitSet();
      assertFalse(filter.isRedundantException(storeName, String.valueOf(status.code())));
    } finally {
      filter.shutdown();
    }
  }

  @Test
  public void testClear() {
    long duration = 10000000;
    RedundantExceptionFilter filter =
        new RedundantExceptionFilter(RedundantExceptionFilter.DEFAULT_BITSET_SIZE, duration);
    try {
      String store = "testClear";
      Exception e = new VeniceException("testException");
      filter.isRedundantException(store, e);
      assertTrue(filter.isRedundantException(store, e));
      filter.clearBitSet();
      assertFalse(filter.isRedundantException(store, e));
    } finally {
      filter.shutdown();
    }
  }

  @Test
  public void testGetFilter() {
    RedundantExceptionFilter filter = RedundantExceptionFilter.getRedundantExceptionFilter();
    RedundantExceptionFilter filter1 = RedundantExceptionFilter.getRedundantExceptionFilter();
    assertTrue(filter == filter1);
    filter.shutdown();
    filter1.shutdown();
  }

  @Test
  public void testGetIndex() {
    RedundantExceptionFilter filter = new RedundantExceptionFilter();
    try {
      String key1 = "key1";
      String key2 = "key2";

      int index1 = filter.getIndex(key1);
      int index2 = filter.getIndex(key2);

      assertNotEquals(index1, index2, "Indexes should be different for different keys.");

      index1 = filter.getIndex(key1);
      index2 = filter.getIndex(key1);

      assertEquals(index1, index2, "Indexes should be Equal for same keys.");
    } finally {
      filter.shutdown();
    }
  }

  @Test
  public void testIsRedundant() {
    RedundantExceptionFilter filter = new RedundantExceptionFilter();
    try {
      int index1 = 1;
      int index2 = 2;
      assertFalse(filter.isRedundant(index1, false));
      assertFalse(filter.isRedundant(index1, true));
      assertTrue(filter.isRedundant(index1, true));
      assertFalse(filter.isRedundant(index2, true));
      assertTrue(filter.isRedundant(index2, true));

      filter.clearBitSet();
      assertFalse(filter.isRedundant(index1, true));
      assertTrue(filter.isRedundant(index1, false));
      assertTrue(filter.isRedundant(index1, true));
      assertFalse(filter.isRedundant(index2, true));
      assertTrue(filter.isRedundant(index2, true));
    } finally {
      filter.shutdown();
    }
  }

  @Test
  public void testConstructorWithDefaults() {
    RedundantExceptionFilter filter = new RedundantExceptionFilter();
    assertNotNull(filter);
    filter.shutdown();
  }

  @Test
  public void testConstructorWithCustomValues() {
    int bitSetSize = 1000;
    long duration = TimeUnit.SECONDS.toMillis(30);

    RedundantExceptionFilter filter = new RedundantExceptionFilter(bitSetSize, duration);
    assertNotNull(filter);
    filter.shutdown();
  }
}
