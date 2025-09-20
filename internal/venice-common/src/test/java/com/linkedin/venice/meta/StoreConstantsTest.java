package com.linkedin.venice.meta;

import org.testng.Assert;
import org.testng.annotations.Test;


public class StoreConstantsTest {
  @Test(description = "Test BATCH GET limit constant")
  public void testBatchGetLimitConstant() {
    // The batch get limit is set to 500 in Store for the DEFAULT_BATCH_GET_LIMIT constant.
    int expectedBatchGetLimit = 500;
    Assert.assertEquals(Store.DEFAULT_BATCH_GET_LIMIT, expectedBatchGetLimit);
  }
}
