package com.linkedin.venice.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test cases for Venice {@link StoreUtils}
 */
public class TestStoreUtils {
  @Test
  public void testGetMaxRecordSizeBytes() {
    long maxRecordSizeBytes = 1000L;
    Assert.assertEquals(
        StoreUtils.getMaxRecordSizeBytes(true, -1L),
        StoreUtils.DEFAULT_MAX_RECORD_SIZE_BYTES_WITH_CHUNKING);
    Assert.assertEquals(StoreUtils.getMaxRecordSizeBytes(true, maxRecordSizeBytes), maxRecordSizeBytes);
    Assert.assertEquals(StoreUtils.getMaxRecordSizeBytes(false, -1L), -1L);
    Assert.assertEquals(StoreUtils.getMaxRecordSizeBytes(false, maxRecordSizeBytes), maxRecordSizeBytes);
  }
}
