package com.linkedin.venice.meta;

import static org.mockito.Mockito.mock;

import org.testng.Assert;
import org.testng.annotations.Test;


public class StoreVersionInfoTest {
  @Test
  public void testSetAndGet() {
    Store store = mock(Store.class);
    Version version = mock(Version.class);
    StoreVersionInfo storeVersionInfo = new StoreVersionInfo(store, version);
    Assert.assertEquals(storeVersionInfo.getStore(), store);
    Assert.assertEquals(storeVersionInfo.getVersion(), version);
  }
}
