package com.linkedin.venice.exceptions;

import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceNoStoreExceptionTest {
  @Test
  public void testException() {
    String storeName = "test_store";
    String clusterName = "test_cluster";

    VeniceNoStoreException e1 = new VeniceNoStoreException(storeName);
    Assert.assertEquals(e1.getStoreName(), storeName);
    Assert.assertEquals(e1.getErrorType(), ErrorType.STORE_NOT_FOUND);

    VeniceNoStoreException e2 = new VeniceNoStoreException(storeName, clusterName);
    Assert.assertEquals(e2.getStoreName(), storeName);
    Assert.assertEquals(e2.getClusterName(), clusterName);
    Assert.assertEquals(e2.getErrorType(), ErrorType.STORE_NOT_FOUND);

    String additionalMessage = "additional_message";
    VeniceNoStoreException e3 = new VeniceNoStoreException(storeName, clusterName, additionalMessage);
    Assert.assertEquals(e3.getStoreName(), storeName);
    Assert.assertEquals(e3.getClusterName(), clusterName);
    Assert.assertTrue(e3.toString().contains(additionalMessage));
    Assert.assertEquals(e3.getErrorType(), ErrorType.STORE_NOT_FOUND);

    VeniceException e = new VeniceException("Error");
    VeniceNoStoreException e4 = new VeniceNoStoreException(storeName, e);
    Assert.assertEquals(e4.getStoreName(), storeName);
    Assert.assertEquals(e4.getCause(), e);
    Assert.assertEquals(e4.getErrorType(), ErrorType.STORE_NOT_FOUND);

    VeniceNoStoreException e5 = new VeniceNoStoreException(storeName, clusterName, e);
    Assert.assertEquals(e5.getStoreName(), storeName);
    Assert.assertEquals(e5.getCause(), e);
    Assert.assertEquals(e5.getClusterName(), clusterName);
    Assert.assertEquals(e5.getErrorType(), ErrorType.STORE_NOT_FOUND);
  }
}
