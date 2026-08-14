package com.linkedin.venice.blobtransfer;

import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BlobPeersDiscoveryResponseTest {
  @Test
  public void testServerHostNamesDefaultToEmpty() {
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();

    Assert.assertTrue(response.getServerHostNames().isEmpty());
    Assert.assertFalse(response.isSourceAware());
  }

  @Test
  public void testSetServerHostNamesToleratesNull() {
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setServerHostNames(null);

    Assert.assertTrue(response.getServerHostNames().isEmpty());
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testServerHostNamesAreImmutable() {
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setServerHostNames(Collections.singleton("server-host"));

    response.getServerHostNames().add("other-server");
  }
}
