package com.linkedin.venice.controllerapi.request;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;


public class ClusterDiscoveryRequestTest {
  @Test
  public void testClusterDiscoveryRequest() {
    // Case 1: Store name is provided
    ClusterDiscoveryRequest clusterDiscoveryRequest = new ClusterDiscoveryRequest("storeName");
    assertNotNull(clusterDiscoveryRequest.getStoreName());
    assertEquals(clusterDiscoveryRequest.getStoreName(), "storeName");

    // Case 2: Store name is not provided
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> new ClusterDiscoveryRequest(null));
    assertTrue(exception.getMessage().contains("The request is missing the store_name"));
  }
}
