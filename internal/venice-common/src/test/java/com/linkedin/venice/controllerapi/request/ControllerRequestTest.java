package com.linkedin.venice.controllerapi.request;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;


public class ControllerRequestTest {
  private static final String CLUSTER = "cluster";
  private static final String STORE_NAME = "store_name";

  @Test
  public void testValidInputs() {
    // Test with only cluster name
    ControllerRequest request1 = new ControllerRequest("testCluster");
    assertEquals("testCluster", request1.getClusterName());
    assertNull(request1.getStoreName());

    // Test with cluster name and store name
    ControllerRequest request2 = new ControllerRequest("testCluster", "testStore");
    assertEquals("testCluster", request2.getClusterName());
    assertEquals("testStore", request2.getStoreName());
  }

  @Test
  public void testInvalidClusterName() {
    Exception exception1 = expectThrows(IllegalArgumentException.class, () -> new ControllerRequest(null));
    assertEquals("The request is missing the cluster_name, which is a mandatory field.", exception1.getMessage());

    Exception exception2 = expectThrows(IllegalArgumentException.class, () -> new ControllerRequest(""));
    assertEquals("The request is missing the cluster_name, which is a mandatory field.", exception2.getMessage());

    Exception exception3 = expectThrows(IllegalArgumentException.class, () -> new ControllerRequest(null, "testStore"));
    assertEquals("The request is missing the cluster_name, which is a mandatory field.", exception3.getMessage());

    Exception exception4 = expectThrows(IllegalArgumentException.class, () -> new ControllerRequest("", "testStore"));
    assertEquals("The request is missing the cluster_name, which is a mandatory field.", exception4.getMessage());
  }

  @Test
  public void testInvalidStoreName() {
    Exception exception1 =
        expectThrows(IllegalArgumentException.class, () -> new ControllerRequest("testCluster", null));
    assertEquals("The request is missing the store_name, which is a mandatory field.", exception1.getMessage());

    Exception exception2 = expectThrows(IllegalArgumentException.class, () -> new ControllerRequest("testCluster", ""));
    assertEquals("The request is missing the store_name, which is a mandatory field.", exception2.getMessage());
  }

  @Test
  public void testValidateParam() {
    assertEquals("validParam", ControllerRequest.validateParam("validParam", CLUSTER));

    Exception exception1 =
        expectThrows(IllegalArgumentException.class, () -> ControllerRequest.validateParam(null, CLUSTER));
    assertEquals("The request is missing the cluster, which is a mandatory field.", exception1.getMessage());

    Exception exception2 =
        expectThrows(IllegalArgumentException.class, () -> ControllerRequest.validateParam("", STORE_NAME));
    assertEquals("The request is missing the store_name, which is a mandatory field.", exception2.getMessage());
  }
}
