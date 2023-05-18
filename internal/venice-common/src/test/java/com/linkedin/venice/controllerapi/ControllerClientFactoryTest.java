package com.linkedin.venice.controllerapi;

import java.util.Optional;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ControllerClientFactoryTest {
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String STORE_NAME = "test_store";
  private static final String NON_EXISTENT_STORE_NAME = "test_missing_store";
  private static final String DISCOVERY_CLUSTER_NAME = "*";
  private static final String DISCOVERY_URL = "localhost:2020";

  @BeforeClass
  public void setup() {
    ControllerClientFactory.setUnitTestMode();
  }

  @AfterClass
  public void teardown() {
    ControllerClientFactory.resetUnitTestMode();
  }

  @Test
  public void testSharedControllerClient() {
    ControllerClient mockControllerClient = Mockito.mock(ControllerClient.class);
    ControllerClientFactory.setControllerClient("*", DISCOVERY_URL, mockControllerClient);
    try (ControllerClient client =
        ControllerClientFactory.discoverAndConstructControllerClient(STORE_NAME, DISCOVERY_URL, Optional.empty(), 1)) {
      Assert.assertEquals(client.getClusterName(), CLUSTER_NAME);
    }

    // ControllerClient mockControllerClient = Mockito.mock(ControllerClient.class);
    // ControllerClientFactory.setControllerClient(CLUSTER_NAME, DISCOVERY_URL, mockControllerClient);
  }
}
