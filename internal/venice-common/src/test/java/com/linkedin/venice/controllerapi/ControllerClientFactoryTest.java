package com.linkedin.venice.controllerapi;

import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
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
  public void setUp() {
    ControllerClientFactory.setUnitTestMode();
  }

  @AfterClass
  public void teardown() {
    ControllerClientFactory.resetUnitTestMode();
  }

  @Test
  public void testSharedControllerClient() {
    ControllerClient mockControllerClient = Mockito.mock(ControllerClient.class);
    D2ServiceDiscoveryResponse discoveryResponse = new D2ServiceDiscoveryResponse();
    discoveryResponse.setName(STORE_NAME);
    discoveryResponse.setCluster(CLUSTER_NAME);
    Mockito.doReturn(discoveryResponse).when(mockControllerClient).discoverCluster(STORE_NAME);

    ControllerClientFactory.setControllerClient(DISCOVERY_CLUSTER_NAME, DISCOVERY_URL, mockControllerClient);
    try (ControllerClient client =
        ControllerClientFactory.discoverAndConstructControllerClient(STORE_NAME, DISCOVERY_URL, Optional.empty(), 1)) {
      Assert.assertEquals(client.getClusterName(), CLUSTER_NAME);
    }

    D2ServiceDiscoveryResponse nonExistentDiscoveryResponse = new D2ServiceDiscoveryResponse();
    nonExistentDiscoveryResponse.setErrorType(ErrorType.STORE_NOT_FOUND);
    nonExistentDiscoveryResponse.setError("Store " + NON_EXISTENT_STORE_NAME + " not found");
    nonExistentDiscoveryResponse.setName(NON_EXISTENT_STORE_NAME);
    Mockito.doReturn(nonExistentDiscoveryResponse).when(mockControllerClient).discoverCluster(NON_EXISTENT_STORE_NAME);

    Assert.assertThrows(
        VeniceNoStoreException.class,
        () -> ControllerClientFactory
            .discoverAndConstructControllerClient(NON_EXISTENT_STORE_NAME, DISCOVERY_URL, Optional.empty(), 1));

    D2ServiceDiscoveryResponse nonExistentDiscoveryResponseLegacy = new D2ServiceDiscoveryResponse();
    nonExistentDiscoveryResponseLegacy.setError("Store " + NON_EXISTENT_STORE_NAME + " not found");
    nonExistentDiscoveryResponseLegacy.setName(NON_EXISTENT_STORE_NAME);
    Mockito.doReturn(nonExistentDiscoveryResponseLegacy)
        .when(mockControllerClient)
        .discoverCluster(NON_EXISTENT_STORE_NAME);

    Assert.assertThrows(
        VeniceException.class,
        () -> ControllerClientFactory
            .discoverAndConstructControllerClient(NON_EXISTENT_STORE_NAME, DISCOVERY_URL, Optional.empty(), 1));

    ControllerClientFactory.release(mockControllerClient);
  }
}
