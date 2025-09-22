package com.linkedin.venice;

import com.linkedin.venice.controllerapi.ChildAwareResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.StoreInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.cli.ParseException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCheckMigrationStatus {
  String SRC_CLUSTER_NAME = "venice-source";
  String DESTINATION_CLUSTER_NAME = "venice-destination";
  String CONTROLLER_URL = "http://somePlaceElse.com:12345";
  String STORE_NAME = "TestStore";
  String SYSTEM_STORE_NAME = "venice_system_store_davinci_" + STORE_NAME;

  @Test
  public void testCheckMigrationStatus() throws ParseException, IOException {
    String[] checkMigrationStatusArgs = { "--migration-status", "--url", CONTROLLER_URL, "--store", STORE_NAME,
        "--cluster-src", SRC_CLUSTER_NAME, "--cluster-dest", DESTINATION_CLUSTER_NAME };

    // Mock source controller client
    ControllerClient mockSourceControllerClient = Mockito.mock(ControllerClient.class);
    StoreInfo mockStoreInfo = new StoreInfo();
    mockStoreInfo.setVersions(new ArrayList<>());
    StoreResponse mockStoreResponse = Mockito.mock(StoreResponse.class);
    MultiStoreResponse mockMultiStoreResponse = new MultiStoreResponse();
    D2ServiceDiscoveryResponse mockD2ServiceDiscoveryResponse = Mockito.mock(D2ServiceDiscoveryResponse.class);
    Mockito.when(mockD2ServiceDiscoveryResponse.getCluster()).thenReturn(SRC_CLUSTER_NAME);
    mockMultiStoreResponse.setStores(new String[] { SYSTEM_STORE_NAME, STORE_NAME });
    ChildAwareResponse mockChildAwareResponse = Mockito.mock(ChildAwareResponse.class);
    Mockito.when(mockChildAwareResponse.getChildDataCenterControllerUrlMap()).thenReturn(new HashMap<>());
    Mockito.when(mockSourceControllerClient.listChildControllers(SRC_CLUSTER_NAME)).thenReturn(mockChildAwareResponse);
    Mockito.when(mockStoreResponse.getStore()).thenReturn(mockStoreInfo);
    Mockito.when(mockSourceControllerClient.getStore(STORE_NAME)).thenReturn(mockStoreResponse);
    Mockito.when(mockSourceControllerClient.getStore(SYSTEM_STORE_NAME)).thenReturn(mockStoreResponse);
    Mockito.when(mockSourceControllerClient.getClusterName()).thenReturn(SRC_CLUSTER_NAME);
    Mockito.when(mockSourceControllerClient.getLeaderControllerUrl()).thenReturn(CONTROLLER_URL);
    Mockito.when(mockSourceControllerClient.discoverCluster(STORE_NAME)).thenReturn(mockD2ServiceDiscoveryResponse);
    Mockito.when(mockSourceControllerClient.queryStoreList()).thenReturn(mockMultiStoreResponse);

    ControllerClient mockDestinationControllerClient = Mockito.mock(ControllerClient.class);
    mockMultiStoreResponse.setStores(new String[] { SYSTEM_STORE_NAME, STORE_NAME });
    Mockito.when(mockStoreResponse.getStore()).thenReturn(mockStoreInfo);
    Mockito.when(mockDestinationControllerClient.getStore(STORE_NAME)).thenReturn(mockStoreResponse);
    Mockito.when(mockDestinationControllerClient.getStore(SYSTEM_STORE_NAME)).thenReturn(mockStoreResponse);
    Mockito.when(mockDestinationControllerClient.getClusterName()).thenReturn(SRC_CLUSTER_NAME);
    Mockito.when(mockDestinationControllerClient.getLeaderControllerUrl()).thenReturn(CONTROLLER_URL);
    Mockito.when(mockDestinationControllerClient.discoverCluster(STORE_NAME))
        .thenReturn(mockD2ServiceDiscoveryResponse);
    Mockito.when(mockDestinationControllerClient.queryStoreList()).thenReturn(mockMultiStoreResponse);

    // Store migration status output via closure PrintFunction
    Set<String> statusOutput = new HashSet<String>();
    AdminTool.PrintFunction printFunction = (message) -> {
      statusOutput.add(message.trim());
    };

    AdminTool.checkMigrationStatus(
        AdminTool.getCommandLine(checkMigrationStatusArgs),
        printFunction,
        mockSourceControllerClient,
        mockDestinationControllerClient);

    Assert.assertTrue(statusOutput.contains(String.format("%s\t%s", SRC_CLUSTER_NAME, CONTROLLER_URL)));
    Assert
        .assertTrue(statusOutput.contains(String.format("%s exists in this cluster %s", STORE_NAME, SRC_CLUSTER_NAME)));
    Assert.assertTrue(statusOutput.contains(String.format("%s.isMigrating = false", STORE_NAME)));
    Assert.assertTrue(statusOutput.contains(String.format("%s.largestUsedVersion = 0", STORE_NAME)));
    Assert.assertTrue(statusOutput.contains(String.format("%s.currentVersion = 0", STORE_NAME)));
    Assert.assertTrue(statusOutput.contains(String.format("%s.versions =", STORE_NAME)));
    Assert.assertTrue(
        statusOutput.contains(
            String.format("%s belongs to cluster %s according to cluster discovery", STORE_NAME, SRC_CLUSTER_NAME)));

    Assert.assertFalse(statusOutput.contains(String.format("%s\t%s", DESTINATION_CLUSTER_NAME, CONTROLLER_URL)));
    Assert.assertFalse(
        statusOutput.contains(String.format("%s exists in this cluster %s", STORE_NAME, DESTINATION_CLUSTER_NAME)));
    Assert.assertFalse(
        statusOutput.contains(
            String.format(
                "%s belongs to cluster %s according to cluster discovery",
                STORE_NAME,
                DESTINATION_CLUSTER_NAME)));
  }
}
