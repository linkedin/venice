package com.linkedin.venice.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.AdminOperationProtocolVersionControllerResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.utils.Time;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAdminOperationVersionDetection {
  private static final int TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int NUMBER_OF_CONTROLLERS = 2;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0",
  // "venice-cluster1",
  // ...];

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    // Create multi-region multi-cluster setup
    Properties parentControllerProperties = new Properties();
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(NUMBER_OF_CONTROLLERS)
            .numberOfChildControllers(NUMBER_OF_CONTROLLERS)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .sslToStorageNodes(true)
            .forkServer(false)
            .serverProperties(serverProperties)
            .parentControllerProperties(parentControllerProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testGetAdminOperationVersionForParentControllers() {
    String clusterName = CLUSTER_NAMES[0]; // "venice-cluster0"
    VeniceControllerWrapper parentController =
        multiRegionMultiClusterWrapper.getLeaderParentControllerWithRetries(clusterName);
    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());

    AdminOperationProtocolVersionControllerResponse response =
        parentControllerClient.getAdminOperationProtocolVersionFromControllers(clusterName);
    assertFalse(response.isError());
    assertEquals(
        response.getLocalAdminOperationProtocolVersion(),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Map<String, Long> urlToVersionMap = response.getControllerUrlToVersionMap();
    assertEquals(urlToVersionMap.size(), 2);
    assertEquals(response.getCluster(), clusterName);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testAdminOperationVersionForChildControllers() {
    String clusterName = CLUSTER_NAMES[0]; // "venice-cluster0"

    VeniceMultiClusterWrapper childRegionMultiClusterWrapper = multiRegionMultiClusterWrapper.getChildRegions().get(0);
    VeniceClusterWrapper childRegionClusterWrapper = childRegionMultiClusterWrapper.getClusters().get(clusterName);
    ControllerClient childControllerClient = childRegionClusterWrapper.getVeniceControllers()
        .stream()
        .filter(controller -> controller.isLeaderController(clusterName))
        .findFirst()
        .map(controller -> new ControllerClient(clusterName, controller.getControllerUrl()))
        .orElse(null);

    AdminOperationProtocolVersionControllerResponse response =
        childControllerClient.getAdminOperationProtocolVersionFromControllers(clusterName);
    assertFalse(response.isError());
    assertEquals(
        response.getLocalAdminOperationProtocolVersion(),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    Map<String, Long> urlToVersionMap = response.getControllerUrlToVersionMap();
    assertEquals(urlToVersionMap.size(), 2);
    assertEquals(response.getCluster(), clusterName);
  }
}
