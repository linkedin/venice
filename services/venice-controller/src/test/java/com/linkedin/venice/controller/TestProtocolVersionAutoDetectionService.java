package com.linkedin.venice.controller;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.consumer.AdminMetadata;
import com.linkedin.venice.controller.stats.ProtocolVersionAutoDetectionStats;
import com.linkedin.venice.controllerapi.AdminOperationProtocolVersionControllerResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import com.linkedin.venice.utils.TestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * This test is to verify that the protocol version auto-detection service works as expected
 * in a multi-cluster environment.
 * The test is designed to simulate a scenario where there are multiple clusters and regions,
 * with different host states and admin operation versions.
 * The test checks the following:
 * 1. The protocol version auto-detection service correctly detects the smallest local admin operation protocol version
 *   for all consumers in the cluster.
 * 2. The service correctly updates the admin operation protocol version for the cluster.
 * 3. The service handles the case where the admin operation protocol version is -1, indicating that no update is needed.
 * 4. The service handles the case where the request to get the admin operation protocol version fails.
 */
public class TestProtocolVersionAutoDetectionService {
  private VeniceHelixAdmin admin;
  private AdminConsumerService adminConsumerService;
  private Map<String, Map<String, Long>> regionToHostToVersionMap = new HashMap<>();
  private Map<String, Map<String, Boolean>> hostToClusterToLeaderStateMap = new HashMap<>();
  private String clusterName;
  private static final String CLUSTER_VENICE_0 = "venice0";
  private static final String CLUSTER_VENICE_1 = "venice1";
  private static final String REGION_DC_0 = "dc0";
  private static final String REGION_DC_1 = "dc1";
  private static final String REGION_DC_2 = "dc2";
  private static final long DEFAULT_SLEEP_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    this.clusterName = CLUSTER_VENICE_0;
    /**
     * Parent controllers are in dc0
     * Child controllers are in dc1, and dc2.
     * Host details:
     * | regionName | hostName | adminOperationVersion | stateInVenice0 | stateInVenice1 |
     * |------------|----------|-----------------------|----------------|----------------|
     * | dc0        | host1    | 1L                    | Leader         | Standby        |
     * | dc0        | host2    | 2L                    | Standby        | Leader         |
     * | dc0        | host3    | 3L                    | Standby        | Standby        |
     * | dc1        | host4    | 3L                    | Leader         | Standby        |
     * | dc1        | host5    | 2L                    | Standby        | Leader         |
     * | dc1        | host6    | 1L                    | Standby        | Standby        |
     * | dc2        | host7    | 4L                    | Leader         | Standby        |
     * | dc2        | host8    | 5L                    | Standby        | Leader         |
     * | dc2        | host9    | 6L                    | Standby        | Standby        |
     */
    addHostToClusterToStateMap("host1", REGION_DC_0, 1L, true, false);
    addHostToClusterToStateMap("host2", REGION_DC_0, 2L, false, true);
    addHostToClusterToStateMap("host3", REGION_DC_0, 3L, false, false);
    addHostToClusterToStateMap("host4", REGION_DC_1, 3L, true, false);
    addHostToClusterToStateMap("host5", REGION_DC_1, 2L, false, true);
    addHostToClusterToStateMap("host6", REGION_DC_1, 1L, false, false);
    addHostToClusterToStateMap("host7", REGION_DC_2, 4L, true, false);
    addHostToClusterToStateMap("host8", REGION_DC_2, 5L, false, true);
    addHostToClusterToStateMap("host9", REGION_DC_2, 6L, false, false);

    // Mock admin
    admin = mock(VeniceHelixAdmin.class);
    doReturn(true).when(admin).isLeaderControllerFor(CLUSTER_VENICE_0);
    doReturn(false).when(admin).isLeaderControllerFor(CLUSTER_VENICE_1);
    doReturn(regionToHostToVersionMap.get(REGION_DC_0)).when(admin)
        .getAdminOperationVersionFromControllers(CLUSTER_VENICE_0);
    doCallRealMethod().when(admin).updateAdminOperationProtocolVersion(anyString(), anyLong());

    // Mock AdminConsumerService
    adminConsumerService = mock(AdminConsumerService.class);
    doReturn(adminConsumerService).when(admin).getAdminConsumerService(anyString());
    when(admin.getAdminConsumerService(clusterName)).thenReturn(adminConsumerService);
    doNothing().when(adminConsumerService).updateAdminOperationProtocolVersion(anyString(), anyLong());

    // Mock response for child controllers in different regions
    Map<String, ControllerClient> controllerClientMap = new HashMap<>();
    ControllerClient mockControllerClient = mock(ControllerClient.class);
    controllerClientMap.put(REGION_DC_1, mockControllerClient);
    controllerClientMap.put(REGION_DC_2, mockControllerClient);
    doReturn(controllerClientMap).when(admin).getControllerClientMap(CLUSTER_VENICE_0);
    when(mockControllerClient.getAdminOperationProtocolVersionFromControllers(CLUSTER_VENICE_0))
        .thenReturn(getAdminOperationProtocolVersionResponse(REGION_DC_1, CLUSTER_VENICE_0))
        .thenReturn(getAdminOperationProtocolVersionResponse(REGION_DC_2, CLUSTER_VENICE_0));
  }

  @Test
  public void testProtocolVersionDetection() throws Exception {
    AdminMetadata adminTopicMetadata = new AdminMetadata();
    adminTopicMetadata.setPubSubPosition(InMemoryPubSubPosition.of(1L));
    adminTopicMetadata.setExecutionId(1L);
    adminTopicMetadata.setAdminOperationProtocolVersion(80L);
    doReturn(adminTopicMetadata).when(admin).getAdminTopicMetadata(clusterName, Optional.empty());

    ProtocolVersionAutoDetectionService localProtocolVersionAutoDetectionService =
        new ProtocolVersionAutoDetectionService(
            clusterName,
            admin,
            mock(ProtocolVersionAutoDetectionStats.class),
            DEFAULT_SLEEP_INTERVAL_MS);

    localProtocolVersionAutoDetectionService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(admin, atLeastOnce()).getAdminOperationVersionFromControllers(clusterName);
      verify(adminConsumerService, atLeastOnce()).updateAdminOperationProtocolVersion(clusterName, 1L);
    });

    localProtocolVersionAutoDetectionService.stopInner();
  }

  @Test
  public void testProtocolVersionDetectionWithNoUpdate() throws Exception {
    // When version is -1, no need to update
    AdminMetadata adminMetadata = new AdminMetadata();
    adminMetadata.setPubSubPosition(InMemoryPubSubPosition.of(1L));
    adminMetadata.setExecutionId(1L);

    doReturn(adminMetadata).when(admin).getAdminTopicMetadata(clusterName, Optional.empty());

    ProtocolVersionAutoDetectionService localProtocolVersionAutoDetectionService =
        new ProtocolVersionAutoDetectionService(
            clusterName,
            admin,
            mock(ProtocolVersionAutoDetectionStats.class),
            DEFAULT_SLEEP_INTERVAL_MS);

    localProtocolVersionAutoDetectionService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(admin, atLeastOnce()).getAdminOperationVersionFromControllers(clusterName);
      verify(adminConsumerService, never()).updateAdminOperationProtocolVersion(anyString(), anyLong());
    });

    localProtocolVersionAutoDetectionService.stopInner();
  }

  @Test
  public void testGetLocalAdminOperationProtocolVersionForAllConsumers() {
    ProtocolVersionAutoDetectionService localProtocolVersionAutoDetectionService =
        new ProtocolVersionAutoDetectionService(
            clusterName,
            admin,
            mock(ProtocolVersionAutoDetectionStats.class),
            DEFAULT_SLEEP_INTERVAL_MS);
    long smallestVersion = localProtocolVersionAutoDetectionService
        .getSmallestLocalAdminOperationProtocolVersionForAllConsumers(clusterName);

    assertEquals(smallestVersion, 1L);
  }

  @Test
  public void testGetLocalAdminOperationVersionWithFailRequest() {
    AdminOperationProtocolVersionControllerResponse failResponse =
        new AdminOperationProtocolVersionControllerResponse();
    failResponse.setError("Fail to connect to the controller");

    Map<String, ControllerClient> controllerClientMap = new HashMap<>();
    ControllerClient mockControllerClient = mock(ControllerClient.class);
    controllerClientMap.put(REGION_DC_1, mockControllerClient);
    controllerClientMap.put(REGION_DC_2, mockControllerClient);
    doReturn(controllerClientMap).when(admin).getControllerClientMap(CLUSTER_VENICE_0);
    when(mockControllerClient.getAdminOperationProtocolVersionFromControllers(CLUSTER_VENICE_0))
        .thenReturn(getAdminOperationProtocolVersionResponse(REGION_DC_1, CLUSTER_VENICE_0))
        .thenReturn(failResponse);

    try {
      ProtocolVersionAutoDetectionService localProtocolVersionAutoDetectionService =
          new ProtocolVersionAutoDetectionService(
              clusterName,
              admin,
              mock(ProtocolVersionAutoDetectionStats.class),
              DEFAULT_SLEEP_INTERVAL_MS);

      localProtocolVersionAutoDetectionService
          .getSmallestLocalAdminOperationProtocolVersionForAllConsumers(clusterName);
      fail("Expected VeniceException to be thrown");
    } catch (VeniceException e) {
      assertEquals(
          e.getMessage(),
          "Failed to get admin operation protocol version from child controller dc1: Fail to connect to the controller");
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }

  /**
   * Generate AdminOperationProtocolVersionControllerResponse for getAdminOperationProtocolVersionFromControllers request
   */
  private AdminOperationProtocolVersionControllerResponse getAdminOperationProtocolVersionResponse(
      String region,
      String clusterName) {
    AdminOperationProtocolVersionControllerResponse response = new AdminOperationProtocolVersionControllerResponse();
    Map<String, Long> hostToVersionMap = regionToHostToVersionMap.get(region);

    if (hostToVersionMap != null) {
      for (Map.Entry<String, Long> entry: hostToVersionMap.entrySet()) {
        String hostName = entry.getKey();
        long version = entry.getValue();
        response.getControllerNameToVersionMap().put(hostName, version);
        if (hostToClusterToLeaderStateMap.containsKey(hostName)) {
          Map<String, Boolean> clusterToStateMap = hostToClusterToLeaderStateMap.get(hostName);
          if (clusterToStateMap.get(clusterName) != null && clusterToStateMap.get(clusterName)) {
            // Add the local version for leader controller
            response.setLocalAdminOperationProtocolVersion(version);
            response.setLocalControllerName(hostName);
            response.setCluster(clusterName);
          }
        }
      }
    }
    return response;
  }

  private void addHostToClusterToStateMap(
      String hostName,
      String regionName,
      Long version,
      boolean isLeaderForCluster0,
      boolean isLeaderForCluster1) {
    if (!regionToHostToVersionMap.containsKey(regionName)) {
      regionToHostToVersionMap.put(regionName, new HashMap<>());
    }
    regionToHostToVersionMap.get(regionName).put(hostName, version);

    Map<String, Boolean> clusterToStateMap = new HashMap<>();
    clusterToStateMap.put(CLUSTER_VENICE_0, isLeaderForCluster0);
    clusterToStateMap.put(CLUSTER_VENICE_1, isLeaderForCluster1);
    hostToClusterToLeaderStateMap.put(hostName, clusterToStateMap);
  }
}
