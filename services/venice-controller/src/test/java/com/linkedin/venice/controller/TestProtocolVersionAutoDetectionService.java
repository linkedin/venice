package com.linkedin.venice.controller;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.venice.controller.stats.ProtocolVersionAutoDetectionStats;
import com.linkedin.venice.controllerapi.AdminOperationProtocolVersionControllerResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.utils.TestUtils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestProtocolVersionAutoDetectionService {
  private VeniceParentHelixAdmin parentAdmin;
  private VeniceHelixAdmin admin;
  private VeniceControllerMultiClusterConfig veniceControllerMultiClusterConfig;
  private Map<String, Map<String, Long>> regionToHostToVersionMap = new HashMap<>();
  private Map<String, Map<String, Boolean>> hostToClusterToLeaderStateMap = new HashMap<>();
  private ProtocolVersionAutoDetectionService localProtocolVersionAutoDetectionService;
  private static final String CLUSTER_VENICE_0 = "venice0";
  private static final String CLUSTER_VENICE_1 = "venice1";
  private static final String REGION_DC_0 = "dc0";
  private static final String REGION_DC_1 = "dc1";
  private static final String REGION_DC_2 = "dc2";

  /**
   * This test is to verify that the protocol version auto-detection service
   */
  @BeforeMethod(alwaysRun = true)
  public void setUp() {
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

    // Mock parent admin
    parentAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(parentAdmin).isLeaderControllerFor(CLUSTER_VENICE_0);
    doReturn(false).when(parentAdmin).isLeaderControllerFor(CLUSTER_VENICE_1);
    doReturn(regionToHostToVersionMap.get(REGION_DC_0)).when(parentAdmin)
        .getAdminOperationVersionFromControllers(CLUSTER_VENICE_0);
    doNothing().when(parentAdmin).updateAdminOperationProtocolVersion(anyString(), anyLong());

    admin = mock(VeniceHelixAdmin.class);
    doReturn(admin).when(parentAdmin).getVeniceHelixAdmin();

    // Mock the VeniceControllerMultiClusterConfig
    veniceControllerMultiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);
    Set<String> clusters = new HashSet<>();
    clusters.add(CLUSTER_VENICE_0);
    clusters.add(CLUSTER_VENICE_1);
    doReturn(clusters).when(veniceControllerMultiClusterConfig).getClusters();
    doReturn(TimeUnit.SECONDS.toMillis(5)).when(veniceControllerMultiClusterConfig)
        .getAdminOperationProtocolVersionAutoDetectionIntervalMs();
    doReturn(1).when(veniceControllerMultiClusterConfig).getAdminOperationProtocolVersionAutoDetectionThreadCount();
    doReturn(true).when(veniceControllerMultiClusterConfig).isAdminOperationProtocolVersionAutoDetectionEnabled();

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

  @AfterMethod(alwaysRun = true)
  public void methodCleanUp() throws Exception {
    if (localProtocolVersionAutoDetectionService != null) {
      localProtocolVersionAutoDetectionService.stopInner();
    }
  }

  @Test
  public void testProtocolVersionDetection() throws Exception {
    String clusterName = "venice0";
    Map<String, Long> adminTopicMetadataMap = AdminTopicMetadataAccessor
        .generateMetadataMap(Optional.of(1L), Optional.of(-1L), Optional.of(1L), Optional.of(80L));
    doReturn(adminTopicMetadataMap).when(parentAdmin).getAdminTopicMetadata(clusterName, Optional.empty());

    localProtocolVersionAutoDetectionService = new ProtocolVersionAutoDetectionService(
        parentAdmin,
        veniceControllerMultiClusterConfig,
        mock(ProtocolVersionAutoDetectionStats.class));

    localProtocolVersionAutoDetectionService.startInner();

    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      verify(admin, atLeastOnce()).getAdminOperationVersionFromControllers(clusterName);
      verify(parentAdmin, atLeastOnce()).updateAdminOperationProtocolVersion(clusterName, 1L);
    });
  }

  @Test
  public void testProtocolVersionDetectionWithNoUpdate() throws Exception {
    String clusterName = "venice0";
    Map<String, Long> adminTopicMetadataMap = AdminTopicMetadataAccessor
        .generateMetadataMap(Optional.of(1L), Optional.of(-1L), Optional.of(1L), Optional.of(-1L) // When version is -1,
                                                                                                  // no need to update
        );

    doReturn(adminTopicMetadataMap).when(parentAdmin).getAdminTopicMetadata(clusterName, Optional.empty());
    doNothing().when(parentAdmin).updateAdminOperationProtocolVersion(anyString(), anyLong());

    localProtocolVersionAutoDetectionService = new ProtocolVersionAutoDetectionService(
        parentAdmin,
        veniceControllerMultiClusterConfig,
        mock(ProtocolVersionAutoDetectionStats.class));

    localProtocolVersionAutoDetectionService.startInner();

    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      verify(admin, atLeastOnce()).getAdminOperationVersionFromControllers(clusterName);
      verify(parentAdmin, never()).updateAdminOperationProtocolVersion(anyString(), anyLong());
    });
  }

  @Test
  public void testGetLocalAdminOperationProtocolVersionForAllConsumers() {
    localProtocolVersionAutoDetectionService = new ProtocolVersionAutoDetectionService(
        parentAdmin,
        mock(VeniceControllerMultiClusterConfig.class),
        mock(ProtocolVersionAutoDetectionStats.class));

    // Call the method under test
    long smallestVersion = localProtocolVersionAutoDetectionService.new ProtocolVersionDetectionTask()
        .getLocalAdminOperationProtocolVersionForAllConsumers(CLUSTER_VENICE_0);

    assertEquals(smallestVersion, 1L);
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
        response.getControllerUrlToVersionMap().put(hostName, version);
        if (hostToClusterToLeaderStateMap.containsKey(hostName)) {
          Map<String, Boolean> clusterToStateMap = hostToClusterToLeaderStateMap.get(hostName);
          if (clusterToStateMap.get(clusterName) != null && clusterToStateMap.get(clusterName)) {
            // Add the local version for leader controller
            response.setLocalAdminOperationProtocolVersion(version);
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
