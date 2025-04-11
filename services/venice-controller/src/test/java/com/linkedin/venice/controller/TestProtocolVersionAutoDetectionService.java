package com.linkedin.venice.controller;

import static org.mockito.Mockito.*;

import com.linkedin.venice.controller.stats.ProtocolVersionAutoDetectionStats;
import com.linkedin.venice.utils.TestUtils;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;


public class TestProtocolVersionAutoDetectionService {
  private VeniceParentHelixAdmin admin;
  private VeniceControllerMultiClusterConfig veniceControllerMultiClusterConfig;
  private static final String clusterName = "";

  /**
   * Cluster setups
   * parent hosts in dc0
   * child hosts in dc1, dc2
   * 2 venice clusters in each dc
   *
   */

  /**
   * This test is to verify that the protocol version auto-detection service
   */
  @BeforeMethod
  public void setUp() {
    admin = mock(VeniceParentHelixAdmin.class);

    veniceControllerMultiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);

    Set<String> clusters = new HashSet<>();
    clusters.add(clusterName);
    doReturn(clusters).when(veniceControllerMultiClusterConfig).getClusters();
    doReturn(10L).when(veniceControllerMultiClusterConfig).getAdminOperationProtocolVersionAutoDetectionIntervalMs();
    doReturn(1).when(veniceControllerMultiClusterConfig).getAdminOperationProtocolVersionAutoDetectionThreadCount();
    doReturn(true).when(veniceControllerMultiClusterConfig).isAdminOperationProtocolVersionAutoDetectionEnabled();
  }

  public void testProtocolVersionDetection() throws Exception {

    ProtocolVersionAutoDetectionService service = new ProtocolVersionAutoDetectionService(
        admin,
        veniceControllerMultiClusterConfig,
        mock(ProtocolVersionAutoDetectionStats.class));

    service.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(admin, atLeastOnce()).getLocalAdminOperationProtocolVersion();
    });
  }
}
