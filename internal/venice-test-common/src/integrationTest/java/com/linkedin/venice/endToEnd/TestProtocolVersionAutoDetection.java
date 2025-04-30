package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_PROTOCOL_VERSION_AUTO_DETECTION_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PROTOCOL_VERSION_AUTO_DETECTION_SLEEP_MS;
import static com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.controllerapi.AdminOperationProtocolVersionControllerResponse;
import com.linkedin.venice.controllerapi.AdminTopicMetadataResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestProtocolVersionAutoDetection {
  private static final int TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 1;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final long SERVICE_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5);
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private List<VeniceMultiClusterWrapper> childDatacenters;
  private String clusterName;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties parentControllerProps = new Properties();
    parentControllerProps.put(CONTROLLER_PROTOCOL_VERSION_AUTO_DETECTION_SERVICE_ENABLED, true);
    parentControllerProps.put(CONTROLLER_PROTOCOL_VERSION_AUTO_DETECTION_SLEEP_MS, SERVICE_INTERVAL_MS);

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(2)
            .numberOfChildControllers(2)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .parentControllerProperties(parentControllerProps);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    clusterName = multiRegionMultiClusterWrapper.getClusterNames()[0];
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testProtocolVersionAutoDetection() {
    VeniceControllerWrapper parentController =
        multiRegionMultiClusterWrapper.getLeaderParentControllerWithRetries(clusterName);
    try (ControllerClient parentControllerClient =
        new ControllerClient(clusterName, parentController.getControllerUrl())) {
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        // update the upstream version to LATEST + 1
        AdminTopicMetadataResponse updateResponse = parentControllerClient
            .updateAdminOperationProtocolVersion(clusterName, (long) (LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION + 1));
        assertFalse(updateResponse.isError());
      });

      TestUtils.waitForNonDeterministicAssertion(SERVICE_INTERVAL_MS, TimeUnit.MILLISECONDS, () -> {
        // With ProtocolVersionAutoDetectionService is enabled, the version should be updated to LATEST (smallest
        // version
        // among controllers)
        // Expected the data to be updated within 5 sec (the interval defined in config)
        AdminTopicMetadataResponse adminTopicMetadataResponse =
            parentControllerClient.getAdminTopicMetadata(Optional.empty());
        assertEquals(
            adminTopicMetadataResponse.getAdminOperationProtocolVersion(),
            LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testGetAdminOperationProtocolVersionFromControllers() {
    // parent controller
    VeniceControllerWrapper parentController =
        multiRegionMultiClusterWrapper.getLeaderParentControllerWithRetries(clusterName);
    try (ControllerClient parentControllerClient =
        new ControllerClient(clusterName, parentController.getControllerUrl())) {
      AdminOperationProtocolVersionControllerResponse parentResponse =
          parentControllerClient.getAdminOperationProtocolVersionFromControllers(clusterName);
      assertEquals(parentResponse.getLocalAdminOperationProtocolVersion(), LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
      assertEquals(parentResponse.getControllerUrlToVersionMap().size(), 2);
    }

    // child controller
    Function<Integer, String> connectionString = i -> childDatacenters.get(i).getControllerConnectString();
    try (ControllerClient dc0ControllerClient = new ControllerClient(clusterName, connectionString.apply(0))) {
      AdminOperationProtocolVersionControllerResponse childResponse =
          dc0ControllerClient.getAdminOperationProtocolVersionFromControllers(clusterName);
      assertEquals(childResponse.getLocalAdminOperationProtocolVersion(), LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
      assertEquals(childResponse.getControllerUrlToVersionMap().size(), 2);
    }
  }
}
