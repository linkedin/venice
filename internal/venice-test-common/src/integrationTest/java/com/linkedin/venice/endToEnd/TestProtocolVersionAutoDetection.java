package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.ADMIN_OPERATION_PROTOCOL_VERSION_AUTO_DETECTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.ADMIN_OPERATION_PROTOCOL_VERSION_AUTO_DETECTION_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.ADMIN_OPERATION_PROTOCOL_VERSION_AUTO_DETECTION_THREAD_COUNT;
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
  private static final int NUMBER_OF_CHILD_DATACENTERS = 1;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private List<VeniceMultiClusterWrapper> childDatacenters;
  private String[] clusterNames;
  private String clusterName;

  @BeforeClass
  public void setUp() {
    Properties parentControllerProps = new Properties();
    parentControllerProps.put(ADMIN_OPERATION_PROTOCOL_VERSION_AUTO_DETECTION_ENABLED, true);
    parentControllerProps
        .put(ADMIN_OPERATION_PROTOCOL_VERSION_AUTO_DETECTION_INTERVAL_MS, TimeUnit.SECONDS.toMillis(5));
    parentControllerProps.put(ADMIN_OPERATION_PROTOCOL_VERSION_AUTO_DETECTION_THREAD_COUNT, 1);
    Properties serverProperties = new Properties();

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(2)
            .numberOfChildControllers(2)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .parentControllerProperties(parentControllerProps)
            .childControllerProperties(new Properties())
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    clusterNames = multiRegionMultiClusterWrapper.getClusterNames();
    clusterName = this.clusterNames[0];
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test
  public void testProtocolVersionAutoDetection() {
    VeniceControllerWrapper parentController =
        multiRegionMultiClusterWrapper.getLeaderParentControllerWithRetries(clusterName);
    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());

    // update the upstream version to LATEST + 1
    AdminTopicMetadataResponse updateResponse = parentControllerClient
        .updateAdminOperationProtocolVersion(clusterName, (long) (LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION + 1));
    assertFalse(updateResponse.isError());

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      // With ProtocolVersionAutoDetectionService is enabled, the version should be updated to LATEST (smallest version
      // among controllers)
      // Expected the data to be updated within 5 sec (the interval defined in config)
      AdminTopicMetadataResponse adminTopicMetadataResponse =
          parentControllerClient.getAdminTopicMetadata(Optional.empty());
      assertEquals(adminTopicMetadataResponse.getAdminOperationProtocolVersion(), LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    });
  }

  @Test
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
      AdminOperationProtocolVersionControllerResponse parentResponse =
          dc0ControllerClient.getAdminOperationProtocolVersionFromControllers(clusterName);
      assertEquals(parentResponse.getLocalAdminOperationProtocolVersion(), LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
      assertEquals(parentResponse.getControllerUrlToVersionMap().size(), 2);
    }
  }
}
