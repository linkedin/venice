package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ALLOW_CLUSTER_WIPE;
import static com.linkedin.venice.ConfigKeys.IS_DARK_CLUSTER;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.LOG_COMPACTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.REPUSH_ORCHESTRATOR_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_DELAY_FACTOR;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.controllerapi.AdminTopicMetadataResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.PubSubPositionJsonWireFormat;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.endToEnd.TestHybrid;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAdminToolEndToEnd {
  private static final int TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;

  String clusterName;
  VeniceClusterWrapper venice;

  @BeforeClass
  public void setUp() {
    Properties properties = new Properties();
    String regionName = "dc-0";
    properties.setProperty(LOCAL_REGION_NAME, regionName);
    properties.setProperty(ALLOW_CLUSTER_WIPE, "true");
    properties.setProperty(TOPIC_CLEANUP_DELAY_FACTOR, "0");

    // repushStore() configs
    properties.setProperty(REPUSH_ORCHESTRATOR_CLASS_NAME, TestHybrid.TestRepushOrchestratorImpl.class.getName());
    properties.setProperty(LOG_COMPACTION_ENABLED, "true");

    // dark cluster configs
    properties.setProperty(IS_DARK_CLUSTER, "true");

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .regionName(regionName)
        .numberOfServers(1)
        .numberOfRouters(1)
        .replicationFactor(1)
        .partitionSize(100000)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .extraProperties(properties)
        .build();
    venice = ServiceFactory.getVeniceCluster(options);
    clusterName = venice.getClusterName();
  }

  @AfterClass
  public void cleanUp() {
    venice.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNodeReplicasReadinessCommand() throws Exception {
    VeniceServerWrapper server = venice.getVeniceServers().get(0);
    String[] nodeReplicasReadinessArgs =
        { "--node-replicas-readiness", "--url", venice.getLeaderVeniceController().getControllerUrl(), "--cluster",
            clusterName, "--storage-node", Utils.getHelixNodeIdentifier(Utils.getHostName(), server.getPort()) };
    AdminTool.main(nodeReplicasReadinessArgs);
  }

  @Test(timeOut = 4 * TEST_TIMEOUT)
  public void testUpdateAdminOperationVersion() throws Exception {
    Long newVersion = 80L;
    PubSubPositionJsonWireFormat defaultPosition = PubSubSymbolicPosition.EARLIEST.toJsonWireFormat();
    String storeName = Utils.getUniqueString("test-store");
    try (VeniceTwoLayerMultiRegionMultiClusterWrapper venice =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
            new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
                .numberOfClusters(1)
                .numberOfParentControllers(1)
                .numberOfChildControllers(1)
                .numberOfServers(1)
                .numberOfRouters(1)
                .replicationFactor(1)
                .build());) {
      String clusterName = venice.getClusterNames()[0];

      // Get the parent con†roller
      VeniceControllerWrapper parentController = venice.getParentControllers().get(0);
      ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());

      // Verify the original metadata - default value
      AdminTopicMetadataResponse originalMetadata = parentControllerClient.getAdminTopicMetadata(Optional.empty());
      Assert.assertEquals(originalMetadata.getAdminOperationProtocolVersion(), -1L);
      Assert.assertEquals(originalMetadata.getExecutionId(), -1L);
      Assert.assertEquals(originalMetadata.getPosition(), defaultPosition);
      Assert.assertEquals(originalMetadata.getUpstreamPosition(), defaultPosition);

      // Create store
      NewStoreResponse newStoreResponse =
          parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());
      VersionCreationResponse versionCreationResponse =
          parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-push-1"), 1L);
      Assert.assertFalse(versionCreationResponse.isError());

      // Update store config
      ControllerResponse updateStore =
          parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setBatchGetLimit(100));
      Assert.assertFalse(updateStore.isError());

      // Check the baseline metadata
      AdminTopicMetadataResponse metdataAfterStoreCreation =
          parentControllerClient.getAdminTopicMetadata(Optional.empty());
      long baselineExecutionId = metdataAfterStoreCreation.getExecutionId();
      PubSubPositionJsonWireFormat baselinePosition = metdataAfterStoreCreation.getPosition();
      PubSubPositionJsonWireFormat baselineUpstreamPosition = metdataAfterStoreCreation.getUpstreamPosition();
      long baselineAdminVersion = metdataAfterStoreCreation.getAdminOperationProtocolVersion();

      // Execution id and offset should be positive now since we have created a store and updated the store config
      Assert.assertEquals(baselineAdminVersion, -1L);
      Assert.assertTrue(baselineExecutionId > 0);
      Assert.assertNotNull(baselinePosition);
      Assert.assertNotEquals(defaultPosition, baselinePosition);
      Assert.assertEquals(defaultPosition, baselineUpstreamPosition);

      // Update the admin operation version to newVersion - 80
      String[] updateAdminOperationVersionArgs =
          { "--update-admin-operation-protocol-version", "--url", parentController.getControllerUrl(), "--cluster",
              clusterName, "--admin-operation-protocol-version", newVersion.toString() };

      AdminTool.main(updateAdminOperationVersionArgs);

      // Verify the admin operation metadata version is updated and the remaining data is unchanged
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        AdminTopicMetadataResponse updatedMetadata = parentControllerClient.getAdminTopicMetadata(Optional.empty());
        Assert.assertEquals(updatedMetadata.getAdminOperationProtocolVersion(), (long) newVersion);
        Assert.assertEquals(updatedMetadata.getExecutionId(), baselineExecutionId);
        Assert.assertEquals(updatedMetadata.getPosition(), baselinePosition);
        Assert.assertEquals(updatedMetadata.getUpstreamPosition(), baselineUpstreamPosition);
      });
    }
  }
}
