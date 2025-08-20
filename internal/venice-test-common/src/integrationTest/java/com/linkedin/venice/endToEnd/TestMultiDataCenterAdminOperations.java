package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.writer.VeniceWriter.MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ExecutionIdAccessor;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.AdminTopicMetadataResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.ConfigCommonUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestMultiDataCenterAdminOperations {
  private static final Logger LOGGER = LogManager.getLogger(TestMultiDataCenterAdminOperations.class);
  private static final int TEST_TIMEOUT = 360 * Time.MS_PER_SECOND;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 2;

  // Do not use venice-cluster1 as it is used for testing failed admin messages
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0",
                                                                                                         // "venice-cluster1",
                                                                                                         // ...];

  private List<VeniceMultiClusterWrapper> childClusters;
  private List<List<VeniceControllerWrapper>> childControllers;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  private final byte[] emptyKeyBytes = new byte[] { 'a' };

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    Properties parentControllerProperties = new Properties();
    // Setup segment duration time to be 5 seconds so that we can test the admin operations when current segment is
    // ended
    parentControllerProperties.put(MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, TimeUnit.SECONDS.toMillis(5));
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    parentControllerProperties.setProperty(
        ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS,
        String.valueOf(Long.MAX_VALUE));

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .serverProperties(serverProperties)
            .parentControllerProperties(parentControllerProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    childClusters = multiRegionMultiClusterWrapper.getChildRegions();
    childControllers = childClusters.stream()
        .map(veniceClusterWrapper -> new ArrayList<>(veniceClusterWrapper.getControllers().values()))
        .collect(Collectors.toList());
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();

    LOGGER.info(
        "parentControllers: {}",
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(", ")));

    int i = 0;
    for (VeniceMultiClusterWrapper multiClusterWrapper: childClusters) {
      LOGGER.info(
          "childCluster{} controllers: {}",
          i++,
          multiClusterWrapper.getControllers()
              .values()
              .stream()
              .map(VeniceControllerWrapper::getControllerUrl)
              .collect(Collectors.joining(", ")));
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testHybridConfigPartitionerConfigConflict() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("test_conflict_store");
    String parentControllerUrl = multiRegionMultiClusterWrapper.getControllerConnectString();

    // Create store first
    ControllerClient controllerClient = new ControllerClient(clusterName, parentControllerUrl);

    TestUtils.assertCommand(controllerClient.createNewStore(storeName, "test_owner", "\"int\"", "\"int\""));

    // Make store from batch -> hybrid
    TestUtils.assertCommand(
        controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(259200).setHybridOffsetLagThreshold(1000)),
        "There is error in setting hybrid config.");

    // Try to update partitioner config on hybrid store, expect to fail.
    TestUtils.assertCommandFailure(
        controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setPartitionerClass("testClassName")),
        "There should be error in setting partitioner config in hybrid store.");

    // Try to make store back to non-hybrid store.
    TestUtils.assertCommand(
        controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(-1).setHybridOffsetLagThreshold(-1)),
        "There is error in setting hybrid config.");

    // Make sure store is not hybrid.
    Assert.assertNull(controllerClient.getStore(storeName).getStore().getHybridStoreConfig());

    // Try to update partitioner config on batch store, it should succeed now.
    TestUtils.assertCommand(
        controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams()
                .setPartitionerClass("com.linkedin.venice.partitioner.DefaultVenicePartitioner")),
        "There is error in setting partitioner config in non-hybrid store.");
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testFailedAdminMessages() {
    String clusterName = CLUSTER_NAMES[1];
    VeniceControllerWrapper parentController =
        multiRegionMultiClusterWrapper.getLeaderParentControllerWithRetries(clusterName);
    Admin admin = parentController.getVeniceAdmin();
    VeniceWriterFactory veniceWriterFactory = admin.getVeniceWriterFactory();
    VeniceWriter<byte[], byte[], byte[]> veniceWriter = veniceWriterFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(AdminTopicUtils.getTopicNameFromClusterName(clusterName)).build());
    AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();
    long executionId = parentController.getVeniceAdmin().getLastSucceedExecutionId(clusterName) + 1;
    // send a bad admin message
    veniceWriter.put(
        emptyKeyBytes,
        getStoreUpdateMessage(clusterName, "store-not-exist", "store-owner", executionId, adminOperationSerializer),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    List<VeniceControllerWrapper> controllersToTest = new ArrayList<>();
    controllersToTest.add(parentController);
    childControllers.forEach(controllerList -> controllersToTest.add(controllerList.get(0)));

    // Check if all regions received the bad admin message
    TestUtils.waitForNonDeterministicCompletion(60, TimeUnit.SECONDS, () -> {
      for (VeniceControllerWrapper controller: controllersToTest) {
        AdminConsumerService adminConsumerService = controller.getAdminConsumerServiceByCluster(clusterName);
        if (adminConsumerService.getFailingOffset() < 0) {
          return false;
        }
      }
      return true;
    });

    // Cleanup the failing admin message
    for (VeniceControllerWrapper controller: controllersToTest) {
      AdminConsumerService adminConsumerService = controller.getAdminConsumerServiceByCluster(clusterName);
      adminConsumerService.setOffsetToSkip(clusterName, adminConsumerService.getFailingOffset(), false);
    }

    AdminConsumerService parentAdminConsumerService = parentController.getAdminConsumerServiceByCluster(clusterName);
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      boolean allFailedMessagesSkipped = parentAdminConsumerService.getFailingOffset() == -1;
      for (List<VeniceControllerWrapper> controllerWrappers: childControllers) {
        AdminConsumerService childAdminConsumerService =
            controllerWrappers.get(0).getAdminConsumerServiceByCluster(clusterName);
        allFailedMessagesSkipped &= childAdminConsumerService.getFailingOffset() == -1;
      }
      return allFailedMessagesSkipped;
    });
  }

  @Test(timeOut = 2 * TEST_TIMEOUT)
  public void testFailedAdminMessageWhenBadSemanticIsDetected() {
    String storeName = Utils.getUniqueString("test-store");
    String clusterName = CLUSTER_NAMES[0];
    VeniceControllerWrapper parentController =
        multiRegionMultiClusterWrapper.getLeaderParentControllerWithRetries(clusterName);
    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());
    // Update the admin operation version to new version - 85 - to test bad message
    AdminTopicMetadataResponse updateProtocolVersionResponse =
        parentControllerClient.updateAdminOperationProtocolVersion(clusterName, 85L);
    assertFalse(updateProtocolVersionResponse.isError(), "Failed to update protocol version");

    // Create store
    NewStoreResponse newStoreResponse =
        parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"");
    Assert.assertFalse(newStoreResponse.isError());
    emptyPushToStore(parentControllerClient, storeName, 1);

    // Get current execution ID
    VeniceParentHelixAdmin parentAdmin = (VeniceParentHelixAdmin) parentController.getVeniceAdmin();
    ExecutionIdAccessor executionIdAccessor = parentAdmin.getVeniceHelixAdmin().getExecutionIdAccessor();
    long beforeBadMessageExecutionId = executionIdAccessor.getLastGeneratedExecutionId(clusterName);

    // Store update with invalid semantic
    ControllerResponse updateStore =
        parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setGlobalRtDivEnabled(true));
    Assert.assertTrue(updateStore.isError());
    assertTrue(updateStore.getError().contains("New semantic is being used"));

    // After sending a bad message with invalid semantic usage, the execution ID should not change
    long afterBadMessageExecutionId = executionIdAccessor.getLastGeneratedExecutionId(clusterName);

    assertEquals(
        beforeBadMessageExecutionId,
        afterBadMessageExecutionId,
        "Execution ID should not change after sending a bad message with invalid semantic usage");

    // Sleep to ensure new segment is created
    Utils.sleep(TimeUnit.SECONDS.toMillis(5));

    // A new push should pass
    emptyPushToStore(parentControllerClient, storeName, 2);

    // Reset the admin operation protocol version back to the latest version
    updateProtocolVersionResponse = parentControllerClient.updateAdminOperationProtocolVersion(
        clusterName,
        (long) AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertFalse(updateProtocolVersionResponse.isError(), "Failed to update protocol version");
  }

  private byte[] getStoreUpdateMessage(
      String clusterName,
      String storeName,
      String owner,
      long executionId,
      AdminOperationSerializer adminOperationSerializer) {
    UpdateStore updateStore = (UpdateStore) AdminMessageType.UPDATE_STORE.getNewInstance();
    updateStore.clusterName = clusterName;
    updateStore.storeName = storeName;
    updateStore.owner = owner;
    updateStore.partitionNum = 20;
    updateStore.currentVersion = 1;
    updateStore.enableReads = true;
    updateStore.enableWrites = true;
    updateStore.replicateAllConfigs = true;
    updateStore.updatedConfigsList = Collections.emptyList();
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = updateStore;
    adminMessage.executionId = executionId;
    updateStore.storeLifecycleHooks = Collections.emptyList();
    updateStore.blobTransferInServerEnabled = ConfigCommonUtils.ActivationState.NOT_SPECIFIED.name();
    return adminOperationSerializer
        .serialize(adminMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
  }

  private void emptyPushToStore(ControllerClient parentControllerClient, String storeName, int expectedVersion) {
    VersionCreationResponse vcr = parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-push"), 1L);
    assertFalse(vcr.isError());
    assertEquals(
        vcr.getVersion(),
        expectedVersion,
        "requesting a topic for a push should provide version number " + expectedVersion);

    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(storeName, expectedVersion),
        parentControllerClient,
        30,
        TimeUnit.SECONDS);
  }

}
