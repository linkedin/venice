package com.linkedin.venice.endToEnd;

import static org.testng.Assert.assertFalse;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0",
                                                                                                         // "venice-cluster1",
                                                                                                         // ...];

  private List<VeniceMultiClusterWrapper> childClusters;
  private List<List<VeniceControllerWrapper>> childControllers;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  private final byte[] emptyKeyBytes = new byte[] { 'a' };

  @BeforeClass
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));
    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        1,
        1,
        1,
        1,
        1,
        Optional.empty(),
        Optional.empty(),
        Optional.of(new VeniceProperties(serverProperties)),
        false);

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

  @AfterClass
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testHybridConfigPartitionerConfigConflict() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("store");
    String parentControllerUrl = multiRegionMultiClusterWrapper.getControllerConnectString();

    // Create store first
    ControllerClient controllerClient = new ControllerClient(clusterName, parentControllerUrl);
    controllerClient.createNewStore(storeName, "test_owner", "\"int\"", "\"int\"");

    // Make store from batch -> hybrid
    ControllerResponse response = controllerClient.updateStore(
        storeName,
        new UpdateStoreQueryParams().setHybridRewindSeconds(259200).setHybridOffsetLagThreshold(1000));
    assertFalse(response.isError(), "There is error in setting hybrid config");

    // Try to update partitioner config on hybrid store, expect to fail.
    response =
        controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setPartitionerClass("testClassName"));
    Assert.assertTrue(response.isError(), "There should be error in setting partitioner config in hybrid store");

    // Try to make store back to non-hybrid store.
    response = controllerClient.updateStore(
        storeName,
        new UpdateStoreQueryParams().setHybridRewindSeconds(-1).setHybridOffsetLagThreshold(-1));
    assertFalse(response.isError(), "There is error in setting hybrid config");

    // Make sure store is not hybrid.
    Assert.assertNull(controllerClient.getStore(storeName).getStore().getHybridStoreConfig());

    // Try to update partitioner config on batch store, it should succeed now.
    response = controllerClient.updateStore(
        storeName,
        new UpdateStoreQueryParams().setPartitionerClass("com.linkedin.venice.partitioner.DefaultVenicePartitioner"));
    assertFalse(response.isError(), "There is error in setting partitioner config in non-hybrid store");
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testFailedAdminMessages() {
    String clusterName = CLUSTER_NAMES[0];
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
    return adminOperationSerializer.serialize(adminMessage);
  }
}
