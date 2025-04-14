package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_ENABLE_HYBRID_STORE_PARTITION_COUNT_UPDATE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ENABLE_REAL_TIME_TOPIC_VERSIONING;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.meta.Version.DEFAULT_RT_VERSION_NUMBER;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestHybridStoreRepartitioningWithMultiDataCenter {
  private static final int TEST_TIMEOUT = 60_000; // ms
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  List<TopicManager> topicManagers;
  PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  ControllerClient parentControllerClient;
  ControllerClient[] childControllerClients;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    String clusterName = CLUSTER_NAMES[0];
    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID, 2);
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 3);
    controllerProps.put(DEFAULT_PARTITION_SIZE, 1024);
    controllerProps.put(CONTROLLER_ENABLE_REAL_TIME_TOPIC_VERSIONING, true);
    controllerProps.put(CONTROLLER_ENABLE_HYBRID_STORE_PARTITION_COUNT_UPDATE, true);
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .parentControllerProperties(controllerProps)
            .childControllerProperties(controllerProps);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    List<VeniceMultiClusterWrapper> childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllerClient = ControllerClient.constructClusterControllerClient(CLUSTER_NAMES[0], parentControllerURLs);
    childControllerClients = new ControllerClient[childDatacenters.size()];
    for (int i = 0; i < childDatacenters.size(); i++) {
      childControllerClients[i] =
          new ControllerClient(clusterName, childDatacenters.get(i).getControllerConnectString());
    }
    topicManagers = new ArrayList<>(2);
    topicManagers
        .add(childDatacenters.get(0).getControllers().values().iterator().next().getVeniceAdmin().getTopicManager());
    topicManagers
        .add(childDatacenters.get(1).getControllers().values().iterator().next().getVeniceAdmin().getTopicManager());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  private static void verifyStoreState(
      ControllerClient controllerClient,
      String storeName,
      int expectedLargestUsedRTVersionNumber,
      String expectedRealTimeTopicNameInStoreConfig,
      String expectedRealTimeTopicNameInBackupVersionConfig,
      String expectedRealTimeTopicNameInVersionConfig) {
    StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
    Assert.assertNotNull(storeInfo.getHybridStoreConfig());

    String oldStyleRealTimeTopicName = Utils.composeRealTimeTopic(storeName);
    String realTimeTopicNameInStoreConfig = storeInfo.getHybridStoreConfig().getRealTimeTopicName();
    String realTimeTopicInStore = Utils.getRealTimeTopicName(storeInfo);
    Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), expectedLargestUsedRTVersionNumber);
    Assert.assertEquals(realTimeTopicNameInStoreConfig, expectedRealTimeTopicNameInStoreConfig);

    List<Version> versions = storeInfo.getVersions();
    if (versions.isEmpty()) {
      Assert.assertEquals(
          realTimeTopicInStore,
          expectedRealTimeTopicNameInStoreConfig.isEmpty()
              ? oldStyleRealTimeTopicName
              : expectedRealTimeTopicNameInStoreConfig);
      return;
    }

    Assert.assertEquals(
        realTimeTopicInStore,
        expectedRealTimeTopicNameInVersionConfig.isEmpty()
            ? oldStyleRealTimeTopicName
            : expectedRealTimeTopicNameInVersionConfig);

    Version backupVersion = versions.get(0);
    Version currentVersion = versions.get(versions.size() - 1);

    String realTimeTopicNameInVersionConfig = currentVersion.getHybridStoreConfig().getRealTimeTopicName();
    String realTimeTopicNameInBackupVersionConfig = backupVersion.getHybridStoreConfig().getRealTimeTopicName();
    String realTimeTopicInVersion = Utils.getRealTimeTopicName(currentVersion);
    String realTimeTopicInBackupVersion = Utils.getRealTimeTopicName(backupVersion);

    Assert.assertEquals(realTimeTopicNameInVersionConfig, expectedRealTimeTopicNameInVersionConfig);
    Assert.assertEquals(realTimeTopicNameInBackupVersionConfig, expectedRealTimeTopicNameInBackupVersionConfig);
    Assert.assertEquals(
        realTimeTopicInVersion,
        expectedRealTimeTopicNameInVersionConfig.isEmpty()
            ? oldStyleRealTimeTopicName
            : expectedRealTimeTopicNameInVersionConfig);
    Assert.assertEquals(
        realTimeTopicInBackupVersion,
        expectedRealTimeTopicNameInBackupVersionConfig.isEmpty()
            ? oldStyleRealTimeTopicName
            : expectedRealTimeTopicNameInBackupVersionConfig);
  }

  /*
  This tests verifies the stores that are present before rolling out RT versioning changes.
  If the store does not have `realTimeTopicName` and/or `largestUsedRTVersion` configs, that came in RT Versioning changes,
  it should still be able to return the right RT name and all store operations and push should work.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testOldStoresWithHybridStoreVersioning() {
    String storeName = Utils.getUniqueString("TestOldStoresWithHybridStoreVersioning");

    NewStoreResponse newStoreResponse =
        parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
    Assert.assertFalse(
        newStoreResponse.isError(),
        "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo storeInfo = childControllerClient.getStore(storeName).getStore();
        Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), 1);
      });
    }

    // make it an old-style store by removing the rt name and setting largestUsedRTVersionNumber to
    // DEFAULT_RT_VERSION_NUMBER
    String newRealTimeTopicNameInConfig = "";
    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams();
    updateStoreParams.setIncrementalPushEnabled(true)
        .setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS)
        .setNumVersionsToPreserve(2)
        .setHybridRewindSeconds(1000)
        .setActiveActiveReplicationEnabled(true)
        .setRealTimeTopicName(newRealTimeTopicNameInConfig)
        .setLargestUsedRTVersionNumber(DEFAULT_RT_VERSION_NUMBER)
        .setHybridOffsetLagThreshold(1000);
    TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams);

    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo storeInfo = childControllerClient.getStore(storeName).getStore();
        Assert.assertNotNull(storeInfo.getHybridStoreConfig());
        verifyStoreState(childControllerClient, storeName, 0, newRealTimeTopicNameInConfig, null, null);
      });
    }

    // create new version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient childControllerClient: childControllerClients) {
      Assert.assertEquals(childControllerClient.getStore(storeName).getStore().getCurrentVersion(), 1);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        verifyStoreState(
            childControllerClient,
            storeName,
            0,
            newRealTimeTopicNameInConfig,
            newRealTimeTopicNameInConfig,
            newRealTimeTopicNameInConfig);
      });
    }
  }

  /**
   * This test creates a store, do a push, update it's partition count, delete it, recreate it, and do a push again.
   * At all steps, RT name is verified.
   */
  @Test(timeOut = 2 * TEST_TIMEOUT)
  public void testRealTimeTopicVersioning() {
    String storeName = Utils.getUniqueString("TestRealTimeTopicVersioning");

    NewStoreResponse newStoreResponse =
        parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
    Assert.assertFalse(
        newStoreResponse.isError(),
        "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getLargestUsedRTVersionNumber(), 1);
    }

    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams();
    updateStoreParams.setIncrementalPushEnabled(true)
        .setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS)
        .setNumVersionsToPreserve(2)
        .setHybridRewindSeconds(1000)
        .setActiveActiveReplicationEnabled(true)
        .setHybridOffsetLagThreshold(1000);
    TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams);

    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        String expectedRealTimeTopicName = Utils.composeRealTimeTopic(storeName, 1);
        verifyStoreState(childControllerClient, storeName, 1, expectedRealTimeTopicName, null, null);
      });
    }

    // create new version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient childControllerClient: childControllerClients) {
      String expectedRealTimeTopicName = Utils.composeRealTimeTopic(storeName, 1);
      StoreInfo storeInfo = childControllerClient.getStore(storeName).getStore();
      Assert.assertEquals(storeInfo.getCurrentVersion(), 1);
      verifyStoreState(
          childControllerClient,
          storeName,
          1,
          expectedRealTimeTopicName,
          expectedRealTimeTopicName,
          expectedRealTimeTopicName);
    }

    TestWriteUtils.updateStore(storeName, parentControllerClient, new UpdateStoreQueryParams().setPartitionCount(2));

    for (int i = 0; i < childControllerClients.length; i++) {
      final int index = i;
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        String expectedRealTimeTopicNameInVersion = Utils.composeRealTimeTopic(storeName, 1);
        String expectedRealTimeTopicNameInStore = Utils.composeRealTimeTopic(storeName, 2);
        verifyStoreState(
            childControllerClients[0],
            storeName,
            2,
            expectedRealTimeTopicNameInStore,
            expectedRealTimeTopicNameInVersion,
            expectedRealTimeTopicNameInVersion);
        PubSubTopic realTimePubSubTopic = pubSubTopicRepository.getTopic(expectedRealTimeTopicNameInVersion);
        // verify rt topic is created with the default partition count = 3, note that updateStore has not yet created a
        // new version with partition count = 2
        Assert.assertEquals(topicManagers.get(index).getPartitionCount(realTimePubSubTopic), 3);
      });
    }

    // create new version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (int i = 0; i < childControllerClients.length; i++) {
      final int idx = i;
      Assert.assertEquals(childControllerClients[idx].getStore(storeName).getStore().getCurrentVersion(), 2);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        String expectedRealTimeTopicNameInBackupVersion = Utils.composeRealTimeTopic(storeName, 1);
        // because we updated partition count, rt version should increase to v2
        String expectedRealTimeTopicNameInStore = Utils.composeRealTimeTopic(storeName, 2);
        String expectedRealTimeTopicNameInVersion = Utils.composeRealTimeTopic(storeName, 2);

        // verify rt topic name
        verifyStoreState(
            childControllerClients[idx],
            storeName,
            2,
            expectedRealTimeTopicNameInStore,
            expectedRealTimeTopicNameInBackupVersion,
            expectedRealTimeTopicNameInVersion);

        PubSubTopic newRtPubSubTopic = pubSubTopicRepository.getTopic(expectedRealTimeTopicNameInVersion);
        // verify rt topic is created with the updated partition count = 2
        Assert.assertEquals(topicManagers.get(idx).getPartitionCount(newRtPubSubTopic), 2);
      });
    }

    // create another version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 3);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Assert.assertEquals(controllerClient.getStore(storeName).getStore().getVersions().size(), 2);
        // rt version should not change because there is no more partition count update
        String expectedRealTimeTopicName = Utils.composeRealTimeTopic(storeName, 2);
        // verify rt topic name
        verifyStoreState(
            childControllerClients[0],
            storeName,
            2,
            expectedRealTimeTopicName,
            expectedRealTimeTopicName,
            expectedRealTimeTopicName);
      });
    }

    // now delete and recreate the store with the same name

    updateStoreParams = new UpdateStoreQueryParams();
    updateStoreParams.setEnableReads(false).setEnableWrites(false);

    TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams);

    ControllerResponse deleteStoreResponse = parentControllerClient.retryableRequest(5, c -> c.deleteStore(storeName));
    Assert.assertFalse(
        deleteStoreResponse.isError(),
        "The DeleteStoreResponse returned an error: " + deleteStoreResponse.getError());

    for (ControllerClient controllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse getStoreResponse = controllerClient.getStore(storeName);
        Assert.assertEquals(getStoreResponse.getErrorType(), ErrorType.STORE_NOT_FOUND);
      });
    }

    newStoreResponse =
        parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
    Assert.assertFalse(
        newStoreResponse.isError(),
        "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getLargestUsedRTVersionNumber(), 3);
    }

    // make it hybrid
    updateStoreParams = new UpdateStoreQueryParams();
    updateStoreParams.setIncrementalPushEnabled(true)
        .setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS)
        .setNumVersionsToPreserve(2)
        .setHybridRewindSeconds(1000)
        .setActiveActiveReplicationEnabled(true)
        .setEnableWrites(true)
        .setEnableReads(true)
        .setHybridOffsetLagThreshold(1000);
    TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      for (ControllerClient controllerClient: childControllerClients) {
        String expectedRealTimeTopicName = Utils.composeRealTimeTopic(storeName, 3);
        verifyStoreState(controllerClient, storeName, 3, expectedRealTimeTopicName, null, null);
      }
    });

    // create a version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 4);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        String expectedRealTimeTopicName = Utils.composeRealTimeTopic(storeName, 3);
        verifyStoreState(
            controllerClient,
            storeName,
            3,
            expectedRealTimeTopicName,
            expectedRealTimeTopicName,
            expectedRealTimeTopicName);
      });
    }

    TestWriteUtils.updateStore(storeName, parentControllerClient, new UpdateStoreQueryParams().setPartitionCount(1));

    for (ControllerClient controllerClient: childControllerClients) {
      // we updated partition count, so largestUsedRTVersion should have increased
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getLargestUsedRTVersionNumber(), 4);
    }

    // create new version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (int i = 0; i < childControllerClients.length; i++) {
      final int idx = i;
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo storeInfo = childControllerClients[idx].getStore(storeName).getStore();
        Assert.assertEquals(storeInfo.getVersions().size(), 2);

        String expectedRealTimeTopicNameInBackupVersion = Utils.composeRealTimeTopic(storeName, 3);
        // because we updated partition count, rt version should increase to v2
        String expectedRealTimeTopicName = Utils.composeRealTimeTopic(storeName, 4);

        PubSubTopic newRtPubSubTopic = pubSubTopicRepository.getTopic(expectedRealTimeTopicName);
        // verify rt topic is created with the updated partition count = 2
        Assert.assertEquals(topicManagers.get(idx).getPartitionCount(newRtPubSubTopic), 1);

        Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), 4);
        verifyStoreState(
            childControllerClients[idx],
            storeName,
            4,
            expectedRealTimeTopicName,
            expectedRealTimeTopicNameInBackupVersion,
            expectedRealTimeTopicName);
      });
    }

    // create another version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient childControllerClient: childControllerClients) {
      Assert.assertEquals(childControllerClient.getStore(storeName).getStore().getCurrentVersion(), 6);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Assert.assertEquals(childControllerClient.getStore(storeName).getStore().getVersions().size(), 2);

        // rt version should not change because there is no more partition count update
        String expectedRealTimeTopicName = Utils.composeRealTimeTopic(storeName, 4);
        verifyStoreState(
            childControllerClient,
            storeName,
            4,
            expectedRealTimeTopicName,
            expectedRealTimeTopicName,
            expectedRealTimeTopicName);
      });
    }
  }
}
