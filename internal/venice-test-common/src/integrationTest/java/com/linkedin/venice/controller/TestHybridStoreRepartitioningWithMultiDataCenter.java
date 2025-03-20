package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_ENABLE_HYBRID_STORE_PARTITION_COUNT_UPDATE;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;

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
  private static final int TEST_TIMEOUT = 90_000; // ms
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private List<VeniceMultiClusterWrapper> childDatacenters;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  List<TopicManager> topicManagers;
  PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  ControllerClient parentControllerClient;
  ControllerClient[] childControllerClients;

  @BeforeClass
  public void setUp() {
    String clusterName = CLUSTER_NAMES[0];
    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID, 2);
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 3);
    controllerProps.put(DEFAULT_PARTITION_SIZE, 1024);
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
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
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

        // Assert.assertEquals(realTimeTopicNameInStore, newRealTimeTopicNameInCofnig);
      });
    }
    // make it an old-style store by removing the rt name
    String newRealTimeTopicNameInCofnig = "";
    String newRealTimeTopicName = storeName + Version.REAL_TIME_TOPIC_SUFFIX;
    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams();
    updateStoreParams.setIncrementalPushEnabled(true)
        .setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS)
        .setNumVersionsToPreserve(2)
        .setHybridRewindSeconds(1000)
        .setActiveActiveReplicationEnabled(true)
        .setRealTimeTopicName(newRealTimeTopicNameInCofnig)
        .setLargestUsedRTVersionNumber(0)
        .setHybridOffsetLagThreshold(1000);
    TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams);

    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo storeInfo = childControllerClient.getStore(storeName).getStore();
        Assert.assertNotNull(storeInfo.getHybridStoreConfig());
        String realTimeTopicNameInStore = storeInfo.getHybridStoreConfig().getRealTimeTopicName();
        Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), 0);

        // Assert.assertEquals(realTimeTopicNameInStore, newRealTimeTopicNameInCofnig);
      });
    }

    // create new version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 1);
    }

    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo storeInfo = childControllerClient.getStore(storeName).getStore();
        Version version = storeInfo.getVersions().get(0);
        String realTimeTopicNameInStore = storeInfo.getHybridStoreConfig().getRealTimeTopicName();
        String realTimeTopicNameInVersion = version.getHybridStoreConfig().getRealTimeTopicName();
        String realTimeTopicInStoreThroughAPI = Utils.getRealTimeTopicName(storeInfo);
        String realTimeTopicInVersionThroughAPI = Utils.getRealTimeTopicName(version);

        Assert.assertEquals(realTimeTopicNameInStore, newRealTimeTopicNameInCofnig);
        Assert.assertEquals(realTimeTopicNameInVersion, newRealTimeTopicNameInCofnig);
        Assert.assertEquals(realTimeTopicInStoreThroughAPI, newRealTimeTopicName);
        Assert.assertEquals(realTimeTopicInVersionThroughAPI, newRealTimeTopicName);
      });
    }
  }

  /**
   * This test creates a store, do a push, update it's partition count, delete it, recreate it, and do a push again.
   * At all steps, RT name is verified.
   */
  @Test(timeOut = 5 * TEST_TIMEOUT)
  public void testRealTimeTopicVersioning() {
    String storeName = Utils.getUniqueString("TestRealTimeTopicVersioning");

    NewStoreResponse newStoreResponse =
        parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
    Assert.assertFalse(
        newStoreResponse.isError(),
        "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    for (ControllerClient controllerClient: childControllerClients) {
      StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
      Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), 1);
    }

    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams();
    updateStoreParams.setIncrementalPushEnabled(true)
        .setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS)
        .setNumVersionsToPreserve(2)
        .setHybridRewindSeconds(1000)
        .setActiveActiveReplicationEnabled(true)
        .setHybridOffsetLagThreshold(1000);
    TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams);

    for (ControllerClient controllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
        Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), 1);
      });
    }

    // create new version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient controllerClient: childControllerClients) {
      StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
      Assert.assertEquals(
          storeInfo.getHybridStoreConfig().getRealTimeTopicName(),
          storeName + "_v1" + Version.REAL_TIME_TOPIC_SUFFIX);
      Assert.assertEquals(storeInfo.getCurrentVersion(), 1);
    }

    TestWriteUtils.updateStore(storeName, parentControllerClient, new UpdateStoreQueryParams().setPartitionCount(2));

    for (int i = 0; i < childControllerClients.length; i++) {
      final int index = i;
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo storeInfo = childControllerClients[index].getStore(storeName).getStore();
        String realTimeTopicNameInVersion = Utils.getRealTimeTopicName(storeInfo.getVersions().get(0));
        Assert.assertEquals(realTimeTopicNameInVersion, storeName + "_v1" + Version.REAL_TIME_TOPIC_SUFFIX);

        PubSubTopic realTimePubSubTopic = pubSubTopicRepository.getTopic(realTimeTopicNameInVersion);
        // verify rt topic is created with the default partition count = 3
        Assert.assertEquals(topicManagers.get(index).getPartitionCount(realTimePubSubTopic), 3);
        Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), 2);
      });
    }

    // create new version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 2);
    }

    for (int i = 0; i < childControllerClients.length; i++) {
      final int idx = i;
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo storeInfo = childControllerClients[idx].getStore(storeName).getStore();
        String realTimeTopicNameInBackupVersion = Utils.getRealTimeTopicName(storeInfo.getVersions().get(0));
        String realTimeTopicNameInCurrentVersion = Utils.getRealTimeTopicName(storeInfo.getVersions().get(1));
        String expectedRealTimeTopicNameInBackVersion = storeName + "_v1" + Version.REAL_TIME_TOPIC_SUFFIX;

        // because we updated partition count, rt version should increase to v2
        String expectedRealTimeTopicName = storeName + "_v2" + Version.REAL_TIME_TOPIC_SUFFIX;

        // verify rt topic name
        Assert.assertEquals(realTimeTopicNameInBackupVersion, expectedRealTimeTopicNameInBackVersion);
        Assert.assertEquals(realTimeTopicNameInCurrentVersion, expectedRealTimeTopicName);

        PubSubTopic newRtPubSubTopic = pubSubTopicRepository.getTopic(realTimeTopicNameInCurrentVersion);
        // verify rt topic is created with the updated partition count = 2
        Assert.assertEquals(topicManagers.get(idx).getPartitionCount(newRtPubSubTopic), 2);

        // we updated partition count and create a new VT version, so largestUsedRTVersion should have increased
        Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), 2);
      });
    }

    // create another version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 3);
    }

    for (ControllerClient controllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
        List<Version> versions = storeInfo.getVersions();
        int versionCount = versions.size();
        Assert.assertEquals(versionCount, 2);
        String realTimeTopicNameInBackupVersion = Utils.getRealTimeTopicName(versions.get(0));
        String realTimeTopicNameInCurrentVersion = Utils.getRealTimeTopicName(versions.get(1));

        // rt version should not change because there is no more partition count update
        String expectedRealTimeTopicName = storeName + "_v2" + Version.REAL_TIME_TOPIC_SUFFIX;

        // verify rt topic name
        Assert.assertEquals(realTimeTopicNameInBackupVersion, expectedRealTimeTopicName);
        Assert.assertEquals(realTimeTopicNameInCurrentVersion, expectedRealTimeTopicName);

        Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), 2);
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
      StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
      Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), 3);
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
        StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
        Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), 3);
        Assert.assertEquals(Utils.getRealTimeTopicName(storeInfo), storeName + "_v3" + Version.REAL_TIME_TOPIC_SUFFIX);
      }
    });

    // create a version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 4);
    }

    for (ControllerClient controllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
        String realTimeTopicNameInVersion = Utils.getRealTimeTopicName(storeInfo.getVersions().get(0));
        Assert.assertEquals(realTimeTopicNameInVersion, storeName + "_v3" + Version.REAL_TIME_TOPIC_SUFFIX);
        Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), 3);
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
        List<Version> versions = storeInfo.getVersions();
        int versionCount = versions.size();
        Assert.assertEquals(versionCount, 2);
        String realTimeTopicNameInBackupVersion = Utils.getRealTimeTopicName(versions.get(0));
        String realTimeTopicNameInCurrentVersion = Utils.getRealTimeTopicName(versions.get(1));
        String expectedRealTimeTopicNameInBackVersion = storeName + "_v3" + Version.REAL_TIME_TOPIC_SUFFIX;

        // because we updated partition count, rt version should increase to v2
        String expectedRealTimeTopicName = storeName + "_v4" + Version.REAL_TIME_TOPIC_SUFFIX;

        // verify rt topic name
        Assert.assertEquals(realTimeTopicNameInBackupVersion, expectedRealTimeTopicNameInBackVersion);
        Assert.assertEquals(realTimeTopicNameInCurrentVersion, expectedRealTimeTopicName);

        PubSubTopic newRtPubSubTopic = pubSubTopicRepository.getTopic(realTimeTopicNameInCurrentVersion);
        // verify rt topic is created with the updated partition count = 2
        Assert.assertEquals(topicManagers.get(idx).getPartitionCount(newRtPubSubTopic), 1);

        Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), 4);
      });
    }

    // create another version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 6);
    }

    for (ControllerClient controllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
        List<Version> versions = storeInfo.getVersions();
        int versionCount = versions.size();
        Assert.assertEquals(versionCount, 2);
        String realTimeTopicNameInBackupVersion = Utils.getRealTimeTopicName(versions.get(0));
        String realTimeTopicNameInCurrentVersion = Utils.getRealTimeTopicName(versions.get(1));

        // rt version should not change because there is no more partition count update
        String expectedRealTimeTopicName = storeName + "_v4" + Version.REAL_TIME_TOPIC_SUFFIX;

        // verify rt topic name
        Assert.assertEquals(realTimeTopicNameInBackupVersion, expectedRealTimeTopicName);
        Assert.assertEquals(realTimeTopicNameInCurrentVersion, expectedRealTimeTopicName);

        Assert.assertEquals(storeInfo.getLargestUsedRTVersionNumber(), 4);
      });
    }
  }
}
