package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_ENABLE_HYBRID_STORE_PARTITION_COUNT_UPDATE;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_NUMBER_OF_PARTITION_FOR_HYBRID;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
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

  @BeforeClass
  public void setUp() {
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

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
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
  public void testUpdateRealTimeTopicName() {
    String storeName = Utils.getUniqueString("TestUpdateRealTimeTopicName");
    String clusterName = CLUSTER_NAMES[0];
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    ControllerClient parentControllerClient =
        ControllerClient.constructClusterControllerClient(clusterName, parentControllerURLs);
    ControllerClient[] childControllerClients = new ControllerClient[childDatacenters.size()];
    for (int i = 0; i < childDatacenters.size(); i++) {
      childControllerClients[i] =
          new ControllerClient(clusterName, childDatacenters.get(i).getControllerConnectString());
    }

    NewStoreResponse newStoreResponse =
        parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
    Assert.assertFalse(
        newStoreResponse.isError(),
        "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    String newRealTimeTopicName = "NewRealTimeTopicName" + Version.REAL_TIME_TOPIC_SUFFIX;
    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams();
    updateStoreParams.setIncrementalPushEnabled(true)
        .setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS)
        .setNumVersionsToPreserve(2)
        .setHybridRewindSeconds(1000)
        .setActiveActiveReplicationEnabled(true)
        .setRealTimeTopicName(newRealTimeTopicName)
        .setHybridOffsetLagThreshold(1000);
    TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams);

    // create new version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 1);
    }

    for (int i = 0; i < childControllerClients.length; i++) {
      final int index = i;
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo storeInfo = childControllerClients[index].getStore(storeName).getStore();
        String realTimeTopicNameInStore = storeInfo.getHybridStoreConfig().getRealTimeTopicName();
        String realTimeTopicNameInVersion = Utils.getRealTimeTopicName(storeInfo.getVersions().get(0));

        Assert.assertEquals(realTimeTopicNameInVersion, newRealTimeTopicName);
        Assert.assertEquals(realTimeTopicNameInStore, newRealTimeTopicName);
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testHybridStoreRepartitioning() {
    String storeName = Utils.getUniqueString("TestHybridStoreRepartitioning");
    String clusterName = CLUSTER_NAMES[0];
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    ControllerClient parentControllerClient =
        ControllerClient.constructClusterControllerClient(clusterName, parentControllerURLs);
    ControllerClient[] childControllerClients = new ControllerClient[childDatacenters.size()];
    for (int i = 0; i < childDatacenters.size(); i++) {
      childControllerClients[i] =
          new ControllerClient(clusterName, childDatacenters.get(i).getControllerConnectString());
    }

    NewStoreResponse newStoreResponse =
        parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
    Assert.assertFalse(
        newStoreResponse.isError(),
        "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams();
    updateStoreParams.setIncrementalPushEnabled(true)
        .setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS)
        .setNumVersionsToPreserve(2)
        .setHybridRewindSeconds(1000)
        .setActiveActiveReplicationEnabled(true)
        .setHybridOffsetLagThreshold(1000);
    TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams);

    // create new version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 1);
    }

    TestWriteUtils.updateStore(storeName, parentControllerClient, new UpdateStoreQueryParams().setPartitionCount(2));

    for (int i = 0; i < childControllerClients.length; i++) {
      final int index = i;
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreInfo storeInfo = childControllerClients[index].getStore(storeName).getStore();
        String realTimeTopicNameInVersion = Utils.getRealTimeTopicName(storeInfo.getVersions().get(0));
        PubSubTopic realTimePubSubTopic = pubSubTopicRepository.getTopic(realTimeTopicNameInVersion);

        // verify rt topic is created with the default partition count = 3
        Assert.assertEquals(topicManagers.get(index).getPartitionCount(realTimePubSubTopic), 3);
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
        String expectedRealTimeTopicNameInStoreConfig =
            Utils.createNewRealTimeTopicName(realTimeTopicNameInBackupVersion);
        String actualRealTimeTopicNameInStoreConfig = storeInfo.getHybridStoreConfig().getRealTimeTopicName();
        PubSubTopic newRtPubSubTopic = pubSubTopicRepository.getTopic(realTimeTopicNameInCurrentVersion);

        // verify rt topic name
        Assert.assertNotEquals(realTimeTopicNameInBackupVersion, realTimeTopicNameInCurrentVersion);
        Assert.assertEquals(realTimeTopicNameInCurrentVersion, actualRealTimeTopicNameInStoreConfig);
        Assert.assertEquals(actualRealTimeTopicNameInStoreConfig, expectedRealTimeTopicNameInStoreConfig);

        // verify rt topic is created with the updated partition count = 2
        Assert.assertEquals(topicManagers.get(idx).getPartitionCount(newRtPubSubTopic), 2);
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testLargestUsedRTVersionNumber() {
    String storeName = Utils.getUniqueString("TestLargestUsedRTVersionNumber");
    String clusterName = CLUSTER_NAMES[0];
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    ControllerClient parentControllerClient =
        ControllerClient.constructClusterControllerClient(clusterName, parentControllerURLs);
    ControllerClient[] childControllerClients = new ControllerClient[childDatacenters.size()];
    for (int i = 0; i < childDatacenters.size(); i++) {
      childControllerClients[i] =
          new ControllerClient(clusterName, childDatacenters.get(i).getControllerConnectString());
    }

    NewStoreResponse newStoreResponse =
        parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
    Assert.assertFalse(
        newStoreResponse.isError(),
        "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams();
    updateStoreParams.setEnableReads(false).setEnableWrites(false);

    TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams);

    ControllerResponse deleteStoreResponse =
        childControllerClients[0].retryableRequest(5, c -> c.deleteStore(storeName));
    Assert.assertFalse(
        deleteStoreResponse.isError(),
        "The DeleteStoreResponse returned an error: " + deleteStoreResponse.getError());

    newStoreResponse =
        childControllerClients[0].retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
    Assert.assertFalse(
        newStoreResponse.isError(),
        "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    String newRealTimeTopicName = "NewRealTimeTopicName" + Version.REAL_TIME_TOPIC_SUFFIX;
    updateStoreParams = new UpdateStoreQueryParams();
    updateStoreParams.setIncrementalPushEnabled(true)
        .setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS)
        .setNumVersionsToPreserve(2)
        .setHybridRewindSeconds(1000)
        .setActiveActiveReplicationEnabled(true)
        .setRealTimeTopicName(newRealTimeTopicName)
        .setEnableWrites(true)
        .setEnableReads(true)
        .setHybridOffsetLagThreshold(1000);
    TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams);

    // create new version by doing an empty push
    parentControllerClient
        .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, 60L * Time.MS_PER_SECOND);

    for (ControllerClient controllerClient: childControllerClients) {
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 1);
      Assert.assertEquals(controllerClient.getStore(storeName).getStore().getLargestUsedRTVersionNumber(), 0);
    }
  }
}
