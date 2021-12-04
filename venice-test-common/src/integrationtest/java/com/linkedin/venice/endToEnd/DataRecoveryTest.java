package com.linkedin.venice.endToEnd;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ReadyForDataRecoveryResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.*;


public class DataRecoveryTest {
  private static final long TEST_TIMEOUT = 120_000;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;
  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.put(SERVER_SHARED_CONSUMER_POOL_ENABLED, "true");
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, "true");
    serverProperties.put(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, "2");

    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1000);
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);

    controllerProps.put(LF_MODEL_DEPENDENCY_CHECK_DISABLED, "true");
    controllerProps.put(AGGREGATE_REAL_TIME_SOURCE_REGION, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    controllerProps.put(NATIVE_REPLICATION_FABRIC_WHITELIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    int parentKafkaPort = Utils.getFreePort();
    controllerProps.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + parentKafkaPort);
    controllerProps.put(ALLOW_CLUSTER_WIPE, "true");
    controllerProps.put(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, "1000");
    controllerProps.put(MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE, "0");

    multiColoMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(NUMBER_OF_CHILD_DATACENTERS, NUMBER_OF_CLUSTERS, 1,
            1, 2, 1, 2, Optional.of(new VeniceProperties(controllerProps)), Optional.of(controllerProps),
            Optional.of(new VeniceProperties(serverProperties)), false, MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST,
            false, Optional.of(parentKafkaPort));
    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStartDataRecoveryAPIs() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("dataRecovery-store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName,
        parentController.getControllerUrl()); ControllerClient dc0Client = new ControllerClient(clusterName,
        childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client = new ControllerClient(clusterName,
            childDatacenters.get(1).getControllerConnectString())) {
      List<ControllerClient> dcControllerClientList = Arrays.asList(dc0Client, dc1Client);
      TestUtils.createAndVerifyStoreInAllRegions(storeName, parentControllerClient, dcControllerClientList);
      Assert.assertFalse(parentControllerClient.updateStore(storeName,
          new UpdateStoreQueryParams().setLeaderFollowerModel(true)
              .setHybridRewindSeconds(10)
              .setHybridOffsetLagThreshold(2)
              .setHybridDataReplicationPolicy(DataReplicationPolicy.AGGREGATE)
              .setNativeReplicationEnabled(true)
              .setPartitionCount(1)).isError());
      TestUtils.verifyDCConfigNativeAndActiveRepl(dc0Client, storeName, true, false);
      TestUtils.verifyDCConfigNativeAndActiveRepl(dc1Client, storeName, true, false);
      Assert.assertFalse(
          parentControllerClient.emptyPush(storeName, "empty-push-" + System.currentTimeMillis(), 1000).isError());
      TestUtils.waitForNonDeterministicPushCompletion(Version.composeKafkaTopic(storeName, 1), parentControllerClient,
          30, TimeUnit.SECONDS, Optional.empty());
      ReadyForDataRecoveryResponse response =
          parentControllerClient.isStoreVersionReadyForDataRecovery("dc-0", "dc-1", storeName, 1, Optional.empty());
      Assert.assertFalse(response.isError());
      Assert.assertFalse(response.isReady());
      // Prepare dc-1 for data recovery
      Assert.assertFalse(
          parentControllerClient.prepareDataRecovery("dc-0", "dc-1", storeName, 1, Optional.empty()).isError());
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        ReadyForDataRecoveryResponse readinessResponse =
            parentControllerClient.isStoreVersionReadyForDataRecovery("dc-0", "dc-1", storeName, 1, Optional.empty());
        Assert.assertTrue(readinessResponse.isReady(), readinessResponse.getReason());
      });
      // Initiate data recovery
      Assert.assertFalse(
          parentControllerClient.dataRecovery("dc-0", "dc-1", storeName, 1, false, true, Optional.empty()).isError());
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Admin dc1Admin = childDatacenters.get(1).getMasterController(clusterName).getVeniceAdmin();
        Assert.assertTrue(dc1Admin.getStore(clusterName, storeName).containsVersion(1));
        Assert.assertTrue(dc1Admin.getTopicManager().containsTopic(Version.composeKafkaTopic(storeName, 1)));
      });
    }
  }
}
