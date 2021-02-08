package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.utils.TestPushUtils.*;

/**
 * This simulates the native replication ramp up plan.
 *
 * setUp:
 *  create a new kafka cluster to simulate corp-venice-native.
 *  create 2 mirrormaker between corp-venice-native and dc-1 and dc-2.
 *  setup the native replication source fabric to be corp-venice-native.
 *
 * testNativeReplicationForBatchPushFromNewCorp
 *   This test verifies with above setup push job succeeds where dc-0 works in native replication mode and consumes remotely from
 *   corp-venice-native cluster and dc-1 and dc-2 works in O/O + KMM mode.
 *
 *
 * testNativeReplicationForBatchPushWithSourceOverride
 *  This verifies that push job succeeds when we override the source kafka cluster for a particular store.
 *  Here we don't use the above corp-venice-native cluster at all. dc-0 and dc-1 are in native replication mode.
 *  dc-2 operates in O/O mode and performs local consumption. It also acts as source fabric for native replication.
 *  dc-0 and dc-1 consumes remotely from dc-2.
 *
 *  This mapping is used to easily map the plan to test.
 *  dc-0 -> prod-lor1
 *  dc-1 -> prod-ltx1
 *  dc-2 -> prod-lva1
 */




public class TestPushJobWithNativeReplicationFromCorpNative {
  private static final int TEST_TIMEOUT = 90_000; // ms

  private static final int NUMBER_OF_CHILD_DATACENTERS = 3;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
      // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  KafkaBrokerWrapper corpVeniceNativeKafka;

  @DataProvider(name = "storeSize")
  public static Object[][] storeSize() {
    return new Object[][]{{50, 2}};
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    //create a kafka for new corp-venice-native cluster
    corpVeniceNativeKafka = ServiceFactory.getKafkaBroker(ServiceFactory.getZkServer());

    /**
     * Reduce leader promotion delay to 3 seconds;
     * Create a testing environment with 1 parent fabric and 3 child fabrics;
     * Set server and replication factor to 2 to ensure at least 1 leader replica and 1 follower replica;
     * KMM whitelist config allows replicating all topics.
     */
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.put(SERVER_SHARED_CONSUMER_POOL_ENABLED, "true");
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");

    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1000);
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, "corp-venice-native");
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, "corp-venice-native");
    controllerProps.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + "corp-venice-native", corpVeniceNativeKafka.getAddress());
    controllerProps.put(CHILD_DATA_CENTER_KAFKA_ZK_PREFIX + "." + "corp-venice-native", corpVeniceNativeKafka.getZkAddress());

    multiColoMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(NUMBER_OF_CHILD_DATACENTERS, NUMBER_OF_CLUSTERS, 1,
            1, 2, 1, 2, Optional.of(new VeniceProperties(controllerProps)),
            Optional.of(controllerProps), Optional.of(new VeniceProperties(serverProperties)), false,
            MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);
    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();

    //setup an additional MirrorMaker between corp-native-kafka and dc-1 and dc-2.
    multiColoMultiClusterWrapper.addMirrorMakerBetween(corpVeniceNativeKafka, childDatacenters.get(1).getKafkaBrokerWrapper(), MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);
    multiColoMultiClusterWrapper.addMirrorMakerBetween(corpVeniceNativeKafka, childDatacenters.get(2).getKafkaBrokerWrapper(), MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);

  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
    IOUtils.closeQuietly(corpVeniceNativeKafka);
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "storeSize")
  public void testNativeReplicationForBatchPushFromNewCorp(int recordCount, int partitionCount) throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, recordCount);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    String keySchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.VALUE_FIELD_PROP)).schema().toString();
    /**
     * Enable L/F and native replication features.
     */
    UpdateStoreQueryParams updateStoreParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(partitionCount)
            .setLeaderFollowerModel(true)
            .setNativeReplicationEnabled(true);

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

    ControllerClient dc0Client = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    ControllerClient dc1Client = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
    ControllerClient dc2Client = new ControllerClient(clusterName, childDatacenters.get(2).getControllerConnectString());

    /**
     * Check the update store command in parent controller has been propagated into child controllers, before
     * sending any commands directly into child controllers, which can help avoid race conditions.
     */
    TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
    TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, true);
    TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, storeName, true);

    //disable L/F+ native replication for dc-1 and dc-2.
    UpdateStoreQueryParams updateStoreParams1 = new UpdateStoreQueryParams()
        .setLeaderFollowerModel(false)
        .setNativeReplicationEnabled(false);
    TestPushUtils.updateStore(clusterName, storeName, dc1Client, updateStoreParams1);
    TestPushUtils.updateStore(clusterName, storeName, dc2Client, updateStoreParams1);

    /**
     * Run an update store command against parent controller to update an irrelevant store config; other untouched configs
     * should not be updated.
     */
    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());
    UpdateStoreQueryParams irrelevantUpdateStoreParams = new UpdateStoreQueryParams().setReadComputationEnabled(true);
    TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, irrelevantUpdateStoreParams);

    //verify all the datacenter is configured correctly.
    TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
    TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, false);
    TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, storeName, false);


    try (KafkaPushJob job = new KafkaPushJob("Test push job", props)) {
      job.run();

      //Verify the kafka URL being returned to the push job is the same as corp-venice-native kafka url.
      Assert.assertEquals(job.getKafkaUrl(), corpVeniceNativeKafka.getAddress());
    }
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      // Current version should become 1
      for (int version : parentController.getVeniceAdmin()
          .getCurrentVersionsForMultiColos(clusterName, storeName)
          .values()) {
        Assert.assertEquals(version, 1);
      }

      // Verify the data in the second child fabric which consumes remotely
      VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1);
      String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
      try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= recordCount; ++i) {
          String expected = "test_name_" + i;
          String actual = client.get(Integer.toString(i)).get().toString();
          Assert.assertEquals(actual, expected);
        }
      }
    });
  }

  @Test(dependsOnMethods = "testNativeReplicationForBatchPushFromNewCorp", timeOut = TEST_TIMEOUT, dataProvider = "storeSize")
  public void testNativeReplicationForBatchPushWithSourceOverride(int recordCount, int partitionCount) throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, recordCount);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    String keySchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.VALUE_FIELD_PROP)).schema().toString();

    //Enable L/F and native replication features.
    UpdateStoreQueryParams updateStoreParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(partitionCount)
            .setLeaderFollowerModel(true)
            .setNativeReplicationEnabled(true)
            .setNativeReplicationSourceFabric("dc-2");

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

    ControllerClient dc0Client = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    ControllerClient dc1Client = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
    ControllerClient dc2Client = new ControllerClient(clusterName, childDatacenters.get(2).getControllerConnectString());

    /**
     * Check the update store command in parent controller has been propagated into child controllers, before
     * sending any commands directly into child controllers, which can help avoid race conditions.
     */
    TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
    TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, true);
    TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, storeName, true);

    //disable L/F+ native replication for dc-2.
    UpdateStoreQueryParams updateStoreParams1 = new UpdateStoreQueryParams()
        .setLeaderFollowerModel(false)
        .setNativeReplicationEnabled(false);
    TestPushUtils.updateStore(clusterName, storeName, dc2Client, updateStoreParams1);

    //verify all the datacenter is configured correctly.
    TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
    TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, true);
    TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, storeName, false);

    try (KafkaPushJob job = new KafkaPushJob("Test push job", props)) {
      job.run();

      //Verify the kafka URL being returned to the push job is the same as dc-2 kafka url.
      Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(2).getKafkaBrokerWrapper().getAddress());
    }
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      // Current version should become 1
      for (int version : parentController.getVeniceAdmin()
          .getCurrentVersionsForMultiColos(clusterName, storeName)
          .values()) {
        Assert.assertEquals(version, 1);
      }

      // Verify the data in the second child fabric which consumes remotely
      VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1);
      String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
      try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= recordCount; ++i) {
          String expected = "test_name_" + i;
          String actual = client.get(Integer.toString(i)).get().toString();
          Assert.assertEquals(actual, expected);
        }
      }
    });
  }
}
