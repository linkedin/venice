package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.venice.CommonConfigKeys.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.*;
import static com.linkedin.venice.samza.VeniceSystemFactory.*;
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
  public static final Logger LOGGER = Logger.getLogger(TestPushJobWithNativeReplicationFromCorpNative.class);

  private static final int TEST_TIMEOUT = 90_000; // ms
  private static final int TEST_TIMEOUT_LARGE = 120_000; // ms


  private static final int NUMBER_OF_CHILD_DATACENTERS = 3;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
      // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  KafkaBrokerWrapper corpVeniceNativeKafka;
  KafkaBrokerWrapper corpDefaultParentKafka;

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
    serverProperties.put(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, "true");
    serverProperties.put(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, "1");

    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1000);
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, "corp-venice-native");
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, "corp-venice-native");
    controllerProps.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + "corp-venice-native", corpVeniceNativeKafka.getAddress());
    controllerProps.put(CHILD_DATA_CENTER_KAFKA_ZK_PREFIX + "." + "corp-venice-native", corpVeniceNativeKafka.getZkAddress());
    controllerProps.put(LF_MODEL_DEPENDENCY_CHECK_DISABLED, "true");
    controllerProps.put(AGGREGATE_REAL_TIME_SOURCE_REGION, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    controllerProps.put(NATIVE_REPLICATION_FABRIC_WHITELIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    int parentKafkaPort = Utils.getFreePort();
    controllerProps.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME, "localhost:" + parentKafkaPort);

    multiColoMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
            NUMBER_OF_CHILD_DATACENTERS,
            NUMBER_OF_CLUSTERS,
            1,
            1,
            2,
            1,
            2,
            Optional.of(new VeniceProperties(controllerProps)),
            Optional.of(controllerProps),
            Optional.of(new VeniceProperties(serverProperties)),
            false,
            MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST,
            false,
            Optional.of(parentKafkaPort));
    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();

    //setup an additional MirrorMaker between corp-native-kafka and dc-1 and dc-2.
    multiColoMultiClusterWrapper.addMirrorMakerBetween(corpVeniceNativeKafka, childDatacenters.get(1).getKafkaBrokerWrapper(), MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);
    multiColoMultiClusterWrapper.addMirrorMakerBetween(corpVeniceNativeKafka, childDatacenters.get(2).getKafkaBrokerWrapper(), MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);
    corpDefaultParentKafka = multiColoMultiClusterWrapper.getParentKafkaBrokerWrapper();
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
    String keySchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.VALUE_FIELD_PROP)).schema().toString();
    /**
     * Enable L/F and native replication features.
     */
    UpdateStoreQueryParams updateStoreParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(partitionCount)
            .setLeaderFollowerModel(true)
            .setNativeReplicationEnabled(true);

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

    try (ControllerClient dc0Client = new ControllerClient(clusterName,
        childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client = new ControllerClient(clusterName,
            childDatacenters.get(1).getControllerConnectString());
        ControllerClient dc2Client = new ControllerClient(clusterName,
            childDatacenters.get(2).getControllerConnectString())) {

      /**
       * Check the update store command in parent controller has been propagated into child controllers, before
       * sending any commands directly into child controllers, which can help avoid race conditions.
       */
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, storeName, true);

      //disable L/F+ native replication for dc-1 and dc-2.
      UpdateStoreQueryParams updateStoreParams1 =
          new UpdateStoreQueryParams().setLeaderFollowerModel(false).setNativeReplicationEnabled(false);
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
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(parentControllerClient, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, false);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, storeName, false);
    }

    try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
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

      // Verify the data in the first child fabric which consumes remotely
      VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(0);
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

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "storeSize")
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
    String keySchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.VALUE_FIELD_PROP)).schema().toString();

    //Enable L/F and native replication features.
    UpdateStoreQueryParams updateStoreParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(partitionCount)
            .setLeaderFollowerModel(true)
            .setNativeReplicationEnabled(true)
            .setNativeReplicationSourceFabric("dc-2");

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

    try (ControllerClient dc0Client = new ControllerClient(clusterName,
        childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client = new ControllerClient(clusterName,
            childDatacenters.get(1).getControllerConnectString());
        ControllerClient dc2Client = new ControllerClient(clusterName,
            childDatacenters.get(2).getControllerConnectString())) {

      /**
       * Check the update store command in parent controller has been propagated into child controllers, before
       * sending any commands directly into child controllers, which can help avoid race conditions.
       */
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, storeName, true);

      //disable L/F+ native replication for dc-2.
      UpdateStoreQueryParams updateStoreParams1 =
          new UpdateStoreQueryParams().setLeaderFollowerModel(false).setNativeReplicationEnabled(false);
      TestPushUtils.updateStore(clusterName, storeName, dc2Client, updateStoreParams1);

      //verify all the datacenter is configured correctly.
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, storeName, false);
    }

    try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
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
      VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(1);
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

  @Test(timeOut = TEST_TIMEOUT)
  public void testNativeReplicationForIncrementalPush() throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, 100);
    String keySchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.VALUE_FIELD_PROP)).schema().toString();

    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setPartitionCount(2)
        .setLeaderFollowerModel(true)
        .setNativeReplicationEnabled(true)
        .setIncrementalPushEnabled(true)
        .setIncrementalPushPolicy(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC);

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

    try (ControllerClient dc0Client = new ControllerClient(clusterName,
        childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client = new ControllerClient(clusterName,
            childDatacenters.get(1).getControllerConnectString());
        ControllerClient dc2Client = new ControllerClient(clusterName,
            childDatacenters.get(2).getControllerConnectString())) {

      /**
       * Check the update store command in parent controller has been propagated into child controllers, before
       * sending any commands directly into child controllers, which can help avoid race conditions.
       */
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, storeName, true);

      //disable L/F+ native replication for dc-1 and dc-2.
      UpdateStoreQueryParams updateStoreParams1 =
          new UpdateStoreQueryParams().setLeaderFollowerModel(false).setNativeReplicationEnabled(false);
      TestPushUtils.updateStore(clusterName, storeName, dc1Client, updateStoreParams1);
      TestPushUtils.updateStore(clusterName, storeName, dc2Client, updateStoreParams1);

      //verify all the datacenter is configured correctly.
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, false);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, storeName, false);
    }

    // Write batch data
    TestPushUtils.runPushJob("Test push job", props);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      // Current version should become 3
      for (int version : parentController.getVeniceAdmin()
          .getCurrentVersionsForMultiColos(clusterName, storeName)
          .values()) {
        Assert.assertEquals(version, 1);
      }
    });

    //Run incremental push job
    props.setProperty(INCREMENTAL_PUSH, "true");
    TestPushUtils.writeSimpleAvroFileWithUserSchema2(inputDir);
    TestPushUtils.runPushJob("Test incremental push job", props);

    //Verify following in child controller
    for (VeniceMultiClusterWrapper childDataCenter : childDatacenters) {
      //Verify version level incremental push config is set correctly. The current version should be 1.
      Optional<Version> version =
          childDataCenter.getRandomController().getVeniceAdmin().getStore(clusterName, storeName).getVersion(1);
      Assert.assertTrue(version.get().isIncrementalPushEnabled());
    }

    //Verify the data in the first child fabric which consumes remotely
    VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(0);
    String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
    try(AvroGenericStoreClient<String, Object> client =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
      for (int i = 1; i <= 150; ++i) {
        String expected = i <= 50 ? "test_name_" + i : "test_name_" + (i * 2);
        String actual = client.get(Integer.toString(i)).get().toString();
        Assert.assertEquals(actual, expected);
      }
    }
  }

  public static void verifyIncrementalPushData(List<VeniceMultiClusterWrapper> childDatacenters, String clusterName, String storeName, int maxKey, int valueMultiplier) throws Exception {
    //Verify the data in the first child fabric which consumes remotely
    int j = 0;
    for (VeniceMultiClusterWrapper childDataCenter : childDatacenters) {
      logger.info("ssen: verifying dc-" + j++);
      String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
      try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= maxKey; ++i) {
          String expected = i <= 50 ? "test_name_" + i : "test_name_" + (i * valueMultiplier);
          String actual = client.get(Integer.toString(i)).get().toString();
          Assert.assertEquals(actual, expected);
        }
      }
    }
  }

  /**
   * The objective of this test is to verify the full rollout/migration of an Incremental push to RT policy store from without NR
   * to NR mode following through the 3 phases of rollout.
   * This test is a bit time consuming a it runs many push jobs. So timeout is increased.
   * @throws Exception
   */
  @Test(enabled=true, timeOut = TEST_TIMEOUT_LARGE)
  public void testNativeReplicationForIncrementalPushRTRollout() throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDirBatch = getTempDataDirectory();
    File inputDirInc = getTempDataDirectory();
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    String inputDirPathBatch = "file:" + inputDirBatch.getAbsolutePath();
    String inputDirPathInc = "file:" + inputDirInc.getAbsolutePath();

    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());
    ControllerClient dc0ControllerClient = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    ControllerClient dc1ControllerClient = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
    ControllerClient dc2ControllerClient = new ControllerClient(clusterName, childDatacenters.get(2).getControllerConnectString());
    String storeName = TestUtils.getUniqueString("store");

    try {
      Properties propsBatch = defaultH2VProps(parentController.getControllerUrl(), inputDirPathBatch, storeName);
      propsBatch.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
      Properties propsInc = defaultH2VProps(parentController.getControllerUrl(), inputDirPathInc, storeName);
      propsInc.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

      Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDirBatch, true, 100);
      String keySchemaStr =
          recordSchema.getField(propsBatch.getProperty(VenicePushJob.KEY_FIELD_PROP)).schema().toString();
      String valueSchemaStr =
          recordSchema.getField(propsBatch.getProperty(VenicePushJob.VALUE_FIELD_PROP)).schema().toString();

      propsInc.setProperty(INCREMENTAL_PUSH, "true");
      TestPushUtils.writeSimpleAvroFileWithUserSchema2(inputDirInc);

      UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
          .setPartitionCount(2)
          .setHybridOffsetLagThreshold(TEST_TIMEOUT_LARGE)
          .setHybridRewindSeconds(2L)
          .setIncrementalPushEnabled(true)
          .setIncrementalPushPolicy(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME);
      createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, propsBatch, updateStoreParams).close();

      UpdateStoreQueryParams enableNativeRepl =
          new UpdateStoreQueryParams().setLeaderFollowerModel(true).setNativeReplicationEnabled(true);

      //Print all the kafka cluster URL's
      LOGGER.info("KafkaURL dc-0:" + childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
      LOGGER.info("KafkaURL dc-1:" + childDatacenters.get(1).getKafkaBrokerWrapper().getAddress());
      LOGGER.info("KafkaURL dc-2:" + childDatacenters.get(2).getKafkaBrokerWrapper().getAddress());
      LOGGER.info("KafkaURL Parent Corp:" + corpDefaultParentKafka.getAddress());
      LOGGER.info("KafkaURL Venice Native Corp:" + corpVeniceNativeKafka.getAddress());

      //Base state, Store in not in NR mode, batch push and inc push should succeed.
      try (VenicePushJob job = new VenicePushJob("Test push job batch without NR", propsBatch)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), parentController.getKafkaBootstrapServers(false));
      }
      try (VenicePushJob job = new VenicePushJob("Test push job incremental without NR", propsInc)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), corpDefaultParentKafka.getAddress());
      }
      verifyIncrementalPushData(childDatacenters, clusterName, storeName, 150, 2);


      /**
       * Phase1 Rollout
       */
      //Setup:
      //configure NR and L/F in parent and dc-0
      enableNativeRepl.setRegionsFilter("dc-0,parent.parent");
      TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, enableNativeRepl);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0ControllerClient, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1ControllerClient, storeName, false);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2ControllerClient, storeName, false);

      //Test
      try (VenicePushJob job = new VenicePushJob("Test push job batch with NR phase 1", propsBatch)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), corpVeniceNativeKafka.getAddress());
      }
      try (VenicePushJob job = new VenicePushJob("Test push job incremental phase 1", propsInc)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), corpDefaultParentKafka.getAddress());
      }

      //Verify
      for (VeniceMultiClusterWrapper childDataCenter : childDatacenters) {
        //Verify version level incremental push config is set correctly. The current version should be 1.
        Optional<Version> version =
            childDataCenter.getRandomController().getVeniceAdmin().getStore(clusterName, storeName).getVersion(2);
        Assert.assertTrue(version.get().isIncrementalPushEnabled());
      }
      verifyIncrementalPushData(childDatacenters, clusterName, storeName, 150, 2);


      /**
       * Phase2 Rollout
       */
      /*
      //Setup
      UpdateStoreQueryParams changeSourceFabric = new UpdateStoreQueryParams().setNativeReplicationSourceFabric("dc-2");
      enableNativeRepl.setRegionsFilter("dc-1");
      TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, enableNativeRepl);
      TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, changeSourceFabric);
      // Verify all the datacenter is configured correctly.
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0ControllerClient, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1ControllerClient, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2ControllerClient, storeName, false);

      //Test
      try (VenicePushJob job = new VenicePushJob("Test push job batch with NR phase 2", propsBatch)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(2).getKafkaBrokerWrapper().getAddress());
      }
      try (VenicePushJob job = new VenicePushJob("Test push job incremental phase 2", propsInc)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), corpDefaultParentKafka.getAddress());
      }

      //Verify
      for (VeniceMultiClusterWrapper childDataCenter : childDatacenters) {
        //Verify version level incremental push config is set correctly. The current version should be 1.
        Optional<Version> version =
            childDataCenter.getRandomController().getVeniceAdmin().getStore(clusterName, storeName).getVersion(3);
        Assert.assertTrue(version.get().isIncrementalPushEnabled());
      }
      verifyIncrementalPushData(childDatacenters, clusterName, storeName, 150, 2);


       */

      /**
       * Phase3 Rollout
       */
      /*
      //Setup
      enableNativeRepl.setRegionsFilter("dc-2");
      TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, enableNativeRepl);
      TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, changeSourceFabric);
      // Verify all the datacenter is configured correctly.
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0ControllerClient, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1ControllerClient, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2ControllerClient, storeName, true);

      //Test
      try (VenicePushJob job = new VenicePushJob("Test push job batch with NR phase 3", propsBatch)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(2).getKafkaBrokerWrapper().getAddress());
      }
      try (VenicePushJob job = new VenicePushJob("Test push job incremental phase 3", propsInc)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), corpDefaultParentKafka.getAddress());
      }

      //Verify
      for (VeniceMultiClusterWrapper childDataCenter : childDatacenters) {
        //Verify version level incremental push config is set correctly. The current version should be 4.
        Optional<Version> version =
            childDataCenter.getRandomController().getVeniceAdmin().getStore(clusterName, storeName).getVersion(4);
        Assert.assertTrue(version.get().isIncrementalPushEnabled());
      }
      verifyIncrementalPushData(childDatacenters, clusterName, storeName, 150, 2);

       */
    } finally {
      ControllerResponse deleteStoreResponse = parentControllerClient.disableAndDeleteStore(storeName);
      Assert.assertFalse(deleteStoreResponse.isError(),
          "Failed to delete the test store: " + deleteStoreResponse.getError());
      IOUtils.closeQuietly(parentControllerClient);
      IOUtils.closeQuietly(dc0ControllerClient);
      IOUtils.closeQuietly(dc1ControllerClient);
      IOUtils.closeQuietly(dc2ControllerClient);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testClusterLevelAdminCommandForNativeReplication() throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    String batchOnlyStoreName = TestUtils.getUniqueString("batch-store");
    String keySchemaStr = STRING_SCHEMA;
    String valueSchemaStr = STRING_SCHEMA;

    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl())) {
      NewStoreResponse newStoreResponse = parentControllerClient.createNewStore(batchOnlyStoreName, "", keySchemaStr, valueSchemaStr);
      Assert.assertFalse(newStoreResponse.isError());

      /**
       * Enable L/F in dc-0; explicitly disable L/F in parent controller to mimic to real prod environment;
       * the {@link com.linkedin.venice.ConfigKeys#LF_MODEL_DEPENDENCY_CHECK_DISABLED} config should take care of
       * the L/F config in parent controllers.
       */
      UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams()
          .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
          .setLeaderFollowerModel(true)
          .setRegionsFilter("dc-0");
      ControllerResponse controllerResponse = parentControllerClient.updateStore(batchOnlyStoreName, updateStoreParams);
      Assert.assertFalse(controllerResponse.isError());

      /**
       * Create a hybrid store
       */
      String hybridStoreName = TestUtils.getUniqueString("hybrid-store");
      newStoreResponse = parentControllerClient.createNewStore(hybridStoreName, "", keySchemaStr, valueSchemaStr);
      Assert.assertFalse(newStoreResponse.isError());
      updateStoreParams = new UpdateStoreQueryParams()
          .setHybridRewindSeconds(10)
          .setHybridOffsetLagThreshold(2);
      controllerResponse = parentControllerClient.updateStore(hybridStoreName, updateStoreParams);
      Assert.assertFalse(controllerResponse.isError());
      updateStoreParams = new UpdateStoreQueryParams()
          .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
          .setLeaderFollowerModel(true)
          .setRegionsFilter("dc-0");
      controllerResponse = parentControllerClient.updateStore(hybridStoreName, updateStoreParams);
      Assert.assertFalse(controllerResponse.isError());

      /**
       * Create an incremental push enabled store
       */
      String incrementPushStoreName = TestUtils.getUniqueString("incremental-push-store");
      newStoreResponse = parentControllerClient.createNewStore(incrementPushStoreName, "", keySchemaStr, valueSchemaStr);
      Assert.assertFalse(newStoreResponse.isError());
      updateStoreParams = new UpdateStoreQueryParams().setIncrementalPushEnabled(true);
      controllerResponse = parentControllerClient.updateStore(incrementPushStoreName, updateStoreParams);
      Assert.assertFalse(controllerResponse.isError());
      updateStoreParams = new UpdateStoreQueryParams()
          .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
          .setLeaderFollowerModel(true)
          .setRegionsFilter("dc-0");
      controllerResponse = parentControllerClient.updateStore(incrementPushStoreName, updateStoreParams);
      Assert.assertFalse(controllerResponse.isError());

      final Optional<String> newNativeReplicationSource = Optional.of("new-nr-source");
      try (ControllerClient dc0Client = new ControllerClient(clusterName,
          childDatacenters.get(0).getControllerConnectString()); ControllerClient dc1Client = new ControllerClient(clusterName,
          childDatacenters.get(1).getControllerConnectString()); ControllerClient dc2Client = new ControllerClient(clusterName,
          childDatacenters.get(2).getControllerConnectString())) {

        /**
         * Run admin command to convert all batch-only stores in the cluster to native replication
         */
        controllerResponse = parentControllerClient.configureNativeReplicationForCluster(true,
            VeniceUserStoreType.BATCH_ONLY.toString(), newNativeReplicationSource, Optional.of("parent.parent,dc-0"));
        Assert.assertFalse(controllerResponse.isError());

        /**
         * Batch-only stores should have native replication enabled in dc-0 but not in dc-1 or dc-2; hybrid stores or
         * incremental push stores shouldn't have native replication enabled in any region.
         */
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(parentControllerClient, batchOnlyStoreName, true, newNativeReplicationSource);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, batchOnlyStoreName, true, newNativeReplicationSource);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, batchOnlyStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, batchOnlyStoreName, false);

        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(parentControllerClient, hybridStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, hybridStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, hybridStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, hybridStoreName, false);

        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(parentControllerClient, incrementPushStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, incrementPushStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, incrementPushStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, incrementPushStoreName, false);

        /**
         * Second test:
         * 1. Revert the cluster to previous state
         * 2. Test the cluster level command that converts all hybrid stores to native replication
         */
        controllerResponse = parentControllerClient.configureNativeReplicationForCluster(false,
            VeniceUserStoreType.BATCH_ONLY.toString(), Optional.empty(), Optional.empty());
        Assert.assertFalse(controllerResponse.isError());
        controllerResponse = parentControllerClient.configureNativeReplicationForCluster(true,
            VeniceUserStoreType.HYBRID_ONLY.toString(), newNativeReplicationSource, Optional.of("parent.parent,dc-0"));
        Assert.assertFalse(controllerResponse.isError());

        /**
         * Hybrid stores should have native replication enabled in dc-0 but not in dc-1 or dc-2; batch-only stores or
         * incremental push stores shouldn't have native replication enabled in any region.
         */
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(parentControllerClient, batchOnlyStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, batchOnlyStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, batchOnlyStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, batchOnlyStoreName, false);

        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(parentControllerClient, hybridStoreName, true, newNativeReplicationSource);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, hybridStoreName, true, newNativeReplicationSource);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, hybridStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, hybridStoreName, false);

        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(parentControllerClient, incrementPushStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, incrementPushStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, incrementPushStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, incrementPushStoreName, false);

        /**
         * Third test:
         * 1. Revert the cluster to previous state
         * 2. Test the cluster level command that converts all incremental push stores to native replication
         */
        controllerResponse = parentControllerClient.configureNativeReplicationForCluster(false,
            VeniceUserStoreType.HYBRID_ONLY.toString(), Optional.empty(), Optional.empty());
        Assert.assertFalse(controllerResponse.isError());
        controllerResponse = parentControllerClient.configureNativeReplicationForCluster(true,
            VeniceUserStoreType.INCREMENTAL_PUSH.toString(), newNativeReplicationSource, Optional.of("parent.parent,dc-0"));
        Assert.assertFalse(controllerResponse.isError());

        /**
         * Incremental push stores should have native replication enabled in dc-0 but not in dc-1 or dc-2; batch-only stores or
         * hybrid stores shouldn't have native replication enabled in any region.
         */
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(parentControllerClient, batchOnlyStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, batchOnlyStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, batchOnlyStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, batchOnlyStoreName, false);

        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(parentControllerClient, hybridStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, hybridStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, hybridStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, hybridStoreName, false);

        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(parentControllerClient, incrementPushStoreName, true, newNativeReplicationSource);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, incrementPushStoreName, true, newNativeReplicationSource);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, incrementPushStoreName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2Client, incrementPushStoreName, false);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "storeSize")
  public void testNativeReplicationForHybridStore(int recordCount, int partitionCount) throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, recordCount);
    String keySchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.VALUE_FIELD_PROP)).schema().toString();

    // First push: native replication is not enabled in any fabric.
    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams()
        .setPartitionCount(partitionCount)
        .setHybridRewindSeconds(TEST_TIMEOUT)
        .setHybridOffsetLagThreshold(2L)
        .setHybridDataReplicationPolicy(DataReplicationPolicy.AGGREGATE);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams);

    try (VenicePushJob job = new VenicePushJob("Test push job 1", props)) {
      job.run();
      Assert.assertEquals(job.getKafkaUrl(), parentController.getKafkaBootstrapServers(false));
    }

    SystemProducer veniceProducer = null;
    try {
      // Start Samza processor (aggregated mode)
      Map<String, String> samzaConfig = new HashMap<>();
      String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
      samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, Version.PushType.STREAM.toString());
      samzaConfig.put(configPrefix + VENICE_STORE, storeName);
      samzaConfig.put(configPrefix + VENICE_AGGREGATE, "true");
      samzaConfig.put(D2_ZK_HOSTS_PROPERTY, "invalid_child_zk_address");
      samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, parentController.getKafkaZkAddress());
      samzaConfig.put(DEPLOYMENT_ID, TestUtils.getUniqueString("venice-push-id"));
      samzaConfig.put(SSL_ENABLED, "false");
      VeniceSystemFactory factory = new VeniceSystemFactory();
      veniceProducer = factory.getProducer("venice", new MapConfig(samzaConfig), null);
      veniceProducer.start();
      Assert.assertEquals(((VeniceSystemProducer)veniceProducer).getKafkaBootstrapServers(),
          parentController.getKafkaBootstrapServers(false));
      for (int i = 1; i <= 10; i++) {
        sendStreamingRecord(veniceProducer, storeName, i);
      }

      VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(0);
      String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
      try (AvroGenericStoreClient<String, Object> client =
          ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          // Current version should become 1
          for (int version : parentController.getVeniceAdmin().getCurrentVersionsForMultiColos(clusterName, storeName).values()) {
            Assert.assertEquals(version, 1);
          }
          // Verify the data in dc-0 which consumes local VT and RT
          for (int i = 1; i <= 10; ++i) {
            String expected = "stream_" + i;
            String actual = client.get(Integer.toString(i)).get().toString();
            Assert.assertEquals(actual, expected);
          }
          for (int i = 11; i <= 50; ++i) {
            String expected = "test_name_" + i;
            String actual = client.get(Integer.toString(i)).get().toString();
            Assert.assertEquals(actual, expected);
          }
        });
      }

      // Second push: native replication is enabled in parent and dc-0, but disabled in dc-1 and dc-2.
      // Batch source region is corp-venice-native. Real-time source region is parent.
      UpdateStoreQueryParams enableNativeRepl = new UpdateStoreQueryParams()
          .setLeaderFollowerModel(true)
          .setNativeReplicationEnabled(true);
      UpdateStoreQueryParams disableNativeRepl = new UpdateStoreQueryParams()
          .setLeaderFollowerModel(false)
          .setNativeReplicationEnabled(false);

      try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());
          ControllerClient dc0ControllerClient = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
          ControllerClient dc1ControllerClient = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
          ControllerClient dc2ControllerClient = new ControllerClient(clusterName, childDatacenters.get(2).getControllerConnectString())) {

        TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, enableNativeRepl);
        TestPushUtils.updateStore(clusterName, storeName, dc1ControllerClient, disableNativeRepl);
        TestPushUtils.updateStore(clusterName, storeName, dc2ControllerClient, disableNativeRepl);

        // Verify all the datacenters are configured correctly.
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0ControllerClient, storeName, true);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1ControllerClient, storeName, false);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2ControllerClient, storeName, false);
      }

      try (VenicePushJob job = new VenicePushJob("Test push job 2", props)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), corpVeniceNativeKafka.getAddress());
      }
      for (int i = 11; i <= 20; i++) {
        sendStreamingRecord(veniceProducer, storeName, i);
      }

      try (AvroGenericStoreClient<String, Object> client =
          ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          // Current version should become 2
          for (int version : parentController.getVeniceAdmin().getCurrentVersionsForMultiColos(clusterName, storeName).values())  {
            Assert.assertEquals(version, 2);
          }
          // Verify the data in dc-0 which consumes local VT and remote RT
          for (int i = 1; i <= 20; ++i) {
            String expected = "stream_" + i;
            String actual = client.get(Integer.toString(i)).get().toString();
            Assert.assertEquals(actual, expected);
          }
          for (int i = 21; i <= 50; ++i) {
            String expected = "test_name_" + i;
            String actual = client.get(Integer.toString(i)).get().toString();
            Assert.assertEquals(actual, expected);
          }
        });
      }

      // Third push: native replication is enabled in parent, dc-0 and dc-1, but disabled dc-2.
      // Batch source region is dc-2. Real-time source region is parent.
      UpdateStoreQueryParams changeSourceFabric = new UpdateStoreQueryParams()
          .setNativeReplicationSourceFabric("dc-2");

      try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());
          ControllerClient dc0ControllerClient = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
          ControllerClient dc1ControllerClient = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
          ControllerClient dc2ControllerClient = new ControllerClient(clusterName, childDatacenters.get(2).getControllerConnectString())) {
        TestPushUtils.updateStore(clusterName, storeName, dc1ControllerClient, enableNativeRepl);
        TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, changeSourceFabric);

        // Verify all the datacenter is configured correctly.
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0ControllerClient, storeName, true);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1ControllerClient, storeName, true);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc2ControllerClient, storeName, false);
      }

      try (VenicePushJob job = new VenicePushJob("Test push job 3", props)) {
        job.run();
        Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(2).getKafkaBrokerWrapper().getAddress());
      }
      for (int i = 21; i <= 30; i++) {
        sendStreamingRecord(veniceProducer, storeName, i);
      }

      childDataCenter = childDatacenters.get(1);
      routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
      try (AvroGenericStoreClient<String, Object> client =
          ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          // Current version should become 3
          for (int version : parentController.getVeniceAdmin().getCurrentVersionsForMultiColos(clusterName, storeName).values())  {
            Assert.assertEquals(version, 3);
          }
          // Verify the data in dc-1 which consumes local VT and remote RT
          for (int i = 1; i <= 30; ++i) {
            String expected = "stream_" + i;
            String actual = client.get(Integer.toString(i)).get().toString();
            Assert.assertEquals(actual, expected);
          }
          for (int i = 31; i <= 50; ++i) {
            String expected = "test_name_" + i;
            String actual = client.get(Integer.toString(i)).get().toString();
            Assert.assertEquals(actual, expected);
          }
        });
      }
    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
  }
}
