package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.helix.HelixBaseRoutingRepository;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
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
import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.samza.VeniceSystemFactory.*;
import static com.linkedin.venice.utils.TestPushUtils.*;


public class  TestPushJobWithNativeReplication {
  private static final Logger logger = Logger.getLogger(TestPushJobWithNativeReplication.class);
  private static final int TEST_TIMEOUT = 120_000; // ms

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0", "venice-cluster1", ...];

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  @DataProvider(name = "storeSize")
  public static Object[][] storeSize() {
    return new Object[][]{{50, 2}, {10000, 100}};
  }


  @BeforeClass(alwaysRun = true)
  public void setUp() {
    /**
     * Reduce leader promotion delay to 3 seconds;
     * Create a testing environment with 1 parent fabric and 2 child fabrics;
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
    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
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
        MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);
    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "storeSize")
  public void testNativeReplicationForBatchPush(int recordCount, int partitionCount) throws Exception {
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
    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setPartitionCount(partitionCount)
        .setLeaderFollowerModel(true)
        .setNativeReplicationEnabled(true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

    try (ControllerClient dc0Client = new ControllerClient(clusterName,
        childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client = new ControllerClient(clusterName,
            childDatacenters.get(1).getControllerConnectString())) {

      //verify the update store command has taken effect before starting the push job.
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, true);
    }

    try (KafkaPushJob job = new KafkaPushJob("Test push job", props)) {
      job.run();

      //Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
      Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
    }

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      // Current version should become 1
      for (int version : parentController.getVeniceAdmin().getCurrentVersionsForMultiColos(clusterName, storeName).values())  {
        Assert.assertEquals(version, 1);
      }

      //Verify the data in the second child fabric which consumes remotely
      VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1);
      String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
      try(AvroGenericStoreClient<String, Object> client =
          ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= recordCount; ++i) {
          String expected = "test_name_" + i;
          String actual = client.get(Integer.toString(i)).get().toString();
          Assert.assertEquals(actual, expected);
        }
      }

      /**
       * The offset metadata recorded in storage node must be smaller than or equal to the end offset of the local version topic
       */
      String versionTopic = Version.composeKafkaTopic(storeName, 1);
      VeniceServer serverInRemoteFabric = childDataCenter.getClusters().get(clusterName).getVeniceServers().get(0).getVeniceServer();
      Set<Integer> partitionIds = serverInRemoteFabric.getStorageService().getStorageEngineRepository().getLocalStorageEngine(versionTopic).getPartitionIds();
      Assert.assertFalse(partitionIds.isEmpty());
      int partitionId = partitionIds.iterator().next();
      // Get the end offset of the selected partition from version topic
      long latestOffsetInVersionTopic = childDataCenter.getRandomController().getVeniceAdmin().getTopicManager().getLatestOffset(versionTopic, partitionId);
      // Get the offset metadata of the selected partition from storage node
      StorageMetadataService metadataService = serverInRemoteFabric.getStorageMetadataService();
      OffsetRecord offsetRecord = metadataService.getLastOffset(versionTopic, partitionId);

      Assert.assertTrue(offsetRecord.getOffset() <= latestOffsetInVersionTopic);
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNativeReplicationWithLeadershipHandover() throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    int recordCount = 10000;
    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, recordCount);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    VeniceControllerWrapper parentController =
        parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    String keySchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.VALUE_FIELD_PROP)).schema().toString();

    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setPartitionCount(1)
        .setLeaderFollowerModel(true)
        .setNativeReplicationEnabled(true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

    try (ControllerClient dc0Client = new ControllerClient(clusterName,
        childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client = new ControllerClient(clusterName,
            childDatacenters.get(1).getControllerConnectString())) {

      //verify the update store command has taken effect before starting the push job.
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, true);
    }

    Thread pushJobThread = new Thread(() -> {
      TestPushUtils.runPushJob("Test push job", props);
    });
    pushJobThread.start();

    /**
     * Restart leader SN (server1) to trigger leadership handover during batch consumption.
     * When server1 stops, sever2 will be promoted to leader. When server1 starts, due to full-auto rebalance, server2:
     * 1) Will be demoted to follower. Leader->standby transition during remote consumption will be tested.
     * 2) Or remain as leader. In this case, Leader->standby transition during remote consumption won't be tested.
     * TODO: Use semi-auto rebalance and assign a server as the leader to make sure leader->standby always happen.
     */
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, () -> {
      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1).getClusters().get(clusterName);
      String topic = Version.composeKafkaTopic(storeName, 1);
      HelixBaseRoutingRepository routingDataRepo = veniceClusterWrapper.getRandomVeniceRouter().getRoutingDataRepository();
      Assert.assertTrue(routingDataRepo.containsKafkaTopic(topic));

      Instance leaderNode = routingDataRepo.getLeaderInstance(topic, 0);
      Assert.assertNotNull(leaderNode);
      logger.info("Restart server port " + leaderNode.getPort());
      veniceClusterWrapper.stopAndRestartVeniceServer(leaderNode.getPort());
    });

    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      // Current version should become 1
      for (int version : parentController.getVeniceAdmin().getCurrentVersionsForMultiColos(clusterName, storeName).values())  {
        Assert.assertEquals(version, 1);
      }

      // Verify the data in the second child fabric which consumes remotely
      VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1);
      String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
      try (AvroGenericStoreClient<String, Object> client =
          ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
        for (int i = 1; i <= recordCount; ++i) {
          String expected = "test_name_" + i;
          String actual = client.get(Integer.toString(i)).get().toString();
          Assert.assertEquals(actual, expected);
        }
      }
    });
    pushJobThread.join();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNativeReplicationForHybrid() throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    VeniceControllerWrapper parentController =
            parentControllers.stream().filter(c -> c.isMasterController(clusterName)).findAny().get();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, 10);
    String keySchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.VALUE_FIELD_PROP)).schema().toString();

    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams()
            .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setLeaderFollowerModel(true)
            .setNativeReplicationEnabled(true)
            .setHybridRewindSeconds(10)
            .setHybridOffsetLagThreshold(10);

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

    try (ControllerClient dc0Client = new ControllerClient(clusterName,
        childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client = new ControllerClient(clusterName,
            childDatacenters.get(1).getControllerConnectString())) {

      //verify the update store command has taken effect before starting the push job.
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, true);
    }
    // Write batch data
    TestPushUtils.runPushJob("Test push job", props);

    //Verify version level hybrid config is set correctly. The current version should be 1.
    VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1);
    Optional<Version> version = childDataCenter.getRandomController().getVeniceAdmin().getStore(clusterName, storeName).getVersion(1);
    HybridStoreConfig hybridConfig = version.get().getHybridStoreConfig();
    Assert.assertNotNull(hybridConfig);
    Assert.assertEquals(hybridConfig.getRewindTimeInSeconds(), 10);
    Assert.assertEquals(hybridConfig.getOffsetLagThresholdToGoOnline(), 10);


    // Write Samza data (aggregated mode)
    SystemProducer veniceProducer = null;
    try {
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

      //Verify the kafka URL being returned to Samza is the same as dc-0 kafka url.
      Assert.assertEquals(((VeniceSystemProducer)veniceProducer).getKafkaBootstrapServers(),
              childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
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
    String keySchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.VALUE_FIELD_PROP)).schema().toString();

    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setLeaderFollowerModel(true)
        .setNativeReplicationEnabled(true)
        .setIncrementalPushEnabled(true)
        .setIncrementalPushPolicy(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC);

    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

    try (ControllerClient dc0Client = new ControllerClient(clusterName,
        childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client = new ControllerClient(clusterName,
            childDatacenters.get(1).getControllerConnectString())) {

      //verify the update store command has taken effect before starting the push job.
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
      TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, true);
    }

    // Write batch data
    TestPushUtils.runPushJob("Test push job", props);

    //Run incremental push job
    props.setProperty(INCREMENTAL_PUSH, "true");
    TestPushUtils.writeSimpleAvroFileWithUserSchema2(inputDir);
    TestPushUtils.runPushJob("Test incremental push job", props);

    //Verify following in child controller
    VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1);

    //Verify version level incremental push config is set correctly. The current version should be 1.
    Optional<Version> version = childDataCenter.getRandomController().getVeniceAdmin().getStore(clusterName, storeName).getVersion(1);
    Assert.assertTrue(version.get().isIncrementalPushEnabled());

    //Verify the data in the second child fabric which consumes remotely
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

}
