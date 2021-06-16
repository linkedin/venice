package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.helix.HelixBaseRoutingRepository;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.DataReplicationPolicy;
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
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
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
import static com.linkedin.venice.meta.IngestionMode.*;
import static com.linkedin.venice.meta.PersistenceType.*;
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
    return new Object[][]{{50, 2}, {10000, 20}};
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
    serverProperties.put(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, "true");
    serverProperties.put(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, "1");

    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1000);
    controllerProps.put(AGGREGATE_REAL_TIME_SOURCE_REGION, "dc-parent-0");
    controllerProps.put(NATIVE_REPLICATION_FABRIC_WHITELIST, "dc-parent-0");
    int parentKafkaPort = Utils.getFreePort();
    controllerProps.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + ".dc-parent-0", "localhost:" + parentKafkaPort);
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
        MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST,
        false,
        Optional.of(parentKafkaPort));
    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "storeSize")
  public void testNativeReplicationForBatchPush(int recordCount, int partitionCount) throws Exception {
    motherOfAllTests(
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(partitionCount),
        recordCount,
        (parentController, clusterName, storeName, props, inputDir) -> {
          try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
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
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNativeReplicationWithLeadershipHandover() throws Exception {
    int recordCount = 10000;
    motherOfAllTests(
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1),
        recordCount,
        (parentController, clusterName, storeName, props, inputDir) -> {
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
            VeniceClusterWrapper veniceClusterWrapper =
                childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1).getClusters().get(clusterName);
            String topic = Version.composeKafkaTopic(storeName, 1);
            HelixBaseRoutingRepository routingDataRepo =
                veniceClusterWrapper.getRandomVeniceRouter().getRoutingDataRepository();
            Assert.assertTrue(routingDataRepo.containsKafkaTopic(topic));

            Instance leaderNode = routingDataRepo.getLeaderInstance(topic, 0);
            Assert.assertNotNull(leaderNode);
            logger.info("Restart server port " + leaderNode.getPort());
            veniceClusterWrapper.stopAndRestartVeniceServer(leaderNode.getPort());
          });

          TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
            for (int version : parentController.getVeniceAdmin()
                .getCurrentVersionsForMultiColos(clusterName, storeName)
                .values()) {
              Assert.assertEquals(version, 1, "Current version should become 1!");
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
          pushJobThread.join();
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNativeReplicationWithDaVinciAndIngestionIsolation() throws Exception {
    int recordCount = 100;
    motherOfAllTests(
        updateStoreParams -> updateStoreParams
        .setPartitionCount(2),
        recordCount,
        (parentController, clusterName, storeName, props, inputDir) -> {
          try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
            job.run();

            //Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
          }

          //Test Da-vinci client is able to consume from NR colo which is consuming remotely
          VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(1);
          String baseDataPath = TestUtils.getTempDataDirectory().getAbsolutePath();
          int applicationListenerPort = Utils.getFreePort();
          int servicePort = Utils.getFreePort();
          VeniceProperties backendConfig = new PropertyBuilder().put(DATA_BASE_PATH, baseDataPath)
              .put(PERSISTENCE_TYPE, ROCKS_DB)
              .put(SERVER_INGESTION_MODE, ISOLATED)
              .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
              .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
              .put(D2_CLIENT_ZK_HOSTS_ADDRESS, childDataCenter.getClusters().get(clusterName).getZk().getAddress())
              .build();

          D2Client d2Client = new D2ClientBuilder().setZkHosts(childDataCenter.getClusters().get(clusterName).getZk().getAddress())
              .setZkSessionTimeout(3, TimeUnit.SECONDS)
              .setZkStartupTimeout(3, TimeUnit.SECONDS)
              .build();
          D2ClientUtils.startClient(d2Client);
          MetricsRepository metricsRepository = new MetricsRepository();

          try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository,
              backendConfig)) {
            DaVinciClient<String, Object> client = factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig());
            client.subscribeAll().get();
            for (int i = 1; i <= recordCount; ++i) {
              String expected = "test_name_" + i;
              String actual = client.get(Integer.toString(i)).get().toString();
              Assert.assertEquals(actual, expected);
            }
          } finally {
            D2ClientUtils.shutdownClient(d2Client);
          }
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNativeReplicationForHybrid() throws Exception {
    motherOfAllTests(
        updateStoreQueryParams -> updateStoreQueryParams
            .setPartitionCount(2)
            .setHybridRewindSeconds(TEST_TIMEOUT)
            .setHybridOffsetLagThreshold(2)
            .setHybridDataReplicationPolicy(DataReplicationPolicy.AGGREGATE),
        50,
        (parentController, clusterName, storeName, props, inputDir) -> {
          // Write batch data
          TestPushUtils.runPushJob("Test push job", props);

          //Verify version level hybrid config is set correctly. The current version should be 1.
          VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1);
          Optional<Version> version =
              childDataCenter.getRandomController().getVeniceAdmin().getStore(clusterName, storeName).getVersion(1);
          HybridStoreConfig hybridConfig = version.get().getHybridStoreConfig();
          Assert.assertNotNull(hybridConfig);
          Assert.assertEquals(hybridConfig.getRewindTimeInSeconds(), TEST_TIMEOUT);
          Assert.assertEquals(hybridConfig.getOffsetLagThresholdToGoOnline(), 2);
          Assert.assertEquals(hybridConfig.getDataReplicationPolicy(), DataReplicationPolicy.AGGREGATE);

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

            //Verify the kafka URL being returned to Samza is the same as parent colo kafka url.
            Assert.assertEquals(((VeniceSystemProducer) veniceProducer).getKafkaBootstrapServers(),
                parentController.getKafkaBootstrapServers(false));

            for (int i = 1; i <= 10; i++) {
              sendStreamingRecord(veniceProducer, storeName, i);
            }

            String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
            try (AvroGenericStoreClient<String, Object> client =
                ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
              TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
                // Current version should become 1
                for (int versionNum : parentController.getVeniceAdmin().getCurrentVersionsForMultiColos(clusterName, storeName).values())  {
                  Assert.assertEquals(versionNum, 1);
                }

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
          } finally {
            if (veniceProducer != null) {
              veniceProducer.stop();
            }
          }
        });
  }

  @Test(timeOut = TEST_TIMEOUT, enabled=false)
  public void testNativeReplicationForIncrementalPush() throws Exception {
    motherOfAllTests(
        updateStoreQueryParams -> updateStoreQueryParams
            .setPartitionCount(2)
            .setIncrementalPushEnabled(true)
            .setIncrementalPushPolicy(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC),
        100,
        (parentController, clusterName, storeName, props, inputDir) -> {
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
          for (VeniceMultiClusterWrapper childDataCenter : childDatacenters) {
            //Verify version level incremental push config is set correctly. The current version should be 1.
            Optional<Version> version =
                childDataCenter.getRandomController().getVeniceAdmin().getStore(clusterName, storeName).getVersion(1);
            Assert.assertTrue(version.get().isIncrementalPushEnabled());
          }

          //Verify following in child controller
          VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1);

          //Verify the data in the second child fabric which consumes remotely
          String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
          try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
              ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
            for (int i = 1; i <= 150; ++i) {
              String expected = i <= 50 ? "test_name_" + i : "test_name_" + (i * 2);
              String actual = client.get(Integer.toString(i)).get().toString();
              Assert.assertEquals(actual, expected);
            }
          }
        });
  }

  private interface NativeReplTest {
    void run(VeniceControllerWrapper parentController, String clusterName, String storeName, Properties props, File inputDir) throws Exception;
  }

  private void motherOfAllTests(
      Function<UpdateStoreQueryParams, UpdateStoreQueryParams> updateStoreParamsTransformer,
      int recordCount,
      NativeReplTest test) throws Exception {
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

    UpdateStoreQueryParams updateStoreParams = updateStoreParamsTransformer.apply(
        new UpdateStoreQueryParams()
            .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setLeaderFollowerModel(true)
            .setNativeReplicationEnabled(true));

    ControllerClient parentControllerClient = null;
    try {
      parentControllerClient = createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams);

      try (ControllerClient dc0Client = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
          ControllerClient dc1Client = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {

        //verify the update store command has taken effect before starting the push job.
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc0Client, storeName, true);
        TestPushJobWithNativeReplicationAndKMM.verifyDCConfigNativeRepl(dc1Client, storeName, true);
      }

      test.run(parentController, clusterName, storeName, props, inputDir);
    } finally {
      FileUtils.deleteDirectory(inputDir);
      if (null != parentControllerClient) {
        ControllerResponse deleteStoreResponse = parentControllerClient.disableAndDeleteStore(storeName);
        IOUtils.closeQuietly(parentControllerClient);
        Assert.assertFalse(deleteStoreResponse.isError(), "Failed to delete the test store: " + deleteStoreResponse.getError());
      }
    }
  }
}
