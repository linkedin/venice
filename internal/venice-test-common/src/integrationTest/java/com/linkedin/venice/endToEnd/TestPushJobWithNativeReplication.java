package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.EMERGENCY_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_KAFKA_PRODUCER_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.INCREMENTAL_PUSH;
import static com.linkedin.venice.hadoop.VenicePushJob.INPUT_PATH_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.hadoop.VenicePushJob.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.hadoop.VenicePushJob.SOURCE_KAFKA;
import static com.linkedin.venice.hadoop.VenicePushJob.TARGETED_REGION_PUSH_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJob.TARGETED_REGION_PUSH_LIST;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.PARENT_D2_SERVICE_NAME;
import static com.linkedin.venice.samza.VeniceSystemFactory.DEPLOYMENT_ID;
import static com.linkedin.venice.samza.VeniceSystemFactory.DOT;
import static com.linkedin.venice.samza.VeniceSystemFactory.SYSTEMS_PREFIX;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_AGGREGATE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_CHILD_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_CHILD_D2_ZK_HOSTS;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_CONTROLLER_D2_SERVICE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_D2_ZK_HOSTS;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PUSH_TYPE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_STORE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithUserSchema;
import static org.testng.Assert.assertFalse;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.ExceptionType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.status.BatchJobHeartbeatConfigs;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.config.MapConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestPushJobWithNativeReplication {
  private static final Logger LOGGER = LogManager.getLogger(TestPushJobWithNativeReplication.class);
  private static final int TEST_TIMEOUT = 2 * Time.MS_PER_MINUTE;

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0",
                                                                                                         // "venice-cluster1",
                                                                                                         // ...];
  private static final String DEFAULT_NATIVE_REPLICATION_SOURCE = "dc-0";

  private static final String VPJ_HEARTBEAT_STORE_CLUSTER = CLUSTER_NAMES[0]; // "venice-cluster0"
  private static final String VPJ_HEARTBEAT_STORE_NAME =
      AvroProtocolDefinition.BATCH_JOB_HEARTBEAT.getSystemStoreName();

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @DataProvider(name = "storeSize")
  public static Object[][] storeSize() {
    return new Object[][] { { 50, 2 }, { 1000, 10 } };
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    /**
     * Reduce leader promotion delay to 3 seconds;
     * Create a testing environment with 1 parent fabric and 2 child fabrics;
     * Set server and replication factor to 2 to ensure at least 1 leader replica and 1 follower replica;
     */
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, "true");
    serverProperties.put(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, "1");

    Properties controllerProps = new Properties();
    // This property is required for test stores that have 10 partitions
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 10);
    controllerProps
        .put(BatchJobHeartbeatConfigs.HEARTBEAT_STORE_CLUSTER_CONFIG.getConfigName(), VPJ_HEARTBEAT_STORE_CLUSTER);
    controllerProps.put(BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG.getConfigName(), true);
    controllerProps.put(EMERGENCY_SOURCE_REGION, "dc-0");

    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
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
        false);
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "storeSize")
  public void testNativeReplicationForBatchPush(int recordCount, int partitionCount) throws Exception {
    motherOfAllTests(
        "testNativeReplicationForBatchPush",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(partitionCount).setAmplificationFactor(2),
        recordCount,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
            job.run();

            // Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
          }

          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
            // Current version should become 1
            for (int version: parentControllerClient.getStore(storeName)
                .getStore()
                .getColoToCurrentVersions()
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

            /**
             * The offset metadata recorded in storage node must be smaller than or equal to the end offset of the local version topic
             */
            String versionTopic = Version.composeKafkaTopic(storeName, 1);
            VeniceServer serverInRemoteFabric =
                childDataCenter.getClusters().get(clusterName).getVeniceServers().get(0).getVeniceServer();
            Set<Integer> partitionIds = serverInRemoteFabric.getStorageService()
                .getStorageEngineRepository()
                .getLocalStorageEngine(versionTopic)
                .getPartitionIds();
            Assert.assertFalse(partitionIds.isEmpty());
            int partitionId = partitionIds.iterator().next();
            // Get the end offset of the selected partition from version topic
            PubSubTopicPartition versionTopicPartition =
                new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(versionTopic), partitionId);
            long latestOffsetInVersionTopic = childDataCenter.getRandomController()
                .getVeniceAdmin()
                .getTopicManager()
                .getPartitionLatestOffsetAndRetry(versionTopicPartition, 5);
            // Get the offset metadata of the selected partition from storage node
            StorageMetadataService metadataService = serverInRemoteFabric.getStorageMetadataService();
            OffsetRecord offsetRecord = metadataService.getLastOffset(versionTopic, partitionId);

            Assert.assertTrue(offsetRecord.getLocalVersionTopicOffset() <= latestOffsetInVersionTopic);
          });
        });
  }

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testNativeReplicationWithLeadershipHandover() throws Exception {
    int recordCount = 10000;
    motherOfAllTests(
        "testNativeReplicationWithLeadershipHandover",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1).setAmplificationFactor(2),
        recordCount,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          Thread pushJobThread = new Thread(() -> TestWriteUtils.runPushJob("Test push job", props));
          pushJobThread.start();
          try {
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
              // Get Leadership information from External View repo in controller.
              HelixExternalViewRepository routingDataRepo = veniceClusterWrapper.getLeaderVeniceController()
                  .getVeniceHelixAdmin()
                  .getHelixVeniceClusterResources(clusterName)
                  .getRoutingDataRepository();
              Assert.assertTrue(routingDataRepo.containsKafkaTopic(topic));

              Instance leaderNode = routingDataRepo.getLeaderInstance(topic, 0);
              Assert.assertNotNull(leaderNode);
              LOGGER.info("Restart server port {}", leaderNode.getPort());
              veniceClusterWrapper.stopAndRestartVeniceServer(leaderNode.getPort());
            });

            TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, true, () -> {
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
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
          } finally {
            TestUtils.shutdownThread(pushJobThread);
          }
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNativeReplicationWithIngestionIsolationInDaVinci() throws Exception {
    int recordCount = 100;
    motherOfAllTests(
        "testNativeReplicationWithIngestionIsolationInDaVinci",
        updateStoreParams -> updateStoreParams.setPartitionCount(2).setAmplificationFactor(2),
        recordCount,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
            job.run();
            // Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
          }

          // Test Da-vinci client is able to consume from NR region which is consuming remotely
          VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(1);

          try (DaVinciClient<String, Object> daVinciClient = ServiceFactory.getGenericAvroDaVinciClientWithRetries(
              storeName,
              childDataCenter.getClusters().get(clusterName).getZk().getAddress(),
              new DaVinciConfig(),
              TestUtils.getIngestionIsolationPropertyMap())) {
            daVinciClient.subscribeAll().get();
            for (int i = 1; i <= recordCount; ++i) {
              String expected = "test_name_" + i;
              String actual = daVinciClient.get(Integer.toString(i)).get().toString();
              Assert.assertEquals(actual, expected);
            }
          }
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNativeReplicationForHybrid() throws Exception {
    motherOfAllTests(
        "testNativeReplicationForHybrid",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1)
            .setAmplificationFactor(2)
            .setHybridRewindSeconds(TEST_TIMEOUT)
            .setHybridOffsetLagThreshold(2)
            .setHybridDataReplicationPolicy(DataReplicationPolicy.AGGREGATE),
        50,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          // Write batch data
          TestWriteUtils.runPushJob("Test push job", props);

          // Verify version level hybrid config is set correctly. The current version should be 1.
          VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1);
          Optional<Version> version =
              childDataCenter.getRandomController().getVeniceAdmin().getStore(clusterName, storeName).getVersion(1);
          HybridStoreConfig hybridConfig = version.get().getHybridStoreConfig();
          Assert.assertNotNull(hybridConfig);
          Assert.assertEquals(hybridConfig.getRewindTimeInSeconds(), TEST_TIMEOUT);
          Assert.assertEquals(hybridConfig.getOffsetLagThresholdToGoOnline(), 2);
          Assert.assertEquals(hybridConfig.getDataReplicationPolicy(), DataReplicationPolicy.AGGREGATE);

          // Write Samza data (aggregated mode)
          Map<String, String> samzaConfig = new HashMap<>();
          String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
          samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, Version.PushType.STREAM.toString());
          samzaConfig.put(configPrefix + VENICE_STORE, storeName);
          samzaConfig.put(configPrefix + VENICE_AGGREGATE, "true");
          samzaConfig.put(VENICE_CHILD_D2_ZK_HOSTS, childDatacenters.get(0).getZkServerWrapper().getAddress());
          samzaConfig.put(VENICE_CHILD_CONTROLLER_D2_SERVICE, D2_SERVICE_NAME);
          samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, multiRegionMultiClusterWrapper.getZkServerWrapper().getAddress());
          samzaConfig.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, PARENT_D2_SERVICE_NAME);
          samzaConfig.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id"));
          samzaConfig.put(SSL_ENABLED, "false");
          VeniceSystemFactory factory = new VeniceSystemFactory();
          try (VeniceSystemProducer veniceProducer =
              factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
            veniceProducer.start();

            // Verify the kafka URL being returned to Samza is the same as parent region kafka url.
            Assert.assertEquals(
                veniceProducer.getKafkaBootstrapServers(),
                multiRegionMultiClusterWrapper.getParentKafkaBrokerWrapper().getAddress());

            for (int i = 1; i <= 10; i++) {
              sendStreamingRecord(veniceProducer, storeName, i);
            }

            String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
            try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
                ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
              TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
                // Current version should become 1
                for (int versionNum: parentControllerClient.getStore(storeName)
                    .getStore()
                    .getColoToCurrentVersions()
                    .values()) {
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
          }
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNativeReplicationForIncrementalPush() throws Exception {
    File inputDirInc = getTempDataDirectory();

    motherOfAllTests(
        "testNativeReplicationForIncrementalPush",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1)
            .setHybridOffsetLagThreshold(TEST_TIMEOUT)
            .setHybridRewindSeconds(2L)
            .setIncrementalPushEnabled(true),
        100,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          try (VenicePushJob job = new VenicePushJob("Batch Push", props)) {
            job.run();
            // Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
          }

          props.setProperty(INCREMENTAL_PUSH, "true");
          props.put(INPUT_PATH_PROP, inputDirInc);
          props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

          TestWriteUtils.writeSimpleAvroFileWithUserSchema2(inputDirInc);
          try (VenicePushJob job = new VenicePushJob("Incremental Push", props)) {
            job.run();
          }
          NativeReplicationTestUtils.verifyIncrementalPushData(childDatacenters, clusterName, storeName, 150, 2);
        });
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "storeSize")
  public void testActiveActiveForHeartbeatSystemStores(int recordCount, int partitionCount) throws Exception {
    motherOfAllTests(
        "testActiveActiveForHeartbeatSystemStores",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(partitionCount)
            .setIncrementalPushEnabled(true),
        recordCount,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          // Enable VPJ to send liveness heartbeat.
          props.put(BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG.getConfigName(), true);
          // Prevent heartbeat from being deleted when the VPJ run finishes.
          props.put(BatchJobHeartbeatConfigs.HEARTBEAT_LAST_HEARTBEAT_IS_DELETE_CONFIG.getConfigName(), false);

          try (
              ControllerClient dc0Client =
                  new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
              ControllerClient dc1Client =
                  new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {
            // verify the update store command has taken effect before starting the push job.
            NativeReplicationTestUtils
                .verifyDCConfigNativeRepl(Arrays.asList(dc0Client, dc1Client), VPJ_HEARTBEAT_STORE_NAME, true);
          }

          try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
            job.run();

            // Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
          }

          TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
            // Current version should become 1
            for (int version: parentControllerClient.getStore(storeName)
                .getStore()
                .getColoToCurrentVersions()
                .values()) {
              Assert.assertEquals(version, 1);
            }

            // Verify that the data are in all child fabrics including the first child fabric which consumes remotely.
            for (VeniceMultiClusterWrapper childDataCenter: childDatacenters) {
              String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();

              // Verify that user store data can be read in all fabrics.
              try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
                  ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
                for (int i = 1; i <= recordCount; ++i) {
                  String expected = "test_name_" + i;
                  String actual = client.get(Integer.toString(i)).get().toString();
                  Assert.assertEquals(actual, expected);
                }
              }

              // Try to read the latest heartbeat value generated from the user store VPJ push in this
              // fabric/datacenter.
              try (AvroGenericStoreClient<BatchJobHeartbeatKey, BatchJobHeartbeatValue> client =
                  ClientFactory.getAndStartGenericAvroClient(
                      ClientConfig.defaultGenericClientConfig(VPJ_HEARTBEAT_STORE_NAME).setVeniceURL(routerUrl))) {

                final BatchJobHeartbeatKey key = new BatchJobHeartbeatKey();
                key.storeName = storeName;
                key.storeVersion = 1; // User store should be on version one.
                GenericRecord heartbeatValue = client.get(key).get();
                Assert.assertNotNull(heartbeatValue);
                Assert.assertEquals(heartbeatValue.getSchema(), BatchJobHeartbeatValue.getClassSchema());
              }
            }
          });
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testClusterLevelAdminCommandForNativeReplication() throws Exception {
    motherOfAllTests(
        "testClusterLevelAdminCommandForNativeReplication",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1),
        10,
        (parentControllerClient, clusterName, batchOnlyStoreName, props, inputDir) -> {
          // Create a hybrid store
          String hybridStoreName = Utils.getUniqueString("hybrid-store");
          NewStoreResponse newStoreResponse =
              parentControllerClient.createNewStore(hybridStoreName, "", STRING_SCHEMA, STRING_SCHEMA);
          Assert.assertFalse(newStoreResponse.isError());
          UpdateStoreQueryParams updateStoreParams =
              new UpdateStoreQueryParams().setHybridRewindSeconds(10).setHybridOffsetLagThreshold(2);
          assertCommand(parentControllerClient.updateStore(hybridStoreName, updateStoreParams));

          /**
           * Create an incremental push enabled store
           */
          String incrementPushStoreName = Utils.getUniqueString("incremental-push-store");
          newStoreResponse =
              parentControllerClient.createNewStore(incrementPushStoreName, "", STRING_SCHEMA, STRING_SCHEMA);
          Assert.assertFalse(newStoreResponse.isError());
          updateStoreParams = new UpdateStoreQueryParams().setIncrementalPushEnabled(true);
          assertCommand(parentControllerClient.updateStore(incrementPushStoreName, updateStoreParams));

          final Optional<String> defaultNativeReplicationSource = Optional.of(DEFAULT_NATIVE_REPLICATION_SOURCE);
          final Optional<String> newNativeReplicationSource = Optional.of("new-nr-source");
          /**
           * Run admin command to disable native replication for all batch-only stores in the cluster
           */
          assertCommand(
              parentControllerClient.configureNativeReplicationForCluster(
                  false,
                  VeniceUserStoreType.BATCH_ONLY.toString(),
                  Optional.empty(),
                  Optional.empty()));

          childDatacenters.get(0)
              .getClusters()
              .get(clusterName)
              .useControllerClient(
                  dc0Client -> childDatacenters.get(1).getClusters().get(clusterName).useControllerClient(dc1Client -> {
                    List<ControllerClient> allControllerClients =
                        Arrays.asList(parentControllerClient, dc0Client, dc1Client);

                    /**
                     * Batch-only stores should have native replication enabled; hybrid stores or incremental push stores
                     * have native replication enabled with dc-0 as source.
                     */
                    NativeReplicationTestUtils
                        .verifyDCConfigNativeRepl(allControllerClients, batchOnlyStoreName, false);
                    NativeReplicationTestUtils.verifyDCConfigNativeRepl(
                        allControllerClients,
                        hybridStoreName,
                        true,
                        defaultNativeReplicationSource);
                    NativeReplicationTestUtils.verifyDCConfigNativeRepl(
                        allControllerClients,
                        incrementPushStoreName,
                        true,
                        defaultNativeReplicationSource);

                    /**
                     * Second test:
                     * 1. Revert the cluster to previous state
                     * 2. Test the cluster level command that converts all hybrid stores to native replication
                     */
                    assertCommand(
                        parentControllerClient.configureNativeReplicationForCluster(
                            true,
                            VeniceUserStoreType.BATCH_ONLY.toString(),
                            newNativeReplicationSource,
                            Optional.empty()));
                    assertCommand(
                        parentControllerClient.configureNativeReplicationForCluster(
                            false,
                            VeniceUserStoreType.HYBRID_ONLY.toString(),
                            Optional.empty(),
                            Optional.empty()));

                    /**
                     * Hybrid stores shouldn't have native replication enabled; batch-only stores should have native replication
                     * enabled with the new source fabric and incremental push stores should have native replication enabled
                     * with original source fabric.
                     */
                    NativeReplicationTestUtils.verifyDCConfigNativeRepl(
                        allControllerClients,
                        batchOnlyStoreName,
                        true,
                        newNativeReplicationSource);
                    NativeReplicationTestUtils.verifyDCConfigNativeRepl(allControllerClients, hybridStoreName, false);
                    NativeReplicationTestUtils.verifyDCConfigNativeRepl(
                        allControllerClients,
                        incrementPushStoreName,
                        true,
                        defaultNativeReplicationSource);

                    /**
                     * Third test:
                     * 1. Revert the cluster to previous state
                     * 2. Test the cluster level command that disables native replication for all incremental push stores
                     */
                    assertCommand(
                        parentControllerClient.configureNativeReplicationForCluster(
                            true,
                            VeniceUserStoreType.HYBRID_ONLY.toString(),
                            newNativeReplicationSource,
                            Optional.empty()));
                    assertCommand(
                        parentControllerClient.configureNativeReplicationForCluster(
                            false,
                            VeniceUserStoreType.INCREMENTAL_PUSH.toString(),
                            Optional.empty(),
                            Optional.empty()));

                    /**
                     * Incremental push stores shouldn't have native replication enabled; batch-only stores and hybrid stores
                     * should have native replication enabled with the new source fabric.
                     */
                    NativeReplicationTestUtils.verifyDCConfigNativeRepl(
                        allControllerClients,
                        batchOnlyStoreName,
                        true,
                        newNativeReplicationSource);
                    NativeReplicationTestUtils.verifyDCConfigNativeRepl(
                        allControllerClients,
                        hybridStoreName,
                        true,
                        newNativeReplicationSource);
                    NativeReplicationTestUtils
                        .verifyDCConfigNativeRepl(allControllerClients, incrementPushStoreName, false);

                    /**
                     * Fourth test:
                     * Test the cluster level command that enables native replication for all incremental push stores
                     */
                    assertCommand(
                        parentControllerClient.configureNativeReplicationForCluster(
                            true,
                            VeniceUserStoreType.INCREMENTAL_PUSH.toString(),
                            newNativeReplicationSource,
                            Optional.empty()));

                    /**
                     * All stores should have native replication enabled with the new source fabric
                     */
                    NativeReplicationTestUtils.verifyDCConfigNativeRepl(
                        allControllerClients,
                        batchOnlyStoreName,
                        true,
                        newNativeReplicationSource);
                    NativeReplicationTestUtils.verifyDCConfigNativeRepl(
                        allControllerClients,
                        hybridStoreName,
                        true,
                        newNativeReplicationSource);
                    NativeReplicationTestUtils.verifyDCConfigNativeRepl(
                        allControllerClients,
                        incrementPushStoreName,
                        true,
                        newNativeReplicationSource);
                  }));
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testMultiDataCenterRePushWithIncrementalPush() throws Exception {
    motherOfAllTests(
        "testMultiDataCenterRePushWithIncrementalPush",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1),
        100,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
            job.run();

            // Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
          }
          VeniceWriter<String, String, byte[]> incPushToRTWriter = null;
          try {
            assertFalse(
                parentControllerClient
                    .updateStore(
                        storeName,
                        new UpdateStoreQueryParams().setIncrementalPushEnabled(true)
                            .setHybridOffsetLagThreshold(1)
                            .setHybridRewindSeconds(Time.SECONDS_PER_DAY))
                    .isError());

            // Update the store to L/F hybrid and enable INCREMENTAL_PUSH_SAME_AS_REAL_TIME.
            props.setProperty(SOURCE_KAFKA, "true");
            props.setProperty(
                KAFKA_INPUT_BROKER_URL,
                multiRegionMultiClusterWrapper.getParentKafkaBrokerWrapper().getAddress());
            props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
            props.setProperty(VeniceWriter.ENABLE_CHUNKING, "false");
            props.setProperty(KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(storeName, 1));

            try (VenicePushJob rePushJob = new VenicePushJob("Test re-push job re-push", props)) {
              rePushJob.run();
            }
            String incPushToRTVersion = System.currentTimeMillis() + "_test_inc_push_to_rt";
            VeniceControllerWrapper parentController =
                parentControllers.stream().filter(c -> c.isLeaderController(clusterName)).findAny().get();
            incPushToRTWriter = startIncrementalPush(
                parentControllerClient,
                storeName,
                parentController.getVeniceAdmin().getVeniceWriterFactory(),
                incPushToRTVersion);
            final String newVersionTopic = Version.composeKafkaTopic(
                storeName,
                parentControllerClient.getStore(storeName).getStore().getLargestUsedVersionNumber());
            // Incremental push shouldn't be blocked and we will complete it once the new re-push is started.
            String incValuePrefix = "inc_test_";
            int newRePushVersion = Version.parseVersionFromKafkaTopicName(newVersionTopic) + 1;
            VeniceWriter<String, String, byte[]> finalIncPushToRTWriter = incPushToRTWriter;
            CompletableFuture.runAsync(() -> {
              TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
                Assert.assertEquals(
                    parentControllerClient.getStore(storeName).getStore().getLargestUsedVersionNumber(),
                    newRePushVersion);
              });
              for (int i = 1; i <= 10; i++) {
                finalIncPushToRTWriter.put(Integer.toString(i), incValuePrefix + i, 1);
              }
              finalIncPushToRTWriter.broadcastEndOfIncrementalPush(incPushToRTVersion, new HashMap<>());
            });
            // The re-push should complete and contain all the incremental push to RT data.
            props.setProperty(KAFKA_INPUT_TOPIC, newVersionTopic);
            try (VenicePushJob rePushJob = new VenicePushJob("Test re-push job re-push", props)) {
              rePushJob.run();
            }
            // Rewind should be overwritten.
            Optional<Version> latestVersion =
                parentControllerClient.getStore(storeName).getStore().getVersion(newRePushVersion);
            Assert.assertTrue(latestVersion.isPresent());
            Assert.assertEquals(
                latestVersion.get().getHybridStoreConfig().getRewindTimeInSeconds(),
                VenicePushJob.DEFAULT_RE_PUSH_REWIND_IN_SECONDS_OVERRIDE);
            for (int dataCenterIndex = 0; dataCenterIndex < NUMBER_OF_CHILD_DATACENTERS; dataCenterIndex++) {
              String routerUrl =
                  childDatacenters.get(dataCenterIndex).getClusters().get(clusterName).getRandomRouterURL();
              verifyVeniceStoreData(storeName, routerUrl, incValuePrefix, 10);
            }
          } finally {
            if (incPushToRTWriter != null) {
              incPushToRTWriter.close();
            }
          }
        });
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testEmptyPush(boolean toParent) {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("testEmptyPush");
    String parentControllerUrl = parentControllers.get(0).getControllerUrl();
    String childControllerUrl = childDatacenters.get(0).getControllerConnectString();

    // Create store first
    try (ControllerClient controllerClientToParent = new ControllerClient(clusterName, parentControllerUrl)) {
      assertCommand(controllerClientToParent.createNewStore(storeName, "test_owner", "\"int\"", "\"int\""));
      assertCommand(
          controllerClientToParent.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(1000)));
    }

    try (ControllerClient controllerClient =
        new ControllerClient(clusterName, toParent ? parentControllerUrl : childControllerUrl)) {
      VersionCreationResponse response = controllerClient.emptyPush(storeName, "test_push_id", 1000);
      if (toParent) {
        assertFalse(response.isError(), "Empty push to parent colo should succeed");
      } else {
        Assert.assertTrue(response.isError(), "Empty push to child colo should be blocked");
      }
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Failed to create new store version.*", timeOut = TEST_TIMEOUT)
  public void testPushDirectlyToChildRegion() throws IOException {
    // In multi-region setup, the batch push to child controller should be disabled.
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testPushDirectlyToChildColo");
    Properties props = IntegrationTestPushUtils.defaultVPJProps(childDatacenters.get(0), inputDirPath, storeName);
    createStoreForJob(clusterName, recordSchema, props).close();

    TestWriteUtils.runPushJob("Test push job", props);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testControllerBlocksConcurrentBatchPush() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("testControllerBlocksConcurrentBatchPush");
    String pushId1 = Utils.getUniqueString(storeName + "_push");
    String pushId2 = Utils.getUniqueString(storeName + "_push");
    String parentControllerUrl = parentControllers.get(0).getControllerUrl();

    // Create store first
    try (ControllerClient controllerClient = new ControllerClient(clusterName, parentControllerUrl)) {
      controllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"");
      controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(100));

      assertCommand(
          controllerClient.requestTopicForWrites(
              storeName,
              1L,
              Version.PushType.BATCH,
              pushId1,
              false,
              true,
              false,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              false,
              -1));

      VersionCreationResponse vcr2 = controllerClient.requestTopicForWrites(
          storeName,
          1L,
          Version.PushType.BATCH,
          pushId2,
          false,
          true,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          false,
          -1);
      Assert.assertTrue(vcr2.isError());
      Assert.assertEquals(vcr2.getErrorType(), ErrorType.CONCURRENT_BATCH_PUSH);
      Assert.assertEquals(vcr2.getExceptionType(), ExceptionType.BAD_REQUEST);
    }
  }

  /**
   * The targeted region push should only push to the source region defined in the native replication, other regions should
   * not receive any data.
   * @throws IOException
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testTargetedRegionPushJob() throws Exception {
    motherOfAllTests(
        "testTargetedRegionPushJob",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1),
        100,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          // start a regular push job
          try (VenicePushJob job = new VenicePushJob("Test push job 1", props)) {
            job.run();
            // Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());

            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              // Current version should become 1 at both 2 data centers
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 1);
              }
            });
          }

          // start a targeted region push which should only increase the version to 2 in dc-0
          props.put(TARGETED_REGION_PUSH_ENABLED, true);
          try (VenicePushJob job = new VenicePushJob("Test push job 2", props)) {
            job.run(); // the job should succeed

            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              Map<String, Integer> coloVersions =
                  parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

              coloVersions.forEach((colo, version) -> {
                if (colo.equals(DEFAULT_NATIVE_REPLICATION_SOURCE)) {
                  Assert.assertEquals((int) version, 2);
                } else {
                  Assert.assertEquals((int) version, 1);
                }
              });
            });
          }

          // specify two regions, so both dc-0 and dc-1 is updated to version 3
          props.setProperty(TARGETED_REGION_PUSH_LIST, "dc-0, dc-1");
          try (VenicePushJob job = new VenicePushJob("Test push job 3", props)) {
            job.run(); // the job should succeed

            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              // Current version should become 1 at both 2 data centers
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 3);
              }
            });
          }

          // emergency source is dc-0 so dc-1 isn't selected to be the source fabric but the push should still complete
          props.setProperty(TARGETED_REGION_PUSH_LIST, "dc-1");
          try (VenicePushJob job = new VenicePushJob("Test push job 4", props)) {
            job.run(); // the job should succeed

            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              Map<String, Integer> coloVersions =
                  parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

              coloVersions.forEach((colo, version) -> {
                if (colo.equals("dc-1")) {
                  Assert.assertEquals((int) version, 4);
                } else {
                  Assert.assertEquals((int) version, 3);
                }
              });
            });
          }
        });
  }

  private interface NativeReplicationTest {
    void run(
        ControllerClient parentControllerClient,
        String clusterName,
        String storeName,
        Properties props,
        File inputDir) throws Exception;
  }

  private void motherOfAllTests(
      String storeNamePrefix,
      Function<UpdateStoreQueryParams, UpdateStoreQueryParams> updateStoreParamsTransformer,
      int recordCount,
      NativeReplicationTest test) throws Exception {
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString(storeNamePrefix);
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

    UpdateStoreQueryParams updateStoreParams = updateStoreParamsTransformer
        .apply(new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));

    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, recordCount);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    try {
      createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

      childDatacenters.get(0)
          .getClusters()
          .get(clusterName)
          .useControllerClient(
              dc0Client -> childDatacenters.get(1)
                  .getClusters()
                  .get(clusterName)
                  .useControllerClient(
                      dc1Client -> TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
                        // verify the update store command has taken effect before starting the push job.
                        StoreInfo store = dc0Client.getStore(storeName).getStore();
                        Assert.assertNotNull(store);
                        Assert.assertEquals(store.getStorageQuotaInByte(), Store.UNLIMITED_STORAGE_QUOTA);
                        store = dc1Client.getStore(storeName).getStore();
                        Assert.assertNotNull(store);
                        Assert.assertEquals(store.getStorageQuotaInByte(), Store.UNLIMITED_STORAGE_QUOTA);
                      })));

      makeSureSystemStoreIsPushed(clusterName, storeName);
      try (ControllerClient parentControllerClient =
          ControllerClient.constructClusterControllerClient(clusterName, parentControllerUrls)) {
        test.run(parentControllerClient, clusterName, storeName, props, inputDir);
      }
    } finally {
      FileUtils.deleteDirectory(inputDir);
    }
  }

  private void makeSureSystemStoreIsPushed(String clusterName, String storeName) {
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, true, () -> {
      for (int i = 0; i < childDatacenters.size(); i++) {
        VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(i);
        final int iCopy = i;
        childDataCenter.getClusters().get(clusterName).useControllerClient(controllerClient -> {
          String systemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
          StoreResponse storeResponse = controllerClient.getStore(systemStoreName);
          Assert.assertFalse(storeResponse.isError());
          Assert.assertTrue(
              storeResponse.getStore().getCurrentVersion() > 0,
              systemStoreName + " is not ready for DC-" + iCopy);

          systemStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
          StoreResponse storeResponse2 =
              controllerClient.getStore(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName));
          Assert.assertFalse(storeResponse2.isError());
          Assert.assertTrue(
              storeResponse2.getStore().getCurrentVersion() > 0,
              systemStoreName + " is not ready for DC-" + iCopy);
        });
      }
    });
  }

  private VeniceWriter<String, String, byte[]> startIncrementalPush(
      ControllerClient controllerClient,
      String storeName,
      VeniceWriterFactory veniceWriterFactory,
      String incrementalPushVersion) {
    VersionCreationResponse response = controllerClient.requestTopicForWrites(
        storeName,
        1024,
        Version.PushType.INCREMENTAL,
        "test-incremental-push",
        true,
        true,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        false,
        -1);
    assertFalse(response.isError());
    Assert.assertNotNull(response.getKafkaTopic());
    VeniceWriter veniceWriter = veniceWriterFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(response.getKafkaTopic())
            .setKeySerializer(new VeniceAvroKafkaSerializer(STRING_SCHEMA))
            .setValueSerializer(new VeniceAvroKafkaSerializer(STRING_SCHEMA))
            .build());
    veniceWriter.broadcastStartOfIncrementalPush(incrementalPushVersion, new HashMap<>());
    return veniceWriter;
  }

  private void verifyVeniceStoreData(String storeName, String routerUrl, String valuePrefix, int keyCount)
      throws ExecutionException, InterruptedException {
    try (AvroGenericStoreClient<String, Object> client = ClientFactory
        .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
      for (int i = 1; i <= keyCount; ++i) {
        String expected = valuePrefix + i;
        Object actual = client.get(Integer.toString(i)).get(); /* client.get().get() returns a Utf8 object */
        Assert.assertNotNull(actual, "Unexpected null value for key: " + i);
        Assert.assertEquals(actual.toString(), expected);
      }
    }
  }

}
