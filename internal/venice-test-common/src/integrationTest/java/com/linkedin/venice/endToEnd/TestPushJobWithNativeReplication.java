package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.EMERGENCY_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_STATUS_STORE_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
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
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.status.BatchJobHeartbeatConfigs;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

  private static final String SYSTEM_STORE_CLUSTER = CLUSTER_NAMES[0]; // "venice-cluster0"
  private static final String VPJ_HEARTBEAT_STORE_NAME =
      AvroProtocolDefinition.BATCH_JOB_HEARTBEAT.getSystemStoreName();

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;

  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private D2Client d2Client;
  private VeniceServerWrapper serverWrapper;

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
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    Properties controllerProps = new Properties();
    // This property is required for test stores that have 10 partitions
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 10);
    controllerProps.put(BatchJobHeartbeatConfigs.HEARTBEAT_STORE_CLUSTER_CONFIG.getConfigName(), SYSTEM_STORE_CLUSTER);
    controllerProps.put(BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG.getConfigName(), true);
    controllerProps.put(PUSH_JOB_STATUS_STORE_CLUSTER_NAME, SYSTEM_STORE_CLUSTER);
    controllerProps.put(EMERGENCY_SOURCE_REGION, "dc-0");

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(2)
            .forkServer(false)
            .parentControllerProperties(controllerProps)
            .childControllerProperties(controllerProps)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    VeniceClusterWrapper clusterWrapper =
        multiRegionMultiClusterWrapper.getChildRegions().get(0).getClusters().get(CLUSTER_NAMES[0]);
    d2Client = new D2ClientBuilder().setZkHosts(clusterWrapper.getZk().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
    serverWrapper = clusterWrapper.getVeniceServers().get(0);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    D2ClientUtils.shutdownClient(d2Client);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "storeSize")
  public void testNativeReplicationForBatchPush(int recordCount, int partitionCount) throws Exception {
    motherOfAllTests(
        "testNativeReplicationForBatchPush",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(partitionCount),
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

            String pushJobDetailsStoreName = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
            waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
              MultiSchemaResponse multiSchemaResponse =
                  assertCommand(parentControllerClient.getAllValueSchema(pushJobDetailsStoreName));
              assertNotNull(multiSchemaResponse.getSchemas(), pushJobDetailsStoreName + " schemas are null!");
              Assert.assertEquals(
                  multiSchemaResponse.getSchemas().length,
                  AvroProtocolDefinition.PUSH_JOB_DETAILS.currentProtocolVersion.get().intValue(),
                  "Number of schemas for " + pushJobDetailsStoreName + " is not as expected!");
            });

            // Verify push job details are populated
            try (AvroGenericStoreClient<PushJobStatusRecordKey, Object> client =
                ClientFactory.getAndStartGenericAvroClient(
                    ClientConfig.defaultGenericClientConfig(pushJobDetailsStoreName).setVeniceURL(routerUrl))) {
              PushJobStatusRecordKey key = new PushJobStatusRecordKey();
              key.setStoreName(storeName);
              key.setVersionNumber(1);
              Object value = client.get(key).get();
              Assert.assertNotNull(value);
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
                .getLatestOffsetWithRetries(versionTopicPartition, 5);
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
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1),
        recordCount,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          Thread pushJobThread = new Thread(() -> IntegrationTestPushUtils.runVPJ(props));
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
                  Object val = client.get(Integer.toString(i)).get();
                  Assert.assertNotNull(val, "Value should not be null for key " + i);
                  Assert.assertEquals(val.toString(), expected);
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
        updateStoreParams -> updateStoreParams.setPartitionCount(2),
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
            .setHybridRewindSeconds(TEST_TIMEOUT)
            .setHybridOffsetLagThreshold(2),
        50,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          // Write batch data
          IntegrationTestPushUtils.runVPJ(props);

          // Verify version level hybrid config is set correctly. The current version should be 1.
          VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(0);
          Version version =
              childDataCenter.getRandomController().getVeniceAdmin().getStore(clusterName, storeName).getVersion(1);
          HybridStoreConfig hybridConfig = version.getHybridStoreConfig();
          Assert.assertNotNull(hybridConfig);
          Assert.assertEquals(hybridConfig.getRewindTimeInSeconds(), TEST_TIMEOUT);
          Assert.assertEquals(hybridConfig.getOffsetLagThresholdToGoOnline(), 2);

          try (VeniceSystemProducer veniceProducer =
              IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {

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
            .setActiveActiveReplicationEnabled(true)
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

          TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema2(inputDirInc);
          try (VenicePushJob job = new VenicePushJob("Incremental Push", props)) {
            job.run();
          }
          NativeReplicationTestUtils.verifyIncrementalPushData(childDatacenters, clusterName, storeName, 150, 2);
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testActiveActiveForHeartbeatSystemStores() throws Exception {
    int recordCount = 50;
    int partitionCount = 2;
    motherOfAllTests(
        "testActiveActiveForHeartbeatSystemStores",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(partitionCount),
        recordCount,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          try (
              ControllerClient dc0Client =
                  new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
              ControllerClient dc1Client =
                  new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {
            // verify the update store command has taken effect before starting the push job.
            NativeReplicationTestUtils
                .verifyDCConfigNativeRepl(Arrays.asList(dc0Client, dc1Client), VPJ_HEARTBEAT_STORE_NAME, true);
          }

          // Enable VPJ to send liveness heartbeat.
          props.put(BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG.getConfigName(), true);
          // Prevent heartbeat from being deleted when the VPJ run finishes.
          props.put(BatchJobHeartbeatConfigs.HEARTBEAT_LAST_HEARTBEAT_IS_DELETE_CONFIG.getConfigName(), false);

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
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testPushDirectlyToChildColo");
    Properties props = IntegrationTestPushUtils.defaultVPJProps(childDatacenters.get(0), inputDirPath, storeName);
    createStoreForJob(clusterName, recordSchema, props).close();

    IntegrationTestPushUtils.runVPJ(props);
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
  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testTargetedRegionPushJobFullConsumptionForBatchStore() throws Exception {
    // make sure the participant store is up and running in dest region otherwise the test will be flaky
    // the participant store is needed for data recovery
    String destClusterName = CLUSTER_NAMES[0];

    try (ControllerClient controllerClient =
        new ControllerClient(destClusterName, childDatacenters.get(1).getControllerConnectString())) {
      // Verify the participant store is up and running in dest region.
      // Participant store is needed for checking kill record existence and dest region readiness for data recovery.
      String participantStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(destClusterName);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(participantStoreName, 1),
          controllerClient,
          10,
          TimeUnit.MINUTES);
    }
    motherOfAllTests(
        "testTargetedRegionPushJobBatchStore",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1),
        100,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          props.put(TARGETED_REGION_PUSH_ENABLED, false);
          // props.put(POST_VALIDATION_CONSUMPTION_ENABLED, false);
          try (VenicePushJob job = new VenicePushJob("Test push job 1", props)) {
            job.run(); // the job should succeed

            TestUtils.waitForNonDeterministicAssertion(45, TimeUnit.SECONDS, () -> {
              // Current version should become 1
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 1);
              }
            });
          }
          String dataDBPathV1 = serverWrapper.getDataDirectory() + "/rocksdb/" + storeName + "_v1";
          long storeSize = FileUtils.sizeOfDirectory(new File(dataDBPathV1));
          try (VenicePushJob job = new VenicePushJob("Test push job 2", props)) {
            job.run();

            TestUtils.waitForNonDeterministicAssertion(45, TimeUnit.SECONDS, () -> {
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 2);
              }
            });
          }
          props.put(TARGETED_REGION_PUSH_ENABLED, true);
          TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 20);
          try (VenicePushJob job = new VenicePushJob("Test push job 3", props)) {
            job.run(); // the job should succeed
            File directory = new File(serverWrapper.getDataDirectory() + "/rocksdb/");
            File[] storeDBDirs = directory.listFiles(File::isDirectory);
            long totalStoreSize = 0;
            if (storeDBDirs != null) {
              for (File storeDB: storeDBDirs) {
                if (storeDB.getName().startsWith(storeName)) {
                  long size = FileUtils
                      .sizeOfDirectory(new File(serverWrapper.getDataDirectory() + "/rocksdb/" + storeDB.getName()));
                  ;
                  totalStoreSize += size;
                }
              }
              Assert.assertTrue(
                  storeSize * 2 >= totalStoreSize,
                  "2x of store size " + storeSize + " is more than total " + totalStoreSize);
            }
            TestUtils.waitForNonDeterministicAssertion(45, TimeUnit.SECONDS, () -> {
              // Current version should become 2
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 3);
              }
              // should be able to read all 20 records.
              validateDaVinciClient(storeName, 20);
              validatePushJobDetails(clusterName, storeName);
            });
          }
        });
  }

  @Test(enabled = false) // Disable till hybrid stores are supported for target region push
  public void testTargetedRegionPushJobFullConsumptionForHybridStore() throws Exception {
    motherOfAllTests(
        "testTargetedRegionPushJobHybridStore",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1)
            .setHybridRewindSeconds(10)
            .setHybridOffsetLagThreshold(2),
        100,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          props.put(TARGETED_REGION_PUSH_ENABLED, true);
          try (VenicePushJob job = new VenicePushJob("Test push job 1", props)) {
            job.run(); // the job should succeed

            TestUtils.waitForNonDeterministicAssertion(45, TimeUnit.SECONDS, () -> {
              // Current version should become 2
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 2);
              }
            });
          }
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testTargetRegionPushWithDeferredVersionSwap() throws Exception {
    motherOfAllTests(
        "testTargetRegionPushWithDeferredVersionSwap",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1),
        100,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          // start a regular push job
          try (VenicePushJob job = new VenicePushJob("Test regular push job", props)) {
            job.run();

            // Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());

            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              // Current version should become 1 all data centers
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 1);
              }
            });
          }

          // start target version push with deferred swap
          props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
          try (VenicePushJob job = new VenicePushJob("Test target region push w. deferred version swap", props)) {
            job.run();

            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              Map<String, Integer> coloVersions =
                  parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

              coloVersions.forEach((colo, version) -> {
                if (colo.equals(DEFAULT_NATIVE_REPLICATION_SOURCE)) {
                  Assert.assertEquals((int) version, 2); // Version should only be swapped in dc-0
                } else {
                  Assert.assertEquals((int) version, 1); // The remaining regions shouldn't be swapped
                }
              });
            });
          }
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testKilledRepushJobVersionStatus() throws Exception {
    motherOfAllTests(
        "testKilledRepushJobVersionStatus",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1),
        100,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          // start a regular push job
          try (VenicePushJob job = new VenicePushJob("Test regular push job", props)) {
            job.run();

            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              // Current version should become 1 all data centers
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 1);
              }
            });
          }

          // create repush topic
          parentControllerClient.requestTopicForWrites(
              storeName,
              1000,
              Version.PushType.BATCH,
              Version.generateRePushId("2"),
              true,
              true,
              false,
              Optional.empty(),
              Optional.empty(),
              Optional.of("dc-1"),
              false,
              -1,
              false,
              null,
              1,
              false);

          // kill repush version
          parentControllerClient.killOfflinePushJob(Version.composeKafkaTopic(storeName, 2));

          // verify parent version status is marked as killed
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
            StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
            Assert.assertEquals(parentStore.getVersion(2).get().getStatus(), VersionStatus.KILLED);
          });

          // verify child version status is marked as killed
          for (VeniceMultiClusterWrapper childDatacenter: childDatacenters) {
            ControllerClient childControllerClient =
                new ControllerClient(clusterName, childDatacenter.getControllerConnectString());
            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              StoreResponse store = childControllerClient.getStore(storeName);
              Optional<Version> version = store.getStore().getVersion(2);
              assertNotNull(version);
              assertEquals(version.get().getStatus(), VersionStatus.KILLED);
            });
          }

          TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, true, () -> {
            JobStatusQueryResponse jobStatusQueryResponse = assertCommand(
                parentControllerClient
                    .queryOverallJobStatus(Version.composeKafkaTopic(storeName, 2), Optional.empty()));
            ExecutionStatus executionStatus = ExecutionStatus.valueOf(jobStatusQueryResponse.getStatus());
            assertEquals(executionStatus, ExecutionStatus.ERROR);
          });
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

    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, recordCount);
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

  private void validateDaVinciClient(String storeName, int recordCount)
      throws ExecutionException, InterruptedException {
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    DaVinciClient<String, Object> client;
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .build();
    MetricsRepository metricsRepository = new MetricsRepository();
    DaVinciConfig clientConfig = new DaVinciConfig();
    clientConfig.setStorageClass(StorageClass.DISK);
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        multiRegionMultiClusterWrapper)) {
      client = factory.getGenericAvroClient(storeName, clientConfig);
      client.start();
      client.subscribeAll().get(30, TimeUnit.SECONDS);
      for (int i = 1; i <= recordCount; i++) {
        Assert.assertNotNull(client.get(Integer.toString(i)));
      }
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  private void validatePushJobDetails(String clusterName, String storeName)
      throws ExecutionException, InterruptedException {
    String pushJobDetailsStoreName = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
    VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(NUMBER_OF_CHILD_DATACENTERS - 1);
    String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();

    // Verify push job details are populated
    try (AvroGenericStoreClient<PushJobStatusRecordKey, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(pushJobDetailsStoreName).setVeniceURL(routerUrl))) {
      PushJobStatusRecordKey key = new PushJobStatusRecordKey();
      key.setStoreName(storeName);
      key.setVersionNumber(1);
      GenericRecord value = (GenericRecord) client.get(key).get();
      HashMap<Utf8, List<PushJobDetailsStatusTuple>> map =
          (HashMap<Utf8, List<PushJobDetailsStatusTuple>>) value.get("coloStatus");
      Assert.assertEquals(map.size(), 2);
      List<PushJobDetailsStatusTuple> status = map.get(new Utf8("dc-0"));
      Assert.assertEquals(
          ((GenericRecord) status.get(status.size() - 1)).get("status"),
          PushJobDetailsStatus.COMPLETED.getValue());
      status = map.get(new Utf8("dc-1"));
      Assert.assertEquals(
          ((GenericRecord) status.get(status.size() - 1)).get("status"),
          PushJobDetailsStatus.COMPLETED.getValue());

    }
  }

  private void makeSureSystemStoreIsPushed(String clusterName, String storeName) {
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, true, () -> {
      for (int i = 0; i < childDatacenters.size(); i++) {
        VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(i);
        final int iCopy = i;
        childDataCenter.getClusters().get(clusterName).useControllerClient(cc -> {
          assertStoreHealth(cc, VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), iCopy);
          assertStoreHealth(cc, VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName), iCopy);
          assertStoreHealth(cc, VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix(), iCopy);
        });
      }
    });
  }

  private void assertStoreHealth(ControllerClient controllerClient, String systemStoreName, int dcNumber) {
    StoreResponse storeResponse = assertCommand(controllerClient.getStore(systemStoreName));
    Assert.assertTrue(
        storeResponse.getStore().getCurrentVersion() > 0,
        systemStoreName + " is not ready for DC-" + dcNumber);
  }
}
