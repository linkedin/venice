package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.ExceptionType;
import com.linkedin.venice.exceptions.VeniceException;
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
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.status.BatchJobHeartbeatConfigs;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
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
import java.util.concurrent.atomic.AtomicReference;
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

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.CommonConfigKeys.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.*;
import static com.linkedin.venice.samza.VeniceSystemFactory.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class  TestPushJobWithNativeReplication {
  private static final Logger logger = LogManager.getLogger(TestPushJobWithNativeReplication.class);
  private static final int TEST_TIMEOUT = 120_000; // ms

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0", "venice-cluster1", ...];

  private static final String VPJ_HEARTBEAT_STORE_CLUSTER = CLUSTER_NAMES[0]; // "venice-cluster0"
  private static final String VPJ_HEARTBEAT_STORE_NAME = "venice_system_store_BATCH_JOB_HEARTBEAT";

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;

  @DataProvider(name = "storeSize")
  public static Object[][] storeSize() {
    return new Object[][]{
        {50, 2},
        {1000, 10}
    };
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    /**
     * Reduce leader promotion delay to 3 seconds;
     * Create a testing environment with 1 parent fabric and 2 child fabrics;
     * Set server and replication factor to 2 to ensure at least 1 leader replica and 1 follower replica;
     * KMM allowlist config allows replicating all topics.
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
    controllerProps.put(AGGREGATE_REAL_TIME_SOURCE_REGION, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    controllerProps.put(NATIVE_REPLICATION_FABRIC_ALLOWLIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    int parentKafkaPort = Utils.getFreePort();
    controllerProps.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME, "localhost:" + parentKafkaPort);
    controllerProps.put(BatchJobHeartbeatConfigs.HEARTBEAT_STORE_CLUSTER_CONFIG.getConfigName(), VPJ_HEARTBEAT_STORE_CLUSTER);
    controllerProps.put(BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG.getConfigName(), true);
    controllerProps.put(ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_ALL_STORES, true);

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
        MirrorMakerWrapper.DEFAULT_TOPIC_ALLOWLIST,
        false,
        Optional.of(parentKafkaPort));
    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiColoMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "storeSize")
  public void testNativeReplicationForBatchPush(int recordCount, int partitionCount) throws Exception {
    motherOfAllTests(
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(partitionCount).setAmplificationFactor(2),
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
            long latestOffsetInVersionTopic = childDataCenter.getRandomController().getVeniceAdmin().getTopicManager().getPartitionLatestOffset(versionTopic, partitionId);
            // Get the offset metadata of the selected partition from storage node
            StorageMetadataService metadataService = serverInRemoteFabric.getStorageMetadataService();
            OffsetRecord offsetRecord = metadataService.getLastOffset(versionTopic, partitionId);

            Assert.assertTrue(offsetRecord.getLocalVersionTopicOffset() <= latestOffsetInVersionTopic);
          });
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNativeReplicationWithLeadershipHandover() throws Exception {
    int recordCount = 10000;
    motherOfAllTests(
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1).setAmplificationFactor(2),
        recordCount,
        (parentController, clusterName, storeName, props, inputDir) -> {
          Thread pushJobThread = new Thread(() -> TestPushUtils.runPushJob("Test push job", props));
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
          } finally {
            TestUtils.shutdownThread(pushJobThread);
          }
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNativeReplicationWithIngestionIsolationInDaVinci() throws Exception {
    int recordCount = 100;
    motherOfAllTests(
        updateStoreParams -> updateStoreParams
        .setPartitionCount(2).setAmplificationFactor(2),
        recordCount,
        (parentController, clusterName, storeName, props, inputDir) -> {
          try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
            job.run();
            // Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
          }
          // Setup meta system store for Da Vinci usage.
          ControllerClient controllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());
          TestUtils.createMetaSystemStore(controllerClient, storeName, Optional.of(logger));

          //Test Da-vinci client is able to consume from NR colo which is consuming remotely
          VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(1);

          D2Client d2Client = new D2ClientBuilder().setZkHosts(childDataCenter.getClusters().get(clusterName).getZk().getAddress())
              .setZkSessionTimeout(3, TimeUnit.SECONDS)
              .setZkStartupTimeout(3, TimeUnit.SECONDS)
              .build();
          D2ClientUtils.startClient(d2Client);

          try (DaVinciClient<String, Object> daVinciClient = ServiceFactory.getGenericAvroDaVinciClientWithRetries(
              storeName, childDataCenter.getClusters().get(clusterName).getZk().getAddress(), new DaVinciConfig(),
              TestUtils.getIngestionIsolationPropertyMap())) {
            daVinciClient.subscribeAll().get();
            for (int i = 1; i <= recordCount; ++i) {
              String expected = "test_name_" + i;
              String actual = daVinciClient.get(Integer.toString(i)).get().toString();
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
            .setPartitionCount(1)
            .setAmplificationFactor(2)
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
          Map<String, String> samzaConfig = new HashMap<>();
          String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
          samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, Version.PushType.STREAM.toString());
          samzaConfig.put(configPrefix + VENICE_STORE, storeName);
          samzaConfig.put(configPrefix + VENICE_AGGREGATE, "true");
          samzaConfig.put(D2_ZK_HOSTS_PROPERTY, "invalid_child_zk_address");
          samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, parentController.getKafkaZkAddress());
          samzaConfig.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id"));
          samzaConfig.put(SSL_ENABLED, "false");
          VeniceSystemFactory factory = new VeniceSystemFactory();
          try (VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
            veniceProducer.start();

            //Verify the kafka URL being returned to Samza is the same as parent colo kafka url.
            Assert.assertEquals(veniceProducer.getKafkaBootstrapServers(),
                parentController.getKafkaBootstrapServers(false));

            for (int i = 1; i <= 10; i++) {
              sendStreamingRecord(veniceProducer, storeName, i);
            }

            String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();
            try (AvroGenericStoreClient<String, Object> client =
                ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
              TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
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
          }
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testNativeReplicationForIncrementalPush() throws Exception {
    File inputDirInc = getTempDataDirectory();

    motherOfAllTests(
        updateStoreQueryParams -> updateStoreQueryParams
            .setPartitionCount(1)
            .setHybridOffsetLagThreshold(TEST_TIMEOUT)
            .setHybridRewindSeconds(2L)
            .setIncrementalPushEnabled(true),
        100,
        (parentController, clusterName, storeName, props, inputDir) -> {
          try (VenicePushJob job = new VenicePushJob("Batch Push", props)) {
            job.run();
            //Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
          }

          props.setProperty(INCREMENTAL_PUSH, "true");
          props.put(INPUT_PATH_PROP, inputDirInc);
          props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

          TestPushUtils.writeSimpleAvroFileWithUserSchema2(inputDirInc);
          try (VenicePushJob job = new VenicePushJob("Incremental Push", props)) {
            job.run();
          }
          NativeReplicationTestUtils.verifyIncrementalPushData(childDatacenters, clusterName, storeName, 150, 2);
        });
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "storeSize")
  public void testNativeReplicationForHeartbeatSystemStores(int recordCount, int partitionCount) throws Exception {
    motherOfAllTests(
        updateStoreQueryParams -> updateStoreQueryParams
            .setPartitionCount(partitionCount)
            .setIncrementalPushEnabled(true),
        recordCount,
        (parentController, clusterName, storeName, props, inputDir) -> {
          // Enable VPJ to send liveness heartbeat.
          props.put(BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG.getConfigName(), true);
          props.put(BatchJobHeartbeatConfigs.HEARTBEAT_STORE_NAME_CONFIG.getConfigName(), VPJ_HEARTBEAT_STORE_NAME);
          // Prevent heartbeat from being deleted when the VPJ run finishes.
          props.put(BatchJobHeartbeatConfigs.HEARTBEAT_LAST_HEARTBEAT_IS_DELETE_CONFIG.getConfigName(), false);


          try (ControllerClient dc0Client = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
              ControllerClient dc1Client = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString());
              ControllerClient parentClient = new ControllerClient(clusterName, parentController.getControllerUrl())) {

            TestPushUtils.updateStore(
                clusterName,
                VPJ_HEARTBEAT_STORE_NAME,
                parentClient,
                new UpdateStoreQueryParams().setLeaderFollowerModel(true).setNativeReplicationEnabled(true)
            );

            //verify the update store command has taken effect before starting the push job.
            NativeReplicationTestUtils.verifyDCConfigNativeRepl(Arrays.asList(dc0Client, dc1Client), VPJ_HEARTBEAT_STORE_NAME, true);
          }

          try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
            job.run();

            //Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
          }
          TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
            // Current version should become 1
            for (int version : parentController.getVeniceAdmin()
                .getCurrentVersionsForMultiColos(clusterName, storeName)
                .values()) {
              Assert.assertEquals(version, 1);
            }

            // Verify that the data are in all child fabrics including the first child fabric which consumes remotely.
            for (VeniceMultiClusterWrapper childDataCenter : childDatacenters) {
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

              // Try to read the latest heartbeat value generated from the user store VPJ push in this fabric/datacenter.
              try (AvroGenericStoreClient<BatchJobHeartbeatKey, BatchJobHeartbeatValue> client = ClientFactory.getAndStartGenericAvroClient(
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
  public void testNativeReplicationForSourceOverride() throws Exception {
    motherOfAllTests(
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1),
        100,
        (parentController, clusterName, storeName, props, inputDir) -> {

          try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl())) {
            UpdateStoreQueryParams updateStoreParams =
                new UpdateStoreQueryParams().setNativeReplicationSourceFabric("dc-1");
            TestPushUtils.updateStore(clusterName, storeName, parentControllerClient, updateStoreParams);
          }

          try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
            job.run();
            //Verify the kafka URL being returned to the push job is the same as dc-1 kafka url.
            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(1).getKafkaBrokerWrapper().getAddress());
          }
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testClusterLevelAdminCommandForNativeReplication() throws Exception {

    motherOfAllTests(updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1),
        10,
        (parentController, clusterName, batchOnlyStoreName, props, inputDir) -> {
          try (ControllerClient parentClient = new ControllerClient(clusterName, parentController.getControllerUrl());
              ControllerClient dc0Client = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
              ControllerClient dc1Client = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {
            List<ControllerClient> allControllerClients = Arrays.asList(parentClient, dc0Client, dc1Client);
            // Create a hybrid store
            String hybridStoreName = Utils.getUniqueString("hybrid-store");
            NewStoreResponse newStoreResponse = parentClient.createNewStore(hybridStoreName, "", STRING_SCHEMA, STRING_SCHEMA);
            Assert.assertFalse(newStoreResponse.isError());
            UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams()
                .setHybridRewindSeconds(10)
                .setHybridOffsetLagThreshold(2);
            ControllerResponse controllerResponse = parentClient.updateStore(hybridStoreName, updateStoreParams);
            Assert.assertFalse(controllerResponse.isError());

            /**
             * Create an incremental push enabled store
             */
            String incrementPushStoreName = Utils.getUniqueString("incremental-push-store");
            newStoreResponse = parentClient.createNewStore(incrementPushStoreName, "", STRING_SCHEMA, STRING_SCHEMA);
            Assert.assertFalse(newStoreResponse.isError());
            updateStoreParams = new UpdateStoreQueryParams().setIncrementalPushEnabled(true);
            controllerResponse = parentClient.updateStore(incrementPushStoreName, updateStoreParams);
            Assert.assertFalse(controllerResponse.isError());


            final Optional<String> newNativeReplicationSource = Optional.of("new-nr-source");
            /**
             * Run admin command to convert all batch-only stores in the cluster to native replication
             */
            controllerResponse = parentClient.configureNativeReplicationForCluster(true,
                VeniceUserStoreType.BATCH_ONLY.toString(), newNativeReplicationSource, Optional.empty());
            Assert.assertFalse(controllerResponse.isError());
            /**
             * Batch-only stores should have native replication enabled in dc-0 but not in dc-1 or dc-2; hybrid stores or
             * incremental push stores shouldn't have native replication enabled in any region.
             */
            NativeReplicationTestUtils.verifyDCConfigNativeRepl(allControllerClients, batchOnlyStoreName, true, newNativeReplicationSource);
            NativeReplicationTestUtils.verifyDCConfigNativeRepl(allControllerClients, hybridStoreName, false);
            NativeReplicationTestUtils.verifyDCConfigNativeRepl(allControllerClients, incrementPushStoreName, false);

            /**
             * Second test:
             * 1. Revert the cluster to previous state
             * 2. Test the cluster level command that converts all hybrid stores to native replication
             */
            controllerResponse = parentClient.configureNativeReplicationForCluster(false,
                VeniceUserStoreType.BATCH_ONLY.toString(), Optional.empty(), Optional.empty());
            Assert.assertFalse(controllerResponse.isError());
            controllerResponse = parentClient.configureNativeReplicationForCluster(true,
                VeniceUserStoreType.HYBRID_ONLY.toString(), newNativeReplicationSource, Optional.empty());
            Assert.assertFalse(controllerResponse.isError());

            /**
             * Hybrid stores should have native replication enabled in dc-0 but not in dc-1 or dc-2; batch-only stores or
             * incremental push stores shouldn't have native replication enabled in any region.
             */
            NativeReplicationTestUtils.verifyDCConfigNativeRepl(allControllerClients, batchOnlyStoreName, false);
            NativeReplicationTestUtils.verifyDCConfigNativeRepl(allControllerClients, hybridStoreName, true, newNativeReplicationSource);
            NativeReplicationTestUtils.verifyDCConfigNativeRepl(allControllerClients, incrementPushStoreName, false);

            /**
             * Third test:
             * 1. Revert the cluster to previous state
             * 2. Test the cluster level command that converts all incremental push stores to native replication
             */
            controllerResponse = parentClient.configureNativeReplicationForCluster(false,
                VeniceUserStoreType.HYBRID_ONLY.toString(), Optional.empty(), Optional.empty());
            Assert.assertFalse(controllerResponse.isError());
            controllerResponse = parentClient.configureNativeReplicationForCluster(true,
                VeniceUserStoreType.INCREMENTAL_PUSH.toString(), newNativeReplicationSource, Optional.empty());
            Assert.assertFalse(controllerResponse.isError());

            /**
             * Incremental push stores should have native replication enabled in dc-0 but not in dc-1 or dc-2; batch-only stores or
             * hybrid stores shouldn't have native replication enabled in any region.
             */
            NativeReplicationTestUtils.verifyDCConfigNativeRepl(allControllerClients, batchOnlyStoreName, false);
            NativeReplicationTestUtils.verifyDCConfigNativeRepl(allControllerClients, hybridStoreName, false);
            NativeReplicationTestUtils.verifyDCConfigNativeRepl(allControllerClients, incrementPushStoreName, true, newNativeReplicationSource);
          }
        });
  }



  @Test(timeOut = TEST_TIMEOUT)
  public void testMultiDataCenterRePushWithIncrementalPush() throws Exception {
    motherOfAllTests(
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1),
        100,
        (parentController, clusterName, storeName, props, inputDir) -> {
          try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
            job.run();

            //Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getKafkaBrokerWrapper().getAddress());
          }
          VeniceWriter<String, String, byte[]> incPushToRTWriter = null;

          try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl())) {
            assertFalse(parentControllerClient.updateStore(storeName,
                new UpdateStoreQueryParams().setIncrementalPushEnabled(true)
                    .setIncrementalPushPolicy(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)
                    .setHybridOffsetLagThreshold(1)
                    .setHybridRewindSeconds(Time.SECONDS_PER_DAY)).isError());

            // Update the store to L/F hybrid and enable INCREMENTAL_PUSH_SAME_AS_REAL_TIME.
            props.setProperty(SOURCE_KAFKA, "true");
            props.setProperty(KAFKA_INPUT_BROKER_URL,
                multiColoMultiClusterWrapper.getParentKafkaBrokerWrapper().getAddress());
            props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
            props.setProperty(VeniceWriter.ENABLE_CHUNKING, "false");
            props.setProperty(ALLOW_KIF_REPUSH_FOR_INC_PUSH_FROM_VT_TO_VT, "true");
            props.setProperty(KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(storeName, 1));

            try (VenicePushJob rePushJob = new VenicePushJob("Test re-push job re-push", props)) {
              rePushJob.run();
            }
            String incPushToRTVersion = System.currentTimeMillis() + "_test_inc_push_to_rt";
            incPushToRTWriter = startIncrementalPush(parentControllerClient, storeName,
                parentController.getVeniceAdmin().getVeniceWriterFactory(), incPushToRTVersion);
            final String newVersionTopic = Version.composeKafkaTopic(storeName,
                parentControllerClient.getStore(storeName).getStore().getLargestUsedVersionNumber());
            // Incremental push shouldn't be blocked and we will complete it once the new re-push is started.
            String incValuePrefix = "inc_test_";
            int newRePushVersion = Version.parseVersionFromKafkaTopicName(newVersionTopic) + 1;
            VeniceWriter<String, String, byte[]> finalIncPushToRTWriter = incPushToRTWriter;
            CompletableFuture.runAsync(() -> {
              TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
                Assert.assertEquals(parentControllerClient.getStore(storeName).getStore().getLargestUsedVersionNumber(),
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
            Assert.assertEquals(latestVersion.get().getHybridStoreConfig().getRewindTimeInSeconds(), VenicePushJob.DEFAULT_RE_PUSH_REWIND_IN_SECONDS_OVERRIDE);
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

  @Test (dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testEmptyPush(boolean toParent) {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("store");
    String parentControllerUrl = parentControllers.get(0).getControllerUrl();
    String childControllerUrl = childDatacenters.get(0).getControllerConnectString();

    // Create store first
    ControllerClient controllerClientToParent = new ControllerClient(clusterName, parentControllerUrl);
    controllerClientToParent.createNewStore(storeName, "test_owner", "\"int\"", "\"int\"");

    ControllerClient controllerClient = new ControllerClient(clusterName, toParent ? parentControllerUrl : childControllerUrl);
    VersionCreationResponse response = controllerClient.emptyPush(storeName, "test_push_id", 1000);
    if (toParent) {
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
    } else {
      Assert.assertTrue(response.isError(), "Empty push to child colo should be blocked");
    }
  }

  @Test (expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Failed to create new store version.*", timeOut = TEST_TIMEOUT)
  public void testPushDirectlyToChildColo() throws IOException {
    // In multi-colo setup, the batch push to child controller should be disabled.
    String clusterName = CLUSTER_NAMES[0];
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    String childControllerUrl = childDatacenters.get(0).getControllerConnectString();
    Properties props = defaultH2VProps(childControllerUrl, inputDirPath, storeName);
    createStoreForJob(clusterName, recordSchema, props).close();

    TestPushUtils.runPushJob("Test push job", props);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testControllerBlocksConcurrentBatchPush() {
    String clusterName = CLUSTER_NAMES[0];
    String storeName = Utils.getUniqueString("blocksConcurrentBatchPush");
    String pushId1 = Utils.getUniqueString(storeName + "_push");
    String pushId2 = Utils.getUniqueString(storeName + "_push");
    String parentControllerUrl = parentControllers.get(0).getControllerUrl();

    // Create store first
    ControllerClient controllerClient = new ControllerClient(clusterName, parentControllerUrl);

    controllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"");

    VersionCreationResponse vcr1 =
        controllerClient.requestTopicForWrites(storeName, 1L, Version.PushType.BATCH, pushId1, false, true, false,
            Optional.empty(), Optional.empty(), Optional.empty(), false, -1);
    Assert.assertFalse(vcr1.isError());

    VersionCreationResponse vcr2 =
        controllerClient.requestTopicForWrites(storeName, 1L, Version.PushType.BATCH, pushId2, false, true, false,
            Optional.empty(), Optional.empty(), Optional.empty(), false, -1);
    Assert.assertTrue(vcr2.isError());
    Assert.assertEquals(vcr2.getErrorType(), ErrorType.CONCURRENT_BATCH_PUSH);
    Assert.assertEquals(vcr2.getExceptionType(), ExceptionType.BAD_REQUEST);
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
    final AtomicReference<Optional<VeniceControllerWrapper>> atomicOptionalParentController = new AtomicReference<>();
    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS, () -> {
      Optional<VeniceControllerWrapper> optionalParentController = parentControllers.stream()
          .filter(c -> c.isLeaderController(clusterName))
          .findAny();
      atomicOptionalParentController.set(optionalParentController);
      return optionalParentController.isPresent();
    });
    VeniceControllerWrapper parentController = atomicOptionalParentController.get().get();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props = defaultH2VProps(parentController.getControllerUrl(), inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

    UpdateStoreQueryParams updateStoreParams = updateStoreParamsTransformer.apply(
        new UpdateStoreQueryParams()
            .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setLeaderFollowerModel(true)
            .setNativeReplicationEnabled(true));

    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, recordCount);
    String keySchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.VALUE_FIELD_PROP)).schema().toString();

    try {
      createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();

      try (ControllerClient dc0Client = new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
          ControllerClient dc1Client = new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {

        // verify the update store command has taken effect before starting the push job.
        NativeReplicationTestUtils.verifyDCConfigNativeRepl(Arrays.asList(dc0Client, dc1Client), storeName, true);
      }

      makeSureSystemStoreIsPushed(clusterName, storeName);
      test.run(parentController, clusterName, storeName, props, inputDir);
    } finally {
      FileUtils.deleteDirectory(inputDir);
    }
  }

  private void makeSureSystemStoreIsPushed(String clusterName, String storeName) {
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, true, () -> {
      for (int i = 0; i < childDatacenters.size(); i++) {
        VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(i);
        ControllerClient controllerClient =
            new ControllerClient(clusterName, childDataCenter.getRandomController().getControllerUrl());

        String systemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
        StoreResponse storeResponse =
            controllerClient.getStore(systemStoreName);
        Assert.assertFalse(storeResponse.isError());
        Assert.assertTrue(storeResponse.getStore().getCurrentVersion() > 0, systemStoreName + " is not ready for DC-" + i);

        systemStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
        StoreResponse storeResponse2 =
            controllerClient.getStore(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName));
        Assert.assertFalse(storeResponse2.isError());
        Assert.assertTrue(storeResponse2.getStore().getCurrentVersion() > 0, systemStoreName + " is not ready for DC-" + i);
      }
    });
  }

  private VeniceWriter<String, String, byte[]> startIncrementalPush(ControllerClient controllerClient, String storeName,
      VeniceWriterFactory veniceWriterFactory, String incrementalPushVersion) {
    VersionCreationResponse response = controllerClient.requestTopicForWrites(storeName, 1024,
        Version.PushType.INCREMENTAL, "test-incremental-push", true, true, false, Optional.empty(),
        Optional.empty(), Optional.empty(), false, -1);
    assertFalse(response.isError());
    Assert.assertNotNull(response.getKafkaTopic());
    VeniceWriter veniceWriter  =
        veniceWriterFactory.createVeniceWriter(response.getKafkaTopic(),
            new VeniceAvroKafkaSerializer(STRING_SCHEMA), new VeniceAvroKafkaSerializer(STRING_SCHEMA));
    veniceWriter.broadcastStartOfIncrementalPush(incrementalPushVersion, new HashMap<>());
    return veniceWriter;
  }

  private void verifyVeniceStoreData(String storeName, String routerUrl, String valuePrefix, int keyCount)
      throws ExecutionException, InterruptedException {
    try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
      for (int i = 1; i <= keyCount; ++i) {
        String expected = valuePrefix + i;
        Object actual = client.get(Integer.toString(i)).get(); /* client.get().get() returns a Utf8 object */
        Assert.assertNotNull(actual, "Unexpected null value for key: " + i);
        Assert.assertEquals(actual.toString(), expected);
      }
    }
  }

}
