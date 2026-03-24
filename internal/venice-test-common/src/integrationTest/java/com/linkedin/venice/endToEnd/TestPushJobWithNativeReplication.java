package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.EMERGENCY_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_STATUS_STORE_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.annotation.PubSubAgnosticTest;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.integration.utils.IntegrationTestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.status.BatchJobHeartbeatConfigs;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


@PubSubAgnosticTest
public class TestPushJobWithNativeReplication extends AbstractMultiRegionTest {
  private static final Logger LOGGER = LogManager.getLogger(TestPushJobWithNativeReplication.class);
  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;

  private static final String SYSTEM_STORE_CLUSTER = CLUSTER_NAME; // "venice-cluster0" from base class
  private static final String VPJ_HEARTBEAT_STORE_NAME =
      AvroProtocolDefinition.BATCH_JOB_HEARTBEAT.getSystemStoreName();

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @DataProvider(name = "storeSize")
  public static Object[][] storeSize() {
    return new Object[][] { { 50, 2 }, { 1000, 10 } };
  }

  @Override
  protected boolean shouldCreateD2Client() {
    return true;
  }

  @Override
  protected Properties getExtraServerProperties() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    return serverProperties;
  }

  @Override
  protected Properties getExtraControllerProperties() {
    Properties controllerProps = new Properties();
    // This property is required for test stores that have 10 partitions
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 10);
    controllerProps.put(BatchJobHeartbeatConfigs.HEARTBEAT_STORE_CLUSTER_CONFIG.getConfigName(), SYSTEM_STORE_CLUSTER);
    controllerProps.put(BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG.getConfigName(), true);
    controllerProps.put(PUSH_JOB_STATUS_STORE_CLUSTER_NAME, SYSTEM_STORE_CLUSTER);
    controllerProps.put(EMERGENCY_SOURCE_REGION, "dc-0");
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED, true);
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS, 100);
    return controllerProps;
  }

  @Override
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    super.setUp();
    IntegrationTestUtils.waitForParticipantStorePushInAllRegions(CLUSTER_NAME, childDatacenters);
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
            Assert.assertEquals(
                job.getPushDestinationPubsubBroker(),
                childDatacenters.get(0).getPubSubBrokerWrapper().getAddress());
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
              waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
                Object value = client.get(key).get();
                Assert.assertNotNull(value);
              });
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
            // Get the end position of the selected partition from version topic
            PubSubTopicPartition versionTopicPartition =
                new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(versionTopic), partitionId);

            TopicManager tmInChildRegion = childDataCenter.getRandomController().getVeniceAdmin().getTopicManager();
            PubSubPosition latestPositionInVersionTopic =
                tmInChildRegion.getLatestPositionWithRetries(versionTopicPartition, 5);
            // Get the offset metadata of the selected partition from storage node
            StorageMetadataService metadataService = serverInRemoteFabric.getStorageMetadataService();
            OffsetRecord offsetRecord = metadataService.getLastOffset(
                versionTopic,
                partitionId,
                serverInRemoteFabric.getKafkaStoreIngestionService().getPubSubContext());

            assertTrue(
                tmInChildRegion.diffPosition(
                    versionTopicPartition,
                    latestPositionInVersionTopic,
                    offsetRecord.getCheckpointedLocalVtPosition()) >= 0,
                "The offset recorded in storage node is larger than the end offset of version topic. "
                    + "versionTopic: " + versionTopic + ", partitionId: " + partitionId
                    + ", latestPositionInVersionTopic: " + latestPositionInVersionTopic + ", offsetRecord: "
                    + offsetRecord);
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
              VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(1).getClusters().get(clusterName);
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
              VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(1);
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
  public void testNativeReplicationWithDaVinci() throws Exception {
    int recordCount = 100;
    motherOfAllTests(
        "testNativeReplicationWithDaVinci",
        updateStoreParams -> updateStoreParams.setPartitionCount(2),
        recordCount,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
            job.run();
            // Verify the kafka URL being returned to the push job is the same as dc-0 kafka url.
            Assert.assertEquals(
                job.getPushDestinationPubsubBroker(),
                childDatacenters.get(0).getPubSubBrokerWrapper().getAddress());
          }

          // Test Da-vinci client is able to consume from NR region which is consuming remotely
          VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(1);

          try (DaVinciClient<String, Object> daVinciClient = ServiceFactory.getGenericAvroDaVinciClientWithRetries(
              storeName,
              childDataCenter.getClusters().get(clusterName).getZk().getAddress(),
              new DaVinciConfig(),
              new HashMap<>())) {
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
            Assert.assertEquals(
                job.getPushDestinationPubsubBroker(),
                childDatacenters.get(0).getPubSubBrokerWrapper().getAddress());
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
    String clusterName = CLUSTER_NAME;
    String storeName = Utils.getUniqueString("testEmptyPush");
    String parentControllerUrl = getParentControllerUrl();
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
    String clusterName = CLUSTER_NAME;
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
