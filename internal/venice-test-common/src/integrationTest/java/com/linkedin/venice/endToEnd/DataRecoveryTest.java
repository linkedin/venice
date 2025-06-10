package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED;
import static com.linkedin.venice.ConfigKeys.ALLOW_CLUSTER_WIPE;
import static com.linkedin.venice.ConfigKeys.MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecordWithKeyPrefix;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ReadyForDataRecoveryResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DataRecoveryTest {
  private static final long TEST_TIMEOUT = 120_000;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);
  private static final int VERSION_ID_UNSET = -1;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private List<VeniceMultiClusterWrapper> childDatacenters;
  private String clusterName;
  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");

    Properties controllerProps = new Properties();
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    controllerProps.put(ALLOW_CLUSTER_WIPE, "true");
    controllerProps.put(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, "1000");
    controllerProps.put(MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE, "0");
    controllerProps.put(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, Boolean.FALSE.toString());
    controllerProps.put(PARTICIPANT_MESSAGE_STORE_ENABLED, Boolean.TRUE.toString());
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
    clusterName = CLUSTER_NAMES[0];

    try (ControllerClient controllerClient =
        new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {
      // Verify the participant store is up and running in dest region.
      // Participant store is needed for checking kill record existence and dest region readiness for data recovery.
      String participantStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(participantStoreName, 1),
          controllerClient,
          2,
          TimeUnit.MINUTES);
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStartDataRecoveryAPIs() {
    String storeName = Utils.getUniqueString("dataRecovery-store");
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
        ControllerClient dc0Client =
            new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client =
            new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {
      List<ControllerClient> dcControllerClientList = Arrays.asList(dc0Client, dc1Client);
      TestUtils.createAndVerifyStoreInAllRegions(storeName, parentControllerClient, dcControllerClientList);
      Assert.assertFalse(
          parentControllerClient
              .updateStore(
                  storeName,
                  new UpdateStoreQueryParams().setHybridRewindSeconds(10)
                      .setHybridOffsetLagThreshold(2)
                      .setNativeReplicationEnabled(true)
                      .setPartitionCount(1))
              .isError());
      TestUtils.verifyDCConfigNativeAndActiveRepl(storeName, true, false, dc0Client, dc1Client);
      Assert.assertFalse(
          parentControllerClient.emptyPush(storeName, "empty-push-" + System.currentTimeMillis(), 1000).isError());
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
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
        Admin dc1Admin = childDatacenters.get(1).getLeaderController(clusterName).getVeniceAdmin();
        Assert.assertTrue(dc1Admin.getStore(clusterName, storeName).containsVersion(1));
        Assert.assertTrue(
            dc1Admin.getTopicManager()
                .containsTopic(pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1))));
      });
    }
  }

  private void performDataRecoveryTest(
      String storeName,
      ControllerClient parentControllerClient,
      ControllerClient destColoClient,
      String src,
      String dest,
      int times,
      int expectedStartVersionOnDest) throws ExecutionException, InterruptedException {
    for (int version = expectedStartVersionOnDest; version < times + expectedStartVersionOnDest; version++) {
      // Prepare dest fabric for data recovery.
      Assert.assertFalse(
          parentControllerClient.prepareDataRecovery(src, dest, storeName, VERSION_ID_UNSET, Optional.empty())
              .isError());
      // Initiate data recovery, a new version will be created in dest fabric.
      Assert.assertFalse(
          parentControllerClient.dataRecovery(src, dest, storeName, VERSION_ID_UNSET, false, true, Optional.empty())
              .isError());
      int finalVersion = version;
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        Assert.assertEquals(destColoClient.getStore(storeName).getStore().getCurrentVersion(), finalVersion);
      });
      try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setVeniceURL(childDatacenters.get(1).getClusters().get(clusterName).getRandomRouterURL()))) {
        for (int i = 0; i < 10; i++) {
          Object v = client.get(String.valueOf(i)).get();
          Assert.assertNotNull(v, "Batch data should be consumed already in data center " + dest);
          Assert.assertEquals(v.toString(), String.valueOf(i));
        }
      }
    }
  }

  @Test(timeOut = 2 * TEST_TIMEOUT)
  public void testBatchOnlyDataRecovery() throws Exception {
    String storeName = Utils.getUniqueString("dataRecovery-store-batch");
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
        ControllerClient dc0Client =
            new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client =
            new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {
      List<ControllerClient> dcControllerClientList = Arrays.asList(dc0Client, dc1Client);
      TestUtils.createAndVerifyStoreInAllRegions(storeName, parentControllerClient, dcControllerClientList);
      Assert.assertFalse(
          parentControllerClient
              .updateStore(
                  storeName,
                  new UpdateStoreQueryParams().setNativeReplicationEnabled(true).setPartitionCount(1))
              .isError());
      TestUtils.verifyDCConfigNativeAndActiveRepl(storeName, true, false, dc0Client, dc1Client);
      VersionCreationResponse versionCreationResponse = parentControllerClient.requestTopicForWrites(
          storeName,
          1024,
          Version.PushType.BATCH,
          Version.guidBasedDummyPushId(),
          true,
          false,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          false,
          -1);
      Assert.assertFalse(versionCreationResponse.isError());
      PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
          childDatacenters.get(0).getKafkaBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();

      List<PubSubBrokerWrapper> pubSubBrokerWrappers =
          childDatacenters.stream().map(VeniceMultiClusterWrapper::getKafkaBrokerWrapper).collect(Collectors.toList());
      Map<String, String> additionalConfigs = PubSubBrokerWrapper.getBrokerDetailsForClients(pubSubBrokerWrappers);
      TestUtils.writeBatchData(
          versionCreationResponse,
          STRING_SCHEMA.toString(),
          STRING_SCHEMA.toString(),
          IntStream.range(0, 10).mapToObj(i -> new AbstractMap.SimpleEntry<>(String.valueOf(i), String.valueOf(i))),
          HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID,
          pubSubProducerAdapterFactory,
          additionalConfigs,
          pubSubBrokerWrappers.get(0).getPubSubPositionTypeRegistry());
      TestUtils.waitForNonDeterministicPushCompletion(
          versionCreationResponse.getKafkaTopic(),
          parentControllerClient,
          60,
          TimeUnit.SECONDS);

      // Verify that if same data center are given as both src and dest to data recovery api, client response should
      // have errors.
      String sameFabricName = "dc-0";
      Assert.assertTrue(
          parentControllerClient
              .prepareDataRecovery(sameFabricName, sameFabricName, storeName, VERSION_ID_UNSET, Optional.empty())
              .isError());
      Assert.assertTrue(
          parentControllerClient
              .dataRecovery(sameFabricName, sameFabricName, storeName, VERSION_ID_UNSET, false, true, Optional.empty())
              .isError());

      /*
       * Before data recovery, current version in dc-0 and dc-1 is 1.
       * With two rounds of dc-0 -> dc-1 data recovery, current version in dc-1 changes to 2 and then 3.
       * Then with two rounds of dc-1 -> dc-0 data recovery, current version in dc-0 becomes 3 and then 4.
       */
      performDataRecoveryTest(storeName, parentControllerClient, dc1Client, "dc-0", "dc-1", 2, 2);
      performDataRecoveryTest(storeName, parentControllerClient, dc0Client, "dc-1", "dc-0", 2, 3);
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testHybridAADataRecovery() throws Exception {
    String storeName = Utils.getUniqueString("dataRecovery-store-hybrid-AA");
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
        ControllerClient dc0Client =
            new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client =
            new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString())) {
      List<ControllerClient> dcControllerClientList = Arrays.asList(dc0Client, dc1Client);
      TestUtils.createAndVerifyStoreInAllRegions(storeName, parentControllerClient, dcControllerClientList);
      Assert.assertFalse(
          parentControllerClient
              .updateStore(
                  storeName,
                  new UpdateStoreQueryParams().setHybridRewindSeconds(10)
                      .setHybridOffsetLagThreshold(2)
                      .setNativeReplicationEnabled(true)
                      .setActiveActiveReplicationEnabled(true)
                      .setPartitionCount(1))
              .isError());
      TestUtils.verifyDCConfigNativeAndActiveRepl(storeName, true, true, dc0Client, dc1Client);
      Assert.assertFalse(
          parentControllerClient.emptyPush(storeName, "empty-push-" + System.currentTimeMillis(), 1000).isError());
      String versionTopic = Version.composeKafkaTopic(storeName, 1);
      TestUtils.waitForNonDeterministicPushCompletion(versionTopic, parentControllerClient, 60, TimeUnit.SECONDS);

      try (VeniceSystemProducer veniceProducer =
          IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, 0, storeName)) {
        for (int i = 0; i < 10; i++) {
          sendStreamingRecordWithKeyPrefix(veniceProducer, storeName, "dc-0_", i);
        }
      }

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
      TestUtils.waitForNonDeterministicPushCompletion(versionTopic, parentControllerClient, 60, TimeUnit.SECONDS);

      try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setVeniceURL(childDatacenters.get(1).getClusters().get(clusterName).getRandomRouterURL()))) {
        for (int i = 0; i < 10; i++) {
          final String keyId = String.valueOf(i);
          TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
            Object v = client.get("dc-0_" + keyId).get();
            Assert.assertNotNull(v, "Batch data should be consumed already in data center dc-1");
            Assert.assertEquals(v.toString(), "stream_" + keyId);
          });
        }
      }
    }
  }
}
