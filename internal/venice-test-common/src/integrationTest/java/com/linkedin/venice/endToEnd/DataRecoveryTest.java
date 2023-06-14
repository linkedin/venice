package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.ALLOW_CLUSTER_WIPE;
import static com.linkedin.venice.ConfigKeys.MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_KAFKA_PRODUCER_ENABLED;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
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
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.SystemProducer;
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
  private List<VeniceControllerWrapper> parentControllers;
  private String clusterName;
  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties serverProperties = new Properties();
    // If the delay is too short, dc-1 participant store new leader will skip local VT consumption, switch to remote VT,
    // replicate duplicate SOP/EOP to local VT and miss TopicSwitch, resulting in stuck participant store push job.
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 5L);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(SERVER_SHARED_KAFKA_PRODUCER_ENABLED, "true");
    serverProperties.put(SERVER_KAFKA_PRODUCER_POOL_SIZE_PER_KAFKA_CLUSTER, "2");

    Properties controllerProps = new Properties();
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    controllerProps.put(ALLOW_CLUSTER_WIPE, "true");
    controllerProps.put(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, "1000");
    controllerProps.put(MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE, "0");
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
                      .setHybridDataReplicationPolicy(DataReplicationPolicy.AGGREGATE)
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

  @Test(timeOut = TEST_TIMEOUT)
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
      TestUtils.writeBatchData(
          versionCreationResponse,
          STRING_SCHEMA,
          STRING_SCHEMA,
          IntStream.range(0, 10).mapToObj(i -> new AbstractMap.SimpleEntry<>(String.valueOf(i), String.valueOf(i))),
          HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID);
      TestUtils.waitForNonDeterministicPushCompletion(
          versionCreationResponse.getKafkaTopic(),
          parentControllerClient,
          60,
          TimeUnit.SECONDS);
      // Prepare dc-1 for data recovery
      Assert.assertFalse(
          parentControllerClient.prepareDataRecovery("dc-0", "dc-1", storeName, VERSION_ID_UNSET, Optional.empty())
              .isError());
      // Initiate data recovery, a new version will be created in dest fabric
      Assert.assertFalse(
          parentControllerClient
              .dataRecovery("dc-0", "dc-1", storeName, VERSION_ID_UNSET, false, true, Optional.empty())
              .isError());
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        Assert.assertEquals(dc1Client.getStore(storeName).getStore().getCurrentVersion(), 2);
      });
      try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setVeniceURL(childDatacenters.get(1).getClusters().get(clusterName).getRandomRouterURL()))) {
        for (int i = 0; i < 10; i++) {
          Object v = client.get(String.valueOf(i)).get();
          Assert.assertNotNull(v, "Batch data should be consumed already in data center dc-1");
          Assert.assertEquals(v.toString(), String.valueOf(i));
        }
      }
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
                      .setHybridDataReplicationPolicy(DataReplicationPolicy.ACTIVE_ACTIVE)
                      .setNativeReplicationEnabled(true)
                      .setActiveActiveReplicationEnabled(true)
                      .setPartitionCount(1))
              .isError());
      TestUtils.verifyDCConfigNativeAndActiveRepl(storeName, true, true, dc0Client, dc1Client);
      Assert.assertFalse(
          parentControllerClient.emptyPush(storeName, "empty-push-" + System.currentTimeMillis(), 1000).isError());
      String versionTopic = Version.composeKafkaTopic(storeName, 1);
      TestUtils.waitForNonDeterministicPushCompletion(versionTopic, parentControllerClient, 60, TimeUnit.SECONDS);

      Map<String, String> samzaConfig = new HashMap<>();
      String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
      samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, Version.PushType.STREAM.toString());
      samzaConfig.put(configPrefix + VENICE_STORE, storeName);
      samzaConfig.put(configPrefix + VENICE_AGGREGATE, "false");
      samzaConfig.put(VENICE_CHILD_D2_ZK_HOSTS, childDatacenters.get(0).getZkServerWrapper().getAddress());
      samzaConfig.put(VENICE_CHILD_CONTROLLER_D2_SERVICE, D2_SERVICE_NAME);
      samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, parentControllers.get(0).getZkAddress());
      samzaConfig.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, PARENT_D2_SERVICE_NAME);
      samzaConfig.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id"));
      samzaConfig.put(SSL_ENABLED, "false");
      VeniceSystemFactory factory = new VeniceSystemFactory();
      SystemProducer veniceProducer = factory.getProducer("venice", new MapConfig(samzaConfig), null);
      veniceProducer.start();

      for (int i = 0; i < 10; i++) {
        sendStreamingRecordWithKeyPrefix(veniceProducer, storeName, "dc-0_", i);
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
