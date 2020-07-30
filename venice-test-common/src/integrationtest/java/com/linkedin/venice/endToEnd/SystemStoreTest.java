package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.MetadataStoreBasedStoreRepository;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.StoreMetadataType;
import com.linkedin.venice.common.VeniceSystemStore;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.systemstore.schemas.CurrentStoreStates;
import com.linkedin.venice.meta.systemstore.schemas.CurrentVersionStates;
import com.linkedin.venice.meta.systemstore.schemas.StoreAttributes;
import com.linkedin.venice.meta.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataKey;
import com.linkedin.venice.meta.systemstore.schemas.StoreMetadataValue;
import com.linkedin.venice.meta.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.meta.systemstore.schemas.TargetVersionStates;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.meta.VersionStatus.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class SystemStoreTest {
  private static final Logger logger = Logger.getLogger(SystemStoreTest.class);
  private VeniceClusterWrapper venice;
  private VeniceControllerWrapper parentController;
  private ZkServerWrapper parentZk;
  private ControllerClient controllerClient;
  private ControllerClient parentControllerClient;
  private String participantMessageStoreName;
  private VeniceServerWrapper veniceServerWrapper;
  private String clusterName;
  private String zkSharedStoreName = VeniceSystemStore.METADATA_STORE.getPrefix();
  private int metadataStoreVersionNumber;
  private D2Client d2Client;

  @BeforeClass
  public void setup() {
    Properties enableParticipantMessageStore = new Properties();
    Properties serverFeatureProperties = new Properties();
    Properties serverProperties = new Properties();
    enableParticipantMessageStore.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
    enableParticipantMessageStore.setProperty(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, "false");
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    enableParticipantMessageStore.setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS,
        String.valueOf(Long.MAX_VALUE));
    venice = ServiceFactory.getVeniceCluster(1, 0, 1, 1,
        100000, false, false, enableParticipantMessageStore);
    clusterName = venice.getClusterName();
    d2Client = D2TestUtils.getAndStartD2Client(venice.getZk().getAddress());
    serverFeatureProperties.put(VeniceServerWrapper.CLIENT_CONFIG_FOR_CONSUMER, ClientConfig.defaultGenericClientConfig("")
        .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME)
        .setD2Client(d2Client));
    serverProperties.setProperty(PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS, Long.toString(100));
    veniceServerWrapper = venice.addVeniceServer(serverFeatureProperties, serverProperties);
    parentZk = ServiceFactory.getZkServer();
    parentController =
        ServiceFactory.getVeniceParentController(venice.getClusterName(), parentZk.getAddress(), venice.getKafka(),
            new VeniceControllerWrapper[]{venice.getMasterVeniceController()},
            new VeniceProperties(enableParticipantMessageStore), false);
    participantMessageStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(venice.getClusterName());
    controllerClient = venice.getControllerClient();
    parentControllerClient = new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());
    TestUtils.waitForNonDeterministicPushCompletion(Version.composeKafkaTopic(participantMessageStoreName, 1),
        controllerClient, 2, TimeUnit.MINUTES, Optional.of(logger));
    // Set up and configure the Zk shared store for METADATA_STORE
    ControllerResponse controllerResponse =
        parentControllerClient.createNewZkSharedStoreWithDefaultConfigs(zkSharedStoreName, "test");
    assertFalse(controllerResponse.isError(), "Failed to create the new Zk shared store");
    VersionCreationResponse versionCreationResponse = parentControllerClient.newZkSharedStoreVersion(zkSharedStoreName);
    // Verify the new version creation in parent and child controller
    assertFalse(versionCreationResponse.isError(), "Failed to create the new Zk shared store version");
    metadataStoreVersionNumber = versionCreationResponse.getVersion();
    Store zkSharedStore = parentController.getVeniceAdmin().getStore(clusterName, zkSharedStoreName);
    assertTrue(zkSharedStore.containsVersion(metadataStoreVersionNumber), "New version is missing");
    assertEquals(zkSharedStore.getCurrentVersion(), metadataStoreVersionNumber, "Unexpected current version");
  }

  @AfterClass
  public void cleanup() {
    controllerClient.close();
    parentControllerClient.close();
    parentController.close();
    venice.close();
    parentZk.close();
  }

  @Test
  public void testParticipantStoreKill() {
    VersionCreationResponse versionCreationResponse = getNewStoreVersion(parentControllerClient, true);
    assertFalse(versionCreationResponse.isError());
    String topicName = versionCreationResponse.getKafkaTopic();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Verify the push job is STARTED.
      assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.STARTED.toString());
    });
    String metricPrefix = "." + venice.getClusterName() + "-participant_store_consumption_task";
    double killedPushJobCount = venice.getVeniceServers().iterator().next().getMetricsRepository().metrics()
        .get(metricPrefix + "--killed_push_jobs.Count").value();
    ControllerResponse response = parentControllerClient.killOfflinePushJob(topicName);
    assertFalse(response.isError());
    verifyKillMessageInParticipantStore(topicName, true);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Poll job status to verify the job is indeed killed
      assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.ERROR.toString());
    });
    // Verify participant store consumption stats
    String requestMetricExample = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(venice.getClusterName())
        + "--success_request_key_count.Avg";
    Map<String, ? extends Metric> metrics = venice.getVeniceServers().iterator().next().getMetricsRepository().metrics();
    assertEquals(metrics.get(metricPrefix + "--killed_push_jobs.Count").value() - killedPushJobCount, 1.0);
    assertTrue(metrics.get(metricPrefix + "--kill_push_job_latency.Avg").value() > 0);
    // One from the server stats and the other from the client stats.
    assertTrue(metrics.get("." + requestMetricExample).value() > 0);
    assertTrue(metrics.get(".venice-client." + requestMetricExample).value() > 0);
  }

  @Test
  public void testKillWhenVersionIsOnline() {
    String storeName = TestUtils.getUniqueString("testKillWhenVersionIsOnline");
    final VersionCreationResponse versionCreationResponseForOnlineVersion = getNewStoreVersion(parentControllerClient, storeName, true);
    final String topicNameForOnlineVersion = versionCreationResponseForOnlineVersion.getKafkaTopic();
    parentControllerClient.writeEndOfPush(storeName, versionCreationResponseForOnlineVersion.getVersion());
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Verify the push job is COMPLETED and the version is online.
      StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
      assertTrue(storeInfo.getVersions().iterator().hasNext()
          && storeInfo.getVersions().iterator().next().getStatus().equals(VersionStatus.ONLINE),
          "Waiting for a version to become online");
    });
    parentControllerClient.killOfflinePushJob(topicNameForOnlineVersion);

    /**
     * Try to kill an ongoing push, since for the same store, all the admin messages are processed sequentially.
     * When the new version receives kill job, then it is safe to make an assertion about whether the previous
     * version receives a kill-job message or not.
     */
    final VersionCreationResponse versionCreationResponseForBootstrappingVersion = getNewStoreVersion(parentControllerClient, storeName, false);
    final String topicNameForBootstrappingVersion = versionCreationResponseForBootstrappingVersion.getKafkaTopic();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Verify the push job is STARTED.
      assertEquals(controllerClient.queryJobStatus(topicNameForBootstrappingVersion).getStatus(), ExecutionStatus.STARTED.toString());
    });
    ControllerResponse response = parentControllerClient.killOfflinePushJob(topicNameForBootstrappingVersion);
    assertFalse(response.isError());
    verifyKillMessageInParticipantStore(topicNameForBootstrappingVersion, true);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Poll job status to verify the job is indeed killed
      assertEquals(controllerClient.queryJobStatus(topicNameForBootstrappingVersion).getStatus(), ExecutionStatus.ERROR.toString());
    });
    // Then we could verify whether the previous version receives a kill-job or not.
    verifyKillMessageInParticipantStore(topicNameForOnlineVersion, false);

    venice.stopVeniceServer(veniceServerWrapper.getPort());
    // Ensure the partition assignment is 0 before restarting the server
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> venice.getRandomVeniceRouter()
        .getRoutingDataRepository().getPartitionAssignments(topicNameForOnlineVersion).getAssignedNumberOfPartitions() == 0);
    venice.restartVeniceServer(veniceServerWrapper.getPort());
    int expectedOnlineReplicaCount = versionCreationResponseForOnlineVersion.getReplicas();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      for (int p = 0; p < versionCreationResponseForOnlineVersion.getPartitions(); p++) {
        assertEquals(venice.getRandomVeniceRouter().getRoutingDataRepository()
                .getReadyToServeInstances(topicNameForOnlineVersion, p).size(),
            expectedOnlineReplicaCount, "Not all replicas are ONLINE yet");
      }
    });

    // Now, try to delete the version and the corresponding kill message should be present even for an ONLINE version
    // Push a new version so the ONLINE version can be deleted to mimic retiring an old version.
    VersionCreationResponse newVersionResponse = getNewStoreVersion(parentControllerClient, storeName, false);
    parentControllerClient.writeEndOfPush(storeName, newVersionResponse.getVersion());
    TestUtils.waitForNonDeterministicPushCompletion(newVersionResponse.getKafkaTopic(),
        parentControllerClient, 30, TimeUnit.SECONDS, Optional.empty());
    parentControllerClient.deleteOldVersion(storeName, Version.parseVersionFromKafkaTopicName(topicNameForOnlineVersion));
    verifyKillMessageInParticipantStore(topicNameForOnlineVersion, true);
  }

  /**
   * Alternatively, to break the test into smaller ones is to enforce execution order or dependency of tests since if
   * the Zk shared store tests fail then tests related to materializing the metadata store will definitely fail as well.
   */
  @Test
  public void testMetadataStore() throws Exception {
    // Create a new Venice store and materialize the corresponding metadata system store
    String regularVeniceStoreName = TestUtils.getUniqueString("regular_store");
    NewStoreResponse newStoreResponse = parentControllerClient.createNewStore(regularVeniceStoreName, "test",
        STRING_SCHEMA, USER_SCHEMA_STRING);
    assertFalse(newStoreResponse.isError(), "Failed to create the regular Venice store");
    ControllerResponse controllerResponse =
        parentControllerClient.materializeMetadataStoreVersion(regularVeniceStoreName, metadataStoreVersionNumber);
    assertFalse(controllerResponse.isError(), "Failed to materialize the new Zk shared store version");
    String metadataStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreUtils.getMetadataStoreName(regularVeniceStoreName), metadataStoreVersionNumber);
    TestUtils.waitForNonDeterministicPushCompletion(metadataStoreTopic, controllerClient, 30, TimeUnit.SECONDS,
        Optional.empty());

    controllerResponse = parentControllerClient.addValueSchema(regularVeniceStoreName, USER_SCHEMA_STRING_WITH_DEFAULT);
    assertFalse(controllerResponse.isError(), "Failed to add new store value schema");

    // Try to read from the metadata store
    venice.refreshAllRouterMetaData();

    // Create a base client config for metadata store repository.
    ClientConfig<StoreMetadataValue> clientConfig = ClientConfig.defaultSpecificClientConfig(regularVeniceStoreName, StoreMetadataValue.class)
        .setD2ServiceName(ClientConfig.DEFAULT_D2_SERVICE_NAME).setD2Client(d2Client).setVeniceURL(venice.getZk().getAddress());
    // Create MetadataStoreBasedStoreRepository
    MetadataStoreBasedStoreRepository storeRepository = new MetadataStoreBasedStoreRepository(venice.getClusterName(), clientConfig, 60);
    // Create test listener to monitor store repository changes.
    AtomicInteger creationCount = new AtomicInteger(0);
    AtomicInteger changeCount = new AtomicInteger(0);
    AtomicInteger deletionCount = new AtomicInteger(0);
    TestListener testListener = new TestListener();
    storeRepository.registerStoreDataChangedListener(testListener);
    // Verify initial state
    assertListenerCounts(testListener, creationCount.get(), changeCount.get(), deletionCount.get(), "initialization");

    // subscribe() method will wait for up to 30 seconds to subscribe to the store.
    storeRepository.subscribe(regularVeniceStoreName);
    assertListenerCounts(testListener, creationCount.addAndGet(1), changeCount.get(), deletionCount.get(), "store creations");

    StoreMetadataKey storeAttributesKey = new StoreMetadataKey();
    storeAttributesKey.keyStrings = Arrays.asList(regularVeniceStoreName);
    storeAttributesKey.metadataType = StoreMetadataType.STORE_ATTRIBUTES.getValue();
    StoreMetadataKey storeTargetVersionStatesKey = new StoreMetadataKey();
    storeTargetVersionStatesKey.keyStrings = Arrays.asList(regularVeniceStoreName);
    storeTargetVersionStatesKey.metadataType = StoreMetadataType.TARGET_VERSION_STATES.getValue();
    StoreMetadataKey storeCurrentStatesKey = new StoreMetadataKey();
    storeCurrentStatesKey.keyStrings = Arrays.asList(regularVeniceStoreName, clusterName);
    storeCurrentStatesKey.metadataType = StoreMetadataType.CURRENT_STORE_STATES.getValue();
    StoreMetadataKey storeCurrentVersionStatesKey = new StoreMetadataKey();
    storeCurrentVersionStatesKey.keyStrings = Arrays.asList(regularVeniceStoreName, clusterName);
    storeCurrentVersionStatesKey.metadataType = StoreMetadataType.CURRENT_VERSION_STATES.getValue();
    StoreMetadataKey storeKeySchemasKey = new StoreMetadataKey();
    storeKeySchemasKey.keyStrings = Arrays.asList(regularVeniceStoreName, clusterName);
    storeKeySchemasKey.metadataType = StoreMetadataType.STORE_KEY_SCHEMAS.getValue();
    StoreMetadataKey storeValueSchemasKey = new StoreMetadataKey();
    storeValueSchemasKey.keyStrings = Arrays.asList(regularVeniceStoreName, clusterName);
    storeValueSchemasKey.metadataType = StoreMetadataType.STORE_VALUE_SCHEMAS.getValue();

    try (AvroSpecificStoreClient<StoreMetadataKey, StoreMetadataValue> client =
        ClientFactory.getAndStartSpecificAvroClient(ClientConfig.defaultSpecificClientConfig
            (VeniceSystemStoreUtils.getMetadataStoreName(regularVeniceStoreName),
                StoreMetadataValue.class).setVeniceURL(venice.getRandomRouterURL()))) {

      // Subscribe to existing store won't change listener count.
      storeRepository.subscribe(regularVeniceStoreName);
      assertListenerCounts(testListener, creationCount.get(), changeCount.get(), deletionCount.get(), "store creations");
      Store store = storeRepository.getStore(regularVeniceStoreName);
      assertEquals(store.getName(), regularVeniceStoreName);
      assertEquals(store.getVersions().size(), 0);
      assertEquals(store.getCurrentVersion(), 0);

      // Perform some checks to ensure the metadata store values are populated
      StoreAttributes storeAttributes  = (StoreAttributes) client.get(storeAttributesKey).get().metadataUnion;
      assertEquals(storeAttributes.sourceCluster.toString(), clusterName, "Unexpected sourceCluster");
      TargetVersionStates targetVersionStates =
          (TargetVersionStates) client.get(storeTargetVersionStatesKey).get().metadataUnion;
      assertTrue(targetVersionStates.targetVersionStates.isEmpty(), "targetVersionStates should be empty");
      CurrentStoreStates currentStoreStates = (CurrentStoreStates) client.get(storeCurrentStatesKey).get().metadataUnion;
      assertEquals(currentStoreStates.states.name.toString(), regularVeniceStoreName, "Unexpected store name");
      CurrentVersionStates currentVersionStates =
          (CurrentVersionStates) client.get(storeCurrentVersionStatesKey).get().metadataUnion;
      assertEquals(currentVersionStates.currentVersion, Store.NON_EXISTING_VERSION, "Unexpected current version");
      assertTrue(currentVersionStates.currentVersionStates.isEmpty(), "Version list should be empty");
      // New push to the Venice store should be reflected in the corresponding metadata store
      VersionCreationResponse newVersionResponse = parentControllerClient.requestTopicForWrites(regularVeniceStoreName,
          1024, Version.PushType.BATCH, Version.guidBasedDummyPushId(), true, false,
          Optional.empty(), Optional.empty());
      assertFalse(newVersionResponse.isError());
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        assertFalse(((CurrentVersionStates) client.get(storeCurrentVersionStatesKey).get().metadataUnion)
            .currentVersionStates.isEmpty());
      });
      currentVersionStates = (CurrentVersionStates) client.get(storeCurrentVersionStatesKey).get().metadataUnion;
      assertEquals(currentVersionStates.currentVersionStates.get(0).status.toString(), STARTED.name());
      parentControllerClient.writeEndOfPush(regularVeniceStoreName, newVersionResponse.getVersion());
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, () -> {
        assertEquals(((CurrentVersionStates) client.get(storeCurrentVersionStatesKey).get().metadataUnion)
            .currentVersion, newVersionResponse.getVersion());
      });
      currentVersionStates = (CurrentVersionStates) client.get(storeCurrentVersionStatesKey).get().metadataUnion;
      assertEquals(currentVersionStates.currentVersionStates.get(0).status.toString(), ONLINE.name());

      storeRepository.refreshOneStore(regularVeniceStoreName);
      assertListenerCounts(testListener, creationCount.get(), changeCount.addAndGet(1), deletionCount.get(), "change of a store-version");
      store = storeRepository.getStore(regularVeniceStoreName);
      assertEquals(store.getVersions().size(), 1);
      assertEquals(store.getCurrentVersion(), 1);

      // Make sure keySchemaMap and valueSchemaMap has correct number of entries.
      StoreKeySchemas storeKeySchemas = (StoreKeySchemas) client.get(storeKeySchemasKey).get().metadataUnion;
      assertEquals(storeKeySchemas.keySchemaMap.size(), 1);
      StoreValueSchemas storeValueSchemas = (StoreValueSchemas) client.get(storeValueSchemasKey).get().metadataUnion;
      assertEquals(storeValueSchemas.valueSchemaMap.size(), 2);

      // CharSequence is not comparable. We have to peform the CharSequence to String map conversion so the get() from map won't fail.
      Map<String, String> keySchemaMap = Utils.getStringMapFromCharSequenceMap(storeKeySchemas.keySchemaMap);
      CharSequence keySchemaStr = keySchemaMap.get(Integer.toString(1));
      assertNotNull(keySchemaStr);
      assertEquals(keySchemaStr.toString(), STRING_SCHEMA);
      assertEquals(storeRepository.getKeySchema(regularVeniceStoreName).getSchema().toString(), STRING_SCHEMA);

      Map<String, String> valueSchemaMap = Utils.getStringMapFromCharSequenceMap(storeValueSchemas.valueSchemaMap);
      CharSequence valueSchemaStr = valueSchemaMap.get(Integer.toString(1));
      assertNotNull(valueSchemaStr);
      // We need to reparse the schema string so it won't have field ordering issue.
      String parsedSchemaStr = Schema.parse(USER_SCHEMA_STRING).toString();
      assertEquals(valueSchemaStr.toString(), parsedSchemaStr);
      assertEquals(storeRepository.getValueSchema(regularVeniceStoreName, 1).getSchema().toString(), parsedSchemaStr);
      // Check evolved value schema after we materialized the metadata store.
      valueSchemaStr = valueSchemaMap.get(Integer.toString(2));
      assertNotNull(valueSchemaStr);
      parsedSchemaStr = Schema.parse(USER_SCHEMA_STRING_WITH_DEFAULT).toString();
      assertEquals(valueSchemaStr.toString(), parsedSchemaStr);
      assertEquals(storeRepository.getValueSchema(regularVeniceStoreName, 2).getSchema().toString(), parsedSchemaStr);
    }

    // Verify deletion notification
    storeRepository.unsubscribe(regularVeniceStoreName);
    assertListenerCounts(testListener, creationCount.get(), changeCount.get(), deletionCount.addAndGet(1), "store deletion");

    storeRepository.subscribe(regularVeniceStoreName);
    assertListenerCounts(testListener, creationCount.addAndGet(1), changeCount.get(), deletionCount.get(), "store creations");

    // Verify deletion notification
    storeRepository.clear();
    assertListenerCounts(testListener, creationCount.get(), changeCount.get(), deletionCount.addAndGet(1), "store deletion");

    // Verify that refresh() does not double count notifications...
    storeRepository.refresh();
    assertListenerCounts(testListener, creationCount.get(), changeCount.get(), deletionCount.get(), "ReadOnly Repo refresh()");

    // Prepare a new version with a dummy key-value pair.
    GenericRecord expectedValueRecord = new GenericData.Record(Schema.parse(USER_SCHEMA_STRING_WITH_DEFAULT));
    expectedValueRecord.put("id", "testId");
    expectedValueRecord.put("name", "testName");
    expectedValueRecord.put("age", 1);
    venice.createVersion(regularVeniceStoreName,
        STRING_SCHEMA,
        USER_SCHEMA_STRING_WITH_DEFAULT,
        Stream.of(new AbstractMap.SimpleEntry<>("testKey", expectedValueRecord))
    );

    String baseDataPath = TestUtils.getTempDataDirectory().getAbsolutePath();
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(ConfigKeys.DATA_BASE_PATH, baseDataPath)
        .put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
        .build();
    MetricsRepository metricsRepository = new MetricsRepository();
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    long memoryLimit = 1024 * 1024 * 1024; // 1GB
    daVinciConfig.setRocksDBMemoryLimit(memoryLimit);
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig);
      DaVinciClient<String, GenericRecord> client = factory.getAndStartGenericAvroClient(regularVeniceStoreName, daVinciConfig)) {
      client.subscribeAll().get();
      GenericRecord actualValueRecord = client.get("testKey").get();
      assertEquals(actualValueRecord, expectedValueRecord);
    }

      // Dematerialize the metadata store version and it should be cleaned up properly.
    controllerResponse = parentControllerClient.dematerializeMetadataStoreVersion(regularVeniceStoreName,
        metadataStoreVersionNumber);
    assertFalse(controllerResponse.isError(), "Failed to dematerialize metadata store version");
    assertFalse(parentController.getVeniceAdmin().getStore(clusterName, regularVeniceStoreName).isStoreMetadataSystemStoreEnabled());
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      assertFalse(venice.getVeniceControllers().get(0).getVeniceAdmin().isResourceStillAlive(metadataStoreTopic));
      assertTrue(venice.getVeniceControllers().get(0).getVeniceAdmin().isTopicTruncated(metadataStoreTopic));
      assertTrue(venice.getVeniceControllers().get(0).getVeniceAdmin()
          .isTopicTruncated(Version.composeRealTimeTopic(VeniceSystemStoreUtils.getMetadataStoreName(regularVeniceStoreName))));
    });

  }

  @Test
  public void testDeleteStoreDematerializesMetadataStoreVersion() {
    // Create a new Venice store and materialize the corresponding metadata system store
    String regularVeniceStoreName = TestUtils.getUniqueString("regular_store_to_delete");
    NewStoreResponse newStoreResponse = parentControllerClient.createNewStore(regularVeniceStoreName, "test",
        STRING_SCHEMA, STRING_SCHEMA);
    assertFalse(newStoreResponse.isError(), "Failed to create the regular Venice store");
    ControllerResponse controllerResponse =
        parentControllerClient.materializeMetadataStoreVersion(regularVeniceStoreName, metadataStoreVersionNumber);
    assertFalse(controllerResponse.isError(), "Failed to materialize the new Zk shared store version");
    String metadataStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreUtils.getMetadataStoreName(regularVeniceStoreName), metadataStoreVersionNumber);
    TestUtils.waitForNonDeterministicPushCompletion(metadataStoreTopic, controllerClient, 30, TimeUnit.SECONDS,
        Optional.empty());
    assertTrue(venice.getVeniceControllers().get(0).getVeniceAdmin().isResourceStillAlive(metadataStoreTopic));
    assertFalse(venice.getVeniceControllers().get(0).getVeniceAdmin().isTopicTruncated(metadataStoreTopic));
    // Delete the Venice store and verify its metadata store version is dematerialized.
    controllerResponse = parentControllerClient.disableAndDeleteStore(regularVeniceStoreName);
    assertFalse(controllerResponse.isError(), "Failed to delete the regular Venice store");
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      assertFalse(venice.getVeniceControllers().get(0).getVeniceAdmin().isResourceStillAlive(metadataStoreTopic));
      assertTrue(venice.getVeniceControllers().get(0).getVeniceAdmin().isTopicTruncated(metadataStoreTopic));
    });
  }

  @Test
  public void testZkSharedStoreCanBeRecreated() {
    ControllerResponse controllerResponse = parentControllerClient.disableAndDeleteStore(zkSharedStoreName);
    assertFalse(controllerResponse.isError(), "Failed to delete the zk shared store: " + zkSharedStoreName);
    controllerResponse = parentControllerClient.createNewZkSharedStoreWithDefaultConfigs(zkSharedStoreName, "test");
    assertFalse(controllerResponse.isError(), "Failed to recreate the Zk shared store");
    VersionCreationResponse versionCreationResponse = parentControllerClient.newZkSharedStoreVersion(zkSharedStoreName);
    // Verify the new version creation in parent and child controller
    assertFalse(versionCreationResponse.isError(), "Failed to create the new Zk shared store version");
    metadataStoreVersionNumber = versionCreationResponse.getVersion();
    Store zkSharedStore = parentController.getVeniceAdmin().getStore(clusterName, zkSharedStoreName);
    assertTrue(zkSharedStore.containsVersion(metadataStoreVersionNumber), "New version is missing");
    assertEquals(zkSharedStore.getCurrentVersion(), metadataStoreVersionNumber, "Unexpected current version");
  }

  static class TestListener implements StoreDataChangedListener {
    AtomicInteger creationCount = new AtomicInteger(0);
    AtomicInteger changeCount = new AtomicInteger(0);
    AtomicInteger deletionCount = new AtomicInteger(0);

    @Override
    public void handleStoreCreated(Store store) {
      creationCount.incrementAndGet();
    }

    @Override
    public void handleStoreDeleted(String storeName) {
      deletionCount.incrementAndGet();
    }

    @Override
    public void handleStoreChanged(Store store) {
      logger.info("Received handleStoreChanged: " + store.toString());
      changeCount.incrementAndGet();
    }

    public int getCreationCount() {
      return creationCount.get();
    }

    public int getChangeCount() {
      return changeCount.get();
    }

    public int getDeletionCount() {
      return deletionCount.get();
    }
  }
  private void assertListenerCounts(TestListener testListener,
                                    int expectedCreationCount,
                                    int expectedChangeCount,
                                    int expectedDeletionCount,
                                    String details) {
    TestUtils.waitForNonDeterministicAssertion(3000, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(testListener.getCreationCount(), expectedCreationCount,
              "Listener's creation count should be " + expectedCreationCount + " following: " + details);
      Assert.assertEquals(testListener.getChangeCount(), expectedChangeCount,
              "Listener's change count should be " + expectedChangeCount + " following: " + details);
      Assert.assertEquals(testListener.getDeletionCount(), expectedDeletionCount,
              "Listener's deletion count should be " + expectedDeletionCount + " following: " + details);

      logger.info("Successfully asserted that notifications work after " + details);
    });
  }

  private void verifyKillMessageInParticipantStore(String topic, boolean shouldPresent) {
    // Verify the kill push message is in the participant message store.
    ParticipantMessageKey key = new ParticipantMessageKey();
    key.resourceName = topic;
    key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
    try (AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig.defaultSpecificClientConfig(participantMessageStoreName,
                ParticipantMessageValue.class).setVeniceURL(venice.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        try {
          if (shouldPresent) {
            // Verify that the kill offline message has made it to the participant message store.
            assertNotNull(client.get(key).get());
          } else {
            assertNull(client.get(key).get());
          }
        } catch (Exception e) {
          fail();
        }
      });
    }
  }

  private VersionCreationResponse getNewStoreVersion(ControllerClient controllerClient, String storeName, boolean newStore) {
    if (newStore) {
      controllerClient.createNewStore(storeName, "test-user", "\"string\"", "\"string\"");
    }
    return parentControllerClient.requestTopicForWrites(storeName, 1024,
        Version.PushType.BATCH, Version.guidBasedDummyPushId(), true, true, Optional.empty(), Optional.empty());
  }


  private VersionCreationResponse getNewStoreVersion(ControllerClient controllerClient, boolean newStore) {
    return getNewStoreVersion(controllerClient, TestUtils.getUniqueString("test-kill"), newStore);
  }
}
