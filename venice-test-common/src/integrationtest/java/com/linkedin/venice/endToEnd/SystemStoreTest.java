package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.repository.NativeMetadataRepository;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.MetadataStoreUtils;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.TopicManager;
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
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.WriteComputeSchemaAdapter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.CollectionUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.meta.IngestionMode.*;
import static com.linkedin.venice.meta.PersistenceType.*;
import static com.linkedin.venice.meta.VersionStatus.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class SystemStoreTest {
  private static final Logger logger = Logger.getLogger(SystemStoreTest.class);
  private final static String INT_KEY_SCHEMA = "\"int\"";

  private VeniceClusterWrapper venice;
  private VeniceControllerWrapper parentController;
  private ZkServerWrapper parentZk;
  private ControllerClient controllerClient;
  private ControllerClient parentControllerClient;
  private String participantMessageStoreName;
  private VeniceServerWrapper veniceServerWrapper;
  private String clusterName;
  private String zkSharedStoreName;
  private int metadataStoreVersionNumber;
  private D2Client d2Client;

  @BeforeClass
  public void setup() {
    Properties controllerConfig = new Properties();
    Properties serverFeatureProperties = new Properties();
    Properties serverProperties = new Properties();
    controllerConfig.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
    controllerConfig.setProperty(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, "false");
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    controllerConfig.setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS,
        String.valueOf(Long.MAX_VALUE));
    controllerConfig.setProperty(CONTROLLER_AUTO_MATERIALIZE_METADATA_SYSTEM_STORE_ENABLED, String.valueOf(true));
    controllerConfig.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, String.valueOf(true));
    venice = ServiceFactory.getVeniceCluster(1, 0, 1, 1,
        100000, false, false, controllerConfig);
    clusterName = venice.getClusterName();
    zkSharedStoreName = VeniceSystemStoreUtils.getSharedZkNameForMetadataStore(clusterName);
    d2Client = D2TestUtils.getAndStartD2Client(venice.getZk().getAddress());
    serverFeatureProperties.put(VeniceServerWrapper.CLIENT_CONFIG_FOR_CONSUMER, ClientConfig.defaultGenericClientConfig("")
        .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME)
        .setD2Client(d2Client));
    serverProperties.setProperty(PARTICIPANT_MESSAGE_CONSUMPTION_DELAY_MS, Long.toString(100));
    veniceServerWrapper = venice.addVeniceServer(serverFeatureProperties, serverProperties);
    parentZk = ServiceFactory.getZkServer();
    parentController =
        ServiceFactory.getVeniceParentController(venice.getClusterName(), parentZk.getAddress(), venice.getKafka(),
            venice.getVeniceControllers().toArray(new VeniceControllerWrapper[0]),
            new VeniceProperties(controllerConfig), false);
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
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(parentController);
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    IOUtils.closeQuietly(venice);
    IOUtils.closeQuietly(parentZk);
  }

  @Test
  public void testMetadataSystemStoreAutoCreation() {
    // Once the cluster is up and running, the metadata system store should be automatically created
    String metadataSystemStoreName = AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getSystemStoreName();
    StoreResponse metadataSystemStoreFromParent = parentControllerClient.getStore(metadataSystemStoreName);
    StoreResponse metadataSystemStoreFromChild = controllerClient.getStore(metadataSystemStoreName);
    for (StoreResponse storeResponse : Arrays.asList(metadataSystemStoreFromParent, metadataSystemStoreFromChild)) {
      Assert.assertFalse(storeResponse.isError(), metadataSystemStoreName + " should exist in both parent and child");
      StoreInfo storeInfo = storeResponse.getStore();
      assertTrue(storeInfo.getHybridStoreConfig() != null, metadataSystemStoreName + " should be a hybrid store");
      assertTrue(storeInfo.isLeaderFollowerModelEnabled(), metadataSystemStoreName + " should enable L/F model");
      assertTrue(storeInfo.isWriteComputationEnabled(), metadataSystemStoreName + " should enable write compute");
    }
    // Check key/value/derived schemas
    SchemaResponse keySchemaResponse = controllerClient.getKeySchema(metadataSystemStoreName);
    assertFalse(keySchemaResponse.isError(), "Unexpected error while fetching key schema: " + keySchemaResponse.getError());
    assertEquals(Schema.parse(keySchemaResponse.getSchemaStr()), AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE_KEY.getCurrentProtocolVersionSchema());
    SchemaResponse latestValueSchemaResponse = controllerClient.getValueSchema(metadataSystemStoreName, AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion());
    assertFalse(latestValueSchemaResponse.isError(), "Unexpected error while fetching the latest value schemas: " + latestValueSchemaResponse.getError());
    assertEquals(Schema.parse(latestValueSchemaResponse.getSchemaStr()), AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema());
    // Check the derived compute schema for the latest value schema
    String derivedComputeSchema = WriteComputeSchemaAdapter.parse(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema()).toString();
    SchemaResponse derivedSchemaResponse = controllerClient.getValueOrDerivedSchemaId(metadataSystemStoreName, derivedComputeSchema);
    assertFalse(derivedSchemaResponse.isError(), "Unexpected error while fetching the derived schema id for the latest value schema: " + derivedSchemaResponse.getError());
    // Create a new user store and its corresponding meta system store should also be materialized.
    String regularVeniceStoreName = TestUtils.getUniqueString("test_auto_creation");
    NewStoreResponse newStoreResponse = parentControllerClient.createNewStore(regularVeniceStoreName, "test",
        STRING_SCHEMA, USER_SCHEMA_STRING);
    assertFalse(newStoreResponse.isError(), "Failed to create the regular Venice store");
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(regularVeniceStoreName), 1),
        controllerClient, 30, TimeUnit.SECONDS, Optional.empty());
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
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


  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testParticipantStoreThrottlerRestartRouter() {
    VersionCreationResponse versionCreationResponse = getNewStoreVersion(parentControllerClient, true);
    assertFalse(versionCreationResponse.isError());
    String topicName = versionCreationResponse.getKafkaTopic();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Verify the push job is STARTED.
      assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.STARTED.toString());
    });
    ControllerResponse response = parentControllerClient.killOfflinePushJob(topicName);
    assertFalse(response.isError());
    verifyKillMessageInParticipantStore(topicName, true);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Poll job status to verify the job is indeed killed
      assertEquals(controllerClient.queryJobStatus(topicName).getStatus(), ExecutionStatus.ERROR.toString());
    });

    // restart routers to discard in-memory throttler info
    for (VeniceRouterWrapper router : venice.getVeniceRouters()) {
      venice.stopVeniceRouter(router.getPort());
      venice.restartVeniceRouter(router.getPort());
    }
    // Verify still can read from participant stores.
    ParticipantMessageKey key = new ParticipantMessageKey();
    key.resourceName = topicName;
    key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
    try (AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig.defaultSpecificClientConfig(participantMessageStoreName,
                ParticipantMessageValue.class).setVeniceURL(venice.getRandomRouterURL()))) {
      try {
        client.get(key).get();
      } catch (Exception e) {
        fail("Should be able to query participant store successfully");
      }
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
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
    Assert.assertFalse(newVersionResponse.isError(), "Controller error: " + newVersionResponse.getError());
    parentControllerClient.writeEndOfPush(storeName, newVersionResponse.getVersion());
    TestUtils.waitForNonDeterministicPushCompletion(newVersionResponse.getKafkaTopic(),
        parentControllerClient, 30, TimeUnit.SECONDS, Optional.empty());
    parentControllerClient.deleteOldVersion(storeName, Version.parseVersionFromKafkaTopicName(topicNameForOnlineVersion));
    verifyKillMessageInParticipantStore(topicNameForOnlineVersion, true);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testMetadataStore() throws Exception {
    // Create a new Venice store and materialize the corresponding metadata system store
    String regularVeniceStoreName = TestUtils.getUniqueString("regular_store");
    NewStoreResponse newStoreResponse = parentControllerClient.createNewStore(regularVeniceStoreName, "test",
        STRING_SCHEMA, USER_SCHEMA_STRING);
    assertFalse(newStoreResponse.isError(), "Failed to create the regular Venice store");
    String metadataStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreUtils.getMetadataStoreName(regularVeniceStoreName), metadataStoreVersionNumber);
    // The corresponding metadata store should be materialized automatically.
    TestUtils.waitForNonDeterministicPushCompletion(metadataStoreTopic, controllerClient, 30, TimeUnit.SECONDS,
        Optional.empty());

    ControllerResponse controllerResponse = parentControllerClient.addValueSchema(regularVeniceStoreName, USER_SCHEMA_STRING_WITH_DEFAULT);
    assertFalse(controllerResponse.isError(), "Failed to add new store value schema");

    // Try to read from the metadata store
    venice.refreshAllRouterMetaData();

    // Create a base client config for metadata store repository.
    ClientConfig<StoreMetadataValue> clientConfig = ClientConfig.defaultSpecificClientConfig(regularVeniceStoreName,
        StoreMetadataValue.class).setD2ServiceName(ClientConfig.DEFAULT_D2_SERVICE_NAME)
        .setD2Client(d2Client).setVeniceURL(venice.getZk().getAddress());
    // Create MetadataStoreBasedStoreRepository
    NativeMetadataRepository storeRepository = NativeMetadataRepository.getInstance(clientConfig, new VeniceProperties());
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

    StoreMetadataKey storeAttributesKey = MetadataStoreUtils.getStoreAttributesKey(regularVeniceStoreName);
    StoreMetadataKey storeCurrentStatesKey = MetadataStoreUtils.getCurrentStoreStatesKey(regularVeniceStoreName, clusterName);
    StoreMetadataKey storeCurrentVersionStatesKey = MetadataStoreUtils.getCurrentVersionStatesKey(regularVeniceStoreName, clusterName);
    StoreMetadataKey storeKeySchemasKey = MetadataStoreUtils.getStoreKeySchemasKey(regularVeniceStoreName);
    StoreMetadataKey storeValueSchemasKey = MetadataStoreUtils.getStoreValueSchemasKey(regularVeniceStoreName);

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
      CurrentStoreStates currentStoreStates = (CurrentStoreStates) client.get(storeCurrentStatesKey).get().metadataUnion;
      assertEquals(currentStoreStates.states.name.toString(), regularVeniceStoreName, "Unexpected store name");
      CurrentVersionStates currentVersionStates =
          (CurrentVersionStates) client.get(storeCurrentVersionStatesKey).get().metadataUnion;
      assertEquals(currentVersionStates.currentVersion, Store.NON_EXISTING_VERSION, "Unexpected current version");
      assertTrue(currentVersionStates.currentVersionStates.isEmpty(), "Version list should be empty");
      // New push to the Venice store should be reflected in the corresponding metadata store
      VersionCreationResponse newVersionResponse = parentControllerClient.requestTopicForWrites(regularVeniceStoreName,
          1024, Version.PushType.BATCH, Version.guidBasedDummyPushId(), true, false, false,
          Optional.empty(), Optional.empty(), Optional.empty(), false, -1);
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
      Map<String, String> keySchemaMap = CollectionUtils.getStringMapFromCharSequenceMap(storeKeySchemas.keySchemaMap);
      CharSequence keySchemaStr = keySchemaMap.get(Integer.toString(1));
      assertNotNull(keySchemaStr);
      assertEquals(keySchemaStr.toString(), STRING_SCHEMA);
      assertEquals(storeRepository.getKeySchema(regularVeniceStoreName).getSchema().toString(), STRING_SCHEMA);

      Map<String, String> valueSchemaMap = CollectionUtils.getStringMapFromCharSequenceMap(storeValueSchemas.valueSchemaMap);
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

    // Dematerialize the metadata store version and it should be cleaned up properly.
    controllerResponse = parentControllerClient.dematerializeMetadataStoreVersion(regularVeniceStoreName,
        metadataStoreVersionNumber);
    assertFalse(controllerResponse.isError(), "Failed to dematerialize metadata store version");
    assertFalse(parentController.getVeniceAdmin().getStore(clusterName, regularVeniceStoreName).isStoreMetadataSystemStoreEnabled());
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      assertFalse(venice.getVeniceControllers().get(0).getVeniceAdmin().isResourceStillAlive(metadataStoreTopic));
      assertTrue(venice.getVeniceControllers().get(0).getVeniceAdmin().isTopicTruncated(metadataStoreTopic));
    });
  }

  @Test(timeOut = 90 * Time.MS_PER_SECOND)
  public void testDaVinciIngestionWithMetadataStore() throws Exception {
    // Create a new Venice store and materialize the corresponding metadata system store
    String regularVeniceStoreName = TestUtils.getUniqueString("regular_store_daVinci_ingestion");
    NewStoreResponse newStoreResponse = parentControllerClient.createNewStore(regularVeniceStoreName, "test",
        STRING_SCHEMA, USER_SCHEMA_STRING);
    assertFalse(newStoreResponse.isError(), "Failed to create the regular Venice store");
    String metadataStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreUtils.getMetadataStoreName(regularVeniceStoreName), metadataStoreVersionNumber);
    // The corresponding metadata store should be materialized automatically.
    TestUtils.waitForNonDeterministicPushCompletion(metadataStoreTopic, controllerClient, 30, TimeUnit.SECONDS,
        Optional.empty());

    // Prepare a new version with a dummy key-value pair.
    GenericRecord expectedValueRecord = new GenericData.Record(Schema.parse(USER_SCHEMA_STRING));
    expectedValueRecord.put("id", "testId");
    expectedValueRecord.put("name", "testName");
    expectedValueRecord.put("age", 1);
    venice.createVersion(regularVeniceStoreName,
        STRING_SCHEMA,
        USER_SCHEMA_STRING,
        Stream.of(new AbstractMap.SimpleEntry<>("testKey", expectedValueRecord))
    );

    String baseDataPath = TestUtils.getTempDataDirectory().getAbsolutePath();
    // Test Da Vinci client ingestion with system store.
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .build();
    MetricsRepository metricsRepository = new MetricsRepository();
    DaVinciConfig daVinciConfig = new DaVinciConfig();
    long memoryLimit = 1024 * 1024 * 1024; // 1GB
    daVinciConfig.setMemoryLimit(memoryLimit);
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
      DaVinciClient<String, GenericRecord> client = factory.getAndStartGenericAvroClient(regularVeniceStoreName, daVinciConfig);
      client.subscribeAll().get();
      assertEquals(client.get("testKey").get().toString(), expectedValueRecord.toString());
      client.unsubscribeAll();
    }

    int applicationListenerPort = Utils.getFreePort();
    int servicePort = Utils.getFreePort();
    // Test Da Vinci client ingestion with both system store && ingestion isolation
    backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(SERVER_INGESTION_MODE, ISOLATED)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, venice.getZk().getAddress())
        .build();
    metricsRepository = new MetricsRepository();
    daVinciConfig = new DaVinciConfig();
    daVinciConfig.setMemoryLimit(memoryLimit);
    try (CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(d2Client, metricsRepository, backendConfig)) {
      DaVinciClient<String, GenericRecord> client = factory.getAndStartGenericAvroClient(regularVeniceStoreName, daVinciConfig);
      client.subscribeAll().get();
      assertEquals(client.get("testKey").get().toString(), expectedValueRecord.toString());
      client.unsubscribeAll();
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testDeleteStoreDematerializesMetadataStoreVersion() {
    // Create a new Venice store and materialize the corresponding metadata system store
    String regularVeniceStoreName = TestUtils.getUniqueString("regular_store_to_delete");
    NewStoreResponse newStoreResponse = parentControllerClient.createNewStore(regularVeniceStoreName, "test",
        STRING_SCHEMA, STRING_SCHEMA);
    assertFalse(newStoreResponse.isError(), "Failed to create the regular Venice store");
    String metadataStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreUtils.getMetadataStoreName(regularVeniceStoreName), metadataStoreVersionNumber);
    // The corresponding metadata store should be materialized automatically.
    TestUtils.waitForNonDeterministicPushCompletion(metadataStoreTopic, controllerClient, 30, TimeUnit.SECONDS,
        Optional.empty());
    assertTrue(venice.getVeniceControllers().get(0).getVeniceAdmin().isResourceStillAlive(metadataStoreTopic));
    assertFalse(venice.getVeniceControllers().get(0).getVeniceAdmin().isTopicTruncated(metadataStoreTopic));
    // Delete the Venice store and verify its metadata store version is dematerialized.
    ControllerResponse controllerResponse = parentControllerClient.disableAndDeleteStore(regularVeniceStoreName);
    assertFalse(controllerResponse.isError(), "Failed to delete the regular Venice store");
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      assertNull(venice.getVeniceControllers().get(0).getVeniceAdmin().getMetadataStoreWriter().getMetadataStoreWriter(regularVeniceStoreName));
      assertFalse(venice.getVeniceControllers().get(0).getVeniceAdmin().isResourceStillAlive(metadataStoreTopic));
      assertTrue(venice.getVeniceControllers().get(0).getVeniceAdmin().isTopicTruncated(metadataStoreTopic));
      assertTrue(venice.getVeniceControllers().get(0).getVeniceAdmin().isTopicTruncated(
          Version.composeRealTimeTopic(VeniceSystemStoreUtils.getMetadataStoreName(regularVeniceStoreName))));
    });
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND, groups = {"flaky"})
  public void testReMaterializeMetadataSystemStore() throws ExecutionException {
    // Create a new Venice store with metadata system store materialized.
    String regularVeniceStoreName = TestUtils.getUniqueString("regular_store_to_re_materialize");
    NewStoreResponse newStoreResponse = parentControllerClient.createNewStore(regularVeniceStoreName, "test",
        STRING_SCHEMA, STRING_SCHEMA);
    assertFalse(newStoreResponse.isError(), "Failed to create the regular Venice store");
    String metadataStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreUtils.getMetadataStoreName(regularVeniceStoreName), metadataStoreVersionNumber);
    // The corresponding metadata store should be materialized automatically.
    TestUtils.waitForNonDeterministicPushCompletion(metadataStoreTopic, controllerClient, 30, TimeUnit.SECONDS,
        Optional.empty());
    // Dematerialize the metadata system store.
    assertFalse(parentControllerClient.dematerializeMetadataStoreVersion(regularVeniceStoreName,
        metadataStoreVersionNumber).isError());
    verifyKillMessageInParticipantStore(metadataStoreTopic, true);
    // Re-materialize the metadata system store and the kill message should be deleted from the participant store.
    TopicManager topicManager = venice.getMasterVeniceController().getVeniceAdmin().getTopicManager();
    topicManager.ensureTopicIsDeletedAndBlock(metadataStoreTopic);
    assertFalse(parentControllerClient.materializeMetadataStoreVersion(regularVeniceStoreName,
        metadataStoreVersionNumber).isError());
    TestUtils.waitForNonDeterministicPushCompletion(metadataStoreTopic, controllerClient, 60, TimeUnit.SECONDS,
        Optional.empty());
    verifyKillMessageInParticipantStore(metadataStoreTopic, false);
    // Re-materialize the metadata system store again and no errors should occur.
    assertFalse(parentControllerClient.materializeMetadataStoreVersion(regularVeniceStoreName,
        metadataStoreVersionNumber).isError());
    TestUtils.waitForNonDeterministicPushCompletion(metadataStoreTopic, controllerClient, 60, TimeUnit.SECONDS,
        Optional.empty());
    assertEquals(venice.getMasterVeniceController().getAdminConsumerServiceByCluster(clusterName).getFailingOffset(), -1);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
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
        Version.PushType.BATCH, Version.guidBasedDummyPushId(), true, true, false,
        Optional.empty(), Optional.empty(), Optional.empty(), false, -1);
  }


  private VersionCreationResponse getNewStoreVersion(ControllerClient controllerClient, boolean newStore) {
    return getNewStoreVersion(controllerClient, TestUtils.getUniqueString("test-kill"), newStore);
  }
}
