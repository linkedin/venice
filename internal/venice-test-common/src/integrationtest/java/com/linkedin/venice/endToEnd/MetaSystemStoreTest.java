package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.system.store.MetaStoreWriter.*;
import static org.testng.Assert.*;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.repository.NativeMetadataRepository;
import com.linkedin.davinci.repository.ThinClientMetaStoreBasedRepository;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MetaSystemStoreTest {
  private static final Logger LOGGER = LogManager.getLogger(MetaSystemStoreTest.class);
  private static final String INT_KEY_SCHEMA = "\"int\"";

  private static final String VALUE_SCHEMA_1 = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestValue\",\n"
      + "  \"fields\": [\n" + "   {\"name\": \"test_field1\", \"type\": \"string\"}\n" + "  ]\n" + "}";
  private static final String VALUE_SCHEMA_2 = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestValue\",\n"
      + "  \"fields\": [\n" + "   {\"name\": \"test_field1\", \"type\": \"string\"},\n"
      + "   {\"name\": \"test_field2\", \"type\": \"int\", \"default\": 0}\n" + "  ]\n" + "}";

  private VeniceClusterWrapper venice;
  private ControllerClient controllerClient;
  private VeniceControllerWrapper parentController;
  private ZkServerWrapper parentZkServer;

  @BeforeClass
  public void setUp() {
    Properties testProperties = new Properties();
    testProperties
        .put(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, Long.toString(TimeUnit.DAYS.toMillis(7)));
    venice = ServiceFactory.getVeniceCluster(1, 2, 1, 2, 1000000, false, false);
    controllerClient = venice.getControllerClient();
    parentZkServer = ServiceFactory.getZkServer();
    parentController = ServiceFactory.getVeniceController(
        new VeniceControllerCreateOptions.Builder(venice.getClusterName(), parentZkServer, venice.getKafka())
            .childControllers(venice.getVeniceControllers().toArray(new VeniceControllerWrapper[0]))
            .extraProperties(testProperties)
            .build());
  }

  @AfterClass
  public void cleanUp() {
    controllerClient.close();
    parentController.close();
    venice.close();
    parentZkServer.close();
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void bootstrapMetaSystemStore() throws ExecutionException, InterruptedException {
    // Create a new regular store.
    String regularVeniceStoreName = Utils.getUniqueString("venice_store");
    ControllerClient parentControllerClient =
        new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());
    NewStoreResponse newStoreResponse =
        parentControllerClient.createNewStore(regularVeniceStoreName, "test_owner", INT_KEY_SCHEMA, VALUE_SCHEMA_1);
    assertFalse(
        newStoreResponse.isError(),
        "New store: " + regularVeniceStoreName + " should be created successfully, but got error: "
            + newStoreResponse.getError());
    // Do an empty push
    VersionCreationResponse versionCreationResponse =
        controllerClient.emptyPush(regularVeniceStoreName, "test_push_id_1", 100000);
    assertFalse(
        versionCreationResponse.isError(),
        "New version creation should success, but got error: " + versionCreationResponse.getError());
    TestUtils.waitForNonDeterministicPushCompletion(
        versionCreationResponse.getKafkaTopic(),
        controllerClient,
        10,
        TimeUnit.SECONDS);
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(regularVeniceStoreName);

    // Check meta system store property
    Store metaSystemStore =
        venice.getLeaderVeniceController().getVeniceAdmin().getStore(venice.getClusterName(), metaSystemStoreName);
    assertNotNull(metaSystemStore, "Meta System Store shouldn't be null");
    long currentLatestVersionPromoteToCurrentTimestampForMetaSystemStore =
        metaSystemStore.getLatestVersionPromoteToCurrentTimestamp();
    assertTrue(
        currentLatestVersionPromoteToCurrentTimestampForMetaSystemStore > 0,
        "The version promotion timestamp should be positive, but got "
            + currentLatestVersionPromoteToCurrentTimestampForMetaSystemStore);

    // Do an empty push against the meta system store
    versionCreationResponse = controllerClient.emptyPush(metaSystemStoreName, "test_meta_system_store_push_id", 100000);
    assertFalse(
        versionCreationResponse.isError(),
        "New version creation should success, but got error: " + versionCreationResponse.getError());
    TestUtils.waitForNonDeterministicPushCompletion(
        versionCreationResponse.getKafkaTopic(),
        controllerClient,
        10,
        TimeUnit.SECONDS);
    // Check meta system stsore property again
    metaSystemStore =
        venice.getLeaderVeniceController().getVeniceAdmin().getStore(venice.getClusterName(), metaSystemStoreName);
    assertNotNull(metaSystemStore, "Meta System Store shouldn't be null");
    assertTrue(
        metaSystemStore
            .getLatestVersionPromoteToCurrentTimestamp() > currentLatestVersionPromoteToCurrentTimestampForMetaSystemStore,
        "The version promotion timestamp should be changed");

    // Query meta system store
    AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> storeClient = ClientFactory.getAndStartSpecificAvroClient(
        ClientConfig.defaultSpecificClientConfig(metaSystemStoreName, StoreMetaValue.class)
            .setVeniceURL(venice.getRandomRouterURL())
            .setSslFactory(SslUtils.getVeniceLocalSslFactory()));
    // Query store properties
    StoreMetaKey storePropertiesKey = MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {
      {
        put(KEY_STRING_STORE_NAME, regularVeniceStoreName);
        put(KEY_STRING_CLUSTER_NAME, venice.getClusterName());
      }
    });
    StoreMetaValue storeProperties = storeClient.get(storePropertiesKey).get();
    assertTrue(storeProperties != null && storeProperties.storeProperties != null);
    // Query key schema
    StoreMetaKey keySchemaKey = MetaStoreDataType.STORE_KEY_SCHEMAS.getStoreMetaKey(new HashMap<String, String>() {
      {
        put(KEY_STRING_STORE_NAME, regularVeniceStoreName);
        put(KEY_STRING_CLUSTER_NAME, venice.getClusterName());
      }
    });
    StoreMetaValue storeKeySchema = storeClient.get(keySchemaKey).get();
    assertTrue(storeKeySchema != null && storeKeySchema.storeKeySchemas != null);
    StoreKeySchemas keySchemas = storeKeySchema.storeKeySchemas;
    assertEquals(keySchemas.keySchemaMap.size(), 1);
    assertEquals(keySchemas.keySchemaMap.get(new Utf8("1")).toString(), INT_KEY_SCHEMA);
    // Query value schema
    StoreMetaKey valueSchemasKey = MetaStoreDataType.STORE_VALUE_SCHEMAS.getStoreMetaKey(new HashMap<String, String>() {
      {
        put(KEY_STRING_STORE_NAME, regularVeniceStoreName);
        put(KEY_STRING_CLUSTER_NAME, venice.getClusterName());
      }
    });
    StoreMetaValue storeValueSchemas = storeClient.get(valueSchemasKey).get();
    assertTrue(storeValueSchemas != null && storeValueSchemas.storeValueSchemas != null);
    assertEquals(storeValueSchemas.storeValueSchemas.valueSchemaMap.size(), 1);
    Map<String, String> keyMap = new HashMap<>(2);
    keyMap.put(KEY_STRING_STORE_NAME, regularVeniceStoreName);
    keyMap.put(KEY_STRING_SCHEMA_ID, Integer.toString(1));
    StoreMetaKey individualValueSchemaKey = MetaStoreDataType.STORE_VALUE_SCHEMA.getStoreMetaKey(keyMap);
    String valueSchema = storeClient.get(individualValueSchemaKey).get().storeValueSchema.valueSchema.toString();
    assertEquals(Schema.parse(valueSchema), Schema.parse(VALUE_SCHEMA_1));
    // Query replica status
    StoreMetaKey replicaStatusKeyForV1P0 =
        MetaStoreDataType.STORE_REPLICA_STATUSES.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, regularVeniceStoreName);
            put(KEY_STRING_CLUSTER_NAME, venice.getClusterName());
            put(KEY_STRING_VERSION_NUMBER, "1");
            put(KEY_STRING_PARTITION_ID, "0");
          }
        });
    StoreMetaValue replicaStatusForV1P0 = storeClient.get(replicaStatusKeyForV1P0).get();
    // the different situations.
    assertTrue(replicaStatusForV1P0 != null && replicaStatusForV1P0.storeReplicaStatuses != null);

    // Update store config
    controllerClient.updateStore(regularVeniceStoreName, new UpdateStoreQueryParams().setBatchGetLimit(100));
    // Query meta system store to verify the change
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      final StoreMetaValue v = storeClient.get(storePropertiesKey).get();
      assertEquals(v.storeProperties.batchGetLimit, 100);
    });

    // Add a new value schema
    controllerClient.addValueSchema(regularVeniceStoreName, VALUE_SCHEMA_2);
    // Query meta system store to verify the change
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      final StoreMetaValue v = storeClient.get(valueSchemasKey).get();
      assertEquals(v.storeValueSchemas.valueSchemaMap.size(), 2);
      keyMap.put(KEY_STRING_SCHEMA_ID, Integer.toString(2));
      StoreMetaKey schemaKey = MetaStoreDataType.STORE_VALUE_SCHEMA.getStoreMetaKey(keyMap);
      String valueSchema1 = storeClient.get(schemaKey).get().storeValueSchema.valueSchema.toString();
      assertEquals(Schema.parse(valueSchema1), Schema.parse(VALUE_SCHEMA_2));
    });

    // Do the 2nd empty push to the Venice store
    // Do an empty push
    versionCreationResponse = controllerClient.emptyPush(regularVeniceStoreName, "test_push_id_2", 100000);
    assertFalse(
        versionCreationResponse.isError(),
        "New version creation should success, but got error: " + versionCreationResponse.getError());
    TestUtils.waitForNonDeterministicPushCompletion(
        versionCreationResponse.getKafkaTopic(),
        controllerClient,
        10,
        TimeUnit.SECONDS);
    // Query replica status
    StoreMetaKey replicaStatusKeyForV2P0 =
        MetaStoreDataType.STORE_REPLICA_STATUSES.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, regularVeniceStoreName);
            put(KEY_STRING_CLUSTER_NAME, venice.getClusterName());
            put(KEY_STRING_VERSION_NUMBER, "2");
            put(KEY_STRING_PARTITION_ID, "0");
          }
        });
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      StoreMetaValue replicaStatusForV2P0 = storeClient.get(replicaStatusKeyForV2P0).get();
      assertNotNull(replicaStatusForV2P0);
      if (replicaStatusForV2P0 != null) {
        assertEquals(replicaStatusForV2P0.storeReplicaStatuses.size(), 2);
      }
    });

    // Do the 3rd empty push to the Venice store, and the replica status for 1st version should become empty
    versionCreationResponse = controllerClient.emptyPush(regularVeniceStoreName, "test_push_id_3", 100000);
    assertFalse(
        versionCreationResponse.isError(),
        "New version creation should success, but got error: " + versionCreationResponse.getError());
    TestUtils.waitForNonDeterministicPushCompletion(
        versionCreationResponse.getKafkaTopic(),
        controllerClient,
        10,
        TimeUnit.SECONDS);
    // Query replica status
    StoreMetaKey replicaStatusKeyForV3P0 =
        MetaStoreDataType.STORE_REPLICA_STATUSES.getStoreMetaKey(new HashMap<String, String>() {
          {
            put(KEY_STRING_STORE_NAME, regularVeniceStoreName);
            put(KEY_STRING_CLUSTER_NAME, venice.getClusterName());
            put(KEY_STRING_VERSION_NUMBER, "3");
            put(KEY_STRING_PARTITION_ID, "0");
          }
        });
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      StoreMetaValue replicaStatusForV3P0 = storeClient.get(replicaStatusKeyForV3P0).get();
      assertNotNull(replicaStatusForV3P0);
      if (replicaStatusForV3P0 != null) {
        assertEquals(replicaStatusForV3P0.storeReplicaStatuses.size(), 2);
      }

      /**
       * The replica statuses for the deprecated versions should be removed from meta system store.
       */
      StoreMetaValue currentReplicaStatusForV1P0 = storeClient.get(replicaStatusKeyForV1P0).get();
      assertNull(currentReplicaStatusForV1P0);
    });

    // Meta system store should be deleted when the regular Venice store gets deleted.
    ControllerResponse storeDeletionResponse = controllerClient.disableAndDeleteStore(regularVeniceStoreName);
    assertFalse(
        storeDeletionResponse.isError(),
        "Store deletion should success, but got error: " + storeDeletionResponse.getError());
    assertNull(
        venice.getVeniceControllers()
            .get(0)
            .getVeniceAdmin()
            .getMetaStoreWriter()
            .getMetaStoreWriter(metaSystemStoreName));
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      try {
        storeClient.get(replicaStatusKeyForV3P0).get();
        fail("An exception is expected here");
      } catch (Exception e) {
        assertTrue(
            e.getMessage().contains("does not exist"),
            "Any request to meta system store should throw exception before non-existing store");
      }
    });

    /**
     * Wait for the RT topic deletion.
     */
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      ControllerResponse response = controllerClient.checkResourceCleanupForStoreCreation(metaSystemStoreName);
      if (response.isError()) {
        fail("The store cleanup for meta system store: " + metaSystemStoreName + " is not done yet");
      }
    });
    LOGGER.info("Resource cleanup is done for meta system store: {}", metaSystemStoreName);
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testThinClientMetaStoreBasedRepository() throws InterruptedException {
    String regularVeniceStoreName = Utils.getUniqueString("venice_store");
    createStoreAndMaterializeMetaSystemStore(regularVeniceStoreName);
    D2Client d2Client = null;
    NativeMetadataRepository nativeMetadataRepository = null;
    try {
      d2Client = D2TestUtils.getAndStartD2Client(venice.getZk().getAddress());
      ClientConfig<StoreMetaValue> clientConfig = getClientConfig(regularVeniceStoreName, d2Client);
      // Not providing a CLIENT_META_SYSTEM_STORE_VERSION_MAP, should use the default value of 1 for system store
      // current version.
      VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
          .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
          .build();
      nativeMetadataRepository = NativeMetadataRepository.getInstance(clientConfig, backendConfig);
      nativeMetadataRepository.start();
      // ThinClientMetaStoreBasedRepository implementation should be used since CLIENT_USE_META_SYSTEM_STORE_REPOSITORY
      // is set to true without enabling other feature flags.
      Assert.assertTrue(nativeMetadataRepository instanceof ThinClientMetaStoreBasedRepository);
      verifyRepository(nativeMetadataRepository, regularVeniceStoreName);
    } finally {
      if (d2Client != null) {
        D2ClientUtils.shutdownClient(d2Client);
      }
      if (nativeMetadataRepository != null) {
        // Calling clear explicitly here because if the NativeMetadataRepository implementation used happens to
        // initialize
        // a new DaVinciBackend then calling clear will trigger the cleanup logic to ensure the DaVinciBackend is not
        // leaked
        // into other tests.
        nativeMetadataRepository.clear();
      }
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testThinClientMetaStoreBasedRepositoryWithLargeValueSchemas() throws InterruptedException {
    String regularVeniceStoreName = Utils.getUniqueString("venice_store");
    // 1500 fields generate a schema that's roughly 150KB.
    int numberOfLargeSchemaVersions = 15;
    List<String> schemas = generateLargeValueSchemas(1500, numberOfLargeSchemaVersions);
    createStoreAndMaterializeMetaSystemStore(regularVeniceStoreName, schemas.get(0));
    controllerClient.addValueSchema(regularVeniceStoreName, schemas.get(1));
    D2Client d2Client = null;
    NativeMetadataRepository nativeMetadataRepository = null;
    try {
      d2Client = D2TestUtils.getAndStartD2Client(venice.getZk().getAddress());
      ClientConfig<StoreMetaValue> clientConfig = getClientConfig(regularVeniceStoreName, d2Client);
      // Not providing a CLIENT_META_SYSTEM_STORE_VERSION_MAP, should use the default value of 1 for system store
      // current version.
      VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
          .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
          .build();
      nativeMetadataRepository = NativeMetadataRepository.getInstance(clientConfig, backendConfig);
      // ThinClientMetaStoreBasedRepository implementation should be used since CLIENT_USE_META_SYSTEM_STORE_REPOSITORY
      // is set to true without enabling other feature flags.
      Assert.assertTrue(nativeMetadataRepository instanceof ThinClientMetaStoreBasedRepository);
      nativeMetadataRepository.start();
      nativeMetadataRepository.subscribe(regularVeniceStoreName);
      Collection<SchemaEntry> metaStoreSchemaEntries = nativeMetadataRepository.getValueSchemas(regularVeniceStoreName);
      assertEquals(
          metaStoreSchemaEntries.size(),
          venice.getLeaderVeniceController()
              .getVeniceAdmin()
              .getValueSchemas(venice.getClusterName(), regularVeniceStoreName)
              .size(),
          "Number of value schemas should be the same between meta system store and controller");
      for (int i = 2; i < numberOfLargeSchemaVersions; i++) {
        controllerClient.addValueSchema(regularVeniceStoreName, schemas.get(i));
      }
      NativeMetadataRepository finalNativeMetadataRepository = nativeMetadataRepository;
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          () -> assertEquals(
              finalNativeMetadataRepository.getValueSchemas(regularVeniceStoreName).size(),
              numberOfLargeSchemaVersions,
              "There should be " + numberOfLargeSchemaVersions + " versions of value schemas in total"));
      SchemaEntry latestValueSchema = nativeMetadataRepository.getSupersetOrLatestValueSchema(regularVeniceStoreName);
      assertEquals(
          latestValueSchema,
          venice.getLeaderVeniceController()
              .getVeniceAdmin()
              .getValueSchema(venice.getClusterName(), regularVeniceStoreName, latestValueSchema.getId()),
          "NativeMetadataRepository is not returning the right schema id and/or schema pair");
    } finally {
      if (d2Client != null) {
        D2ClientUtils.shutdownClient(d2Client);
      }
      if (nativeMetadataRepository != null) {
        // Calling clear explicitly here because if the NativeMetadataRepository implementation used happens to
        // initialize
        // a new DaVinciBackend then calling clear will trigger the cleanup logic to ensure the DaVinciBackend is not
        // leaked
        // into other tests.
        nativeMetadataRepository.clear();
      }
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testParentControllerAutoMaterializeMetaSystemStore() {
    try (ControllerClient parentControllerClient =
        new ControllerClient(venice.getClusterName(), parentController.getControllerUrl())) {
      String zkSharedMetaSystemSchemaStoreName =
          AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getSystemStoreName();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Store readOnlyStore = parentController.getVeniceAdmin()
            .getReadOnlyZKSharedSystemStoreRepository()
            .getStore(zkSharedMetaSystemSchemaStoreName);
        Assert.assertNotNull(
            readOnlyStore,
            "Store: " + zkSharedMetaSystemSchemaStoreName + " should be initialized by "
                + ClusterLeaderInitializationRoutine.class.getSimpleName());
        Assert.assertTrue(
            readOnlyStore.isHybrid(),
            "Store: " + zkSharedMetaSystemSchemaStoreName + " should be configured to hybrid");
      });
      String storeName = Utils.getUniqueString("new-user-store");
      assertFalse(
          parentControllerClient.createNewStore(storeName, "venice-test", INT_KEY_SCHEMA, VALUE_SCHEMA_1).isError(),
          "Unexpected new store creation failure");
      String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(metaSystemStoreName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);
    }
  }

  private ClientConfig<StoreMetaValue> getClientConfig(String storeName, D2Client d2Client) {
    return ClientConfig.defaultSpecificClientConfig(storeName, StoreMetaValue.class)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setD2Client(d2Client)
        .setVeniceURL(venice.getZk().getAddress());
  }

  private void verifyRepository(NativeMetadataRepository nativeMetadataRepository, String regularVeniceStoreName)
      throws InterruptedException {
    assertNull(nativeMetadataRepository.getStore("Non-existing-store"));
    expectThrows(VeniceNoStoreException.class, () -> nativeMetadataRepository.getStoreOrThrow("Non-existing-store"));
    expectThrows(VeniceNoStoreException.class, () -> nativeMetadataRepository.subscribe("Non-existing-store"));

    nativeMetadataRepository.subscribe(regularVeniceStoreName);
    Store store = nativeMetadataRepository.getStore(regularVeniceStoreName);
    Store controllerStore = new ReadOnlyStore(
        venice.getLeaderVeniceController().getVeniceAdmin().getStore(venice.getClusterName(), regularVeniceStoreName));
    assertEquals(store, controllerStore);
    SchemaEntry keySchema = nativeMetadataRepository.getKeySchema(regularVeniceStoreName);
    SchemaEntry controllerKeySchema = venice.getLeaderVeniceController()
        .getVeniceAdmin()
        .getKeySchema(venice.getClusterName(), regularVeniceStoreName);
    assertEquals(keySchema, controllerKeySchema);
    Collection<SchemaEntry> valueSchemas = nativeMetadataRepository.getValueSchemas(regularVeniceStoreName);
    Collection<SchemaEntry> controllerValueSchemas = venice.getLeaderVeniceController()
        .getVeniceAdmin()
        .getValueSchemas(venice.getClusterName(), regularVeniceStoreName);
    assertEquals(valueSchemas, controllerValueSchemas);
    long storageQuota = 123456789;
    int partitionCount = 3;
    assertFalse(
        controllerClient
            .updateStore(
                regularVeniceStoreName,
                new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota).setPartitionCount(partitionCount))
            .isError());
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      final Store nativeRepoStore = nativeMetadataRepository.getStore(regularVeniceStoreName);
      assertEquals(nativeRepoStore.getPartitionCount(), partitionCount);
      assertEquals(nativeRepoStore.getStorageQuotaInByte(), storageQuota);
    });
    assertFalse(controllerClient.addValueSchema(regularVeniceStoreName, VALUE_SCHEMA_2).isError());
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertEquals(
          nativeMetadataRepository.getValueSchemas(regularVeniceStoreName),
          venice.getLeaderVeniceController()
              .getVeniceAdmin()
              .getValueSchemas(venice.getClusterName(), regularVeniceStoreName));
    });
    VersionCreationResponse versionCreationResponse =
        controllerClient.emptyPush(regularVeniceStoreName, "new_push", 10000);
    assertFalse(versionCreationResponse.isError());
    TestUtils.waitForNonDeterministicPushCompletion(
        versionCreationResponse.getKafkaTopic(),
        controllerClient,
        10,
        TimeUnit.SECONDS);
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertTrue(
          nativeMetadataRepository.getStore(regularVeniceStoreName)
              .getVersion(versionCreationResponse.getVersion())
              .isPresent());
      assertEquals(
          nativeMetadataRepository.getStore(regularVeniceStoreName)
              .getVersionStatus(versionCreationResponse.getVersion()),
          VersionStatus.ONLINE);
    });
  }

  private void createStoreAndMaterializeMetaSystemStore(String storeName) {
    createStoreAndMaterializeMetaSystemStore(storeName, VALUE_SCHEMA_1);
  }

  private void createStoreAndMaterializeMetaSystemStore(String storeName, String valueSchema) {
    // Verify and create Venice regular store if it doesn't exist.
    if (controllerClient.getStore(storeName).getStore() == null) {
      assertFalse(controllerClient.createNewStore(storeName, "test_owner", INT_KEY_SCHEMA, valueSchema).isError());
    }
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    // Ignore transient failures on job status when the cluster is still starting.
    TestUtils.waitForNonDeterministicAssertion(
        10,
        TimeUnit.SECONDS,
        () -> assertNotNull(
            controllerClient.queryJobStatus(Version.composeKafkaTopic(metaSystemStoreName, 1)).getStatus()));
    String metaSystemStoreStatus =
        controllerClient.queryJobStatus(Version.composeKafkaTopic(metaSystemStoreName, 1)).getStatus();
    if (ExecutionStatus.NOT_CREATED.toString().equals(metaSystemStoreStatus)) {
      assertFalse(controllerClient.emptyPush(metaSystemStoreName, "test_meta_system_store_push", 10000).isError());
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(metaSystemStoreName, 1),
          controllerClient,
          30,
          TimeUnit.SECONDS);
    } else if (!ExecutionStatus.COMPLETED.toString().equals(metaSystemStoreStatus)) {
      fail("Unexpected meta system store status: " + metaSystemStoreStatus);
    }
  }

  private List<String> generateLargeValueSchemas(int baseNumberOfFields, int numberOfVersions) {
    List<String> schemas = new ArrayList<>();
    if (baseNumberOfFields < 1) {
      throw new UnsupportedOperationException("Can only generate value schemas with one or more fields");
    }
    StringBuilder valueSchemaBuilder = new StringBuilder();
    valueSchemaBuilder.append("{\"type\": \"record\", \"name\": \"TestValue\", \"fields\": [");
    for (int i = 0; i < baseNumberOfFields; i++) {
      if (valueSchemaBuilder.charAt(valueSchemaBuilder.length() - 1) == '}') {
        valueSchemaBuilder.append(",");
      }
      valueSchemaBuilder.append(generateFieldBlock());
    }
    schemas.add(valueSchemaBuilder + "]}");
    for (int v = 1; v < numberOfVersions; v++) {
      valueSchemaBuilder.append(",");
      valueSchemaBuilder.append(generateFieldBlock());
      schemas.add(valueSchemaBuilder + "]}");
    }
    return schemas;
  }

  private static String generateFieldBlock() {
    return String.format("{\"name\": \"largeSchema%d\", \"type\": \"string\", \"default\": \"\"}", System.nanoTime());
  }
}
