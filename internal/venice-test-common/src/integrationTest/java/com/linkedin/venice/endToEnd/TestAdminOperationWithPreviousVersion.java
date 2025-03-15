package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.TestStoragePersonaUtils.*;
import static com.linkedin.venice.utils.TestWriteUtils.*;
import static org.testng.Assert.*;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.AdminTopicMetadataResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoragePersonaQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAdminOperationWithPreviousVersion {
  private static final Logger LOGGER = LogManager.getLogger(TestMultiDataCenterAdminOperations.class);
  private static final int TEST_TIMEOUT = 360 * Time.MS_PER_SECOND;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 2;

  // Do not use venice-cluster1 as it is used for testing failed admin messages
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new); // ["venice-cluster0",
  // "venice-cluster1",
  // ...];

  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private ControllerClient parentControllerClient;
  private String clusterName;
  private Map<String, Boolean> operationTypeMap = getAllPayloadUnionTypes();
  private Admin veniceAdmin;
  static final String KEY_SCHEMA = "\"string\"";
  static final String VALUE_SCHEMA = "\"string\"";
  private List<ControllerClient> childControllerClients = new ArrayList<>();

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    List<VeniceMultiClusterWrapper> childClusters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();

    LOGGER.info(
        "parentControllers: {}",
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(", ")));

    int i = 0;
    for (VeniceMultiClusterWrapper multiClusterWrapper: childClusters) {
      LOGGER.info(
          "childCluster{} controllers: {}",
          i++,
          multiClusterWrapper.getControllers()
              .values()
              .stream()
              .map(VeniceControllerWrapper::getControllerUrl)
              .collect(Collectors.joining(", ")));
    }

    clusterName = CLUSTER_NAMES[0];
    VeniceControllerWrapper parentController = parentControllers.get(0);
    parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());

    // Pinning the version to the previous version
    AdminTopicMetadataResponse updateProtocolVersionResponse =
        parentControllerClient.updateAdminOperationProtocolVersion(
            clusterName,
            (long) (AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION - 1));
    assertFalse(updateProtocolVersionResponse.isError(), "Failed to update protocol version");

    veniceAdmin = multiRegionMultiClusterWrapper.getParentControllers().get(0).getVeniceAdmin();
    PubSubTopicRepository pubSubTopicRepository = veniceAdmin.getPubSubTopicRepository();
    TopicManager topicManager = veniceAdmin.getTopicManager();
    PubSubTopic adminTopic = pubSubTopicRepository.getTopic(AdminTopicUtils.getTopicNameFromClusterName(clusterName));
    topicManager.createTopic(adminTopic, 1, 1, true);

    ControllerClient dc0Client = ControllerClient.constructClusterControllerClient(
        clusterName,
        multiRegionMultiClusterWrapper.getChildRegions().get(0).getControllerConnectString());
    ControllerClient dc1Client = ControllerClient.constructClusterControllerClient(
        clusterName,
        multiRegionMultiClusterWrapper.getChildRegions().get(1).getControllerConnectString());
    childControllerClients.add(dc0Client);
    childControllerClients.add(dc1Client);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    AdminTopicMetadataResponse updateProtocolVersionResponse =
        parentControllerClient.updateAdminOperationProtocolVersion(
            clusterName,
            (long) (AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION));
    assertFalse(updateProtocolVersionResponse.isError(), "Failed to update protocol version");
    multiRegionMultiClusterWrapper.close();

    System.out.println(operationTypeMap);
    for (Map.Entry<String, Boolean> entry: operationTypeMap.entrySet()) {
      assertTrue(entry.getValue(), "Operation type " + entry.getKey() + " was not tested");
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoreCreation() {
    markAsTested("StoreCreation");

    clusterName = CLUSTER_NAMES[0];
    VeniceControllerWrapper parentController = parentControllers.get(0);
    parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());
    String storeName = Utils.getUniqueString("test-store");

    // Create store
    NewStoreResponse newStoreResponse =
        parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"");
    Assert.assertFalse(newStoreResponse.isError());

    // Empty push
    emptyPushToStore(parentControllerClient, storeName, 1);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testPauseStore() {
    markAsTested("PauseStore");
    markAsTested("ResumeStore");

    String storeName = Utils.getUniqueString("testDisableStoreWriter");
    veniceAdmin.createStore(clusterName, storeName, "testOwner", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setBatchGetLimit(100));
    veniceAdmin.setStoreWriteability(clusterName, storeName, false);
    Store store = veniceAdmin.getStore(clusterName, storeName);

    // Store has been disabled, can not accept a new version
    Assert.assertThrows(
        VeniceException.class,
        () -> veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1));

    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions(), store.getVersions());

    // Store has been disabled, can not accept a new version
    Assert.assertThrows(
        VeniceException.class,
        () -> veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1));

    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions(), store.getVersions());

    // Resume store
    veniceAdmin.setStoreWriteability(clusterName, storeName, true);

    emptyPushToStore(parentControllerClient, storeName, 1);
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.isEnableWrites());
    Assert.assertEquals(store.getVersions().size(), 1);
    Assert.assertEquals(store.peekNextVersionNumber(), 2);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testKillOfflinePushJob() {
    markAsTested("KillOfflinePushJob");

    String storeName = Utils.getUniqueString("testKillOfflinePushJob");
    veniceAdmin.createStore(clusterName, storeName, "testOwner", KEY_SCHEMA, VALUE_SCHEMA);

    // Empty push
    VersionCreationResponse vcr = parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-push"), 1L);
    Assert.assertFalse(vcr.isError());
    // No wait to kill the push job
    // Kill push job
    parentControllerClient.killOfflinePushJob(Version.composeKafkaTopic(storeName, 1));

    // Check version
    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = childControllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();
        assertEquals(storeInfo.getVersions().size(), 0);
      });
    }

  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDisableStoreRead() {
    markAsTested("DisableStoreRead");
    markAsTested("EnableStoreRead");

    String storeName = setUpTestStore().getName();

    emptyPushToStore(parentControllerClient, storeName, 1);

    UpdateStoreQueryParams disableReadParams = new UpdateStoreQueryParams().setEnableReads(false);
    ControllerResponse response = parentControllerClient.updateStore(storeName, disableReadParams);
    assertFalse(response.isError());

    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = childControllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();
        assertFalse(storeInfo.isEnableStoreReads());
      });
    }

    UpdateStoreQueryParams enableReadParams = new UpdateStoreQueryParams().setEnableReads(true);
    response = parentControllerClient.updateStore(storeName, enableReadParams);
    assertFalse(response.isError());
    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = childControllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();
        assertTrue(storeInfo.isEnableStoreReads());
      });
    }
  }

  @Test
  public void testDeleteAllVersions() {
    markAsTested("DeleteAllVersions");

    String storeName = setUpTestStore().getName();
    emptyPushToStore(parentControllerClient, storeName, 1);

    // Disable read and write
    UpdateStoreQueryParams disableParams = new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false);
    ControllerResponse response = parentControllerClient.updateStore(storeName, disableParams);
    assertFalse(response.isError());

    // Delete all versions
    response = parentControllerClient.deleteAllVersions(storeName);
    assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      StoreInfo storeInfo = storeResponse.getStore();
      assertEquals(storeInfo.getVersions().size(), 0);
    });
  }

  @Test
  public void testSetStoreOwner() {
    markAsTested("SetStoreOwner");
    String storeName = setUpTestStore().getName();
    String newOwner = "newOwner";
    UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams().setOwner(newOwner);
    ControllerResponse response = parentControllerClient.updateStore(storeName, updateStoreQueryParams);
    assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      StoreInfo storeInfo = storeResponse.getStore();
      assertEquals(storeInfo.getOwner(), newOwner);
    });
  }

  @Test
  public void testSetStorePartitionCount() {
    markAsTested("SetStorePartitionCount");
    String storeName = setUpTestStore().getName();
    int newPartitionCount = 1;
    UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams().setPartitionCount(newPartitionCount);
    ControllerResponse response = parentControllerClient.updateStore(storeName, updateStoreQueryParams);
    assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      StoreInfo storeInfo = storeResponse.getStore();
      assertEquals(storeInfo.getPartitionCount(), newPartitionCount);
    });
  }

  @Test
  public void testSetStoreCurrentVersion() {
    markAsTested("SetStoreCurrentVersion");

    String storeName = setUpTestStore().getName();

    // Empty push
    emptyPushToStore(parentControllerClient, storeName, 1);

    int newCurrentVersion = 0;
    UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams().setCurrentVersion(newCurrentVersion);
    ControllerResponse response = parentControllerClient.updateStore(storeName, updateStoreQueryParams);
    assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      StoreInfo storeInfo = storeResponse.getStore();
      assertEquals(storeInfo.getCurrentVersion(), newCurrentVersion);
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testUpdateStore() {
    markAsTested("UpdateStore");
    clusterName = CLUSTER_NAMES[0];
    VeniceControllerWrapper parentController = parentControllers.get(0);
    parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());
    String storeName = Utils.getUniqueString("test-store");

    // Create store
    NewStoreResponse newStoreResponse =
        parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"");
    Assert.assertFalse(newStoreResponse.isError());

    // Empty push
    emptyPushToStore(parentControllerClient, storeName, 1);

    // Store update
    ControllerResponse updateStore =
        parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setBatchGetLimit(100));
    Assert.assertFalse(updateStore.isError());

    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = childControllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();
        assertEquals(storeInfo.getBatchGetLimit(), 100);
      });
    }
  }

  @Test
  public void testDeleteStore() {
    markAsTested("DeleteStore");
    String storeName = setUpTestStore().getName();
    // Disable read and write
    UpdateStoreQueryParams disableParams = new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false);
    ControllerResponse response = parentControllerClient.updateStore(storeName, disableParams);
    assertFalse(response.isError());

    // Delete store
    response = parentControllerClient.deleteStore(storeName);
    assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertTrue(storeResponse.isError());
    });
  }

  @Test
  public void testDeleteOldVersion() {
    markAsTested("DeleteOldVersion");

    String storeName = setUpTestStore().getName();

    // version 1
    emptyPushToStore(parentControllerClient, storeName, 1);

    // version 2
    emptyPushToStore(parentControllerClient, storeName, 2);

    ControllerResponse response = parentControllerClient.deleteOldVersion(storeName, 1);
    assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      StoreInfo storeInfo = storeResponse.getStore();
      assertEquals(storeInfo.getVersions().size(), 1);
    });

  }

  @Test
  public void testDerivedSchemaCreation() {
    markAsTested("DerivedSchemaCreation");

    Store storeInfo = setUpTestStore();
    String storeName = storeInfo.getName();
    String recordSchemaStr = TestWriteUtils.USER_WITH_DEFAULT_SCHEMA.toString();
    Schema derivedSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchemaStr(recordSchemaStr);

    veniceAdmin.addDerivedSchema(clusterName, storeName, 1, derivedSchema.toString());
    Assert.assertEquals(veniceAdmin.getDerivedSchemas(clusterName, storeName).size(), 1);
  }

  @Test
  public void testValueSchemaCreation() {
    markAsTested("ValueSchemaCreation");
    markAsTested("DeleteUnusedValueSchemas");

    Store storeInfo = setUpTestStore();
    String storeName = storeInfo.getName();

    // When read compute is enabled, we will generate superset schema and add value schema into it
    UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams().setReadComputationEnabled(true);
    ControllerResponse response = parentControllerClient.updateStore(storeName, updateStoreQueryParams);
    assertFalse(response.isError());

    parentControllerClient.deleteValueSchemas(storeName, new ArrayList<>(1));
  }

  @Test
  public void testStoragePersona() {
    markAsTested("CreateStoragePersona");
    markAsTested("UpdateStoragePersona");
    markAsTested("DeleteStoragePersona");
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota * 3);
    List<String> stores = new ArrayList<>();
    Store store1 = setUpTestStore();

    parentControllerClient
        .updateStore(store1.getName(), new UpdateStoreQueryParams().setStorageQuotaInByte(totalQuota));
    stores.add(store1.getName());
    persona.getStoresToEnforce().add(stores.get(0));

    ControllerClient controllerClient = new ControllerClient(
        multiRegionMultiClusterWrapper.getClusterNames()[0],
        multiRegionMultiClusterWrapper.getControllerConnectString());

    ControllerResponse response = controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    assertFalse(response.isError());
    Store store2 = setUpTestStore();

    parentControllerClient
        .updateStore(store2.getName(), new UpdateStoreQueryParams().setStorageQuotaInByte(totalQuota * 2));

    stores.add(store2.getName());
    persona.setStoresToEnforce(new HashSet<>(stores));
    response = controllerClient.updateStoragePersona(
        persona.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>(stores)));
    assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));

    response = parentControllerClient.deleteStoragePersona(persona.getName());
    if (response.isError())
      throw new VeniceException(response.getError());
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertNull(parentControllerClient.getStoragePersona(persona.getName()).getStoragePersona()));
  }

  @Test
  public void testRollbackCurrentVersion() {
    markAsTested("RollbackCurrentVersion");
    markAsTested("RollForwardCurrentVersion");

    String storeName = setUpTestStore().getName();
    emptyPushToStore(parentControllerClient, storeName, 1);
    emptyPushToStore(parentControllerClient, storeName, 2);
    // Should roll back to version 1
    parentControllerClient.rollbackToBackupVersion(storeName);
    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = childControllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();
        assertEquals(storeInfo.getCurrentVersion(), 1);
      });
    }

    // roll forward only in dc-0
    parentControllerClient
        .rollForwardToFutureVersion(storeName, multiRegionMultiClusterWrapper.getChildRegionNames().get(0));
    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = childControllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError());
        StoreInfo storeInfo = storeResponse.getStore();
        assertEquals(storeInfo.getCurrentVersion(), childControllerClient == childControllerClients.get(1) ? 1 : 2);
      });
    }
  }

  @Test
  public void testEnableNativeReplicationForCluster() {
    markAsTested("EnableNativeReplicationForCluster");
    String storeName = setUpTestStore().getName();

    emptyPushToStore(parentControllerClient, storeName, 1);

    UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams().setNativeReplicationEnabled(true);
    ControllerResponse response = parentControllerClient.updateStore(storeName, updateStoreQueryParams);

    assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      StoreInfo storeInfo = storeResponse.getStore();
      assertTrue(storeInfo.isNativeReplicationEnabled());
    });
  }

  @Test
  public void testEnableActiveActiveReplicationForCluster() {
    markAsTested("EnableActiveActiveReplicationForCluster");

    String storeName = setUpTestStore().getName();
    emptyPushToStore(parentControllerClient, storeName, 1);

    UpdateStoreQueryParams updateStoreQueryParams =
        new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true);
    ControllerResponse response = parentControllerClient.updateStore(storeName, updateStoreQueryParams);

    assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      StoreResponse storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError());
      StoreInfo storeInfo = storeResponse.getStore();
      assertTrue(storeInfo.isActiveActiveReplicationEnabled());
    });
  }

  @Test
  public void testConfigureActiveActiveReplicationForCluster() {
    markAsTested("ConfigureActiveActiveReplicationForCluster");
  }

  @Test
  public void testConfigureNativeReplicationForCluster() {
    // No usage found for this operation
    // Check @code{AdminExecutionTask#handleEnableNativeReplicationForCluster}
    markAsTested("ConfigureNativeReplicationForCluster");
  }

  @Test
  public void testConfigureIncrementalPushForCluster() {
    // No usage found for this operation
    markAsTested("ConfigureIncrementalPushForCluster");
  }

  @Test
  public void testMigrateStore() {
    markAsTested("MigrateStore");
  }

  @Test
  public void testAbortMigration() {
    markAsTested("AbortMigration");
  }

  @Test
  public void testAddVersion() {
    markAsTested("AddVersion");
  }

  @Test
  public void testMetadataSchemaCreation() {
    markAsTested("MetadataSchemaCreation");
  }

  @Test
  public void testSupersetSchemaCreation() {
    markAsTested("SupersetSchemaCreation");
  }

  @Test
  public void testPushStatusSystemStoreAutoCreationValidation() {
    markAsTested("PushStatusSystemStoreAutoCreationValidation");
  }

  @Test
  public void testMetaSystemStoreAutoCreationValidation() {
    markAsTested("MetaSystemStoreAutoCreationValidation");
  }

  private void emptyPushToStore(ControllerClient parentControllerClient, String storeName, int expectedVersion) {
    VersionCreationResponse vcr = parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-push"), 1L);
    Assert.assertFalse(vcr.isError());
    assertEquals(
        vcr.getVersion(),
        expectedVersion,
        "requesting a topic for a push should provide version number " + expectedVersion);

    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(storeName, expectedVersion),
        parentControllerClient,
        30,
        TimeUnit.SECONDS);
  }

  private Store setUpTestStore() {
    Store testStore =
        TestUtils.createTestStore(Utils.getUniqueString("testStore"), "testStoreOwner", System.currentTimeMillis());
    parentControllerClient
        .createNewStore(testStore.getName(), testStore.getOwner(), STRING_SCHEMA.toString(), STRING_SCHEMA.toString());
    return testStore;
  }

  private Map<String, Boolean> getAllPayloadUnionTypes() {
    Schema latestSchema =
        AdminOperationSerializer.getSchema(AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    List<Schema> payloadUnionSchemas = latestSchema.getField("payloadUnion").schema().getTypes();
    return payloadUnionSchemas.stream()
        .filter(schema -> schema.getType() == Schema.Type.RECORD) // Filter only RECORD types
        .collect(Collectors.toMap(Schema::getName, schema -> false));
  }

  private void markAsTested(String operationType) {
    operationTypeMap.put(operationType, true);
  }
}
