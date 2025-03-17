package com.linkedin.venice.endToEnd;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.StatTrackingStoreClient;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.AdminTopicMetadataResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoragePersonaQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.StoreMigrationTestUtil;
import com.linkedin.venice.utils.TestStoragePersonaUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.vpj.VenicePushJobConstants;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAdminOperationWithPreviousVersion {
  private static final Logger LOGGER = LogManager.getLogger(TestMultiDataCenterAdminOperations.class);
  private static final int TEST_TIMEOUT = 360 * Time.MS_PER_SECOND;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 2;
  private static final int RECORD_COUNT = 20;
  private static final String FABRIC0 = "dc-0";

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
  private VeniceMultiClusterWrapper multiClusterWrapper;
  private static final String BASIC_USER_SCHEMA_STRING_WITH_DEFAULT = "{" + "  \"namespace\" : \"example.avro\",  "
      + "  \"type\": \"record\",   " + "  \"name\": \"User\",     " + "  \"fields\": [           "
      + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"\"}  " + "  ] " + " } ";

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    Properties parentControllerProperties = new Properties();
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    parentControllerProperties.setProperty(
        ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS,
        String.valueOf(Long.MAX_VALUE));
    parentControllerProperties.setProperty(ConfigKeys.OFFLINE_JOB_START_TIMEOUT_MS, "180000");

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
            .sslToStorageNodes(true)
            .forkServer(false)
            .serverProperties(serverProperties)
            .parentControllerProperties(parentControllerProperties);
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

    multiClusterWrapper = multiRegionMultiClusterWrapper.getChildRegions().get(0);
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
    // When we create store, we will send both messages below to create system store
    markAsTested("PushStatusSystemStoreAutoCreationValidation");
    markAsTested("MetaSystemStoreAutoCreationValidation");

    clusterName = CLUSTER_NAMES[0];
    VeniceControllerWrapper parentController = parentControllers.get(0);
    parentControllerClient = new ControllerClient(clusterName, parentController.getControllerUrl());
    String storeName = Utils.getUniqueString("test-store");

    // Create store
    NewStoreResponse newStoreResponse =
        parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"");
    assertFalse(newStoreResponse.isError());

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
    assertThrows(
        VeniceException.class,
        () -> veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1));

    assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions(), store.getVersions());

    // Store has been disabled, can not accept a new version
    assertThrows(
        VeniceException.class,
        () -> veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1));

    assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions(), store.getVersions());

    // Resume store
    veniceAdmin.setStoreWriteability(clusterName, storeName, true);

    emptyPushToStore(parentControllerClient, storeName, 1);
    store = veniceAdmin.getStore(clusterName, storeName);
    assertTrue(store.isEnableWrites());
    assertEquals(store.getVersions().size(), 1);
    assertEquals(store.peekNextVersionNumber(), 2);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testKillOfflinePushJob() {
    markAsTested("KillOfflinePushJob");

    String storeName = setUpTestStore().getName();

    // Empty push
    VersionCreationResponse vcr = parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-push"), 1L);
    assertFalse(vcr.isError());
    // No wait to kill the push job
    // Kill push job
    parentControllerClient.killOfflinePushJob(Version.composeKafkaTopic(storeName, 1));

    // Check version
    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = childControllerClient.getStore(storeName);
        assertFalse(storeResponse.isError());
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
        assertFalse(storeResponse.isError());
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
        assertFalse(storeResponse.isError());
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
      assertFalse(storeResponse.isError());
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
      assertFalse(storeResponse.isError());
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
      assertFalse(storeResponse.isError());
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
      assertFalse(storeResponse.isError());
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
    assertFalse(newStoreResponse.isError());

    // Empty push
    emptyPushToStore(parentControllerClient, storeName, 1);

    // Store update
    ControllerResponse updateStore =
        parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setBatchGetLimit(100));
    assertFalse(updateStore.isError());

    for (ControllerClient childControllerClient: childControllerClients) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = childControllerClient.getStore(storeName);
        assertFalse(storeResponse.isError());
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
      assertTrue(storeResponse.isError());
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
      assertFalse(storeResponse.isError());
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
    assertEquals(veniceAdmin.getDerivedSchemas(clusterName, storeName).size(), 1);
  }

  @Test
  public void testValueSchemaCreation() {
    markAsTested("ValueSchemaCreation");
    markAsTested("DeleteUnusedValueSchemas");

    String storeName = Utils.getUniqueString("testValueSchemaCreation-store");

    String valueRecordSchemaStr1 = BASIC_USER_SCHEMA_STRING_WITH_DEFAULT;
    String valueRecordSchemaStr2 = TestWriteUtils.SIMPLE_USER_WITH_DEFAULT_SCHEMA.toString();
    NewStoreResponse newStoreResponse = parentControllerClient
        .retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", valueRecordSchemaStr1));
    assertFalse(newStoreResponse.isError(), "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    SchemaResponse schemaResponse2 =
        parentControllerClient.retryableRequest(5, c -> c.addValueSchema(storeName, valueRecordSchemaStr2));
    assertFalse(schemaResponse2.isError(), "addValueSchema returned error: " + schemaResponse2.getError());

    // Delete value schema
    parentControllerClient.deleteValueSchemas(storeName, new ArrayList<>(1));
  }

  @Test
  public void testStoragePersona() {
    markAsTested("CreateStoragePersona");
    markAsTested("UpdateStoragePersona");
    markAsTested("DeleteStoragePersona");
    long totalQuota = 1000;
    StoragePersona persona = TestStoragePersonaUtils.createDefaultPersona();
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
        () -> assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));

    response = parentControllerClient.deleteStoragePersona(persona.getName());
    if (response.isError())
      throw new VeniceException(response.getError());
    assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> assertNull(parentControllerClient.getStoragePersona(persona.getName()).getStoragePersona()));
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
        assertFalse(storeResponse.isError());
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
        assertFalse(storeResponse.isError());
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
      assertFalse(storeResponse.isError());
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
      assertFalse(storeResponse.isError());
      StoreInfo storeInfo = storeResponse.getStore();
      assertTrue(storeInfo.isActiveActiveReplicationEnabled());
    });
  }

  @Test
  public void testConfigureActiveActiveReplicationForCluster() {
    markAsTested("ConfigureActiveActiveReplicationForCluster");
    TestUtils.assertCommand(
        parentControllerClient.configureActiveActiveReplicationForCluster(
            true,
            VeniceUserStoreType.BATCH_ONLY.toString(),
            Optional.empty()),
        "Failed to configure active-active replication for cluster " + clusterName);
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
  public void testMigrateStore() throws Exception {
    markAsTested("MigrateStore");
    markAsTested("AbortMigration");

    String storeName = Utils.getUniqueString("test");

    String srcClusterName = CLUSTER_NAMES[0]; // venice-cluster0
    String destClusterName = CLUSTER_NAMES[1]; // venice-cluster1

    createAndPushStore(srcClusterName, storeName);
    String srcD2ServiceName = multiClusterWrapper.getClusterToD2().get(srcClusterName);
    String destD2ServiceName = multiClusterWrapper.getClusterToD2().get(destClusterName);
    D2Client d2Client =
        D2TestUtils.getAndStartD2Client(multiClusterWrapper.getClusters().get(srcClusterName).getZk().getAddress());
    ClientConfig clientConfig =
        ClientConfig.defaultGenericClientConfig(storeName).setD2ServiceName(srcD2ServiceName).setD2Client(d2Client);

    String parentControllerUrl = multiRegionMultiClusterWrapper.getChildRegions().get(0).getControllerConnectString();
    try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(clientConfig)) {
      readFromStore(client);
      StoreMigrationTestUtil.startMigration(parentControllerUrl, storeName, srcClusterName, destClusterName);
      StoreMigrationTestUtil
          .completeMigration(parentControllerUrl, storeName, srcClusterName, destClusterName, FABRIC0);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // StoreConfig in router might not be up-to-date. Keep reading from the store. Finally, router will find that
        // cluster discovery changes and redirect the request to dest store. Client's d2ServiceName will be updated.
        readFromStore(client);
        AbstractAvroStoreClient<String, Object> castClient =
            (AbstractAvroStoreClient<String, Object>) ((StatTrackingStoreClient<String, Object>) client)
                .getInnerStoreClient();
        assertTrue(castClient.toString().contains(destD2ServiceName));
      });
    }

    // Test abort migration on parent controller
    try (ControllerClient srcParentControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
        ControllerClient destParentControllerClient = new ControllerClient(destClusterName, parentControllerUrl)) {
      StoreMigrationTestUtil.abortMigration(parentControllerUrl, storeName, true, srcClusterName, destClusterName);
      TestUtils.waitForNonDeterministicAssertion(
          30,
          TimeUnit.SECONDS,
          () -> StoreMigrationTestUtil.checkStatusAfterAbortMigration(
              srcParentControllerClient,
              destParentControllerClient,
              storeName,
              srcClusterName));
    }
  }

  @Test
  public void testAddVersion() {
    markAsTested("AddVersion");

    String storeName = setUpTestStore().getName();
    String pushJobId = "test-push-job-id";

    // Add version
    veniceAdmin.addVersionAndStartIngestion(
        clusterName,
        storeName,
        pushJobId,
        1,
        1,
        Version.PushType.BATCH,
        null,
        -1,
        1,
        false,
        -1);
    assertNotNull(veniceAdmin.getStore(clusterName, storeName).getVersion(1));
    assertEquals(
        veniceAdmin.getStore(clusterName, storeName).getVersions().size(),
        1,
        "There should only be exactly one version added to the test-store");
  }

  @Test
  public void testMetadataSchemaCreation() {
    markAsTested("MetadataSchemaCreation");
    String storeName = Utils.getUniqueString("aa_store");
    String recordSchemaStr = TestWriteUtils.USER_WITH_DEFAULT_SCHEMA.toString();
    Schema metadataSchema = RmdSchemaGenerator.generateMetadataSchema(recordSchemaStr, 1);

    veniceAdmin.createStore(clusterName, storeName, "storeOwner", KEY_SCHEMA, recordSchemaStr);
    veniceAdmin.addReplicationMetadataSchema(clusterName, storeName, 1, 1, metadataSchema.toString());
    Collection<RmdSchemaEntry> metadataSchemas = veniceAdmin.getReplicationMetadataSchemas(clusterName, storeName);
    assertEquals(metadataSchemas.size(), 1);
    assertEquals(metadataSchemas.iterator().next().getSchema(), metadataSchema);
  }

  @Test
  public void testSupersetSchemaCreation() {
    markAsTested("SupersetSchemaCreation");

    Schema valueSchemaV1 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV1.avsc"));
    // Contains f2, f3
    Schema valueSchemaV4 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV4.avsc"));

    String storeName = Utils.getUniqueString("testSupersetSchemaCreation-store");
    NewStoreResponse newStoreResponse = parentControllerClient
        .retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", valueSchemaV1.toString()));
    assertFalse(newStoreResponse.isError(), "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    ControllerResponse updateStoreResponse =
        parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(true));
    assertFalse(updateStoreResponse.isError());

    SchemaResponse schemaResponse2 =
        parentControllerClient.retryableRequest(5, c -> c.addValueSchema(storeName, valueSchemaV4.toString()));
    assertFalse(schemaResponse2.isError(), "addValueSchema returned error: " + schemaResponse2.getError());

    // Verify superset schema id
    StoreResponse storeResponse = parentControllerClient.getStore(storeName);
    assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
    assertEquals(
        storeResponse.getStore().getLatestSuperSetValueSchemaId(),
        3,
        "Superset schema ID should be the last schema");

    // Get the value schema
    SchemaResponse schemaResponse = parentControllerClient.getValueSchema(storeName, 3);
    assertFalse(schemaResponse.isError());
    String supersetSchemaString = schemaResponse.getSchemaStr();
    assertTrue(supersetSchemaString.contains("f0"));
    assertTrue(supersetSchemaString.contains("f1"));
    assertTrue(supersetSchemaString.contains("f2"));
    assertTrue(supersetSchemaString.contains("f3"));
  }

  private void emptyPushToStore(ControllerClient parentControllerClient, String storeName, int expectedVersion) {
    VersionCreationResponse vcr = parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-push"), 1L);
    assertFalse(vcr.isError());
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
    NewStoreResponse response = parentControllerClient.createNewStore(
        testStore.getName(),
        testStore.getOwner(),
        TestWriteUtils.STRING_SCHEMA.toString(),
        TestWriteUtils.STRING_SCHEMA.toString());
    assertFalse(response.isError());
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

  private void readFromStore(AvroGenericStoreClient<String, Object> client)
      throws ExecutionException, InterruptedException {
    int key = ThreadLocalRandom.current().nextInt(RECORD_COUNT) + 1;
    client.get(Integer.toString(key)).get();
  }

  private Properties createAndPushStore(String srcClusterName, String storeName) throws Exception {
    File inputDir = TestWriteUtils.getTempDataDirectory();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    props.put(VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY, true);
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, RECORD_COUNT);
    String keySchemaStr = recordSchema.getField(VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP).schema().toString();

    UpdateStoreQueryParams updateStoreQueryParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setHybridRewindSeconds(TEST_TIMEOUT)
            .setHybridOffsetLagThreshold(2L)
            .setHybridStoreDiskQuotaEnabled(true)
            .setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT)
            .setStorageNodeReadQuotaEnabled(true); // enable this for using fast client
    IntegrationTestPushUtils
        .createStoreForJob(srcClusterName, keySchemaStr, valueSchemaStr, props, updateStoreQueryParams)
        .close();

    // Verify store is created in dc-0
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      StoreResponse response = childControllerClients.get(0).getStore(storeName);
      StoreInfo storeInfo = response.getStore();
      assertNotNull(storeInfo);
    });

    SystemProducer veniceProducer0 = null;
    try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
      job.run();

      // Write streaming records
      veniceProducer0 = IntegrationTestPushUtils
          .getSamzaProducer(multiClusterWrapper.getClusters().get(srcClusterName), storeName, Version.PushType.STREAM);
      for (int i = 1; i <= 10; i++) {
        IntegrationTestPushUtils.sendStreamingRecord(veniceProducer0, storeName, i);
      }
    } catch (Exception e) {
      throw new VeniceException(e);
    } finally {
      if (veniceProducer0 != null) {
        veniceProducer0.stop();
      }
    }

    return props;
  }
}
