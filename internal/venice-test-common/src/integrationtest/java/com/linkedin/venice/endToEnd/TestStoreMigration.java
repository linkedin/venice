package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ZK_SHARED_DAVINCI_PUSH_STATUS_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_CLUSTER_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestPushUtils.getTempDataDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.venice.AdminTool;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.StatTrackingStoreClient;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * The suite includes integration tests for all store migration tools, including migrate-store, complete-migration,
 * end-migration and abort-migration. Test cases cover store migration on parent controller and child controller.
 *
 * Test stores are hybrid stores created in {@link TestStoreMigration#createAndPushStore(String, String, String)}.
 * Test stores enable L/F in child datacenter dc-0 and disable L/F in parent datacenter.
 */
@Test(singleThreaded = true, groups = "flaky") // TODO: remove "flaky" once tests passed consistently in Venice-Flaky
public class TestStoreMigration {
  private static final int TEST_TIMEOUT = 120 * Time.MS_PER_SECOND;
  private static final int RECORD_COUNT = 20;
  private static final String NEW_OWNER = "newtest@linkedin.com";
  private static final String FABRIC0 = "dc-0";
  private static final boolean[] ABORT_MIGRATION_PROMPTS_OVERRIDE = { false, true, true };
  private static final Logger LOGGER = LogManager.getLogger(TestStoreMigration.class);

  private VeniceTwoLayerMultiColoMultiClusterWrapper twoLayerMultiColoMultiClusterWrapper;
  private VeniceMultiClusterWrapper multiClusterWrapper;
  private String srcClusterName;
  private String destClusterName;
  private String parentControllerUrl;
  private String childControllerUrl0;

  @BeforeClass(timeOut = TEST_TIMEOUT)
  public void setUp() {
    Properties parentControllerProperties = new Properties();
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    parentControllerProperties
        .setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    // Required by metadata system store
    parentControllerProperties.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
    parentControllerProperties.setProperty(
        CONTROLLER_ZK_SHARED_DAVINCI_PUSH_STATUS_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED,
        String.valueOf(true));

    // Enable system store auto-materialization
    parentControllerProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, "true");
    parentControllerProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, "true");

    Properties childControllerProperties = new Properties();
    // Required by metadata system store
    childControllerProperties.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");
    childControllerProperties.setProperty(
        CONTROLLER_ZK_SHARED_DAVINCI_PUSH_STATUS_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED,
        String.valueOf(true));

    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);

    // 1 parent controller, 1 child colo, 2 clusters per child colo, 2 servers per cluster
    // RF=2 to test both leader and follower SNs
    twoLayerMultiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
        1,
        2,
        1,
        1,
        2,
        1,
        2,
        Optional.of(new VeniceProperties(parentControllerProperties)),
        Optional.of(childControllerProperties),
        Optional.of(new VeniceProperties(serverProperties)),
        true);

    multiClusterWrapper = twoLayerMultiColoMultiClusterWrapper.getClusters().get(0);
    String[] clusterNames = multiClusterWrapper.getClusterNames();
    Arrays.sort(clusterNames);
    srcClusterName = clusterNames[0]; // venice-cluster0
    destClusterName = clusterNames[1]; // venice-cluster1
    parentControllerUrl = twoLayerMultiColoMultiClusterWrapper.getParentControllers()
        .stream()
        .map(VeniceControllerWrapper::getControllerUrl)
        .collect(Collectors.joining(","));
    childControllerUrl0 = multiClusterWrapper.getControllerConnectString();
  }

  @AfterClass
  public void cleanUp() {
    twoLayerMultiColoMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoreMigrationOnParentController() throws Exception {
    String storeName = Utils.getUniqueString("store");
    createAndPushStore(parentControllerUrl, srcClusterName, storeName);

    String srcD2ServiceName = multiClusterWrapper.getClusterToD2().get(srcClusterName);
    String destD2ServiceName = multiClusterWrapper.getClusterToD2().get(destClusterName);
    D2Client d2Client =
        D2TestUtils.getAndStartD2Client(multiClusterWrapper.getClusters().get(srcClusterName).getZk().getAddress());
    ClientConfig clientConfig =
        ClientConfig.defaultGenericClientConfig(storeName).setD2ServiceName(srcD2ServiceName).setD2Client(d2Client);

    try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(clientConfig)) {
      readFromStore(client);
      startMigration(parentControllerUrl, storeName);
      completeMigration(parentControllerUrl, storeName);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        // StoreConfig in router might not be up-to-date. Keep reading from the store. Finally router will find that
        // cluster discovery changes and redirect the request to dest store. Client's d2ServiceName will be updated.
        readFromStore(client);
        AbstractAvroStoreClient<String, Object> castClient =
            (AbstractAvroStoreClient<String, Object>) ((StatTrackingStoreClient<String, Object>) client)
                .getInnerStoreClient();
        Assert.assertTrue(castClient.toString().contains(destD2ServiceName));
      });
    }

    // Test abort migration on parent controller
    try (ControllerClient srcParentControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
        ControllerClient destParentControllerClient = new ControllerClient(destClusterName, parentControllerUrl)) {
      abortMigration(parentControllerUrl, storeName, true);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        checkStatusAfterAbortMigration(srcParentControllerClient, destParentControllerClient, storeName);
      });
      checkChildStatusAfterAbortMigration(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoreMigrationWithNewPushesAndUpdatesOnParentController() throws Exception {
    String storeName = Utils.getUniqueString("store");
    Properties props = createAndPushStore(parentControllerUrl, srcClusterName, storeName);

    try (ControllerClient srcParentControllerClient = new ControllerClient(srcClusterName, parentControllerUrl);
        ControllerClient destParentControllerClient = new ControllerClient(destClusterName, parentControllerUrl)) {
      startMigration(parentControllerUrl, storeName);
      // Ensure migration status is updated in source parent controller
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertTrue(srcParentControllerClient.getStore(storeName).getStore().isMigrating());
      });

      // Push v2
      TestPushUtils.runPushJob("Test push job 2", props);
      // Update store
      srcParentControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setOwner(NEW_OWNER));

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        // Largest used version number in dest cluster store should be 2
        StoreResponse storeResponse = destParentControllerClient.getStore(storeName);
        int largestUsedVersionNumber = storeResponse.getStore().getLargestUsedVersionNumber();
        Assert.assertEquals(largestUsedVersionNumber, 2);

        // Largest used version number in src cluster store should be 2
        storeResponse = srcParentControllerClient.getStore(storeName);
        largestUsedVersionNumber = storeResponse.getStore().getLargestUsedVersionNumber();
        Assert.assertEquals(largestUsedVersionNumber, 2);

        // Owner of dest cluster store should be updated
        storeResponse = destParentControllerClient.getStore(storeName);
        String owner = storeResponse.getStore().getOwner();
        Assert.assertEquals(owner, NEW_OWNER);

        // Owner of src cluster store should be updated
        storeResponse = srcParentControllerClient.getStore(storeName);
        owner = storeResponse.getStore().getOwner();
        Assert.assertEquals(owner, NEW_OWNER);

        // Test replication metadata version id in src cluster is updated
        storeResponse = srcParentControllerClient.getStore(storeName);
        int replicationMetadataVersionId = storeResponse.getStore().getVersions().get(1).getRmdVersionId();
        Assert.assertEquals(replicationMetadataVersionId, 1);

        // Test replication metadata version id in dest cluster is updated
        storeResponse = destParentControllerClient.getStore(storeName);
        replicationMetadataVersionId = storeResponse.getStore().getVersions().get(1).getRmdVersionId();
        Assert.assertEquals(replicationMetadataVersionId, 1);
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoreMigrationWithMetaSystemStoreOnParentController() throws Exception {
    String storeName = Utils.getUniqueString("store");
    createAndPushStore(parentControllerUrl, srcClusterName, storeName);

    try (ControllerClient srcParentControllerClient = new ControllerClient(srcClusterName, parentControllerUrl)) {
      // Enable the meta system store
      String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
      VersionCreationResponse versionCreationResponseForMetaSystemStore =
          srcParentControllerClient.emptyPush(metaSystemStoreName, "test_meta_system_store_push_1", 10000);
      assertFalse(
          versionCreationResponseForMetaSystemStore.isError(),
          "New version creation for meta system store: " + metaSystemStoreName + " should success, but got error: "
              + versionCreationResponseForMetaSystemStore.getError());
      TestUtils.waitForNonDeterministicPushCompletion(
          versionCreationResponseForMetaSystemStore.getKafkaTopic(),
          srcParentControllerClient,
          30,
          TimeUnit.SECONDS);

      String srcD2ServiceName = multiClusterWrapper.getClusterToD2().get(srcClusterName);
      D2Client d2Client =
          D2TestUtils.getAndStartD2Client(multiClusterWrapper.getClusters().get(srcClusterName).getZk().getAddress());

      ClientConfig<StoreMetaValue> clientConfig =
          ClientConfig.defaultSpecificClientConfig(metaSystemStoreName, StoreMetaValue.class)
              .setD2ServiceName(srcD2ServiceName)
              .setD2Client(d2Client)
              .setStoreName(metaSystemStoreName);

      try (AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> client =
          ClientFactory.getAndStartSpecificAvroClient(clientConfig)) {
        StoreMetaKey storePropertiesKey =
            MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {
              {
                put(KEY_STRING_STORE_NAME, storeName);
                put(KEY_STRING_CLUSTER_NAME, srcClusterName);
              }
            });
        StoreMetaValue storeProperties = client.get(storePropertiesKey).get();
        assertTrue(storeProperties != null && storeProperties.storeProperties != null);

        startMigration(parentControllerUrl, storeName);
        completeMigration(parentControllerUrl, storeName);

        // Verify the meta system store is materialized in the destination cluster and contains correct values.
        StoreMetaKey storePropertiesKeyInDestCluster =
            MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {
              {
                put(KEY_STRING_STORE_NAME, storeName);
                put(KEY_STRING_CLUSTER_NAME, destClusterName);
              }
            });
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          try {
            StoreMetaValue storePropertiesInDestCluster = client.get(storePropertiesKeyInDestCluster).get();
            assertTrue(storePropertiesInDestCluster != null && storePropertiesInDestCluster.storeProperties != null);
          } catch (Exception e) {
            fail("Exception is not unexpected: " + e.getMessage());
          }
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoreMigrationWithDaVinciPushStatusSystemStoreOnParentController() throws Exception {
    String storeName = Utils.getUniqueString("store");
    createAndPushStore(parentControllerUrl, srcClusterName, storeName);

    try (ControllerClient srcParentControllerClient = new ControllerClient(srcClusterName, parentControllerUrl)) {
      // Enable the da vinci push status system store
      String daVinciPushStatusSystemStoreName =
          VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
      VersionCreationResponse versionCreationResponseForDaVinciPushStatusSystemStore = srcParentControllerClient
          .emptyPush(daVinciPushStatusSystemStoreName, "test_davinci_push_status_system_store_push_1", 10000);
      assertFalse(
          versionCreationResponseForDaVinciPushStatusSystemStore.isError(),
          "New version creation for da vinci push status system store: " + daVinciPushStatusSystemStoreName
              + " should success, but got error: " + versionCreationResponseForDaVinciPushStatusSystemStore.getError());
      TestUtils.waitForNonDeterministicPushCompletion(
          versionCreationResponseForDaVinciPushStatusSystemStore.getKafkaTopic(),
          srcParentControllerClient,
          30,
          TimeUnit.SECONDS);
    }

    VeniceProperties backendConfig =
        DaVinciTestContext.getDaVinciPropertyBuilder(multiClusterWrapper.getZkServerWrapper().getAddress())
            .put(PUSH_STATUS_STORE_ENABLED, true)
            .build();

    D2Client srcD2Client =
        D2TestUtils.getAndStartD2Client(multiClusterWrapper.getClusters().get(srcClusterName).getZk().getAddress());
    PushStatusStoreReader srcReader = new PushStatusStoreReader(
        srcD2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        TimeUnit.MINUTES.toSeconds(10));
    try (DaVinciClient daVinciClient = ServiceFactory.getGenericAvroDaVinciClient(
        storeName,
        multiClusterWrapper.getClusters().get(srcClusterName),
        new DaVinciConfig(),
        backendConfig)) {
      daVinciClient.subscribeAll().get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertEquals(srcReader.getPartitionStatus(storeName, 1, 0, Optional.empty()).size(), 1);
      });
    } finally {
      Utils.closeQuietlyWithErrorLogged(srcReader);
      D2ClientUtils.shutdownClient(srcD2Client);
    }

    // migrate store
    startMigration(parentControllerUrl, storeName);
    completeMigration(parentControllerUrl, storeName);
    endMigration(parentControllerUrl, storeName);

    // verify if push status is reporting in destination cluster
    D2Client destD2Client =
        D2TestUtils.getAndStartD2Client(multiClusterWrapper.getClusters().get(destClusterName).getZk().getAddress());
    PushStatusStoreReader destReader = new PushStatusStoreReader(
        destD2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        TimeUnit.MINUTES.toSeconds(10));
    try (DaVinciClient daVinciClient = ServiceFactory.getGenericAvroDaVinciClient(
        storeName,
        multiClusterWrapper.getClusters().get(destClusterName),
        new DaVinciConfig(),
        backendConfig)) {
      daVinciClient.subscribeAll().get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertEquals(destReader.getPartitionStatus(storeName, 1, 0, Optional.empty()).size(), 1);
      });
    } finally {
      Utils.closeQuietlyWithErrorLogged(destReader);
      D2ClientUtils.shutdownClient(destD2Client);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoreMigrationOnChildController() throws Exception {
    String storeName = Utils.getUniqueString("store");
    createAndPushStore(parentControllerUrl, srcClusterName, storeName);

    startMigration(childControllerUrl0, storeName);
    completeMigration(childControllerUrl0, storeName);
    endMigration(childControllerUrl0, storeName);
  }

  private Properties createAndPushStore(String controllerUrl, String clusterName, String storeName) throws Exception {
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Schema recordSchema = TestPushUtils.writeSimpleAvroFileWithUserSchema(inputDir, true, RECORD_COUNT);
    Properties props = TestPushUtils.defaultVPJProps(controllerUrl, inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr =
        recordSchema.getField(props.getProperty(VenicePushJob.VALUE_FIELD_PROP)).schema().toString();

    UpdateStoreQueryParams updateStoreQueryParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setHybridRewindSeconds(TEST_TIMEOUT)
            .setHybridOffsetLagThreshold(2L);
    IntegrationTestPushUtils.createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreQueryParams)
        .close();

    // Verify store is created in dc-0
    try (ControllerClient childControllerClient0 = new ControllerClient(clusterName, childControllerUrl0)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        StoreResponse response = childControllerClient0.getStore(storeName);
        StoreInfo storeInfo = response.getStore();
        assertNotNull(storeInfo);
      });
    }

    SystemProducer veniceProducer0 = null;
    try (VenicePushJob job = new VenicePushJob("Test push job", props)) {
      job.run();

      // Write streaming records
      veniceProducer0 =
          getSamzaProducer(multiClusterWrapper.getClusters().get(clusterName), storeName, Version.PushType.STREAM);
      for (int i = 1; i <= 10; i++) {
        sendStreamingRecord(veniceProducer0, storeName, i);
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

  private void startMigration(String controllerUrl, String storeName) throws Exception {
    String[] startMigrationArgs = { "--migrate-store", "--url", controllerUrl, "--store", storeName, "--cluster-src",
        srcClusterName, "--cluster-dest", destClusterName };
    AdminTool.main(startMigrationArgs);
  }

  private void completeMigration(String controllerUrl, String storeName) throws Exception {
    String[] completeMigration0 = { "--complete-migration", "--url", controllerUrl, "--store", storeName,
        "--cluster-src", srcClusterName, "--cluster-dest", destClusterName, "--fabric", FABRIC0 };
    AdminTool.main(completeMigration0);

    try (ControllerClient destParentControllerClient = new ControllerClient(destClusterName, controllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        // Store discovery should point to the new cluster after completing migration
        ControllerResponse discoveryResponse = destParentControllerClient.discoverCluster(storeName);
        Assert.assertEquals(discoveryResponse.getCluster(), destClusterName);
      });
    }
  }

  private void endMigration(String controllerUrl, String storeName) throws Exception {
    String[] endMigration = { "--end-migration", "--url", controllerUrl, "--store", storeName, "--cluster-src",
        srcClusterName, "--cluster-dest", destClusterName };
    AdminTool.main(endMigration);

    try (ControllerClient srcControllerClient = new ControllerClient(srcClusterName, controllerUrl);
        ControllerClient destControllerClient = new ControllerClient(destClusterName, controllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        // Store should be deleted in source cluster. Store in destination cluster should not be migrating.
        StoreResponse storeResponse = srcControllerClient.getStore(storeName);
        Assert.assertNull(storeResponse.getStore());

        storeResponse = destControllerClient.getStore(storeName);
        Assert.assertNotNull(storeResponse.getStore());
        Assert.assertFalse(storeResponse.getStore().isMigrating());
        Assert.assertFalse(storeResponse.getStore().isMigrationDuplicateStore());
      });
    }
  }

  private void readFromStore(AvroGenericStoreClient<String, Object> client) {
    int key = ThreadLocalRandom.current().nextInt(RECORD_COUNT) + 1;
    client.get(Integer.toString(key));
  }

  private void abortMigration(String controllerUrl, String storeName, boolean force) {
    AdminTool.abortMigration(
        controllerUrl,
        storeName,
        srcClusterName,
        destClusterName,
        force,
        ABORT_MIGRATION_PROMPTS_OVERRIDE);
  }

  private void checkStatusAfterAbortMigration(
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient,
      String storeName) {
    // Migration flag should be false
    // Store should be deleted in dest cluster
    // Cluster discovery should point to src cluster
    StoreResponse storeResponse = srcControllerClient.getStore(storeName);
    Assert.assertNotNull(storeResponse.getStore());
    Assert.assertFalse(storeResponse.getStore().isMigrating());
    storeResponse = destControllerClient.getStore(storeName);
    Assert.assertNull(storeResponse.getStore());
    ControllerResponse discoveryResponse = destControllerClient.discoverCluster(storeName);
    Assert.assertEquals(discoveryResponse.getCluster(), srcClusterName);
  }

  private void checkChildStatusAfterAbortMigration(String storeName) {
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      try (
          ControllerClient srcChildControllerClient =
              new ControllerClient(srcClusterName, multiClusterWrapper.getControllerConnectString());
          ControllerClient destChildControllerClient =
              new ControllerClient(destClusterName, multiClusterWrapper.getControllerConnectString())) {
        checkStatusAfterAbortMigration(srcChildControllerClient, destChildControllerClient, storeName);
      }
    });
  }
}
