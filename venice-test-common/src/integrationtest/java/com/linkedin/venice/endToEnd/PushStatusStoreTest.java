package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.*;
import static com.linkedin.venice.meta.IngestionMode.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class PushStatusStoreTest {
  private static final int TEST_TIMEOUT_MS = 100 * Time.MS_PER_MINUTE; // For debugging purpose

  private VeniceClusterWrapper cluster;
  private ControllerClient controllerClient;
  private D2Client d2Client;
  private PushStatusStoreReader reader;
  private String storeName;
  private VeniceControllerWrapper parentController;
  private ZkServerWrapper parentZkServer;

  @BeforeClass
  public void setup() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    Utils.thisIsLocalhost();
    cluster = ServiceFactory.getVeniceCluster(
        1,
        1,
        1,
        1,
        10000,
        false,
        false,
        extraProperties);
    controllerClient = cluster.getControllerClient();
    d2Client = D2TestUtils.getAndStartD2Client(cluster.getZk().getAddress());
    reader = new PushStatusStoreReader(d2Client, TimeUnit.MINUTES.toSeconds(10));
    extraProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, String.valueOf(true));
    parentZkServer = ServiceFactory.getZkServer();
    parentController = ServiceFactory.getVeniceParentController(cluster.getClusterName(), parentZkServer.getAddress(),
        cluster.getKafka(), cluster.getVeniceControllers().toArray(new VeniceControllerWrapper[0]),
        new VeniceProperties(extraProperties), false);
  }

  @AfterClass
  public void cleanup() {
    IOUtils.closeQuietly(reader);
    D2ClientUtils.shutdownClient(d2Client);
    IOUtils.closeQuietly(controllerClient);
    parentController.close();
    IOUtils.closeQuietly(cluster);
    parentZkServer.close();
  }

  @BeforeMethod
  public void setUpStore() {
    storeName = TestUtils.getUniqueString("store");
    String owner = "test";
    // set up push status store
    TestUtils.assertCommand(controllerClient.createNewStore(storeName, owner, DEFAULT_KEY_SCHEMA, "\"string\""));
    TestUtils.assertCommand(controllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setLeaderFollowerModel(true)
        .setPartitionCount(2)
        .setAmplificationFactor(1)
        .setIncrementalPushEnabled(true)));
    String daVinciPushStatusSystemStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
    VersionCreationResponse versionCreationResponseForDaVinciPushStatusSystemStore =
        controllerClient.emptyPush(daVinciPushStatusSystemStoreName, "test_da_vinci_push_status_system_store_push_1", 10000);
    assertFalse(versionCreationResponseForDaVinciPushStatusSystemStore.isError(),
        "New version creation for Da Vinci push status system store: " + daVinciPushStatusSystemStoreName + " should success, but got error: "
            + versionCreationResponseForDaVinciPushStatusSystemStore.getError());
    TestUtils.waitForNonDeterministicPushCompletion(versionCreationResponseForDaVinciPushStatusSystemStore.getKafkaTopic(),
        controllerClient, 30, TimeUnit.SECONDS, Optional.empty());
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT_MS * 2)
  public void testKafkaPushJob(boolean isIsolated) throws Exception {
    Properties h2vProperties = getH2VProperties();
    // setup initial version
    runH2V(h2vProperties, 1, cluster);
    int applicationListenerPort = Utils.getFreePort();
    int servicePort = Utils.getFreePort();
    VeniceProperties backendConfig = getBackendConfigBuilder()
        .put(SERVER_INGESTION_MODE, isIsolated ? ISOLATED : BUILT_IN)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress())
        .build();
    try (DaVinciClient daVinciClient = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, new DaVinciConfig(), backendConfig)) {
      daVinciClient.subscribeAll().get();
      runH2V(h2vProperties, 2, cluster);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
        assertEquals(reader.getPartitionStatus(storeName, 2, 0, Optional.empty()).size(), 1);
      });
    }
    Admin admin = cluster.getVeniceControllers().get(0).getVeniceAdmin();
    String pushStatusStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName), 1);
    assertTrue(admin.isResourceStillAlive(pushStatusStoreTopic));
    assertFalse(admin.isTopicTruncated(pushStatusStoreTopic));
    TestUtils.assertCommand(controllerClient.disableAndDeleteStore(storeName));

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      assertFalse(admin.isResourceStillAlive(pushStatusStoreTopic));
      assertTrue(!admin.getTopicManager().containsTopic(pushStatusStoreTopic)
          || admin.isTopicTruncated(pushStatusStoreTopic));
    });
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testIncrementalPush() throws Exception {
    VeniceProperties backendConfig = getBackendConfigBuilder().build();
    Properties h2vProperties = getH2VProperties();
    runH2V(h2vProperties, 1, cluster);
    try (DaVinciClient daVinciClient = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, new DaVinciConfig(), backendConfig)) {
      daVinciClient.subscribeAll().get();
      h2vProperties = getH2VProperties();
      h2vProperties.setProperty(INCREMENTAL_PUSH, "true");
      runH2V(h2vProperties, 1, cluster);
      assertEquals(daVinciClient.get(1).get().toString(), "name 1");
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testAutomaticPurge() throws Exception {
    VeniceProperties backendConfig = getBackendConfigBuilder().build();
    Properties h2vProperties = getH2VProperties();
    // setup initial version
    runH2V(h2vProperties, 1, cluster);
    try (DaVinciClient daVinciClient = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, new DaVinciConfig(), backendConfig)) {
      daVinciClient.subscribeAll().get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
        assertEquals(reader.getPartitionStatus(storeName, 1, 0, Optional.empty()).size(), 1);
      });
      runH2V(h2vProperties, 2, cluster);
      runH2V(h2vProperties, 3, cluster);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
        assertEquals(reader.getPartitionStatus(storeName, 1, 0, Optional.empty()).size(), 0);
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testParentControllerAutoMaterializeDaVinciPushStatusSystemStore() {
    try (ControllerClient parentControllerClient = new ControllerClient(cluster.getClusterName(),
        parentController.getControllerUrl())) {
      String zkSharedDaVinciPushStatusSchemaStoreName =
          AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getSystemStoreName();
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Store readOnlyStore = parentController.getVeniceAdmin()
            .getReadOnlyZKSharedSystemStoreRepository()
            .getStore(zkSharedDaVinciPushStatusSchemaStoreName);
        assertNotNull(readOnlyStore, "Store: " + zkSharedDaVinciPushStatusSchemaStoreName + " should be initialized by "
            + ClusterLeaderInitializationRoutine.class.getSimpleName());
        assertTrue(readOnlyStore.isHybrid(),
            "Store: " + zkSharedDaVinciPushStatusSchemaStoreName + " should be configured to hybrid");
      });
      final String regularStoreName = TestUtils.getUniqueString("new-user-store");
      NewStoreResponse newStoreResponse = parentControllerClient.createNewStore(regularStoreName, "venice-test", DEFAULT_KEY_SCHEMA, "\"string\"");
      assertFalse(newStoreResponse.isError(), "Unexpected new store creation failure: " + newStoreResponse);
      String daVinciPushStatusSystemStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(regularStoreName);
      TestUtils.waitForNonDeterministicPushCompletion(Version.composeKafkaTopic(daVinciPushStatusSystemStoreName, 1),
          parentControllerClient, 30, TimeUnit.SECONDS, Optional.empty());

      Store daVinciPushStatusSystemStore = parentController.getVeniceAdmin().getStore(cluster.getClusterName(), daVinciPushStatusSystemStoreName);
      Assert.assertEquals(daVinciPushStatusSystemStore.getLargestUsedVersionNumber(), 1);

      // Do empty pushes to increase the system store's version
      final int emptyPushAttempt = 2;
      for (int i = 0; i < emptyPushAttempt; i++) {
        final int newVersion = parentController.getVeniceAdmin()
            .incrementVersionIdempotent(cluster.getClusterName(), daVinciPushStatusSystemStoreName,
                "push job ID placeholder " + i, 1, 1)
            .getNumber();
        parentController.getVeniceAdmin().writeEndOfPush(cluster.getClusterName(), daVinciPushStatusSystemStoreName, newVersion, true);
        TestUtils.waitForNonDeterministicPushCompletion(Version.composeKafkaTopic(daVinciPushStatusSystemStoreName, newVersion),
            parentControllerClient, 30, TimeUnit.SECONDS, Optional.empty());
      }
      daVinciPushStatusSystemStore = parentController.getVeniceAdmin().getStore(cluster.getClusterName(), daVinciPushStatusSystemStoreName);
      final int systemStoreCurrVersionBeforeBeingDeleted = daVinciPushStatusSystemStore.getLargestUsedVersionNumber();
      Assert.assertEquals(systemStoreCurrVersionBeforeBeingDeleted, 1 + emptyPushAttempt);

      parentControllerClient.disableAndDeleteStore(regularStoreName);
      // Both the system store and user store should be gone at this point
      Assert.assertNull(parentController.getVeniceAdmin().getStore(cluster.getClusterName(), regularStoreName));
      Assert.assertNull(parentController.getVeniceAdmin().getStore(cluster.getClusterName(), daVinciPushStatusSystemStoreName));

      // Create the same regular store again
      newStoreResponse = parentControllerClient.createNewStore(regularStoreName, "venice-test", DEFAULT_KEY_SCHEMA, "\"string\"");
      assertFalse(newStoreResponse.isError(), "Unexpected new store creation failure: " + newStoreResponse);

      // The re-created/materialized per-user store system store should contain a continued version from its last life
      daVinciPushStatusSystemStore = parentController.getVeniceAdmin().getStore(cluster.getClusterName(), daVinciPushStatusSystemStoreName);
      Assert.assertEquals(daVinciPushStatusSystemStore.getLargestUsedVersionNumber(), systemStoreCurrVersionBeforeBeingDeleted + 1);

    }
  }

  private PropertyBuilder getBackendConfigBuilder() {
    PropertyBuilder backendConfigBuilder = new PropertyBuilder()
        .put(ConfigKeys.DATA_BASE_PATH, TestUtils.getTempDataDirectory().getAbsolutePath())
        .put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
        .put(CLIENT_USE_META_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 10)
        .put(ConfigKeys.SERVER_ROCKSDB_STORAGE_CONFIG_CHECK_ENABLED, true)
        .put(PUSH_STATUS_STORE_ENABLED, true);
    return backendConfigBuilder;
  }

  private Properties getH2VProperties() throws Exception {
    // Setup H2V job properties.
    // Produce input data.
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    writeSimpleAvroFileWithIntToStringSchema(inputDir, true);
    Properties h2vProperties = defaultH2VProps(cluster, inputDirPath, storeName);
    return h2vProperties;
  }

  private void runH2V(Properties h2vProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) {
    long h2vStart = System.currentTimeMillis();
    String jobName = TestUtils.getUniqueString("batch-job-" + expectedVersionNumber);
    try (VenicePushJob job = new VenicePushJob(jobName, h2vProperties)) {
      job.run();
      String storeName = (String) h2vProperties.get(VenicePushJob.VENICE_STORE_NAME_PROP);
      cluster.waitVersion(storeName, expectedVersionNumber);
      logger.info("**TIME** H2V" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - h2vStart));
    }
  }

}
