package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
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
  private static final int TEST_TIMEOUT = 60_000; // ms

  private VeniceClusterWrapper cluster;
  private ControllerClient controllerClient;
  private D2Client d2Client;
  private PushStatusStoreReader reader;
  private String storeName;

  @BeforeClass
  public void setup() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    extraProperties.setProperty(CONTROLLER_ZK_SHARED_DAVINCI_PUSH_STATUS_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED, String.valueOf(true));
    extraProperties.setProperty(CONTROLLER_ZK_SHARED_METADATA_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED, String.valueOf(true));
    extraProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, String.valueOf(true));
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
  }

  @AfterClass
  public void cleanup() {
    IOUtils.closeQuietly(reader);
    D2ClientUtils.shutdownClient(d2Client);
    IOUtils.closeQuietly(controllerClient);
    IOUtils.closeQuietly(cluster);
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

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT * 2)
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
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
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

  @Test(timeOut = TEST_TIMEOUT)
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

  @Test(timeOut = TEST_TIMEOUT)
  public void testAutomaticPurge() throws Exception {
    VeniceProperties backendConfig = getBackendConfigBuilder().build();
    Properties h2vProperties = getH2VProperties();
    // setup initial version
    runH2V(h2vProperties, 1, cluster);
    try (DaVinciClient daVinciClient = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, new DaVinciConfig(), backendConfig)) {
      daVinciClient.subscribeAll().get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertEquals(reader.getPartitionStatus(storeName, 1, 0, Optional.empty()).size(), 1);
      });
      runH2V(h2vProperties, 2, cluster);
      runH2V(h2vProperties, 3, cluster);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertEquals(reader.getPartitionStatus(storeName, 1, 0, Optional.empty()).size(), 0);
      });
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
