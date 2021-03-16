package com.linkedin.venice.endToEnd;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.*;
import static com.linkedin.venice.meta.IngestionMode.*;
import static com.linkedin.venice.meta.PersistenceType.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class PushStatusStoreTest {
  private static final int TEST_TIMEOUT = 60_000; // ms

  private VeniceClusterWrapper cluster;
  private VeniceControllerWrapper parentController;
  private ControllerClient parentControllerClient;
  private D2Client d2Client;
  private PushStatusStoreReader reader;
  private Properties h2vProperties;
  private VeniceProperties backendConfig;
  private String storeName;

  @BeforeMethod
  public void setup() throws Exception {
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
    Properties controllerConfig = new Properties();
    controllerConfig.setProperty(CONTROLLER_AUTO_MATERIALIZE_METADATA_SYSTEM_STORE_ENABLED, String.valueOf(true));
    parentController =
        ServiceFactory.getVeniceParentController(cluster.getClusterName(), ServiceFactory.getZkServer().getAddress(), cluster.getKafka(),
            cluster.getVeniceControllers().toArray(new VeniceControllerWrapper[0]),
            new VeniceProperties(controllerConfig), false);
    parentControllerClient = new ControllerClient(cluster.getClusterName(), parentController.getControllerUrl());
    d2Client = D2TestUtils.getAndStartD2Client(cluster.getZk().getAddress());
    reader = new PushStatusStoreReader(d2Client, TimeUnit.MINUTES.toSeconds(10));

    storeName = TestUtils.getUniqueString("store");
    // Produce input data.
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    writeSimpleAvroFileWithIntToIntSchema(inputDir, true);
    // Setup H2V job properties.
    h2vProperties = defaultH2VProps(cluster, inputDirPath, storeName);
    backendConfig = new PropertyBuilder()
        .put(ConfigKeys.DATA_BASE_PATH, TestUtils.getTempDataDirectory().getAbsolutePath())
        .put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB)
        .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 10)
        .put(ConfigKeys.SERVER_ROCKSDB_STORAGE_CONFIG_CHECK_ENABLED, true)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .build();

    // set up push status store
    String owner = "test";
    String zkSharedPushStatusStoreName = VeniceSystemStoreUtils.getSharedZkNameForDaVinciPushStatusStore(cluster.getClusterName());
    String zkSharedMetadataStoreName = VeniceSystemStoreUtils.getSharedZkNameForMetadataStore(cluster.getClusterName());
    TestUtils.assertCommand(parentControllerClient.createNewZkSharedStoreWithDefaultConfigs(zkSharedPushStatusStoreName, owner));
    TestUtils.assertCommand(parentControllerClient.newZkSharedStoreVersion(zkSharedPushStatusStoreName));
    TestUtils.assertCommand(parentControllerClient.createNewZkSharedStoreWithDefaultConfigs(zkSharedMetadataStoreName, owner));
    TestUtils.assertCommand(parentControllerClient.newZkSharedStoreVersion(zkSharedMetadataStoreName));
    TestUtils.assertCommand(parentControllerClient.createNewStore(storeName, owner, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA));
    TestUtils.assertCommand(parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)));
    TestUtils.assertCommand(parentControllerClient.createDaVinciPushStatusStore(storeName));
    String metadataStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreUtils.getMetadataStoreName(storeName), 1);
    // The corresponding metadata store should be materialized automatically.
    TestUtils.waitForNonDeterministicPushCompletion(metadataStoreTopic, cluster.getControllerClient(), 30, TimeUnit.SECONDS,
        Optional.empty());
  }

  @AfterMethod
  public void cleanup() {
    IOUtils.closeQuietly(reader);
    D2ClientUtils.shutdownClient(d2Client);
    IOUtils.closeQuietly(parentControllerClient);
    IOUtils.closeQuietly(parentController);
    IOUtils.closeQuietly(cluster);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT)
  public void testKafkaPushJob(boolean isIsolated) throws Exception {
    // setup initial version
    runH2V(h2vProperties, 1, cluster);
    int applicationListenerPort = Utils.getFreePort();
    int servicePort = Utils.getFreePort();
    VeniceProperties backendConfig = new PropertyBuilder()
        .put(DATA_BASE_PATH, TestUtils.getTempDataDirectory().getAbsolutePath())
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(SERVER_INGESTION_MODE, isIsolated ? ISOLATED : BUILT_IN)
        .put(SERVER_INGESTION_ISOLATION_APPLICATION_PORT, applicationListenerPort)
        .put(SERVER_INGESTION_ISOLATION_SERVICE_PORT, servicePort)
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, cluster.getZk().getAddress())
        .put(ConfigKeys.SERVER_ROCKSDB_STORAGE_CONFIG_CHECK_ENABLED, true)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 10)
        .build();
    try (DaVinciClient daVinciClient = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, new DaVinciConfig(), backendConfig)) {
      daVinciClient.subscribeAll().get();
      runH2V(h2vProperties, 2, cluster);
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        assertEquals(reader.getPartitionStatus(storeName, 2, 0, Optional.empty()).size(), 1);
      });
    }

    String pushStatusStoreTopic =
        Version.composeKafkaTopic(VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName), 1);
    assertTrue(cluster.getVeniceControllers().get(0).getVeniceAdmin().isResourceStillAlive(pushStatusStoreTopic));
    assertFalse(cluster.getVeniceControllers().get(0).getVeniceAdmin().isTopicTruncated(pushStatusStoreTopic));
    parentControllerClient.deleteDaVinciPushStatusStore(storeName);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      cluster.getVeniceControllers().get(0).getVeniceAdmin().getPushStatusStoreRecordDeleter()
          .ifPresent(deleter -> assertNull(deleter.getPushStatusStoreVeniceWriter(storeName)));
      assertFalse(cluster.getVeniceControllers().get(0).getVeniceAdmin().isResourceStillAlive(pushStatusStoreTopic));
      assertTrue(!cluster.getVeniceControllers().get(0).getVeniceAdmin().getTopicManager().containsTopic(pushStatusStoreTopic)
          || cluster.getVeniceControllers().get(0).getVeniceAdmin().isTopicTruncated(pushStatusStoreTopic));
    });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testIncrementalPush() throws Exception {
    assertFalse(parentControllerClient.updateStore(storeName, new UpdateStoreQueryParams()
        .setIncrementalPushEnabled(true)).isError());
    // setup initial version
    runH2V(h2vProperties, 1, cluster);
    try (DaVinciClient daVinciClient = ServiceFactory.getGenericAvroDaVinciClient(storeName, cluster, new DaVinciConfig(), backendConfig)) {
      daVinciClient.subscribeAll().get();
      h2vProperties.setProperty(INCREMENTAL_PUSH, "true");
      runH2V(h2vProperties, 1, cluster);
      // TODO: Add assertions
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testAutomaticPurge() throws Exception {
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

  private static void runH2V(Properties h2vProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) {
    long h2vStart = System.currentTimeMillis();
    String jobName = TestUtils.getUniqueString("batch-job-" + expectedVersionNumber);
    try (KafkaPushJob job = new KafkaPushJob(jobName, h2vProperties)) {
      job.run();
      String storeName = (String) h2vProperties.get(KafkaPushJob.VENICE_STORE_NAME_PROP);
      cluster.waitVersion(storeName, expectedVersionNumber);
      logger.info("**TIME** H2V" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - h2vStart));
    }
  }

}
