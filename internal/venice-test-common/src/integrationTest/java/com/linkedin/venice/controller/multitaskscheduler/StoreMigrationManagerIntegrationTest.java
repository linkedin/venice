package com.linkedin.venice.controller.multitaskscheduler;

import static com.linkedin.venice.ConfigKeys.OFFLINE_JOB_START_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INBOUND_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_QUOTA_ENFORCEMENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.StatTrackingStoreClient;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreMigrationManagerIntegrationTest {
  private StoreMigrationManager storeMigrationManager;
  private static final long TIMEOUT = 10 * Time.MS_PER_SECOND;
  private static final int TEST_TIMEOUT = 180 * Time.MS_PER_SECOND;
  private static final int RECORD_COUNT = 20;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper twoLayerMultiRegionMultiClusterWrapper;
  private VeniceMultiClusterWrapper multiClusterWrapper;

  private String parentControllerUrl;
  private String childControllerUrl0;
  String[] clusterNames;
  private List<String> fabricList;

  @BeforeMethod
  public void setUp() {
    Properties parentControllerProperties = new Properties();
    parentControllerProperties
        .setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(4000));
    parentControllerProperties.setProperty(OFFLINE_JOB_START_TIMEOUT_MS, "180000");

    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_HTTP2_INBOUND_ENABLED, "true");
    serverProperties.put(SERVER_QUOTA_ENFORCEMENT_ENABLED, "true");

    // 1 parent controller, 1 child region, 2 clusters per child region, 2 servers per cluster
    // RF=2 to test both leader and follower SNs
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(2)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(2)
            .sslToStorageNodes(true)
            .forkServer(false)
            .parentControllerProperties(parentControllerProperties)
            .childControllerProperties(null)
            .serverProperties(serverProperties);
    twoLayerMultiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    this.fabricList = twoLayerMultiRegionMultiClusterWrapper.getChildRegionNames();
    multiClusterWrapper = twoLayerMultiRegionMultiClusterWrapper.getChildRegions().get(0);
    clusterNames = multiClusterWrapper.getClusterNames();

    parentControllerUrl = twoLayerMultiRegionMultiClusterWrapper.getControllerConnectString();
    childControllerUrl0 = multiClusterWrapper.getControllerConnectString();
    storeMigrationManager = StoreMigrationManager.createStoreMigrationManager(2, 3, 1, fabricList);
  }

  @AfterMethod
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(twoLayerMultiRegionMultiClusterWrapper);
    storeMigrationManager.shutdown();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testScheduleMigrationWithRealExecutor() throws Exception {
    String storeName = Utils.getUniqueString("TestStore");
    String sourceCluster = clusterNames[0];
    String destCluster = clusterNames[1];
    createAndPushStore(sourceCluster, storeName);

    StoreInfo srcStoreInfo = getStoreConfig(childControllerUrl0, sourceCluster, storeName);
    Optional<Version> sourceCurrentVersion = srcStoreInfo.getVersion(srcStoreInfo.getCurrentVersion());
    Assert.assertTrue(sourceCurrentVersion.isPresent());
    final Version expectedVersion = sourceCurrentVersion.get();

    String srcD2ServiceName = multiClusterWrapper.getClusterToD2().get(sourceCluster);
    String destD2ServiceName = multiClusterWrapper.getClusterToD2().get(destCluster);
    D2Client d2Client =
        D2TestUtils.getAndStartD2Client(multiClusterWrapper.getClusters().get(sourceCluster).getZk().getAddress());
    ClientConfig clientConfig =
        ClientConfig.defaultGenericClientConfig(storeName).setD2ServiceName(srcD2ServiceName).setD2Client(d2Client);

    int initialStep = 1;
    ControllerClient srcParentControllerClient = new ControllerClient(sourceCluster, parentControllerUrl);
    ControllerClient destParentControllerClient = new ControllerClient(destCluster, parentControllerUrl);
    VeniceHelixAdmin veniceHelixAdmin =
        multiClusterWrapper.getClusters().get(destCluster).getLeaderVeniceController().getVeniceHelixAdmin();

    int pauseAfterStep = 4; // Pause after step 4
    // Schedule the migration
    storeMigrationManager.scheduleMigration(
        storeName,
        sourceCluster,
        destCluster,
        initialStep,
        1,
        true,
        srcParentControllerClient,
        destParentControllerClient,
        veniceHelixAdmin.getControllerClientMap(sourceCluster),
        veniceHelixAdmin.getControllerClientMap(destCluster),
        4);
    MigrationRecord migrationRecord = storeMigrationManager.getMigrationRecord(storeName);

    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      assertTrue(migrationRecord.isPaused(), "Migration record should be paused. ");
    });
    // Verify that the migration record is paused after step 4
    assertEquals(
        migrationRecord.getCurrentStep(),
        pauseAfterStep + 1,
        "Migration record should be on step " + (pauseAfterStep + 1));

    try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(clientConfig)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // StoreConfig in router might not be up-to-date. Keep reading from the store. Finally, router will find that
        // cluster discovery changes and redirect the request to dest store. Client's d2ServiceName will be updated.
        readFromStore(client);
        AbstractAvroStoreClient<String, Object> castClient =
            (AbstractAvroStoreClient<String, Object>) ((StatTrackingStoreClient<String, Object>) client)
                .getInnerStoreClient();
        Assert.assertTrue(castClient.toString().contains(destD2ServiceName));
      });
    }

    // Test current version config in the destination cluster is the same as the version config from the source cluster,
    // and they don't match any of the unexpected values.
    StoreInfo destStoreInfo = getStoreConfig(childControllerUrl0, destCluster, storeName);
    Optional<Version> destCurrentVersion = destStoreInfo.getVersion(srcStoreInfo.getCurrentVersion());
    Assert.assertTrue(destCurrentVersion.isPresent());
    final Version destVersionConfig = destCurrentVersion.get();
    Assert.assertEquals(destVersionConfig, expectedVersion);

    storeMigrationManager.resumeMigration(storeName, MigrationRecord.Step.END_MIGRATION);
    // Allow time for the sequentially scheduled tasks to execute
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      assertEquals(
          migrationRecord.getCurrentStep(),
          6,
          "Migration record should be updated to 6 as marked as completed.");
    });
    assertNull(
        storeMigrationManager.getMigrationRecord(storeName),
        "Migration record should be cleaned up from migrationRecords after end migration.");
  }

  private Properties createAndPushStore(String clusterName, String storeName) throws Exception {
    File inputDir = TestWriteUtils.getTempDataDirectory();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(twoLayerMultiRegionMultiClusterWrapper, inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, RECORD_COUNT);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    UpdateStoreQueryParams updateStoreQueryParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setHybridRewindSeconds(TIMEOUT)
            .setHybridOffsetLagThreshold(2L)
            .setHybridStoreDiskQuotaEnabled(true)
            .setLargestUsedRTVersionNumber(2)
            .setRealTimeTopicName(Utils.composeRealTimeTopic(props.getProperty(VENICE_STORE_NAME_PROP), 1))
            .setCompressionStrategy(CompressionStrategy.ZSTD_WITH_DICT)
            .setStorageNodeReadQuotaEnabled(true); // enable this for using fast client
    IntegrationTestPushUtils.createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreQueryParams)
        .close();

    // Verify store is created in dc-0
    try (ControllerClient childControllerClient0 = new ControllerClient(clusterName, childControllerUrl0)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse response = childControllerClient0.getStore(storeName);
        StoreInfo storeInfo = response.getStore();
        Assert.assertNotNull(storeInfo);
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

  private void readFromStore(AvroGenericStoreClient<String, Object> client)
      throws ExecutionException, InterruptedException {
    int key = ThreadLocalRandom.current().nextInt(RECORD_COUNT) + 1;
    client.get(Integer.toString(key)).get();
  }

  private StoreInfo getStoreConfig(String controllerUrl, String clusterName, String storeName) {
    try (ControllerClient controllerClient = new ControllerClient(clusterName, controllerUrl)) {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      if (storeResponse.isError()) {
        throw new VeniceException(
            "Failed to get store configs for store " + storeName + " from cluster " + clusterName + ". Error: "
                + storeResponse.getError());
      }
      return storeResponse.getStore();
    }
  }
}
