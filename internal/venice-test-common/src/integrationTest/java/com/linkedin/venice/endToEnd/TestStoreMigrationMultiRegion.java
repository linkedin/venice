package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.OFFLINE_JOB_START_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INBOUND_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_QUOTA_ENFORCEMENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.StatTrackingStoreClient;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
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
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.StoreMigrationTestUtil;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStoreMigrationMultiRegion {
  private static final int TEST_TIMEOUT = 120 * Time.MS_PER_SECOND;
  private static final int RECORD_COUNT = 20;
  private List<String> fabricList;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper twoLayerMultiRegionMultiClusterWrapper;

  private VeniceMultiClusterWrapper multiClusterWrapper0;
  private VeniceMultiClusterWrapper multiClusterWrapper1;
  private String srcClusterName;
  private String destClusterName;
  private String parentControllerUrl;
  private String childControllerUrl0;
  private String childControllerUrl1;

  @BeforeClass(timeOut = TEST_TIMEOUT)
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties parentControllerProperties = new Properties();
    parentControllerProperties
        .setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(4000));
    parentControllerProperties.setProperty(OFFLINE_JOB_START_TIMEOUT_MS, "180000");

    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    serverProperties.put(SERVER_HTTP2_INBOUND_ENABLED, "true");
    serverProperties.put(SERVER_QUOTA_ENFORCEMENT_ENABLED, "true");

    // 1 parent controller, 2 child region, 2 clusters per child region, 2 servers per cluster
    // RF=2 to test both leader and follower SNs
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(2)
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
    fabricList = twoLayerMultiRegionMultiClusterWrapper.getChildRegionNames();
    // dc-0
    multiClusterWrapper0 = twoLayerMultiRegionMultiClusterWrapper.getChildRegions().get(0);
    String[] clusterNames = multiClusterWrapper0.getClusterNames();
    Arrays.sort(clusterNames);
    srcClusterName = clusterNames[0]; // venice-cluster0
    destClusterName = clusterNames[1]; // venice-cluster1
    parentControllerUrl = twoLayerMultiRegionMultiClusterWrapper.getControllerConnectString();
    childControllerUrl0 = multiClusterWrapper0.getControllerConnectString();
    // dc-1
    multiClusterWrapper1 = twoLayerMultiRegionMultiClusterWrapper.getChildRegions().get(1);
    childControllerUrl1 = multiClusterWrapper1.getControllerConnectString();

    for (String cluster: clusterNames) {
      try (ControllerClient controllerClient0 = new ControllerClient(cluster, childControllerUrl0);
          ControllerClient controllerClient1 = new ControllerClient(cluster, childControllerUrl1)) {
        // Verify the participant store is up and running in child region
        String participantStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(cluster);
        TestUtils.waitForNonDeterministicPushCompletion(
            Version.composeKafkaTopic(participantStoreName, 1),
            controllerClient0,
            3,
            TimeUnit.MINUTES);
        TestUtils.waitForNonDeterministicPushCompletion(
            Version.composeKafkaTopic(participantStoreName, 1),
            controllerClient1,
            3,
            TimeUnit.MINUTES);
      }
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(twoLayerMultiRegionMultiClusterWrapper);
  }

  @Test
  public void testStoreMigrationMultiRegion() throws Exception {
    String storeName = Utils.getUniqueString("test");
    Properties props = createAndPushStore(srcClusterName, storeName);

    // Push v1
    TestWriteUtils.runPushJob("Test push job 1", props);
    // Push v2
    TestWriteUtils.runPushJob("Test push job 2", props);
    // Push v3, failed on dc-1
    pushJob("Test push job 3", props, srcClusterName, storeName, 3);
    // Push v4, failed on dc-1
    pushJob("Test push job 4", props, srcClusterName, storeName, 4);

    String srcD2ServiceName = multiClusterWrapper0.getClusterToD2().get(srcClusterName);
    String destD2ServiceName = multiClusterWrapper0.getClusterToD2().get(destClusterName);
    D2Client d2Client0 =
        D2TestUtils.getAndStartD2Client(multiClusterWrapper0.getClusters().get(srcClusterName).getZk().getAddress());
    ClientConfig clientConfig0 =
        ClientConfig.defaultGenericClientConfig(storeName).setD2ServiceName(srcD2ServiceName).setD2Client(d2Client0);

    D2Client d2Client1 =
        D2TestUtils.getAndStartD2Client(multiClusterWrapper1.getClusters().get(srcClusterName).getZk().getAddress());
    ClientConfig clientConfig1 =
        ClientConfig.defaultGenericClientConfig(storeName).setD2ServiceName(srcD2ServiceName).setD2Client(d2Client1);

    try (AvroGenericStoreClient<String, Object> client0 = ClientFactory.getAndStartGenericAvroClient(clientConfig0);
        AvroGenericStoreClient<String, Object> client1 = ClientFactory.getAndStartGenericAvroClient(clientConfig1)) {
      readFromStore(client0);
      readFromStore(client1);
      StoreMigrationTestUtil.startMigration(parentControllerUrl, storeName, srcClusterName, destClusterName);
      StoreMigrationTestUtil
          .completeMigration(parentControllerUrl, storeName, srcClusterName, destClusterName, fabricList);

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // StoreConfig in router might not be up-to-date. Keep reading from the store. Finally, router will find that
        // cluster discovery changes and redirect the request to dest store. Client's d2ServiceName will be updated.
        readFromStore(client0);
        AbstractAvroStoreClient<String, Object> castClient =
            (AbstractAvroStoreClient<String, Object>) ((StatTrackingStoreClient<String, Object>) client0)
                .getInnerStoreClient();
        Assert.assertTrue(castClient.toString().contains(destD2ServiceName));
      });
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        readFromStore(client1);
        AbstractAvroStoreClient<String, Object> castClient =
            (AbstractAvroStoreClient<String, Object>) ((StatTrackingStoreClient<String, Object>) client1)
                .getInnerStoreClient();
        Assert.assertTrue(castClient.toString().contains(destD2ServiceName));
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

  // Ensure new version is online only on dc-0 by disabling the store's write operations on dc-1
  // to fail push job fail on dc-1 but succeed on dc-0
  private void pushJob(String jobName, Properties props, String clusterName, String storeName, int version) {
    // Disable store's write on dc-1
    try (ControllerClient childControllerClient1 = new ControllerClient(clusterName, childControllerUrl1)) {
      childControllerClient1.updateStore(storeName, new UpdateStoreQueryParams().setEnableWrites(false));
    }
    try (VenicePushJob job = new VenicePushJob(jobName, props)) {
      // Job run async
      CompletableFuture.runAsync(() -> {
        try {
          job.run();
        } catch (Exception exp) {
          throw new VeniceException("Fail to run pushJob " + jobName, exp);
        }
      });
      // verify the version is online on dc-0
      try (ControllerClient childControllerClient0 = new ControllerClient(clusterName, childControllerUrl0)) {
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          StoreResponse response = childControllerClient0.getStore(storeName);
          StoreInfo storeInfo = response.getStore();
          Assert.assertNotNull(storeInfo);
          List<Version> versionList = storeInfo.getVersions();
          Assert.assertTrue(containsVersionOnline(versionList, version));
        });
      }
      job.cancel();
    } catch (Exception e) {
      throw new VeniceException(e);
    }
    // Enable store's write on dc-1
    try (ControllerClient childControllerClient1 = new ControllerClient(clusterName, childControllerUrl1)) {
      childControllerClient1.updateStore(storeName, new UpdateStoreQueryParams().setEnableWrites(true));
    }
  }

  private Properties createAndPushStore(String clusterName, String storeName) throws Exception {
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(twoLayerMultiRegionMultiClusterWrapper, inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, RECORD_COUNT);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    UpdateStoreQueryParams updateStoreQueryParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setHybridRewindSeconds(TEST_TIMEOUT)
            .setHybridOffsetLagThreshold(2L)
            .setHybridStoreDiskQuotaEnabled(true)
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
    // Verify store is created in dc-1
    try (ControllerClient childControllerClient1 = new ControllerClient(clusterName, childControllerUrl1)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        StoreResponse response = childControllerClient1.getStore(storeName);
        StoreInfo storeInfo = response.getStore();
        Assert.assertNotNull(storeInfo);
      });
    }
    return props;
  }

  private void readFromStore(AvroGenericStoreClient<String, Object> client)
      throws ExecutionException, InterruptedException {
    int key = ThreadLocalRandom.current().nextInt(RECORD_COUNT) + 1;
    Assert.assertEquals(
        ((Utf8) client.get(Integer.toString(key)).get()).toString(),
        TestWriteUtils.DEFAULT_USER_DATA_VALUE_PREFIX + key);
  }

  private static boolean containsVersionOnline(List<Version> versions, int versionNum) {
    return versions.stream().anyMatch(v -> v.getNumber() == versionNum && v.getStatus().equals(VersionStatus.ONLINE));
  }

}
