package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.EMERGENCY_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_STATUS_STORE_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.annotation.PubSubAgnosticTest;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.ExceptionType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.IntegrationTestUtils;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.status.BatchJobHeartbeatConfigs;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@PubSubAgnosticTest
public class TestTargetedRegionPushWithNativeReplication extends AbstractMultiRegionTest {
  private static final Logger LOGGER = LogManager.getLogger(TestTargetedRegionPushWithNativeReplication.class);
  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;

  private static final String SYSTEM_STORE_CLUSTER = CLUSTER_NAME;

  private VeniceServerWrapper serverWrapper;

  @Override
  protected boolean shouldCreateD2Client() {
    return true;
  }

  @Override
  protected Properties getExtraServerProperties() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    return serverProperties;
  }

  @Override
  protected Properties getExtraControllerProperties() {
    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 10);
    controllerProps.put(BatchJobHeartbeatConfigs.HEARTBEAT_STORE_CLUSTER_CONFIG.getConfigName(), SYSTEM_STORE_CLUSTER);
    controllerProps.put(BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG.getConfigName(), true);
    controllerProps.put(PUSH_JOB_STATUS_STORE_CLUSTER_NAME, SYSTEM_STORE_CLUSTER);
    controllerProps.put(EMERGENCY_SOURCE_REGION, "dc-0");
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SERVICE_ENABLED, true);
    controllerProps.put(CONTROLLER_DEFERRED_VERSION_SWAP_SLEEP_MS, 100);
    return controllerProps;
  }

  @Override
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    super.setUp();
    VeniceClusterWrapper clusterWrapper =
        multiRegionMultiClusterWrapper.getChildRegions().get(0).getClusters().get(CLUSTER_NAME);
    serverWrapper = clusterWrapper.getVeniceServers().get(0);
    IntegrationTestUtils.waitForParticipantStorePushInAllRegions(CLUSTER_NAME, childDatacenters);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Failed to create new store version.*", timeOut = TEST_TIMEOUT)
  public void testPushDirectlyToChildRegion() throws IOException {
    String clusterName = CLUSTER_NAME;
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testPushDirectlyToChildColo");
    Properties props = IntegrationTestPushUtils.defaultVPJProps(childDatacenters.get(0), inputDirPath, storeName);
    createStoreForJob(clusterName, recordSchema, props).close();
    IntegrationTestPushUtils.runVPJ(props);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testControllerBlocksConcurrentBatchPush() {
    String clusterName = CLUSTER_NAME;
    String storeName = Utils.getUniqueString("testControllerBlocksConcurrentBatchPush");
    String pushId1 = Utils.getUniqueString(storeName + "_push");
    String pushId2 = Utils.getUniqueString(storeName + "_push");
    String parentControllerUrl = getParentControllerUrl();

    try (ControllerClient controllerClient = new ControllerClient(clusterName, parentControllerUrl)) {
      controllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"");
      controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(100));

      assertCommand(
          controllerClient.requestTopicForWrites(
              storeName,
              1L,
              Version.PushType.BATCH,
              pushId1,
              false,
              true,
              false,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              false,
              -1));

      VersionCreationResponse vcr2 = controllerClient.requestTopicForWrites(
          storeName,
          1L,
          Version.PushType.BATCH,
          pushId2,
          false,
          true,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          false,
          -1);
      Assert.assertTrue(vcr2.isError());
      Assert.assertEquals(vcr2.getErrorType(), ErrorType.CONCURRENT_BATCH_PUSH);
      Assert.assertEquals(vcr2.getExceptionType(), ExceptionType.BAD_REQUEST);
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 3)
  public void testTargetedRegionPushJobFullConsumptionForBatchStore() throws Exception {
    motherOfAllTests(
        "testTargetedRegionPushJobBatchStore",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1),
        100,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          props.put(TARGETED_REGION_PUSH_ENABLED, false);
          try (VenicePushJob job = new VenicePushJob("Test push job 1", props)) {
            job.run();

            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 1);
              }
            });
          }
          String dataDBPathV1 = serverWrapper.getDataDirectory() + "/rocksdb/" + storeName + "_v1";
          long storeSize = FileUtils.sizeOfDirectory(new File(dataDBPathV1));
          try (VenicePushJob job = new VenicePushJob("Test push job 2", props)) {
            job.run();

            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 2);
              }
            });
          }
          props.put(TARGETED_REGION_PUSH_ENABLED, true);
          TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, 20);

          String versionTopic3 = Version.composeKafkaTopic(storeName, 3);
          AtomicReference<Throwable> vpjError = new AtomicReference<>();
          Thread vpjThread = new Thread(() -> {
            try (VenicePushJob job = new VenicePushJob("Test push job 3", props)) {
              job.run();
            } catch (Throwable t) {
              vpjError.set(t);
            }
          });
          vpjThread.setDaemon(true);
          vpjThread.start();
          try {
            try (ControllerClient dc0Client =
                new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString())) {
              TestUtils.waitForNonDeterministicPushCompletion(versionTopic3, dc0Client, 30, TimeUnit.SECONDS);
            }

            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              Map<String, Integer> coloVersions =
                  parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();
              Assert.assertNotNull(coloVersions.get("dc-0"), "dc-0 version should not be null");
              Assert.assertEquals((int) coloVersions.get("dc-0"), 3, "Target region dc-0 should be on version 3");
            });

            File directory = new File(serverWrapper.getDataDirectory() + "/rocksdb/");
            File[] storeDBDirs = directory.listFiles(File::isDirectory);
            long totalStoreSize = 0;
            if (storeDBDirs != null) {
              for (File storeDB: storeDBDirs) {
                if (storeDB.getName().startsWith(storeName)) {
                  long size = FileUtils
                      .sizeOfDirectory(new File(serverWrapper.getDataDirectory() + "/rocksdb/" + storeDB.getName()));
                  totalStoreSize += size;
                }
              }
              Assert.assertTrue(
                  storeSize * 2 >= totalStoreSize,
                  "2x of store size " + storeSize + " is more than total " + totalStoreSize);
            }

            TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 3);
              }
            });

            vpjThread.join(30_000);
            assertFalse(vpjThread.isAlive(), "VPJ thread should have completed");
            if (vpjError.get() != null) {
              throw new VeniceException("Push job 3 failed", vpjError.get());
            }

            validateDaVinciClient(storeName, 20);
            validatePushJobDetails(clusterName, storeName);
          } finally {
            if (vpjThread.isAlive()) {
              vpjThread.interrupt();
            }
          }
        });
  }

  @Test(enabled = false) // Disable till hybrid stores are supported for target region push
  public void testTargetedRegionPushJobFullConsumptionForHybridStore() throws Exception {
    motherOfAllTests(
        "testTargetedRegionPushJobHybridStore",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1)
            .setHybridRewindSeconds(10)
            .setHybridOffsetLagThreshold(2),
        100,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          props.put(TARGETED_REGION_PUSH_ENABLED, true);
          try (VenicePushJob job = new VenicePushJob("Test push job 1", props)) {
            job.run();

            TestUtils.waitForNonDeterministicAssertion(45, TimeUnit.SECONDS, () -> {
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 2);
              }
            });
          }
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testTargetRegionPushWithDeferredVersionSwap() throws Exception {
    motherOfAllTests(
        "testTargetRegionPushWithDeferredVersionSwap",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1).setTargetRegionSwapWaitTime(1),
        100,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          try (VenicePushJob job = new VenicePushJob("Test regular push job", props)) {
            job.run();

            Assert.assertEquals(job.getKafkaUrl(), childDatacenters.get(0).getPubSubBrokerWrapper().getAddress());

            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 1);
              }
            });
          }

          props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
          try (VenicePushJob job = new VenicePushJob("Test target region push w. deferred version swap", props)) {
            job.run();

            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              Map<String, Integer> coloVersions =
                  parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

              coloVersions.forEach((colo, version) -> {
                Assert.assertEquals((int) version, 2);
              });
            });
          }
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testKilledRepushJobVersionStatus() throws Exception {
    motherOfAllTests(
        "testKilledRepushJobVersionStatus",
        updateStoreQueryParams -> updateStoreQueryParams.setPartitionCount(1),
        100,
        (parentControllerClient, clusterName, storeName, props, inputDir) -> {
          try (VenicePushJob job = new VenicePushJob("Test regular push job", props)) {
            job.run();

            TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
              for (int version: parentControllerClient.getStore(storeName)
                  .getStore()
                  .getColoToCurrentVersions()
                  .values()) {
                Assert.assertEquals(version, 1);
              }
            });
          }

          VersionCreationResponse versionCreationResponse = parentControllerClient.requestTopicForWrites(
              storeName,
              1000,
              Version.PushType.BATCH,
              Version.generateRePushId("2"),
              true,
              true,
              false,
              Optional.empty(),
              Optional.empty(),
              Optional.of("dc-1"),
              false,
              -1,
              false,
              null,
              1,
              false,
              -1);
          Assert.assertFalse(
              versionCreationResponse.isError(),
              "Failed to create version 2: " + versionCreationResponse.getError());
          Assert.assertEquals(versionCreationResponse.getVersion(), 2, "Expected version 2 to be created");

          for (VeniceMultiClusterWrapper childDatacenter: childDatacenters) {
            try (ControllerClient childControllerClient =
                new ControllerClient(clusterName, childDatacenter.getControllerConnectString())) {
              TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
                StoreResponse store = childControllerClient.getStore(storeName);
                Optional<Version> version = store.getStore().getVersion(2);
                assertTrue(version.isPresent(), "Version 2 should be created in child datacenter before killing");
              });
            }
          }

          parentControllerClient.killOfflinePushJob(Version.composeKafkaTopic(storeName, 2));

          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
            StoreInfo parentStore = parentControllerClient.getStore(storeName).getStore();
            Assert.assertEquals(parentStore.getVersion(2).get().getStatus(), VersionStatus.KILLED);
          });

          for (VeniceMultiClusterWrapper childDatacenter: childDatacenters) {
            try (ControllerClient childControllerClient =
                new ControllerClient(clusterName, childDatacenter.getControllerConnectString())) {
              TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
                StoreResponse store = childControllerClient.getStore(storeName);
                Optional<Version> version = store.getStore().getVersion(2);
                assertTrue(
                    !version.isPresent() || version.get().getStatus() == VersionStatus.KILLED,
                    "Version 2 should be killed or removed in child datacenter, but found: "
                        + (version.isPresent() ? version.get().getStatus() : "not present"));
              });
            }
          }

          TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, true, () -> {
            JobStatusQueryResponse jobStatusQueryResponse = assertCommand(
                parentControllerClient
                    .queryOverallJobStatus(Version.composeKafkaTopic(storeName, 2), Optional.empty()));
            ExecutionStatus executionStatus = ExecutionStatus.valueOf(jobStatusQueryResponse.getStatus());
            assertEquals(executionStatus, ExecutionStatus.ERROR);
          });
        });
  }

  private interface NativeReplicationTest {
    void run(
        ControllerClient parentControllerClient,
        String clusterName,
        String storeName,
        Properties props,
        File inputDir) throws Exception;
  }

  private void motherOfAllTests(
      String storeNamePrefix,
      Function<UpdateStoreQueryParams, UpdateStoreQueryParams> updateStoreParamsTransformer,
      int recordCount,
      NativeReplicationTest test) throws Exception {
    String clusterName = CLUSTER_NAME;
    File inputDir = getTempDataDirectory();
    String parentControllerUrls = multiRegionMultiClusterWrapper.getControllerConnectString();
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString(storeNamePrefix);
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    props.put(SEND_CONTROL_MESSAGES_DIRECTLY, true);

    UpdateStoreQueryParams updateStoreParams = updateStoreParamsTransformer
        .apply(new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));

    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, recordCount);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();

    try {
      createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, updateStoreParams).close();
      childDatacenters.get(0)
          .getClusters()
          .get(clusterName)
          .useControllerClient(
              dc0Client -> childDatacenters.get(1)
                  .getClusters()
                  .get(clusterName)
                  .useControllerClient(
                      dc1Client -> TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
                        StoreInfo store = dc0Client.getStore(storeName).getStore();
                        Assert.assertNotNull(store);
                        Assert.assertEquals(store.getStorageQuotaInByte(), Store.UNLIMITED_STORAGE_QUOTA);
                        store = dc1Client.getStore(storeName).getStore();
                        Assert.assertNotNull(store);
                        Assert.assertEquals(store.getStorageQuotaInByte(), Store.UNLIMITED_STORAGE_QUOTA);
                      })));

      makeSureSystemStoreIsPushed(clusterName, storeName);
      try (ControllerClient parentControllerClient =
          ControllerClient.constructClusterControllerClient(clusterName, parentControllerUrls)) {
        test.run(parentControllerClient, clusterName, storeName, props, inputDir);
      }
    } finally {
      FileUtils.deleteDirectory(inputDir);
    }
  }

  private void validateDaVinciClient(String storeName, int recordCount)
      throws ExecutionException, InterruptedException {
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    DaVinciClient<String, Object> client;
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .build();
    MetricsRepository metricsRepository = new MetricsRepository();
    DaVinciConfig clientConfig = new DaVinciConfig();
    clientConfig.setStorageClass(StorageClass.DISK);
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2ClientDC0,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        multiRegionMultiClusterWrapper)) {
      client = factory.getGenericAvroClient(storeName, clientConfig);
      client.start();
      client.subscribeAll().get(30, TimeUnit.SECONDS);
      for (int i = 1; i <= recordCount; i++) {
        Assert.assertNotNull(client.get(Integer.toString(i)));
      }
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  private void validatePushJobDetails(String clusterName, String storeName)
      throws ExecutionException, InterruptedException {
    String pushJobDetailsStoreName = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
    VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(1);
    String routerUrl = childDataCenter.getClusters().get(clusterName).getRandomRouterURL();

    try (AvroGenericStoreClient<PushJobStatusRecordKey, Object> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(pushJobDetailsStoreName).setVeniceURL(routerUrl))) {
      PushJobStatusRecordKey key = new PushJobStatusRecordKey();
      key.setStoreName(storeName);
      key.setVersionNumber(1);
      // Push job details may not have replicated to dc-1 yet, so retry.
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        GenericRecord value = (GenericRecord) client.get(key).get();
        Assert.assertNotNull(value, "Push job details not yet available");
        HashMap<Utf8, List<PushJobDetailsStatusTuple>> map =
            (HashMap<Utf8, List<PushJobDetailsStatusTuple>>) value.get("coloStatus");
        Assert.assertEquals(map.size(), 2);
        List<PushJobDetailsStatusTuple> status = map.get(new Utf8("dc-0"));
        Assert.assertEquals(
            ((GenericRecord) status.get(status.size() - 1)).get("status"),
            PushJobDetailsStatus.COMPLETED.getValue());
        status = map.get(new Utf8("dc-1"));
        Assert.assertEquals(
            ((GenericRecord) status.get(status.size() - 1)).get("status"),
            PushJobDetailsStatus.COMPLETED.getValue());
      });
    }
  }

  private void makeSureSystemStoreIsPushed(String clusterName, String storeName) {
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, true, () -> {
      for (int i = 0; i < childDatacenters.size(); i++) {
        VeniceMultiClusterWrapper childDataCenter = childDatacenters.get(i);
        final int iCopy = i;
        childDataCenter.getClusters().get(clusterName).useControllerClient(cc -> {
          assertStoreHealth(cc, VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), iCopy);
          assertStoreHealth(cc, VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName), iCopy);
          assertStoreHealth(cc, VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix(), iCopy);
        });
      }
    });
  }

  private void assertStoreHealth(ControllerClient controllerClient, String systemStoreName, int dcNumber) {
    StoreResponse storeResponse = assertCommand(controllerClient.getStore(systemStoreName));
    Assert.assertTrue(
        storeResponse.getStore().getCurrentVersion() > 0,
        systemStoreName + " is not ready for DC-" + dcNumber);
  }
}
