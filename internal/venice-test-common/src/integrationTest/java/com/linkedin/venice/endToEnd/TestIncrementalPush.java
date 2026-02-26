package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.endToEnd.TestBatch.TEST_TIMEOUT;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToPartialUpdateOpRecordSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SPARK_NATIVE_INPUT_FORMAT_ENABLED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestIncrementalPush extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT_MS = 180_000;

  @Test(timeOut = TEST_TIMEOUT_MS, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testIncrementalPushPartialUpdateNewFormat(boolean useSparkCompute) throws IOException {
    final String storeName = Utils.getUniqueString("inc_push_update_new_format");
    String parentControllerUrl = getParentControllerUrl();
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    vpjProperties.put(ENABLE_WRITE_COMPUTE, true);
    vpjProperties.put(INCREMENTAL_PUSH, true);
    if (useSparkCompute) {
      vpjProperties.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());
      vpjProperties.setProperty(SPARK_NATIVE_INPUT_FORMAT_ENABLED, String.valueOf(true));
    }

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", keySchemaStr, NAME_RECORD_V2_SCHEMA.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setActiveActiveReplicationEnabled(true)
              .setWriteComputationEnabled(true)
              .setChunkingEnabled(true)
              .setIncrementalPushEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          response.getKafkaTopic(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // VPJ push
      String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();
      try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
        IntegrationTestPushUtils.runVPJ(vpjProperties, 1, childControllerClient);
      }
      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      veniceClusterWrapper.waitVersion(storeName, 1);

      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 1; i < 100; i++) {
              String key = String.valueOf(i);
              GenericRecord value = readValue(storeReader, key);
              assertNotNull(value, "Key " + key + " should not be missing!");
              assertEquals(value.get("firstName").toString(), "first_name_" + key);
              assertEquals(value.get("lastName").toString(), "last_name_" + key);
              assertEquals(value.get("age"), -1);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testIncrementalPushPartialUpdateClassicFormat() throws IOException {
    final String storeName = Utils.getUniqueString("inc_push_update_classic_format");
    String parentControllerUrl = getParentControllerUrl();
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToPartialUpdateOpRecordSchema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    vpjProperties.put(ENABLE_WRITE_COMPUTE, true);
    vpjProperties.put(INCREMENTAL_PUSH, true);

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", keySchemaStr, TestWriteUtils.NAME_RECORD_V1_SCHEMA.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setWriteComputationEnabled(true)
              .setChunkingEnabled(true)
              .setIncrementalPushEnabled(true)
              .setActiveActiveReplicationEnabled(true)
              .setSeparateRealTimeTopicEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          60,
          TimeUnit.SECONDS);

      // VPJ push
      String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();
      try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
        IntegrationTestPushUtils.runVPJ(vpjProperties, 1, childControllerClient);
      }
      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      veniceClusterWrapper.waitVersion(storeName, 1);
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 1; i < 100; i++) {
              String key = String.valueOf(i);
              GenericRecord value = readValue(storeReader, key);
              assertNotNull(value, "Key " + key + " should not be missing!");
              assertEquals(value.get("firstName").toString(), "first_name_" + key);
              assertEquals(value.get("lastName").toString(), "last_name_" + key);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRTTopicDeletionWithHybridAndIncrementalVersions() {
    String storeName = Utils.getUniqueString("testRTTopicDeletion");
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();
    ControllerClient parentControllerClient =
        ControllerClient.constructClusterControllerClient(CLUSTER_NAME, parentControllerURLs);

    NewStoreResponse newStoreResponse =
        parentControllerClient.retryableRequest(5, c -> c.createNewStore(storeName, "", "\"string\"", "\"string\""));
    Assert.assertFalse(
        newStoreResponse.isError(),
        "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    UpdateStoreQueryParams updateStoreParams = new UpdateStoreQueryParams();
    updateStoreParams.setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS)
        .setActiveActiveReplicationEnabled(true)
        .setIncrementalPushEnabled(true)
        .setNumVersionsToPreserve(2)
        .setHybridRewindSeconds(1000)
        .setHybridOffsetLagThreshold(1000);
    TestWriteUtils.updateStore(storeName, parentControllerClient, updateStoreParams);

    VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
    assertEquals(response.getVersion(), 1);
    assertFalse(response.isError(), "Empty push to parent colo should succeed");
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(storeName, 1),
        parentControllerClient,
        60,
        TimeUnit.SECONDS);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testIncrementalPushAfterKilledPush() throws IOException {
    // Setup job properties
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testIncrementalPushAfterKilledPush");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true);
    storeParms.setTargetRegionSwapWaitTime(1);
    storeParms.setHybridRewindSeconds(10L)
        .setHybridOffsetLagThreshold(2L)
        .setIncrementalPushEnabled(true)
        .setActiveActiveReplicationEnabled(true);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAME, keySchemaStr, NAME_RECORD_V1_SCHEMA.toString(), props, storeParms).close();

      // Create V1
      IntegrationTestPushUtils.runVPJ(props);

      // Create V2, kill job
      parentControllerClient.requestTopicForWrites(
          storeName,
          1000,
          Version.PushType.BATCH,
          Version.numberBasedDummyPushId(1),
          true,
          true,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.of("dc-1"),
          false,
          -1);
      parentControllerClient.killOfflinePushJob(Version.composeKafkaTopic(storeName, 2));

      // Start incremental push
      writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir, 200);
      Properties props2 =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
      props2.put(INCREMENTAL_PUSH, true);
      IntegrationTestPushUtils.runVPJ(props2);

      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      veniceClusterWrapper.waitVersion(storeName, 1);
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 1; i < 200; i++) {
              String key = String.valueOf(i);
              GenericRecord value = (GenericRecord) storeReader.get(key).get();
              assertNotNull(value, "Key " + key + " should not be missing!");
              assertEquals(value.get("firstName").toString(), "first_name_" + key);
              assertEquals(value.get("lastName").toString(), "last_name_" + key);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testIncrementalPushIsRecordInOffsetRecord() throws IOException {
    // Setup job properties
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testIncrementalPushIsRecordInOffsetRecord");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setUnusedSchemaDeletionEnabled(true);
    storeParms.setHybridRewindSeconds(10L)
        .setHybridOffsetLagThreshold(2L)
        .setIncrementalPushEnabled(true)
        .setActiveActiveReplicationEnabled(true);
    String parentControllerURLs = multiRegionMultiClusterWrapper.getControllerConnectString();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerURLs)) {
      createStoreForJob(CLUSTER_NAME, keySchemaStr, NAME_RECORD_V1_SCHEMA.toString(), props, storeParms).close();

      // Create V1
      IntegrationTestPushUtils.runVPJ(props);

      // Start incremental push
      writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir, 200);
      Properties props2 =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
      props2.put(INCREMENTAL_PUSH, true);
      IntegrationTestPushUtils.runVPJ(props2);

      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      veniceClusterWrapper.waitVersion(storeName, 1);
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            // pick any server, check partition offset record
            VeniceServerWrapper server = veniceClusterWrapper.getVeniceServers().get(0);
            for (int partitionId = 0; partitionId < 3; partitionId++) {
              OffsetRecord offsetRecord = server.getVeniceServer()
                  .getStorageMetadataService()
                  .getLastOffset(
                      storeName + "_v1",
                      partitionId,
                      server.getVeniceServer().getKafkaStoreIngestionService().getPubSubContext());
              Assert.assertFalse(offsetRecord.getTrackingIncrementalPushStatus().isEmpty());
              // for each push job id, the status should be END_OF_INCREMENTAL_PUSH_RECEIVED
              offsetRecord.getTrackingIncrementalPushStatus().forEach((pushJobId, TsToStatus) -> {
                Assert.assertEquals(TsToStatus.status, ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED.getValue());
              });
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  /**
   * Test that incremental push throttling is applied when writing to the regular RT topic
   * and that the push completes successfully with throttling enabled.
   */
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testIncrementalPushWithThrottling() throws IOException {
    final String storeName = Utils.getUniqueString("inc_push_throttle");
    String parentControllerUrl = parentController.getControllerUrl();
    int recordCount = 5;
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir, recordCount);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient
              .createNewStore(storeName, "test_owner", keySchemaStr, NAME_RECORD_V2_SCHEMA.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setActiveActiveReplicationEnabled(true)
              .setWriteComputationEnabled(true)
              .setChunkingEnabled(true)
              .setIncrementalPushEnabled(true)
              .setHybridRewindSeconds(10L)
              .setHybridOffsetLagThreshold(2L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          response.getKafkaTopic(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Run incremental push with throttling enabled (writing to regular RT topic).
      // Quota of 1 rec/s ensures the throttler blocks ~1s between each record.
      Properties vpjProperties =
          IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
      vpjProperties.put(ENABLE_WRITE_COMPUTE, true);
      vpjProperties.put(INCREMENTAL_PUSH, true);
      vpjProperties.put(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND, 1);

      String childControllerUrl = childDatacenters.get(0).getRandomController().getControllerUrl();
      try (ControllerClient childControllerClient = new ControllerClient(CLUSTER_NAME, childControllerUrl)) {
        String jobName = Utils.getUniqueString("venice-push-job-throttle-rt");
        try (VenicePushJob vpj = new VenicePushJob(jobName, vpjProperties)) {
          vpj.run();
          Assert.assertTrue(
              vpj.getIncrementalPushThrottledTimeMs() > 0,
              "Incremental push throttle time should be positive when quota is 1 rec/s");
        }
        TestUtils.waitForNonDeterministicCompletion(
            5,
            TimeUnit.SECONDS,
            () -> childControllerClient.getStore(storeName).getStore().getCurrentVersion() == 1);
      }

      VeniceClusterWrapper veniceClusterWrapper = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      veniceClusterWrapper.waitVersion(storeName, 1);

      // Verify data was written correctly despite throttling
      try (AvroGenericStoreClient<Object, Object> storeReader = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
          try {
            for (int i = 1; i <= recordCount; i++) {
              String key = String.valueOf(i);
              GenericRecord value = readValue(storeReader, key);
              assertNotNull(value, "Key " + key + " should not be missing!");
              assertEquals(value.get("firstName").toString(), "first_name_" + key);
              assertEquals(value.get("lastName").toString(), "last_name_" + key);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    D2ClientUtils.shutdownClient(d2ClientDC0);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  private GenericRecord readValue(AvroGenericStoreClient<Object, Object> storeReader, String key)
      throws ExecutionException, InterruptedException {
    return (GenericRecord) storeReader.get(key).get();
  }

}
