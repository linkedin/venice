package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.hadoop.VenicePushJob.PushJobCheckpoints.START_DATA_WRITER_JOB;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.PUSH_JOB_STATUS_UPLOAD_ENABLE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PushJobDetailsTest {
  private final Map<Integer, Schema> schemaVersionMap = new HashMap<>();
  private final static int latestSchemaId = 2;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private VeniceClusterWrapper childRegionClusterWrapper;
  private ControllerClient controllerClient;
  private ControllerClient parentControllerClient;
  private Schema recordSchema;
  private String inputDirPath;

  @BeforeClass
  public void setUp() throws IOException {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");

    Properties parentControllerProperties = new Properties();
    // Need to add this in controller props when creating venice system for tests
    parentControllerProperties.setProperty(ConfigKeys.PUSH_JOB_STATUS_STORE_CLUSTER_NAME, "venice-cluster0");
    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        Optional.of(parentControllerProperties),
        Optional.empty(),
        Optional.of(serverProperties),
        false);
    String clusterName = multiRegionMultiClusterWrapper.getClusterNames()[0];

    VeniceMultiClusterWrapper childRegionMultiClusterWrapper = multiRegionMultiClusterWrapper.getChildRegions().get(0);
    childRegionClusterWrapper = childRegionMultiClusterWrapper.getClusters().get(clusterName);

    controllerClient = new ControllerClient(clusterName, childRegionMultiClusterWrapper.getControllerConnectString());
    parentControllerClient =
        new ControllerClient(clusterName, multiRegionMultiClusterWrapper.getControllerConnectString());
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(VeniceSystemStoreUtils.getPushJobDetailsStoreName(), 1),
        controllerClient,
        2,
        TimeUnit.MINUTES);
    File inputDir = getTempDataDirectory();
    inputDirPath = "file://" + inputDir.getAbsolutePath();
    recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    for (int i = 1; i <= latestSchemaId; i++) {
      schemaVersionMap.put(i, Utils.getSchemaFromResource("avro/PushJobDetails/v" + i + "/PushJobDetails.avsc"));
    }
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPushJobDetails() throws ExecutionException, InterruptedException, IOException {
    String testStoreName = "test-push-store";
    parentControllerClient.createNewStore(
        testStoreName,
        "test-user",
        recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString(),
        recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString());
    // Set store quota to unlimited else local VPJ jobs will fail due to quota enforcement NullPointerException because
    // hadoop job client cannot fetch counters properly.
    parentControllerClient
        .updateStore(testStoreName, new UpdateStoreQueryParams().setStorageQuotaInByte(-1).setPartitionCount(2));
    Properties pushJobProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, testStoreName);
    pushJobProps.setProperty(PUSH_JOB_STATUS_UPLOAD_ENABLE, String.valueOf(true));
    try (VenicePushJob testPushJob = new VenicePushJob("test-push-job-details-job", pushJobProps)) {
      testPushJob.run();
    }

    // Verify the sent push job details.
    try (AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig
                .defaultSpecificClientConfig(VeniceSystemStoreUtils.getPushJobDetailsStoreName(), PushJobDetails.class)
                .setVeniceURL(childRegionClusterWrapper.getRandomRouterURL()))) {
      PushJobStatusRecordKey key = new PushJobStatusRecordKey();
      key.storeName = testStoreName;
      key.versionNumber = 1;
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        try {
          assertNotNull(client.get(key).get(), "RT writes are not reflected in store yet");
        } catch (Exception e) {
          fail("Unexpected exception thrown while reading from the venice store", e);
        }
      });

      List<Integer> expectedStatuses = Arrays.asList(
          PushJobDetailsStatus.STARTED.getValue(),
          PushJobDetailsStatus.TOPIC_CREATED.getValue(),
          PushJobDetailsStatus.DATA_WRITER_COMPLETED.getValue(),
          PushJobDetailsStatus.COMPLETED.getValue());

      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        PushJobDetails value = client.get(key).get();
        assertEquals(
            value.clusterName.toString(),
            childRegionClusterWrapper.getClusterName(),
            "Unexpected cluster name from push job details");
        assertTrue(value.reportTimestamp > 0, "Push job details report timestamp is missing");
        assertEquals(
            value.overallStatus.size(),
            expectedStatuses.size(),
            "Unexpected number of overall statuses in push job details");

        for (int i = 0; i < expectedStatuses.size(); i++) {
          assertEquals(value.overallStatus.get(i).status, (int) expectedStatuses.get(i));
          assertTrue(value.overallStatus.get(i).timestamp > 0, "Timestamp for status tuple is missing");
        }
        assertFalse(value.coloStatus.isEmpty(), "Region status shouldn't be empty");
        for (List<PushJobDetailsStatusTuple> tuple: value.coloStatus.values()) {
          assertEquals(
              tuple.get(tuple.size() - 1).status,
              PushJobDetailsStatus.COMPLETED.getValue(),
              "Latest status for every region should be COMPLETED");
          assertTrue(tuple.get(tuple.size() - 1).timestamp > 0, "Timestamp for region status tuple is missing");
        }
        assertTrue(value.jobDurationInMs > 0);
        assertTrue(value.totalNumberOfRecords > 0);
        assertTrue(value.totalKeyBytes > 0);
        assertTrue(value.totalRawValueBytes > 0);
        assertTrue(value.totalCompressedValueBytes > 0);
        assertNotNull(value.pushJobConfigs);
        assertFalse(value.pushJobConfigs.isEmpty());
        assertNotNull(value.producerConfigs);
        assertTrue(value.producerConfigs.isEmpty());
      });
    }

    // Verify records (note, records 1-100 have been pushed)
    try (AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(testStoreName)
            .setVeniceURL(childRegionClusterWrapper.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          for (int i = 1; i < 100; i++) {
            String key = String.valueOf(i);
            Object value = client.get(key).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
            assertEquals(value.toString(), "test_name_" + key);
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }

  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPushJobDetailsFailureTags() throws ExecutionException, InterruptedException {
    String testStoreName = "test-push-failure-store";
    parentControllerClient.createNewStore(
        testStoreName,
        "test-user",
        recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString(),
        recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString());
    // hadoop job client cannot fetch counters properly and should fail the job
    parentControllerClient.updateStore(testStoreName, new UpdateStoreQueryParams().setStorageQuotaInByte(0));
    Properties pushJobProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, testStoreName);
    pushJobProps.setProperty(PUSH_JOB_STATUS_UPLOAD_ENABLE, String.valueOf(true));
    try (VenicePushJob testPushJob = new VenicePushJob("test-push-job-details-job", pushJobProps)) {
      assertThrows(VeniceException.class, testPushJob::run);
    }
    try (AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig
                .defaultSpecificClientConfig(VeniceSystemStoreUtils.getPushJobDetailsStoreName(), PushJobDetails.class)
                .setVeniceURL(childRegionClusterWrapper.getRandomRouterURL()))) {
      PushJobStatusRecordKey key = new PushJobStatusRecordKey();
      key.storeName = testStoreName;
      key.versionNumber = 1;
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        try {
          assertNotNull(client.get(key).get(), "RT writes are not reflected in store yet");
        } catch (Exception e) {
          fail("Unexpected exception thrown while reading from the venice store", e);
        }
      });
      PushJobDetails value = client.get(key).get();
      assertEquals(
          value.pushJobLatestCheckpoint.intValue(),
          START_DATA_WRITER_JOB.getValue(),
          "Unexpected latest push job checkpoint reported");
      assertFalse(value.failureDetails.toString().isEmpty());
    }
  }
}
