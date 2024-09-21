package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.PushJobCheckpoints.DUP_KEY_WITH_DIFF_VALUE;
import static com.linkedin.venice.PushJobCheckpoints.INPUT_DATA_SCHEMA_VALIDATION_FAILED;
import static com.linkedin.venice.PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED;
import static com.linkedin.venice.PushJobCheckpoints.START_DATA_WRITER_JOB;
import static com.linkedin.venice.status.PushJobDetailsStatus.COMPLETED;
import static com.linkedin.venice.status.PushJobDetailsStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_VALUE_PREFIX;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_FAILURE_CHECKPOINTS_TO_DEFINE_USER_ERROR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_STATUS_UPLOAD_ENABLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.PushJobCheckpoints;
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
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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
  private String inputDirPathForFullPush;
  private String inputDirPathForIncPush;
  private MetricsRepository metricsRepository;

  private double batchJobSuccessExpected = 0.0;
  private double incrementalJobSuccessExpected = 0.0;
  private double batchJobFailedUserErrorExpected = 0.0;
  private double batchJobFailedNonUserErrorExpected = 0.0;
  private double incrementalJobFailedUserErrorExpected = 0.0;
  private double incrementalJobFailedNonUserErrorExpected = 0.0;

  @BeforeClass()
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
    metricsRepository = multiRegionMultiClusterWrapper.getParentControllers().get(0).getMetricRepository();
    controllerClient = new ControllerClient(clusterName, childRegionMultiClusterWrapper.getControllerConnectString());
    parentControllerClient =
        new ControllerClient(clusterName, multiRegionMultiClusterWrapper.getControllerConnectString());
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(VeniceSystemStoreUtils.getPushJobDetailsStoreName(), 1),
        controllerClient,
        2,
        TimeUnit.MINUTES);
    File inputDir = getTempDataDirectory();
    inputDirPathForFullPush = "file://" + inputDir.getAbsolutePath();
    recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    for (int i = 1; i <= latestSchemaId; i++) {
      schemaVersionMap.put(i, Utils.getSchemaFromResource("avro/PushJobDetails/v" + i + "/PushJobDetails.avsc"));
    }
    inputDir = getTempDataDirectory();
    inputDirPathForIncPush = "file://" + inputDir.getAbsolutePath();
    TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema2(inputDir);
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  private void validatePushJobMetrics(boolean isSucceeded, boolean isUserError, boolean isIncrementalPush) {
    double batchJobSuccessExpectedCountSinceLastMeasurement = 0.0;
    double incrementalJobSuccessExpectedCountSinceLastMeasurement = 0.0;
    double batchJobFailedUserErrorExpectedCountSinceLastMeasurement = 0.0;
    double batchJobFailedNonUserErrorExpectedCountSinceLastMeasurement = 0.0;
    double incrementalJobFailedUserErrorExpectedCountSinceLastMeasurement = 0.0;
    double incrementalJobFailedNonUserErrorExpectedCountSinceLastMeasurement = 0.0;

    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    if (isSucceeded) {
      if (isIncrementalPush) {
        incrementalJobSuccessExpected += 1.0;
        incrementalJobSuccessExpectedCountSinceLastMeasurement = 1.0;
      } else {
        batchJobSuccessExpected += 1.0;
        batchJobSuccessExpectedCountSinceLastMeasurement = 1.0;
      }
    } else {
      if (isUserError) {
        if (isIncrementalPush) {
          incrementalJobFailedUserErrorExpected += 1.0;
          incrementalJobFailedUserErrorExpectedCountSinceLastMeasurement = 1.0;
        } else {
          batchJobFailedUserErrorExpected += 1.0;
          batchJobFailedUserErrorExpectedCountSinceLastMeasurement = 1.0;
        }
      } else {
        if (isIncrementalPush) {
          incrementalJobFailedNonUserErrorExpected += 1.0;
          incrementalJobFailedNonUserErrorExpectedCountSinceLastMeasurement = 1.0;
        } else {
          batchJobFailedNonUserErrorExpected += 1.0;
          batchJobFailedNonUserErrorExpectedCountSinceLastMeasurement = 1.0;
        }
      }
    }
    double finalIncrementalJobSuccessExpectedCountSinceLastMeasurement =
        incrementalJobSuccessExpectedCountSinceLastMeasurement;
    double finalBatchJobSuccessExpectedCountSinceLastMeasurement = batchJobSuccessExpectedCountSinceLastMeasurement;
    double finalBatchJobFailedUserErrorExpectedCountSinceLastMeasurement =
        batchJobFailedUserErrorExpectedCountSinceLastMeasurement;
    double finalBatchJobFailedNonUserErrorExpectedCountSinceLastMeasurement =
        batchJobFailedNonUserErrorExpectedCountSinceLastMeasurement;
    double finalIncrementalJobFailedUserErrorExpectedCountSinceLastMeasurement =
        incrementalJobFailedUserErrorExpectedCountSinceLastMeasurement;
    double finalIncrementalJobFailedNonUserErrorExpectedCountSinceLastMeasurement =
        incrementalJobFailedNonUserErrorExpectedCountSinceLastMeasurement;

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      try {
        double batchJobSuccess = metrics.containsKey(".venice-cluster0--batch_push_job_success.Count")
            ? metrics.get(".venice-cluster0--batch_push_job_success.Count").value()
            : 0.0;
        double batchJobSuccessCountSinceLastMeasurement =
            metrics.containsKey(".venice-cluster0--batch_push_job_success.CountSinceLastMeasurement")
                ? metrics.get(".venice-cluster0--batch_push_job_success.CountSinceLastMeasurement").value()
                : 0.0;
        double incrementalJobSuccess = metrics.containsKey(".venice-cluster0--incremental_push_job_success.Count")
            ? metrics.get(".venice-cluster0--incremental_push_job_success.Count").value()
            : 0.0;
        double incrementalJobSuccessCountSinceLastMeasurement =
            metrics.containsKey(".venice-cluster0--incremental_push_job_success.CountSinceLastMeasurement")
                ? metrics.get(".venice-cluster0--incremental_push_job_success.CountSinceLastMeasurement").value()
                : 0.0;
        double batchJobFailedUserError = metrics.containsKey(".venice-cluster0--batch_push_job_failed_user_error.Count")
            ? metrics.get(".venice-cluster0--batch_push_job_failed_user_error.Count").value()
            : 0.0;
        double batchJobFailedUserErrorCountSinceLastMeasurement =
            metrics.containsKey(".venice-cluster0--batch_push_job_failed_user_error.CountSinceLastMeasurement")
                ? metrics.get(".venice-cluster0--batch_push_job_failed_user_error.CountSinceLastMeasurement").value()
                : 0.0;
        double batchJobFailedNonUserError =
            metrics.containsKey(".venice-cluster0--batch_push_job_failed_non_user_error.Count")
                ? metrics.get(".venice-cluster0--batch_push_job_failed_non_user_error.Count").value()
                : 0.0;
        double batchJobFailedNonUserErrorCountSinceLastMeasurement =
            metrics.containsKey(".venice-cluster0--batch_push_job_failed_non_user_error.CountSinceLastMeasurement")
                ? metrics.get(".venice-cluster0--batch_push_job_failed_non_user_error.CountSinceLastMeasurement")
                    .value()
                : 0.0;
        double incrementalJobFailedUserError =
            metrics.containsKey(".venice-cluster0--incremental_push_job_failed_user_error.Count")
                ? metrics.get(".venice-cluster0--incremental_push_job_failed_user_error.Count").value()
                : 0.0;
        double incrementalJobFailedUserCountSinceLastMeasurement =
            metrics.containsKey(".venice-cluster0--incremental_push_job_failed_user_error.CountSinceLastMeasurement")
                ? metrics.get(".venice-cluster0--incremental_push_job_failed_user_error.CountSinceLastMeasurement")
                    .value()
                : 0.0;
        double incrementalJobFailedNonUserError =
            metrics.containsKey(".venice-cluster0--incremental_push_job_failed_non_user_error.Count")
                ? metrics.get(".venice-cluster0--incremental_push_job_failed_non_user_error.Count").value()
                : 0.0;
        double incrementalJobFailedNonUserCountSinceLastMeasurement = metrics
            .containsKey(".venice-cluster0--incremental_push_job_failed_non_user_error.CountSinceLastMeasurement")
                ? metrics.get(".venice-cluster0--incremental_push_job_failed_non_user_error.CountSinceLastMeasurement")
                    .value()
                : 0.0;
        assertEquals(
            incrementalJobSuccess,
            incrementalJobSuccessExpected,
            "Incremental push job success metric is incorrect");
        assertEquals(
            incrementalJobSuccessCountSinceLastMeasurement,
            finalIncrementalJobSuccessExpectedCountSinceLastMeasurement,
            "Incremental push job success metric is incorrect");
        assertEquals(batchJobSuccess, batchJobSuccessExpected, "Batch push job success metric is incorrect");
        assertEquals(
            batchJobSuccessCountSinceLastMeasurement,
            finalBatchJobSuccessExpectedCountSinceLastMeasurement,
            "Batch push job success metric is incorrect");
        assertEquals(
            batchJobFailedUserError,
            batchJobFailedUserErrorExpected,
            "Batch push job failed user error metric is incorrect");
        assertEquals(
            batchJobFailedUserErrorCountSinceLastMeasurement,
            finalBatchJobFailedUserErrorExpectedCountSinceLastMeasurement,
            "Batch push job failed user error metric is incorrect");
        assertEquals(
            batchJobFailedNonUserError,
            batchJobFailedNonUserErrorExpected,
            "Batch push job failed non user error metric is incorrect");
        assertEquals(
            batchJobFailedNonUserErrorCountSinceLastMeasurement,
            finalBatchJobFailedNonUserErrorExpectedCountSinceLastMeasurement,
            "Batch push job failed non user error metric is incorrect");
        assertEquals(
            incrementalJobFailedUserError,
            incrementalJobFailedUserErrorExpected,
            "Incremental push job failed user error metric is incorrect");
        assertEquals(
            incrementalJobFailedUserCountSinceLastMeasurement,
            finalIncrementalJobFailedUserErrorExpectedCountSinceLastMeasurement,
            "Incremental push job failed user error metric is incorrect");
        assertEquals(
            incrementalJobFailedNonUserError,
            incrementalJobFailedNonUserErrorExpected,
            "Incremental push job failed non user error metric is incorrect");
        assertEquals(
            incrementalJobFailedNonUserCountSinceLastMeasurement,
            finalIncrementalJobFailedNonUserErrorExpectedCountSinceLastMeasurement,
            "Incremental push job failed non user error metric is incorrect");
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });
  }

  private void validatePushJobDetailsStatus(
      boolean isIncPush,
      String testStoreName,
      int version,
      List<Integer> expectedStatuses,
      PushJobCheckpoints checkpoint,
      boolean isSuccess,
      String failureDetails) {
    // Verify the sent push job details.
    try (AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> client =
        ClientFactory.getAndStartSpecificAvroClient(
            ClientConfig
                .defaultSpecificClientConfig(VeniceSystemStoreUtils.getPushJobDetailsStoreName(), PushJobDetails.class)
                .setVeniceURL(childRegionClusterWrapper.getRandomRouterURL()))) {
      PushJobStatusRecordKey key = new PushJobStatusRecordKey();
      key.storeName = testStoreName;
      key.versionNumber = version;
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        try {
          assertNotNull(client.get(key).get(), "RT writes are not reflected in store yet");
        } catch (Exception e) {
          fail("Unexpected exception thrown while reading from the venice store", e);
        }
      });

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
            "Unexpected number of overall statuses in push job details. curr: " + value.overallStatus + ", expected: "
                + expectedStatuses);

        for (int i = 0; i < expectedStatuses.size(); i++) {
          assertEquals(value.overallStatus.get(i).status, (int) expectedStatuses.get(i));
          assertTrue(value.overallStatus.get(i).timestamp > 0, "Timestamp for status tuple is missing");
        }

        if (isSuccess) {
          assertFalse(value.coloStatus.isEmpty(), "Region status shouldn't be empty");
          for (List<PushJobDetailsStatusTuple> tuple: value.coloStatus.values()) {
            assertEquals(
                tuple.get(tuple.size() - 1).status,
                isIncPush ? END_OF_INCREMENTAL_PUSH_RECEIVED.getValue() : COMPLETED.getValue(),
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
        }

        assertEquals(
            value.pushJobLatestCheckpoint.intValue(),
            checkpoint.getValue(),
            "Unexpected latest push job checkpoint reported");

        assertEquals(value.failureDetails.toString(), failureDetails);
      });
    }
  }

  private void validatePushJobData(String testStoreName, int start, int end, boolean isIncPush) {
    try (AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(testStoreName)
            .setVeniceURL(childRegionClusterWrapper.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          for (int i = start; i <= end; i++) {
            String key = String.valueOf(i);
            Object value = client.get(key).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
            assertEquals(value.toString(), DEFAULT_USER_DATA_VALUE_PREFIX + (isIncPush ? (i * 2) : i));
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testPushJobDetails() throws IOException {
    // case 1: successful batch push job
    String testStoreName = "test-push-store";
    parentControllerClient.createNewStore(
        testStoreName,
        "test-user",
        recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString(),
        recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString());
    // Set store quota to unlimited else local VPJ jobs will fail due to quota enforcement NullPointerException because
    // hadoop job client cannot fetch counters properly.
    parentControllerClient.updateStore(
        testStoreName,
        new UpdateStoreQueryParams().setStorageQuotaInByte(-1).setPartitionCount(2).setIncrementalPushEnabled(true));
    Properties pushJobProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPathForFullPush, testStoreName);
    pushJobProps.setProperty(PUSH_JOB_STATUS_UPLOAD_ENABLE, String.valueOf(true));
    try (VenicePushJob testPushJob = new VenicePushJob("test-push-job-details-job", pushJobProps)) {
      testPushJob.run();
    }

    validatePushJobData(testStoreName, 1, 100, false);
    List<Integer> expectedStatuses = Arrays.asList(
        PushJobDetailsStatus.STARTED.getValue(),
        PushJobDetailsStatus.TOPIC_CREATED.getValue(),
        PushJobDetailsStatus.DATA_WRITER_COMPLETED.getValue(),
        COMPLETED.getValue());
    validatePushJobDetailsStatus(false, testStoreName, 1, expectedStatuses, JOB_STATUS_POLLING_COMPLETED, true, "");
    validatePushJobMetrics(true, false, false);

    // case 2: successful incremental push job
    Properties pushJobPropsInc = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPathForIncPush, testStoreName);
    pushJobPropsInc.setProperty(PUSH_JOB_STATUS_UPLOAD_ENABLE, String.valueOf(true));
    pushJobPropsInc.setProperty(INCREMENTAL_PUSH, String.valueOf(true));
    try (VenicePushJob testPushJob = new VenicePushJob("test-push-job-details-job-with-inc-push", pushJobPropsInc)) {
      testPushJob.run();
    }

    validatePushJobData(testStoreName, 51, 150, true);
    expectedStatuses = Arrays.asList(
        PushJobDetailsStatus.STARTED.getValue(),
        PushJobDetailsStatus.TOPIC_CREATED.getValue(),
        PushJobDetailsStatus.DATA_WRITER_COMPLETED.getValue(),
        COMPLETED.getValue());
    validatePushJobDetailsStatus(true, testStoreName, 1, expectedStatuses, JOB_STATUS_POLLING_COMPLETED, true, "");
    validatePushJobMetrics(true, false, true);

    // case 3: failed batch push job, non-user error:
    // setting the quota to be 0, hadoop job client cannot fetch counters properly and should fail the job
    parentControllerClient.updateStore(testStoreName, new UpdateStoreQueryParams().setStorageQuotaInByte(0));
    try (VenicePushJob testPushJob = new VenicePushJob("test-push-job-details-job-v2", pushJobProps)) {
      assertThrows(VeniceException.class, testPushJob::run);
    }

    expectedStatuses = Arrays.asList(
        PushJobDetailsStatus.STARTED.getValue(),
        PushJobDetailsStatus.TOPIC_CREATED.getValue(),
        PushJobDetailsStatus.ERROR.getValue());
    validatePushJobDetailsStatus(
        false,
        testStoreName,
        2,
        expectedStatuses,
        START_DATA_WRITER_JOB,
        false,
        "com.linkedin.venice.exceptions.VeniceException: Exception or error caught during VenicePushJob: java.io.IOException: Job failed!");
    validatePushJobMetrics(false, false, false);

    // case 4: failed incremental push job, non-user error
    pushJobPropsInc = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPathForIncPush, testStoreName);
    pushJobPropsInc.setProperty(PUSH_JOB_STATUS_UPLOAD_ENABLE, String.valueOf(true));
    pushJobPropsInc.setProperty(INCREMENTAL_PUSH, String.valueOf(true));
    try (VenicePushJob testPushJob = new VenicePushJob("test-push-job-details-job-with-inc-push-v2", pushJobPropsInc)) {
      assertThrows(VeniceException.class, testPushJob::run);
    }

    validatePushJobDetailsStatus(
        true,
        testStoreName,
        2,
        expectedStatuses,
        START_DATA_WRITER_JOB,
        false,
        "com.linkedin.venice.exceptions.VeniceException: Exception or error caught during VenicePushJob: java.io.IOException: Job failed!");
    validatePushJobMetrics(false, false, true);

    // case 5: failed batch push job, user error: data with duplicate keys
    final UpdateStoreQueryParams queryParams = new UpdateStoreQueryParams().setStorageQuotaInByte(-1);
    parentControllerClient.updateStore(testStoreName, queryParams);

    File inputDir = getTempDataDirectory();
    String inputDirPathWithDupKeys = "file://" + inputDir.getAbsolutePath();
    TestWriteUtils.writeSimpleAvroFileWithDuplicateKey(inputDir);

    pushJobProps = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPathWithDupKeys, testStoreName);
    pushJobProps.setProperty(PUSH_JOB_STATUS_UPLOAD_ENABLE, String.valueOf(true));
    try (final VenicePushJob testPushJob = new VenicePushJob("test-push-job-details-job-v3", pushJobProps)) {
      assertThrows(VeniceException.class, testPushJob::run); // Push job should fail
    }

    validatePushJobDetailsStatus(
        false,
        testStoreName,
        3,
        expectedStatuses,
        DUP_KEY_WITH_DIFF_VALUE,
        false,
        "com.linkedin.venice.exceptions.VeniceException: Input data has at least 9 keys that appear more than once but have different values");
    validatePushJobMetrics(false, true, false);

    // case 6: failed incremental push job, user error
    pushJobPropsInc = defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPathWithDupKeys, testStoreName);
    pushJobPropsInc.setProperty(PUSH_JOB_STATUS_UPLOAD_ENABLE, String.valueOf(true));
    pushJobPropsInc.setProperty(INCREMENTAL_PUSH, String.valueOf(true));
    try (VenicePushJob testPushJob = new VenicePushJob("test-push-job-details-job-with-inc-push-v3", pushJobPropsInc)) {
      assertThrows(VeniceException.class, testPushJob::run);
    }

    validatePushJobDetailsStatus(
        true,
        testStoreName,
        3,
        expectedStatuses,
        DUP_KEY_WITH_DIFF_VALUE,
        false,
        "com.linkedin.venice.exceptions.VeniceException: Input data has at least 9 keys that appear more than once but have different values");
    validatePushJobMetrics(false, true, true);

    // case 7: same as case 6 but overriding the user error checkpoints: The failure should not be
    // considered as user error
    pushJobPropsInc
        .setProperty(PUSH_JOB_FAILURE_CHECKPOINTS_TO_DEFINE_USER_ERROR, INPUT_DATA_SCHEMA_VALIDATION_FAILED.toString());
    try (VenicePushJob testPushJob = new VenicePushJob("test-push-job-details-job-with-inc-push-v4", pushJobPropsInc)) {
      assertThrows(VeniceException.class, testPushJob::run);
    }

    validatePushJobDetailsStatus(
        true,
        testStoreName,
        3,
        expectedStatuses,
        DUP_KEY_WITH_DIFF_VALUE,
        false,
        "com.linkedin.venice.exceptions.VeniceException: Input data has at least 9 keys that appear more than once but have different values");
    validatePushJobMetrics(false, false, true);
  }
}
