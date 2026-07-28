package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.getExecutionStatusFromControllerResponse;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SPARK_PRE_WRITE_QUOTA_CHECK;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceStorageQuotaExceededException;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.jobs.ComputeJob;
import com.linkedin.venice.jobs.DataWriterComputeJob;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This class contains only unit tests for VenicePushJob class.
 *
 * For integration tests please refer to TestVenicePushJob
 *
 * todo: Remove dependency on utils from 'venice-test-common' module
 */

public class VenicePushJobStatusAndQuotaTest extends VenicePushJobTestBase {
  @Test
  public void testSendPushJobDetailsStatusWithTerminallyFailedStatus() {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    ControllerClient client = getClient();

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      // Set up initial state and basic metadata
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.STARTED);
      PushJobDetails pushJobDetails = pushJob.getPushJobDetails();
      pushJobDetails.clusterName = TEST_CLUSTER;
      pushJobDetails.failureDetails = "Test failure details";

      // Send terminal status: ERROR
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.ERROR);
      pushJob.sendPushJobDetailsToController(); // Should send this
      verify(client, times(1)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));

      // Attempt to send another terminal status: KILLED
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.KILLED);
      pushJob.sendPushJobDetailsToController(); // Should be skipped (duplicate terminal status)
      verify(client, times(1)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));

      // Call cancel; should not send again since last status was terminal
      pushJob.cancel();
      verify(client, times(1)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));

      // Send non-terminal status: COMPLETED
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.COMPLETED);
      pushJob.sendPushJobDetailsToController(); // Should send this
      verify(client, times(2)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));

      // Call cancel again; last status was non-terminal, so this should send KILLED
      pushJob.cancel();
      verify(client, times(3)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));
    }
  }

  @Test
  public void testSendNonTerminalThenTerminalStatus() {
    Properties props = getVpjRequiredProperties();
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      PushJobDetails pushJobDetails = pushJob.getPushJobDetails();
      pushJobDetails.clusterName = TEST_CLUSTER;
      pushJobDetails.failureDetails = "Test failure details";
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.STARTED);
      pushJob.sendPushJobDetailsToController();
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.COMPLETED);
      pushJob.sendPushJobDetailsToController();
      verify(client, times(2)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));
    }
  }

  @Test
  public void testSendStatusAgainIfDifferentTerminalStatus() {
    Properties props = getVpjRequiredProperties();
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      PushJobDetails pushJobDetails = pushJob.getPushJobDetails();
      pushJobDetails.clusterName = TEST_CLUSTER;
      pushJobDetails.failureDetails = "Test failure details";
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.STARTED);
      pushJob.sendPushJobDetailsToController(); // sends STARTED
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.COMPLETED);
      pushJob.sendPushJobDetailsToController(); // sends COMPLETED
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.KILLED);
      pushJob.sendPushJobDetailsToController(); // should send again because COMPLETED != KILLED
      verify(client, times(3)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));
    }
  }

  @Test
  public void testSkipRepeatedFailedStatus() {
    Properties props = getVpjRequiredProperties();
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      PushJobDetails pushJobDetails = pushJob.getPushJobDetails();
      pushJobDetails.clusterName = TEST_CLUSTER;
      pushJobDetails.failureDetails = "Test failure details";
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.ERROR);
      pushJob.sendPushJobDetailsToController(); // first send
      pushJob.addPushJobDetailsOverallStatus(PushJobDetailsStatus.ERROR);
      pushJob.sendPushJobDetailsToController(); // should skip
      verify(client, times(1)).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));
    }
  }

  @Test
  public void testHandleExceptionDuringSendPushDetailsToController() {
    Properties props = getVpjRequiredProperties();
    ControllerClient client = mock(ControllerClient.class);
    when(client.sendPushJobDetails(anyString(), anyInt(), any(byte[].class))).thenThrow(new RuntimeException("fake"));

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      PushJobDetails pushJobDetails = pushJob.getPushJobDetails();
      pushJobDetails.overallStatus = null;
      pushJobDetails.clusterName = null;
      pushJob.sendPushJobDetailsToController(); // expected to throw an exception during serialization of push job
                                                // details
      verify(client, never()).sendPushJobDetails(anyString(), anyInt(), any(byte[].class));
    }
  }

  @Test
  public void getExecutionStatusFromControllerResponseTest() {
    // some valid cases
    JobStatusQueryResponse response = new JobStatusQueryResponse();
    response.setStatus(ExecutionStatus.COMPLETED.toString());
    assertEquals(getExecutionStatusFromControllerResponse(response), ExecutionStatus.COMPLETED);
    response.setStatus(ExecutionStatus.ERROR.toString());
    assertEquals(getExecutionStatusFromControllerResponse(response), ExecutionStatus.ERROR);
    response.setStatus(ExecutionStatus.DVC_INGESTION_ERROR_OTHER.toString());
    assertEquals(getExecutionStatusFromControllerResponse(response), ExecutionStatus.DVC_INGESTION_ERROR_OTHER);

    // invalid case
    response.setStatus("INVALID_STATUS");
    VeniceException exception =
        Assert.expectThrows(VeniceException.class, () -> getExecutionStatusFromControllerResponse(response));
    Assert.assertTrue(
        exception.getMessage().contains("Invalid ExecutionStatus returned from backend. status: INVALID_STATUS"));

    Map<String, String> extraDetails = new HashMap<>();
    extraDetails.put("extraDetails", "invalid status");
    response.setExtraDetails(extraDetails);
    exception = Assert.expectThrows(VeniceException.class, () -> getExecutionStatusFromControllerResponse(response));
    Assert.assertTrue(
        exception.getMessage()
            .contains(
                "Invalid ExecutionStatus returned from backend. status: INVALID_STATUS, extra details: {extraDetails=invalid status}"));
  }

  @Test
  public void testGetPerColoPushJobDetailsStatusFromExecutionStatus() {
    for (ExecutionStatus status: ExecutionStatus.values()) {
      PushJobDetailsStatus pushJobDetailsStatus =
          VenicePushJob.getPerColoPushJobDetailsStatusFromExecutionStatus(status);
      switch (status) {
        case NOT_CREATED:
        case NEW:
        case NOT_STARTED:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.NOT_CREATED);
          break;
        case STARTED:
        case PROGRESS:
        case CATCH_UP_BASE_TOPIC_OFFSET_LAG:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.STARTED);
          break;
        case END_OF_PUSH_RECEIVED:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.END_OF_PUSH_RECEIVED);
          break;
        case TOPIC_SWITCH_RECEIVED:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.DATA_WRITER_COMPLETED);
          break;
        case START_OF_INCREMENTAL_PUSH_RECEIVED:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.START_OF_INCREMENTAL_PUSH_RECEIVED);
          break;
        case END_OF_INCREMENTAL_PUSH_RECEIVED:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);
          break;
        case COMPLETED:
        case DATA_RECOVERY_COMPLETED:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.COMPLETED);
          break;
        case ERROR:
        case DVC_INGESTION_ERROR_DISK_FULL:
        case DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED:
        case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
        case DVC_INGESTION_ERROR_OTHER:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.ERROR);
          break;
        case START_OF_BUFFER_REPLAY_RECEIVED:
        case DROPPED:
        case WARNING:
        case ARCHIVED:
        case UNKNOWN:
          assertEquals(pushJobDetailsStatus, PushJobDetailsStatus.UNKNOWN);
          break;
        default:
          /* Newly added ExecutionStatus should be mapped properly in
           * VenicePushJob.getPerColoPushJobDetailsStatusFromExecutionStatus, and this test should be updated accordingly.
           */
          fail(status + " is not mapped properly in getPerColoPushJobDetailsStatusFromExecutionStatus");
      }
    }
  }

  /**
   * Tests that the error message for the {@link PushJobCheckpoints#RECORD_TOO_LARGE_FAILED} code path of
   * {@link VenicePushJob#updatePushJobDetailsWithJobDetails(DataWriterTaskTracker)} uses maxRecordSizeBytes.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testUpdatePushJobDetailsWithJobDetailsRecordTooLarge(boolean chunkingEnabled) {
    try (final VenicePushJob vpj = getSpyVenicePushJob(getVpjRequiredProperties(), getClient())) {
      // Setup push job settings and mocks
      PushJobDetails pushJobDetails = vpj.getPushJobDetails();
      pushJobDetails.chunkingEnabled = chunkingEnabled;
      setPushJobSettingDefaults(vpj.getPushJobSetting());
      vpj.setInputStorageQuotaTracker(mock(InputStorageQuotaTracker.class));
      final DataWriterTaskTracker dataWriterTaskTracker = mock(DataWriterTaskTracker.class);
      doReturn(1L).when(dataWriterTaskTracker).getRecordTooLargeFailureCount();

      // The value of chunkingEnabled should dictate the error message returned
      final String errorMessage = vpj.updatePushJobDetailsWithJobDetails(dataWriterTaskTracker);
      final int latestCheckpoint = pushJobDetails.pushJobLatestCheckpoint;
      Assert.assertTrue(errorMessage.contains((chunkingEnabled) ? "100.0 MiB" : "950.0 KiB"), errorMessage);
      Assert.assertEquals(latestCheckpoint, PushJobCheckpoints.RECORD_TOO_LARGE_FAILED.getValue());
    }
  }

  /**
   * Tests that the error message for the {@link PushJobCheckpoints#RECORD_TOO_LARGE_FAILED} code path of
   * {@link VenicePushJob#updatePushJobDetailsWithJobDetails(DataWriterTaskTracker)} uses maxRecordSizeBytes.
   */
  @Test(dataProvider = "Boolean-Compression", dataProviderClass = DataProviderUtils.class)
  public void testUpdatePushJobDetailsWithJobDetailsRecordTooLargeWithCompression(
      boolean enableUncompressedMaxRecordSizeLimit,
      CompressionStrategy compressionStrategy) {
    try (final VenicePushJob vpj = getSpyVenicePushJob(getVpjRequiredProperties(), getClient())) {
      // Setup push job settings and mocks
      PushJobDetails pushJobDetails = vpj.getPushJobDetails();

      setPushJobSettingDefaults(vpj.getPushJobSetting());
      vpj.setInputStorageQuotaTracker(mock(InputStorageQuotaTracker.class));
      vpj.getPushJobSetting().enableUncompressedRecordSizeLimit = enableUncompressedMaxRecordSizeLimit;
      vpj.getPushJobSetting().storeCompressionStrategy = compressionStrategy;

      final DataWriterTaskTracker dataWriterTaskTracker = mock(DataWriterTaskTracker.class);
      doReturn(1L).when(dataWriterTaskTracker).getRecordTooLargeFailureCount();
      doReturn(1L).when(dataWriterTaskTracker).getUncompressedRecordTooLargeFailureCount();

      // The value of chunkingEnabled should dictate the error message returned
      final String errorMessage = vpj.updatePushJobDetailsWithJobDetails(dataWriterTaskTracker);
      Assert.assertTrue(
          errorMessage.contains("records that exceed the maximum record limit of"),
          "Unexpected error message: " + errorMessage);

      if (compressionStrategy.isCompressionEnabled()) {
        if (enableUncompressedMaxRecordSizeLimit) {
          Assert.assertTrue(errorMessage.contains("before compression"), "Unexpected error message: " + errorMessage);
        } else {
          Assert.assertTrue(errorMessage.contains("after compression"), "Unexpected error message: " + errorMessage);
        }
      }

      final int latestCheckpoint = pushJobDetails.pushJobLatestCheckpoint;
      Assert.assertEquals(latestCheckpoint, PushJobCheckpoints.RECORD_TOO_LARGE_FAILED.getValue());
    }
  }

  @Test(dataProvider = "Three-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testShouldBuildZstdCompressionDictionary(
      boolean compressionMetricCollectionEnabled,
      boolean isIncrementalPush,
      boolean inputFileHasRecords) {
    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.compressionMetricCollectionEnabled = compressionMetricCollectionEnabled;
    pushJobSetting.isIncrementalPush = isIncrementalPush;

    for (CompressionStrategy compressionStrategy: CompressionStrategy.values()) {
      pushJobSetting.storeCompressionStrategy = compressionStrategy;

      if (isIncrementalPush) {
        assertFalse(VenicePushJob.shouldBuildZstdCompressionDictionary(pushJobSetting, inputFileHasRecords));
      } else if (compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
        assertTrue(VenicePushJob.shouldBuildZstdCompressionDictionary(pushJobSetting, inputFileHasRecords));
      } else if (compressionMetricCollectionEnabled && inputFileHasRecords) {
        assertTrue(VenicePushJob.shouldBuildZstdCompressionDictionary(pushJobSetting, inputFileHasRecords));
      } else {
        assertFalse(VenicePushJob.shouldBuildZstdCompressionDictionary(pushJobSetting, inputFileHasRecords));
      }
    }
  }

  @Test
  public void testEvaluateCompressionMetricCollectionEnabled() {
    PushJobSetting pushJobSetting = new PushJobSetting();

    // Test with compressionMetricCollectionEnabled == false
    pushJobSetting.compressionMetricCollectionEnabled = false;
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, true));
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, false));

    // reset settings for the below tests
    pushJobSetting.compressionMetricCollectionEnabled = true;
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, false));

    // Test with isIncrementalPush == true
    pushJobSetting.isIncrementalPush = true;
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, true));
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, false));

    // reset settings for the below tests
    pushJobSetting.isIncrementalPush = false;
    assertTrue(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, true));
    assertFalse(VenicePushJob.evaluateCompressionMetricCollectionEnabled(pushJobSetting, false));

  }

  @Test
  public void testUpdatePushJobDetailsSkipsPostWriteQuotaCheckWhenComputeJobPerformsPreWriteQuotaCheck() {
    try (final VenicePushJob vpj = getSpyVenicePushJob(getVpjRequiredProperties(), getClient())) {
      setPushJobSettingDefaults(vpj.getPushJobSetting());
      InputStorageQuotaTracker inputStorageQuotaTracker = mock(InputStorageQuotaTracker.class);
      doReturn(true).when(inputStorageQuotaTracker).exceedQuota(anyLong());
      vpj.setInputStorageQuotaTracker(inputStorageQuotaTracker);

      DataWriterTaskTracker dataWriterTaskTracker = mock(DataWriterTaskTracker.class);
      doReturn(10L).when(dataWriterTaskTracker).getTotalKeySize();
      doReturn(10L).when(dataWriterTaskTracker).getTotalValueSize();

      DataWriterComputeJob dataWriterComputeJob = mock(DataWriterComputeJob.class);
      doReturn(dataWriterComputeJob).when(vpj).getDataWriterComputeJob();
      doReturn(ComputeJob.Status.SUCCEEDED).when(dataWriterComputeJob).getStatus();
      doReturn(dataWriterTaskTracker).when(dataWriterComputeJob).getTaskTracker();
      doReturn(true).when(dataWriterComputeJob).performsPreWriteQuotaCheck();

      vpj.runJobAndUpdateStatus();

      verify(inputStorageQuotaTracker, never()).exceedQuota(anyLong());
      Assert.assertEquals(
          (int) vpj.getPushJobDetails().pushJobLatestCheckpoint,
          PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED.getValue());
    }
  }

  @Test
  public void testRunJobAndUpdateStatusMapsPreWriteQuotaExceptionToQuotaExceededCheckpoint() throws Exception {
    try (final VenicePushJob vpj = getSpyVenicePushJob(getVpjRequiredProperties(), getClient())) {
      setPushJobSettingDefaults(vpj.getPushJobSetting());
      doNothing().when(vpj).checkLastModificationTimeAndLog();

      DataWriterTaskTracker dataWriterTaskTracker = mock(DataWriterTaskTracker.class);
      DataWriterComputeJob dataWriterComputeJob = mock(DataWriterComputeJob.class);
      doReturn(dataWriterComputeJob).when(vpj).getDataWriterComputeJob();
      doReturn(ComputeJob.Status.FAILED).when(dataWriterComputeJob).getStatus();
      doReturn(dataWriterTaskTracker).when(dataWriterComputeJob).getTaskTracker();
      VeniceStorageQuotaExceededException quotaExceededException =
          new VeniceStorageQuotaExceededException("pre-write quota exceeded");
      doReturn(quotaExceededException).when(dataWriterComputeJob).getFailureReason();

      VeniceStorageQuotaExceededException thrown =
          Assert.expectThrows(VeniceStorageQuotaExceededException.class, vpj::runJobAndUpdateStatus);

      Assert.assertSame(thrown, quotaExceededException);
      Assert.assertEquals(
          (int) vpj.getPushJobDetails().pushJobLatestCheckpoint,
          PushJobCheckpoints.QUOTA_EXCEEDED.getValue());
    }
  }

  @Test
  public void testRefreshAndGetCurrentStorageQuotaUpdatesSettingFromController() {
    long originalQuota = 1L;
    long refreshedQuota = 123L;
    ControllerClient client = getClient(storeInfo -> storeInfo.setStorageQuotaInByte(refreshedQuota));

    try (final VenicePushJob vpj = getSpyVenicePushJob(getVpjRequiredProperties(), client)) {
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      setPushJobSettingDefaults(pushJobSetting);
      pushJobSetting.controllerRetries = 1;
      pushJobSetting.storeStorageQuota = originalQuota;

      Assert.assertEquals(vpj.refreshAndGetCurrentStorageQuota(), refreshedQuota);
      Assert.assertEquals(pushJobSetting.storeStorageQuota, refreshedQuota);
    }
  }

  @Test
  public void testRefreshAndGetCurrentStorageQuotaSkipsControllerForRepush() {
    ControllerClient client = mock(ControllerClient.class);

    try (final VenicePushJob vpj = getSpyVenicePushJob(getVpjRequiredProperties(), client)) {
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      setPushJobSettingDefaults(pushJobSetting);
      pushJobSetting.isSourceKafka = true;
      pushJobSetting.storeStorageQuota = 456L;

      Assert.assertEquals(vpj.refreshAndGetCurrentStorageQuota(), 456L);
      verify(client, never()).getStore(anyString());
    }
  }

  @Test
  public void testSparkPreWriteStorageQuotaCheckConfigDefaultsToDisabledAndCanBeEnabled() {
    try (final VenicePushJob vpj = getSpyVenicePushJob(getVpjRequiredProperties(), getClient())) {
      assertFalse(vpj.getPushJobSetting().sparkPreWriteQuotaCheckEnabled);
    }

    Properties props = getVpjRequiredProperties();
    props.setProperty(SPARK_PRE_WRITE_QUOTA_CHECK, String.valueOf(true));

    try (final VenicePushJob vpj = getSpyVenicePushJob(props, getClient())) {
      assertTrue(vpj.getPushJobSetting().sparkPreWriteQuotaCheckEnabled);
    }
  }

  @Test
  public void testUpdatePushJobDetailsRunsPostWriteQuotaCheckWhenComputeJobDoesNotPerformPreWriteQuotaCheck() {
    try (final VenicePushJob vpj = getSpyVenicePushJob(getVpjRequiredProperties(), getClient())) {
      setPushJobSettingDefaults(vpj.getPushJobSetting());
      InputStorageQuotaTracker inputStorageQuotaTracker = mock(InputStorageQuotaTracker.class);
      doReturn(true).when(inputStorageQuotaTracker).exceedQuota(anyLong());
      doReturn(1L).when(inputStorageQuotaTracker).getStoreStorageQuota();
      vpj.setInputStorageQuotaTracker(inputStorageQuotaTracker);

      DataWriterTaskTracker dataWriterTaskTracker = mock(DataWriterTaskTracker.class);
      doReturn(10L).when(dataWriterTaskTracker).getTotalKeySize();
      doReturn(10L).when(dataWriterTaskTracker).getTotalValueSize();

      DataWriterComputeJob dataWriterComputeJob = mock(DataWriterComputeJob.class);
      doReturn(dataWriterComputeJob).when(vpj).getDataWriterComputeJob();
      doReturn(ComputeJob.Status.SUCCEEDED).when(dataWriterComputeJob).getStatus();
      doReturn(dataWriterTaskTracker).when(dataWriterComputeJob).getTaskTracker();
      doReturn(false).when(dataWriterComputeJob).performsPreWriteQuotaCheck();

      Assert.expectThrows(VeniceException.class, vpj::runJobAndUpdateStatus);

      verify(inputStorageQuotaTracker).exceedQuota(20L);
      Assert.assertEquals(
          (int) vpj.getPushJobDetails().pushJobLatestCheckpoint,
          PushJobCheckpoints.QUOTA_EXCEEDED.getValue());
    }
  }

  /**
   * These are mainly for code coverage for the code paths that construct {@link VeniceWriter} instances.
   */
  @Test
  public void testGetVeniceWriter() {
    Properties props = getVpjRequiredProperties();
    props.put(VeniceWriter.CLOSE_TIMEOUT_MS, 1000);
    try (final VenicePushJob vpj = getSpyVenicePushJob(props, getClient())) {
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      setPushJobSettingDefaults(pushJobSetting);
      final VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter = vpj.getVeniceWriter(pushJobSetting);
      Assert.assertNotNull(veniceWriter, "VeniceWriter should've been constructed and returned");
      Assert.assertEquals(veniceWriter, vpj.getVeniceWriter(pushJobSetting), "Second get() should return same object");
    }
  }
}
