package com.linkedin.venice.controller;

import static com.linkedin.venice.controller.VeniceHelixAdmin.emitPushJobDetailsMetrics;
import static com.linkedin.venice.controller.VeniceHelixAdmin.isPushJobFailedUserError;
import static com.linkedin.venice.status.PushJobDetailsStatus.isFailed;
import static com.linkedin.venice.status.PushJobDetailsStatus.isSucceeded;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.controller.stats.PushJobStatusStats;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.utils.DataProviderUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.util.Utf8;
import org.testng.annotations.Test;


public class TestPushJobStatusStats {
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testEmitPushJobDetailsMetrics(boolean isIncrementalPush) {
    PushJobDetails details = mock(PushJobDetails.class);
    Map<CharSequence, CharSequence> pushJobConfigs = new HashMap<>();
    pushJobConfigs.put(new Utf8("incremental.push"), isIncrementalPush ? "true" : "false");
    when(details.getPushJobConfigs()).thenReturn(pushJobConfigs);

    when(details.getClusterName()).thenReturn(new Utf8("cluster1"));
    List<PushJobDetailsStatusTuple> statusTuples = new ArrayList<>();
    when(details.getOverallStatus()).thenReturn(statusTuples);

    Map<String, PushJobStatusStats> pushJobStatusStatsMap = new HashMap<>();
    PushJobStatusStats stats = mock(PushJobStatusStats.class);
    pushJobStatusStatsMap.put("cluster1", stats);

    int numberSuccess = 0;
    int numberUserErrors = 0;
    int numberNonUserErrors = 0;

    for (PushJobDetailsStatus status: PushJobDetailsStatus.values()) {
      boolean recordMetrics = false;
      if (isSucceeded(status) || isFailed(status)) {
        recordMetrics = true;
      }

      statusTuples.add(new PushJobDetailsStatusTuple(status.getValue(), 0L));

      for (PushJobCheckpoints checkpoint: PushJobCheckpoints.values()) {
        when(details.getPushJobLatestCheckpoint()).thenReturn(checkpoint.getValue());
        emitPushJobDetailsMetrics(pushJobStatusStatsMap, details);
        switch (checkpoint) {
          case QUOTA_EXCEEDED:
          case WRITE_ACL_FAILED:
          case DUP_KEY_WITH_DIFF_VALUE:
          case INPUT_DATA_SCHEMA_VALIDATION_FAILED:
          case EXTENDED_INPUT_DATA_SCHEMA_VALIDATION_FAILED:
          case RECORD_TOO_LARGE_FAILED:
          case CONCURRENT_BATCH_PUSH:
          case DATASET_CHANGED:
          case INVALID_INPUT_FILE:
          case DVC_INGESTION_ERROR_DISK_FULL:
          case DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED:
            if (recordMetrics) {
              if (isFailed(status)) {
                assertTrue(isPushJobFailedUserError(status, checkpoint));
                numberUserErrors++;
                if (isIncrementalPush) {
                  verify(stats, times(numberUserErrors)).recordIncrementalPushFailureDueToUserErrorSensor();
                } else {
                  verify(stats, times(numberUserErrors)).recordBatchPushFailureDueToUserErrorSensor();
                }
              } else {
                numberSuccess++;
                if (isIncrementalPush) {
                  verify(stats, times(numberSuccess)).recordIncrementalPushSuccessSensor();
                } else {
                  verify(stats, times(numberSuccess)).recordBatchPushSuccessSensor();
                }
              }
            }
            break;

          case INITIALIZE_PUSH_JOB:
          case NEW_VERSION_CREATED:
          case START_DATA_WRITER_JOB:
          case DATA_WRITER_JOB_COMPLETED:
          case START_JOB_STATUS_POLLING:
          case JOB_STATUS_POLLING_COMPLETED:
          case START_VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB:
          case VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED:
          case ZSTD_DICTIONARY_CREATION_FAILED:
          case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
          case DVC_INGESTION_ERROR_OTHER:
            if (recordMetrics) {
              assertFalse(isPushJobFailedUserError(status, checkpoint));
              if (isFailed(status)) {
                numberNonUserErrors++;
                if (isIncrementalPush) {
                  verify(stats, times(numberNonUserErrors)).recordIncrementalPushFailureNotDueToUserErrorSensor();
                } else {
                  verify(stats, times(numberNonUserErrors)).recordBatchPushFailureNotDueToUserErrorSensor();
                }
              } else {
                numberSuccess++;
                if (isIncrementalPush) {
                  verify(stats, times(numberSuccess)).recordIncrementalPushSuccessSensor();
                } else {
                  verify(stats, times(numberSuccess)).recordBatchPushSuccessSensor();
                }
              }
            }
            break;
          default:
            throw new IllegalArgumentException("Unknown checkpoint: " + checkpoint);
        }
      }
    }
  }
}
