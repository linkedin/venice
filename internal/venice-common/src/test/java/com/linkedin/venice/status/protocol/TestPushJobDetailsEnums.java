package com.linkedin.venice.status.protocol;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.ARCHIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.CATCH_UP_BASE_TOPIC_OFFSET_LAG;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DATA_RECOVERY_COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DROPPED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NEW;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NOT_STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.PROGRESS;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_BUFFER_REPLAY_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.TOPIC_SWITCH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.WARNING;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.status.PushJobDetailsStatus;
import java.util.Arrays;
import java.util.HashSet;
import org.testng.annotations.Test;


public class TestPushJobDetailsEnums {
  /**
   * This is to ensure the known {@link ExecutionStatus} statuses reported as part of
   * push job details can be parsed properly. This test should fail and alert developers when adding new statuses in
   * {@link ExecutionStatus} without modifying this test or {@link PushJobDetailsStatus}.
   */
  @Test
  public void testPushJobDetailsStatusEnums() {
    // A list of known ExecutionStatus that we don't report/expose to job status polling.
    ExecutionStatus[] unreportedStatusesArray =
        { NEW, NOT_STARTED, PROGRESS, START_OF_BUFFER_REPLAY_RECEIVED, TOPIC_SWITCH_RECEIVED, DROPPED, WARNING,
            ARCHIVED, CATCH_UP_BASE_TOPIC_OFFSET_LAG, DATA_RECOVERY_COMPLETED, DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED,
            DVC_INGESTION_ERROR_DISK_FULL, DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES, DVC_INGESTION_ERROR_OTHER };
    HashSet<ExecutionStatus> unreportedStatuses = new HashSet<>(Arrays.asList(unreportedStatusesArray));
    HashSet<Integer> processedSignals = new HashSet<>();
    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (unreportedStatuses.contains(status)) {
        continue; // Ignore parsing of statuses that are never reported.
      }
      Integer intValue = PushJobDetailsStatus.valueOf(status.toString()).getValue();
      assertFalse(
          processedSignals.contains(intValue),
          "Each PushJobDetailsStatus should have its own unique int value. " + status + "don't have one.");
      processedSignals.add(intValue);
    }
  }
}
