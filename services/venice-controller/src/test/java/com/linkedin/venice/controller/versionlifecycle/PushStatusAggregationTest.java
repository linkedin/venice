package com.linkedin.venice.controller.versionlifecycle;

import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.setOf;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;


/**
 * Tests for the push-status aggregation helpers on {@link VersionLifecyclePolicy}:
 * {@code getOverallPushStatus} (Venice + DaVinci roll-up) and {@code getFinalReturnStatus}
 * (multi-region roll-up with majority gating).
 */
public class PushStatusAggregationTest {
  // ---------- getOverallPushStatus ----------
  @Test
  public void overallPushStatusReturnsCompletedWhenBothCompleted() {
    assertEquals(
        VersionLifecyclePolicy.getOverallPushStatus(ExecutionStatus.COMPLETED, ExecutionStatus.COMPLETED),
        ExecutionStatus.COMPLETED);
  }

  @Test
  public void overallPushStatusReturnsErrorWhenEitherIsError() {
    assertEquals(
        VersionLifecyclePolicy.getOverallPushStatus(ExecutionStatus.ERROR, ExecutionStatus.COMPLETED),
        ExecutionStatus.ERROR);
    assertEquals(
        VersionLifecyclePolicy.getOverallPushStatus(ExecutionStatus.ERROR, ExecutionStatus.ERROR),
        ExecutionStatus.ERROR);
  }

  @Test
  public void overallPushStatusPrefersDvcIngestionErrorOverError() {
    // DVC ingestion errors sit higher in STATUS_PRIORITIES than ERROR, so they outrank a Venice ERROR.
    assertEquals(
        VersionLifecyclePolicy
            .getOverallPushStatus(ExecutionStatus.COMPLETED, ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL),
        ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
    assertEquals(
        VersionLifecyclePolicy
            .getOverallPushStatus(ExecutionStatus.ERROR, ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL),
        ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
  }

  // ---------- getFinalReturnStatus ----------

  @Test
  public void finalReturnStatusAllCompletedReturnsCompleted() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.COMPLETED);
    statuses.put("region3", ExecutionStatus.COMPLETED);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.COMPLETED);
  }

  @Test
  public void finalReturnStatusProgressOutranksCompleted() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.PROGRESS);
    statuses.put("region3", ExecutionStatus.COMPLETED);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.PROGRESS);
  }

  @Test
  public void finalReturnStatusErrorBeatsCompleted() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.ERROR);
    statuses.put("region3", ExecutionStatus.COMPLETED);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.ERROR);
  }

  @Test
  public void finalReturnStatusUnknownBeatsErrorWithOneRegionFailed() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.ERROR);
    statuses.put("region3", ExecutionStatus.UNKNOWN);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 1, new StringBuilder()),
        ExecutionStatus.UNKNOWN);
  }

  @Test
  public void finalReturnStatusFailedMajorityDowngradesToProgress() {
    // 2 of 3 regions unreachable → strict majority of 2 not met → downgrade to PROGRESS.
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.UNKNOWN);
    statuses.put("region2", ExecutionStatus.ERROR);
    statuses.put("region3", ExecutionStatus.UNKNOWN);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 2, new StringBuilder()),
        ExecutionStatus.PROGRESS);
  }

  @Test
  public void finalReturnStatusDvcDiskFullOutranksDvcOther() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.COMPLETED);
    statuses.put("region3", ExecutionStatus.DVC_INGESTION_ERROR_OTHER);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.DVC_INGESTION_ERROR_OTHER);

    statuses.put("region3", ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
  }

  @Test
  public void finalReturnStatusDvcDiskFullOutranksMemoryLimit() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED);
    statuses.put("region3", ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
  }

  @Test
  public void finalReturnStatusDvcOtherOutranksMemoryLimit() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED);
    statuses.put("region3", ExecutionStatus.DVC_INGESTION_ERROR_OTHER);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.DVC_INGESTION_ERROR_OTHER);
  }
}
