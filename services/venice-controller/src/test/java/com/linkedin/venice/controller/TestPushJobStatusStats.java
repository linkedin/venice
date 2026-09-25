package com.linkedin.venice.controller;

import static com.linkedin.venice.PushJobCheckpoints.DEFAULT_PUSH_JOB_USER_ERROR_CHECKPOINTS;
import static com.linkedin.venice.PushJobCheckpoints.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.controller.VeniceHelixAdmin.emitPushJobStatusMetrics;
import static com.linkedin.venice.controller.VeniceHelixAdmin.isPushJobFailedDueToUserError;
import static com.linkedin.venice.stats.dimensions.VenicePushJobDurationBucket.AT_OR_OVER_SLA;
import static com.linkedin.venice.stats.dimensions.VenicePushJobDurationBucket.UNDER_SLA;
import static com.linkedin.venice.status.PushJobDetailsStatus.isFailed;
import static com.linkedin.venice.status.PushJobDetailsStatus.isSucceeded;
import static com.linkedin.venice.status.protocol.PushJobDetailsAdditionalMetrics.EXTERNAL_STORAGE_WRITE_TIME_MS;
import static com.linkedin.venice.status.protocol.PushJobDetailsAdditionalMetrics.VENICE_WRITE_TIME_MS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.controller.stats.LogCompactionStats;
import com.linkedin.venice.controller.stats.PushJobStatusStats;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.dimensions.VenicePushJobDataWriterSink;
import com.linkedin.venice.stats.dimensions.VenicePushJobDurationBucket;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.DataProviderUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.util.Utf8;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestPushJobStatusStats {
  private static final String STORE_NAME = "test-store";
  private static final String CLUSTER_NAME = "cluster1";
  private static final long DEFAULT_PUSH_JOB_SLA_MS = TimeUnit.HOURS.toMillis(24);
  private static final Set<PushJobCheckpoints> CUSTOM_USER_ERROR_CHECKPOINTS =
      new HashSet<>(Collections.singletonList(DVC_INGESTION_ERROR_OTHER));

  @Test(dataProvider = "Three-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testEmitPushJobStatusMetrics(
      boolean isIncrementalPush,
      boolean useUserProvidedUserErrorCheckpoints,
      boolean isRepush) {
    Set<PushJobCheckpoints> userErrorCheckpoints =
        useUserProvidedUserErrorCheckpoints ? CUSTOM_USER_ERROR_CHECKPOINTS : DEFAULT_PUSH_JOB_USER_ERROR_CHECKPOINTS;
    String storeName = "test-store";
    PushJobStatusRecordKey key = new PushJobStatusRecordKey(storeName, 1);
    PushJobDetails pushJobDetails = mock(PushJobDetails.class);
    Map<CharSequence, CharSequence> pushJobConfigs = new HashMap<>();
    pushJobConfigs.put(new Utf8("incremental.push"), String.valueOf(isIncrementalPush));
    when(pushJobDetails.getPushJobConfigs()).thenReturn(pushJobConfigs);
    when(pushJobDetails.getJobDurationInMs()).thenReturn(1000L);

    String pushId = (isRepush ? Version.VENICE_RE_PUSH_PUSH_ID_PREFIX : "") + "test-push";
    when(pushJobDetails.getPushId()).thenReturn(pushId);

    when(pushJobDetails.getClusterName()).thenReturn(new Utf8("cluster1"));
    List<PushJobDetailsStatusTuple> statusTuples = new ArrayList<>();
    when(pushJobDetails.getOverallStatus()).thenReturn(statusTuples);

    Map<String, PushJobStatusStats> pushJobStatusStatsMap = new HashMap<>();
    PushJobStatusStats pushJobStatusStats = mock(PushJobStatusStats.class);
    pushJobStatusStatsMap.put("cluster1", pushJobStatusStats);

    Map<String, LogCompactionStats> logCompactionStatsMap = new HashMap<>();
    LogCompactionStats logCompactionStats = mock(LogCompactionStats.class);
    logCompactionStatsMap.put("cluster1", logCompactionStats);

    Cache<String, Boolean> dataWriterSinkWriteTimeEmittedPushIds = Caffeine.newBuilder().build();

    for (PushJobDetailsStatus status: PushJobDetailsStatus.values()) {
      int numberSuccess = 0;
      int numberUserErrors = 0;
      int numberNonUserErrors = 0;
      boolean recordMetrics = false;
      if (isSucceeded(status) || isFailed(status)) {
        recordMetrics = true;
      }

      statusTuples.add(new PushJobDetailsStatusTuple(status.getValue(), 0L));

      for (PushJobCheckpoints checkpoint: PushJobCheckpoints.values()) {
        when(pushJobDetails.getPushJobLatestCheckpoint()).thenReturn(checkpoint.getValue());
        emitPushJobStatusMetrics(
            pushJobStatusStatsMap,
            logCompactionStatsMap,
            key,
            pushJobDetails,
            userErrorCheckpoints,
            DEFAULT_PUSH_JOB_SLA_MS,
            dataWriterSinkWriteTimeEmittedPushIds);
        boolean isUserError = userErrorCheckpoints.contains(checkpoint);

        if (isUserError) {
          if (recordMetrics) {
            if (isFailed(status)) {
              assertTrue(isPushJobFailedDueToUserError(status, pushJobDetails, userErrorCheckpoints));
              numberUserErrors++;
              if (isIncrementalPush) {
                verify(pushJobStatusStats, times(numberUserErrors))
                    .recordIncrementalPushFailureDueToUserErrorSensor(storeName, status, UNDER_SLA);
              } else {
                verify(pushJobStatusStats, times(numberUserErrors))
                    .recordBatchPushFailureDueToUserErrorSensor(storeName, status, UNDER_SLA);
              }
            } else {
              numberSuccess++;
              if (isIncrementalPush) {
                verify(pushJobStatusStats, times(numberSuccess))
                    .recordIncrementalPushSuccessSensor(storeName, status, UNDER_SLA);
              } else {
                verify(pushJobStatusStats, times(numberSuccess))
                    .recordBatchPushSuccessSensor(storeName, status, UNDER_SLA);
              }

              if (isRepush) {
                verify(logCompactionStats, times(numberSuccess)).setCompactionComplete(storeName);
              }
            }
          }
        } else {
          if (recordMetrics) {
            assertFalse(isPushJobFailedDueToUserError(status, pushJobDetails, userErrorCheckpoints));
            if (isFailed(status)) {
              numberNonUserErrors++;
              if (isIncrementalPush) {
                verify(pushJobStatusStats, times(numberNonUserErrors))
                    .recordIncrementalPushFailureNotDueToUserErrorSensor(storeName, status, UNDER_SLA);
              } else {
                verify(pushJobStatusStats, times(numberNonUserErrors))
                    .recordBatchPushFailureNotDueToUserErrorSensor(storeName, status, UNDER_SLA);
              }
            } else {
              numberSuccess++;
              if (isIncrementalPush) {
                verify(pushJobStatusStats, times(numberSuccess))
                    .recordIncrementalPushSuccessSensor(storeName, status, UNDER_SLA);
              } else {
                verify(pushJobStatusStats, times(numberSuccess))
                    .recordBatchPushSuccessSensor(storeName, status, UNDER_SLA);
              }

              if (isRepush) {
                verify(logCompactionStats, times(numberSuccess)).setCompactionComplete(storeName);
              }
            }
          }
        }
      }
    }
  }

  @DataProvider
  public Object[][] pushJobDurationBuckets() {
    long customSlaMs = TimeUnit.HOURS.toMillis(12);
    return new Object[][] { { DEFAULT_PUSH_JOB_SLA_MS, 0L, UNDER_SLA },
        { DEFAULT_PUSH_JOB_SLA_MS, DEFAULT_PUSH_JOB_SLA_MS - 1, UNDER_SLA },
        { DEFAULT_PUSH_JOB_SLA_MS, DEFAULT_PUSH_JOB_SLA_MS, AT_OR_OVER_SLA },
        { DEFAULT_PUSH_JOB_SLA_MS, DEFAULT_PUSH_JOB_SLA_MS + 1, AT_OR_OVER_SLA },
        { DEFAULT_PUSH_JOB_SLA_MS, Long.MAX_VALUE, AT_OR_OVER_SLA }, { customSlaMs, customSlaMs - 1, UNDER_SLA },
        { customSlaMs, customSlaMs, AT_OR_OVER_SLA }, { customSlaMs, customSlaMs + 1, AT_OR_OVER_SLA },
        { customSlaMs, DEFAULT_PUSH_JOB_SLA_MS - 1, AT_OR_OVER_SLA } };
  }

  @Test(dataProvider = "pushJobDurationBuckets")
  public void testKilledPushDurationBucket(
      long slaMs,
      long durationMs,
      VenicePushJobDurationBucket expectedDurationBucket) {
    DataWriterSinkWriteTimeFixture fixture = new DataWriterSinkWriteTimeFixture(null, null);
    fixture.setOverallStatus(PushJobDetailsStatus.KILLED);
    when(fixture.pushJobDetails.getJobDurationInMs()).thenReturn(durationMs);
    fixture.emit(slaMs);

    verify(fixture.pushJobStatusStats)
        .recordBatchPushFailureNotDueToUserErrorSensor(STORE_NAME, PushJobDetailsStatus.KILLED, expectedDurationBucket);
  }

  /**
   * The two data-writer durations are only meaningful once the push has reached a terminal, successful state:
   * a push that failed partway through would otherwise contribute a partial duration to the same histogram as
   * a completed push, with no status dimension to tell them apart. So nothing is observed for the intermediate
   * statuses a push reports on its way to terminal, nor for a terminal but failed status.
   */
  @Test
  public void testDataWriterSinkWriteTimeEmittedOnlyOnSucceededTerminalStatus() {
    DataWriterSinkWriteTimeFixture fixture = new DataWriterSinkWriteTimeFixture(1200L, 300L);
    fixture.emit();
    verifyNoInteractions(fixture.pushJobStatusStats);

    for (PushJobDetailsStatus status: PushJobDetailsStatus.values()) {
      fixture.reset();
      fixture.setOverallStatus(status);
      fixture.emit();

      if (PushJobDetailsStatus.isTerminal(status.getValue()) && isSucceeded(status)) {
        verify(fixture.pushJobStatusStats).recordDataWriterSinkWriteTime(
            STORE_NAME,
            Version.PushType.BATCH,
            VenicePushJobDataWriterSink.EXTERNAL_STORAGE,
            1200L);
        verify(fixture.pushJobStatusStats).recordDataWriterSinkWriteTime(
            STORE_NAME,
            Version.PushType.BATCH,
            VenicePushJobDataWriterSink.VENICE,
            300L);
      } else {
        verify(fixture.pushJobStatusStats, never()).recordDataWriterSinkWriteTime(anyString(), any(), any(), anyLong());
        if (!PushJobDetailsStatus.isTerminal(status.getValue())) {
          verifyNoInteractions(fixture.pushJobStatusStats);
        }
      }
    }
  }

  @Test
  public void testDataWriterSinkWriteTimeSkipsUnreportedDurations() {
    // A null map is the schema default: the push reported no additional metrics, so there is nothing to observe.
    DataWriterSinkWriteTimeFixture bothUnset = new DataWriterSinkWriteTimeFixture(null, null);
    bothUnset.setOverallStatus(PushJobDetailsStatus.COMPLETED);
    bothUnset.emit();
    verify(bothUnset.pushJobStatusStats, never()).recordDataWriterSinkWriteTime(anyString(), any(), any(), anyLong());

    // A push with no external storage configured omits that key but still reports its Venice leg.
    DataWriterSinkWriteTimeFixture externalUnset = new DataWriterSinkWriteTimeFixture(null, 500L);
    externalUnset.setOverallStatus(PushJobDetailsStatus.COMPLETED);
    externalUnset.emit();
    verify(externalUnset.pushJobStatusStats, never())
        .recordDataWriterSinkWriteTime(anyString(), any(), eq(VenicePushJobDataWriterSink.EXTERNAL_STORAGE), anyLong());
    verify(externalUnset.pushJobStatusStats)
        .recordDataWriterSinkWriteTime(STORE_NAME, Version.PushType.BATCH, VenicePushJobDataWriterSink.VENICE, 500L);
  }

  @Test
  public void testDataWriterSinkWriteTimeUsesIncrementalPushTypeDimension() {
    DataWriterSinkWriteTimeFixture fixture = new DataWriterSinkWriteTimeFixture(10L, 20L);
    fixture.setIncrementalPush(true);
    fixture.setOverallStatus(PushJobDetailsStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);
    fixture.setOverallStatus(PushJobDetailsStatus.COMPLETED);
    fixture.emit();

    verify(fixture.pushJobStatusStats).recordDataWriterSinkWriteTime(
        STORE_NAME,
        Version.PushType.INCREMENTAL,
        VenicePushJobDataWriterSink.EXTERNAL_STORAGE,
        10L);
    verify(fixture.pushJobStatusStats).recordDataWriterSinkWriteTime(
        STORE_NAME,
        Version.PushType.INCREMENTAL,
        VenicePushJobDataWriterSink.VENICE,
        20L);
  }

  /**
   * The same terminal PushJobDetails record can be delivered more than once (retries, parent-to-child dual
   * writes). Counters tolerate that, but a repeated duration would skew the distribution, so the durations
   * are observed exactly once per push.
   */
  @Test
  public void testDataWriterSinkWriteTimeIsDeduplicatedAcrossRepeatedTerminalReports() {
    DataWriterSinkWriteTimeFixture fixture = new DataWriterSinkWriteTimeFixture(1200L, 300L);
    fixture.setOverallStatus(PushJobDetailsStatus.COMPLETED);

    fixture.emit();
    fixture.emit();
    fixture.emit();

    verify(fixture.pushJobStatusStats, times(1)).recordDataWriterSinkWriteTime(
        STORE_NAME,
        Version.PushType.BATCH,
        VenicePushJobDataWriterSink.EXTERNAL_STORAGE,
        1200L);
    verify(fixture.pushJobStatusStats, times(1))
        .recordDataWriterSinkWriteTime(STORE_NAME, Version.PushType.BATCH, VenicePushJobDataWriterSink.VENICE, 300L);
    // The push-status counters are intentionally left alone by the dedup.
    verify(fixture.pushJobStatusStats, times(3))
        .recordBatchPushSuccessSensor(STORE_NAME, PushJobDetailsStatus.COMPLETED, UNDER_SLA);
  }

  @Test
  public void testDataWriterSinkWriteTimeDedupIsPerPush() {
    Cache<String, Boolean> sharedDedupCache = Caffeine.newBuilder().build();

    DataWriterSinkWriteTimeFixture firstPush = new DataWriterSinkWriteTimeFixture(1200L, 300L, sharedDedupCache);
    firstPush.setPushId("push-1");
    firstPush.setOverallStatus(PushJobDetailsStatus.COMPLETED);
    firstPush.emit();
    firstPush.emit();

    DataWriterSinkWriteTimeFixture secondPush = new DataWriterSinkWriteTimeFixture(999L, 111L, sharedDedupCache);
    secondPush.setPushId("push-2");
    secondPush.setOverallStatus(PushJobDetailsStatus.COMPLETED);
    secondPush.emit();

    verify(firstPush.pushJobStatusStats, times(1)).recordDataWriterSinkWriteTime(
        STORE_NAME,
        Version.PushType.BATCH,
        VenicePushJobDataWriterSink.EXTERNAL_STORAGE,
        1200L);
    verify(secondPush.pushJobStatusStats, times(1)).recordDataWriterSinkWriteTime(
        STORE_NAME,
        Version.PushType.BATCH,
        VenicePushJobDataWriterSink.EXTERNAL_STORAGE,
        999L);
  }

  /** Wires up the mocks {@code emitPushJobStatusMetrics} needs to exercise the duration emission path. */
  private static class DataWriterSinkWriteTimeFixture {
    final PushJobStatusStats pushJobStatusStats = mock(PushJobStatusStats.class);
    final PushJobStatusRecordKey key = new PushJobStatusRecordKey(STORE_NAME, 3);
    final PushJobDetails pushJobDetails = mock(PushJobDetails.class);
    final Map<String, PushJobStatusStats> pushJobStatusStatsMap = new HashMap<>();
    final Map<String, LogCompactionStats> logCompactionStatsMap = new HashMap<>();
    final Map<CharSequence, CharSequence> pushJobConfigs = new HashMap<>();
    final List<PushJobDetailsStatusTuple> statusTuples = new ArrayList<>();
    final Cache<String, Boolean> dedupCache;

    DataWriterSinkWriteTimeFixture(Long externalStorageWriteTimeMs, Long veniceWriteTimeMs) {
      this(externalStorageWriteTimeMs, veniceWriteTimeMs, Caffeine.newBuilder().build());
    }

    DataWriterSinkWriteTimeFixture(
        Long externalStorageWriteTimeMs,
        Long veniceWriteTimeMs,
        Cache<String, Boolean> dedupCache) {
      this.dedupCache = dedupCache;
      pushJobStatusStatsMap.put(CLUSTER_NAME, pushJobStatusStats);
      logCompactionStatsMap.put(CLUSTER_NAME, mock(LogCompactionStats.class));
      pushJobConfigs.put(new Utf8("incremental.push"), "false");
      when(pushJobDetails.getClusterName()).thenReturn(new Utf8(CLUSTER_NAME));
      when(pushJobDetails.getPushJobConfigs()).thenReturn(pushJobConfigs);
      when(pushJobDetails.getJobDurationInMs()).thenReturn(1000L);
      when(pushJobDetails.getOverallStatus()).thenReturn(statusTuples);
      when(pushJobDetails.getPushId()).thenReturn(new Utf8("test-push"));
      when(pushJobDetails.getPushJobLatestCheckpoint())
          .thenReturn(PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED.getValue());
      // A null argument means the push never reported that leg. Both null leaves the whole map null, which is
      // also what a v6 reader resolves an older v5 record to.
      Map<CharSequence, Long> additionalPushMetrics = null;
      if (externalStorageWriteTimeMs != null || veniceWriteTimeMs != null) {
        additionalPushMetrics = new HashMap<>();
        if (externalStorageWriteTimeMs != null) {
          additionalPushMetrics.put(new Utf8(EXTERNAL_STORAGE_WRITE_TIME_MS), externalStorageWriteTimeMs);
        }
        if (veniceWriteTimeMs != null) {
          additionalPushMetrics.put(new Utf8(VENICE_WRITE_TIME_MS), veniceWriteTimeMs);
        }
      }
      when(pushJobDetails.getAdditionalPushMetrics()).thenReturn(additionalPushMetrics);
    }

    void setIncrementalPush(boolean isIncrementalPush) {
      pushJobConfigs.put(new Utf8("incremental.push"), String.valueOf(isIncrementalPush));
    }

    void setPushId(String pushId) {
      when(pushJobDetails.getPushId()).thenReturn(new Utf8(pushId));
    }

    void setOverallStatus(PushJobDetailsStatus status) {
      statusTuples.add(new PushJobDetailsStatusTuple(status.getValue(), 0L));
    }

    void reset() {
      statusTuples.clear();
      Mockito.reset(pushJobStatusStats);
      dedupCache.invalidateAll();
    }

    void emit() {
      emit(DEFAULT_PUSH_JOB_SLA_MS);
    }

    void emit(long slaMs) {
      emitPushJobStatusMetrics(
          pushJobStatusStatsMap,
          logCompactionStatsMap,
          key,
          pushJobDetails,
          DEFAULT_PUSH_JOB_USER_ERROR_CHECKPOINTS,
          slaMs,
          dedupCache);
    }
  }
}
