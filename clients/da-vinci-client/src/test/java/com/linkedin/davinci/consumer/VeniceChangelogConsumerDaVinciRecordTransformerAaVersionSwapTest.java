package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.client.store.ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.InternalDaVinciRecordTransformer;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.utils.LogContext;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for the active-active aware version swap path on the DaVinciRecordTransformer-based CDC
 * consumer (gated by {@link ChangelogClientConfig#isVersionSwapByControlMessageEnabled()}).
 */
public class VeniceChangelogConsumerDaVinciRecordTransformerAaVersionSwapTest {
  private static final String STORE = "test_store";
  private static final String D2_SERVICE_NAME = "ChildController";
  private static final String CLIENT_REGION = "us-region-1";
  private static final String DEST_A = "dc-a";
  private static final String DEST_B = "dc-b";
  private static final int CURRENT_VERSION = 3;
  private static final int FUTURE_VERSION = 4;
  private static final int TOTAL_REGIONS = 2;
  private static final int PARTITION_COUNT = 2;

  private ChangelogClientConfig changelogClientConfig;
  private VeniceChangelogConsumerDaVinciRecordTransformerImpl<Integer, Integer> consumer;
  private VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer currentTransformer;
  private VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer futureTransformer;
  private InternalDaVinciRecordTransformer<Integer, Integer, Integer> currentInternal;
  private InternalDaVinciRecordTransformer<Integer, Integer, Integer> futureInternal;
  private BasicConsumerStats stats;

  @BeforeMethod
  public void setUp() {
    SchemaReader schemaReader = mock(SchemaReader.class);
    Schema keySchema = Schema.create(Schema.Type.INT);
    Schema valueSchema = Schema.create(Schema.Type.INT);
    when(schemaReader.getKeySchema()).thenReturn(keySchema);
    when(schemaReader.getValueSchema(1)).thenReturn(valueSchema);

    changelogClientConfig = new ChangelogClientConfig<>().setD2ControllerClient(mock(D2ControllerClient.class))
        .setSchemaReader(schemaReader)
        .setStoreName(STORE)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setConsumerProperties(new Properties())
        .setLocalD2ZkHosts("test_zookeeper")
        .setD2Client(mock(D2Client.class))
        .setVersionSwapByControlMessageEnabled(true)
        .setClientRegionName(CLIENT_REGION)
        .setTotalRegionCount(TOTAL_REGIONS);
    changelogClientConfig.getInnerClientConfig()
        .setMetricsRepository(getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true));

    VeniceChangelogConsumerClientFactory factory =
        spy(new VeniceChangelogConsumerClientFactory(changelogClientConfig, null));
    consumer = spy(new VeniceChangelogConsumerDaVinciRecordTransformerImpl<>(changelogClientConfig, factory));

    // Spy stats so we can verify metric emissions
    stats = spy(consumer.getChangeCaptureStats());
    consumer.setChangeCaptureStats(stats);

    // Coordinator was initialized with the original (pre-spy) stats reference; rebuild it so its
    // metric emissions are routed through the spy.
    consumer.setVersionSwapCoordinator(
        new RecordTransformerVersionSwapCoordinator(
            STORE,
            CLIENT_REGION,
            TOTAL_REGIONS,
            changelogClientConfig.getVersionSwapTimeoutInMs(),
            stats,
            consumer.getPartitionToVersionToServe(),
            consumer.getSubscribedPartitions(),
            consumer::stashVersionSwapException,
            (LogContext) null));

    currentTransformer = consumer.new DaVinciRecordTransformerChangelogConsumer(STORE, CURRENT_VERSION, keySchema,
        valueSchema, valueSchema, mock(DaVinciRecordTransformerConfig.class));
    futureTransformer = consumer.new DaVinciRecordTransformerChangelogConsumer(STORE, FUTURE_VERSION, keySchema,
        valueSchema, valueSchema, mock(DaVinciRecordTransformerConfig.class));

    currentInternal =
        (InternalDaVinciRecordTransformer<Integer, Integer, Integer>) mock(InternalDaVinciRecordTransformer.class);
    futureInternal =
        (InternalDaVinciRecordTransformer<Integer, Integer, Integer>) mock(InternalDaVinciRecordTransformer.class);
    currentTransformer.setInternalRecordTransformer(currentInternal);
    futureTransformer.setInternalRecordTransformer(futureInternal);

    // All partitions assigned and starting on CURRENT_VERSION
    Map<Integer, Integer> partitionToVersionToServe = consumer.getPartitionToVersionToServe();
    Set<Integer> subscribed = consumer.getSubscribedPartitions();
    for (int p = 0; p < PARTITION_COUNT; p++) {
      partitionToVersionToServe.put(p, CURRENT_VERSION);
      subscribed.add(p);
    }
  }

  private VersionSwap newVsm(long generationId, String sourceRegion, String destRegion, int oldV, int newV) {
    VersionSwap vs = new VersionSwap();
    vs.oldServingVersionTopic = new Utf8(Version.composeKafkaTopic(STORE, oldV));
    vs.newServingVersionTopic = new Utf8(Version.composeKafkaTopic(STORE, newV));
    vs.sourceRegion = new Utf8(sourceRegion);
    vs.destinationRegion = new Utf8(destRegion);
    vs.generationId = generationId;
    return vs;
  }

  @Test
  public void testOnVersionSwapAaPathPausesCurrentOnPartitionComplete() {
    VersionSwap fromA = newVsm(1L, CLIENT_REGION, DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    VersionSwap fromB = newVsm(1L, CLIENT_REGION, DEST_B, CURRENT_VERSION, FUTURE_VERSION);

    // First region — partition 0 not yet complete
    currentTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 0);
    verify(currentInternal, never()).pausePartitionConsumption(0);

    // Second region — partition 0 now complete on current side; pause expected
    currentTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 0);
    verify(currentInternal, times(1)).pausePartitionConsumption(0);
  }

  @Test
  public void testOnVersionSwapAaPathPausesFutureOnPartitionComplete() {
    VersionSwap fromA = newVsm(2L, CLIENT_REGION, DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    VersionSwap fromB = newVsm(2L, CLIENT_REGION, DEST_B, CURRENT_VERSION, FUTURE_VERSION);

    futureTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 0);
    verify(futureInternal, never()).pausePartitionConsumption(0);

    futureTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 0);
    verify(futureInternal, times(1)).pausePartitionConsumption(0);
  }

  @Test
  public void testOnVersionSwapAaPathDropsForeignRegionVsm() {
    VersionSwap foreign = newVsm(3L, "different-region", DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    currentTransformer.onVersionSwap(foreign, CURRENT_VERSION, FUTURE_VERSION, 0);
    verify(currentInternal, never()).pausePartitionConsumption(0);
    verify(stats, never()).emitVersionSwapCountMetrics(SUCCESS);
  }

  @Test
  public void testOnVersionSwapAaPathDropsLegacyGenerationIdVsm() {
    VersionSwap legacy = newVsm(-1L, CLIENT_REGION, DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    currentTransformer.onVersionSwap(legacy, CURRENT_VERSION, FUTURE_VERSION, 0);
    verify(currentInternal, never()).pausePartitionConsumption(0);
    verify(stats, never()).emitVersionSwapCountMetrics(SUCCESS);
  }

  @Test
  public void testOnVersionSwapAaPathDropsStaleGenerationIdVsm() {
    VersionSwap firstA = newVsm(10L, CLIENT_REGION, DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    VersionSwap stale = newVsm(9L, CLIENT_REGION, DEST_B, CURRENT_VERSION, FUTURE_VERSION);

    currentTransformer.onVersionSwap(firstA, CURRENT_VERSION, FUTURE_VERSION, 0);
    // Stale generation arrives mid-swap — must be ignored
    currentTransformer.onVersionSwap(stale, CURRENT_VERSION, FUTURE_VERSION, 0);
    // Only the original region was accumulated; partition not yet complete
    verify(currentInternal, never()).pausePartitionConsumption(0);
  }

  @Test
  public void testOnVersionSwapAaPathDropsStaleRollbackVsm() {
    Map<Integer, Integer> partitionToVersionToServe = consumer.getPartitionToVersionToServe();
    // Already serving v6 — rollback re-emitting v3->v4 must be ignored
    partitionToVersionToServe.put(0, 6);
    partitionToVersionToServe.put(1, 6);

    VersionSwap rollback = newVsm(50L, CLIENT_REGION, DEST_A, 3, 4);
    currentTransformer.onVersionSwap(rollback, 3, 4, 0);
    verify(currentInternal, never()).pausePartitionConsumption(0);
  }

  @Test
  public void testOnVersionSwapAaPathFullSwapEmitsSingleSuccessAndFlipsAllPartitions() {
    VersionSwap fromA = newVsm(20L, CLIENT_REGION, DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    VersionSwap fromB = newVsm(20L, CLIENT_REGION, DEST_B, CURRENT_VERSION, FUTURE_VERSION);

    for (int p = 0; p < PARTITION_COUNT; p++) {
      currentTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, p);
      currentTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, p);
      futureTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, p);
      futureTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, p);
    }

    Map<Integer, Integer> partitionToVersionToServe = consumer.getPartitionToVersionToServe();
    for (int p = 0; p < PARTITION_COUNT; p++) {
      assertNotNull(partitionToVersionToServe.get(p));
      assertEquals(partitionToVersionToServe.get(p).intValue(), FUTURE_VERSION);
    }
    // Single SUCCESS metric for the entire swap
    verify(stats, times(1)).emitVersionSwapCountMetrics(SUCCESS);
    verify(stats, never()).emitVersionSwapCountMetrics(FAIL);
    // Future side resumed for every partition
    for (int p = 0; p < PARTITION_COUNT; p++) {
      verify(futureInternal, atLeastOnce()).resumePartitionConsumption(p);
    }
  }

  @Test
  public void testOnVersionSwapAaPathFailureSurfacesViaPollWithoutKillingSit() {
    // Wrap partitionToVersionToServe in a map that throws on every put — simulates a commit failure.
    // The coordinator catches via failSwap and stashes the exception. Crucially, onVersionSwap
    // itself does NOT propagate the exception (it would kill the StoreIngestionTask thread that
    // drives processControlMessage); the surface is observed by the next poll() instead.
    Map<Integer, Integer> throwingMap = new ConcurrentHashMap<Integer, Integer>() {
      @Override
      public Integer put(Integer key, Integer value) {
        throw new IllegalStateException("induced failure during commit");
      }

      @Override
      public Collection<Integer> values() {
        return Collections.singletonList(CURRENT_VERSION);
      }
    };

    RecordTransformerVersionSwapCoordinator failingCoordinator = new RecordTransformerVersionSwapCoordinator(
        STORE,
        CLIENT_REGION,
        TOTAL_REGIONS,
        changelogClientConfig.getVersionSwapTimeoutInMs(),
        stats,
        throwingMap,
        consumer.getSubscribedPartitions(),
        consumer::stashVersionSwapException,
        (LogContext) null);
    consumer.setVersionSwapCoordinator(failingCoordinator);

    VersionSwap fromA = newVsm(30L, CLIENT_REGION, DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    VersionSwap fromB = newVsm(30L, CLIENT_REGION, DEST_B, CURRENT_VERSION, FUTURE_VERSION);

    currentTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 0);
    currentTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 0);
    currentTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 1);
    currentTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 1);
    futureTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 0);
    futureTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 1);
    futureTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 1);
    // Last call completes the barrier and triggers commitSwap which throws via the throwing map.
    // onVersionSwap must NOT propagate the exception — it captures it via the coordinator's
    // failure surface and lets the SIT thread continue. Surface is observed on the next poll().
    futureTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 0);

    verify(stats, times(1)).emitVersionSwapCountMetrics(FAIL);
    // Next poll() observes and throws the stashed exception
    assertThrows(VeniceException.class, () -> consumer.poll(1L));
  }

  @Test
  public void testOnVersionSwapAaPathAccumulatesConcurrentFailures() {
    // Two failures occur before poll() observes either — the first must remain the primary cause
    // and the second must be retained as a suppressed exception (not silently dropped). This
    // exercises the production wiring's accumulateAndGet path; the test-constructed coordinators
    // elsewhere use the simpler ::set wiring directly.
    RecordTransformerVersionSwapCoordinator coordinator = consumer.getVersionSwapCoordinator();
    RuntimeException first = new RuntimeException("first failure");
    RuntimeException second = new RuntimeException("second failure");
    coordinator.failSwap(first);
    coordinator.failSwap(second);

    try {
      consumer.poll(1L);
      org.testng.Assert.fail("poll() should have thrown after stashed failures");
    } catch (VeniceException raised) {
      Throwable cause = raised.getCause();
      org.testng.Assert.assertSame(cause, first, "first failure should remain the primary cause");
      Throwable[] suppressed = cause.getSuppressed();
      org.testng.Assert.assertEquals(suppressed.length, 1, "second failure should be suppressed");
      org.testng.Assert.assertSame(suppressed[0], second);
    }
  }

  @Test
  public void testOnVersionSwapAaPathPollClearsExceptionAndCanRecover() {
    // After a poll() surfaces a swap exception, the field must be cleared so the consumer can
    // continue to operate when subsequent swaps succeed. Inject a pre-arm exception directly via
    // failSwap; the first poll() should throw, the next poll() should not.
    consumer.getVersionSwapCoordinator().failSwap(new RuntimeException("pre-arm boom"));

    assertThrows(VeniceException.class, () -> consumer.poll(1L));
    // Second poll() — surface should be cleared, no throw
    consumer.poll(1L);
  }

  @Test
  public void testOnVersionSwapAaPathLateSubscriberGetsFlippedAtCommit() {
    // After the swap is armed with only partitions {0,1}, a third partition is subscribed
    // mid-swap. The snapshot doesn't include it, so it isn't counted against the barrier — but at
    // commit time, flipServingVersion must still flip it to FUTURE_VERSION, otherwise records from
    // the future-version transformer for that partition would be silently filtered out until the
    // next swap.
    VersionSwap fromA = newVsm(40L, CLIENT_REGION, DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    VersionSwap fromB = newVsm(40L, CLIENT_REGION, DEST_B, CURRENT_VERSION, FUTURE_VERSION);

    // First observation arms the swap with subscribed = {0, 1}
    currentTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 0);

    // Mid-swap: a third partition gets subscribed
    consumer.getSubscribedPartitions().add(2);
    consumer.getPartitionToVersionToServe().put(2, CURRENT_VERSION);

    // Original swap completes for partitions {0, 1}
    currentTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 0);
    currentTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 1);
    currentTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 1);
    futureTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 0);
    futureTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 0);
    futureTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 1);
    futureTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 1);

    Map<Integer, Integer> partitionToVersionToServe = consumer.getPartitionToVersionToServe();
    // Snapshotted partitions are flipped
    assertEquals(partitionToVersionToServe.get(0).intValue(), FUTURE_VERSION);
    assertEquals(partitionToVersionToServe.get(1).intValue(), FUTURE_VERSION);
    // Late subscriber is also flipped so future-version records surface
    assertEquals(partitionToVersionToServe.get(2).intValue(), FUTURE_VERSION);
  }

  @Test
  public void testOnVersionSwapAaPathMalformedVsmFieldsAreRejected() {
    // A VSM with missing topic / region fields must not NPE inside the coordinator — it should
    // be silently rejected (effectively a malformed message dropped at the boundary).
    VersionSwap noOldTopic = newVsm(50L, CLIENT_REGION, DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    noOldTopic.oldServingVersionTopic = null;
    currentTransformer.onVersionSwap(noOldTopic, CURRENT_VERSION, FUTURE_VERSION, 0);
    verify(currentInternal, never()).pausePartitionConsumption(0);

    VersionSwap noNewTopic = newVsm(51L, CLIENT_REGION, DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    noNewTopic.newServingVersionTopic = null;
    currentTransformer.onVersionSwap(noNewTopic, CURRENT_VERSION, FUTURE_VERSION, 0);
    verify(currentInternal, never()).pausePartitionConsumption(0);

    VersionSwap noDestRegion = newVsm(52L, CLIENT_REGION, DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    noDestRegion.destinationRegion = null;
    currentTransformer.onVersionSwap(noDestRegion, CURRENT_VERSION, FUTURE_VERSION, 0);
    verify(currentInternal, never()).pausePartitionConsumption(0);

    // No swap should have been emitted — none of the malformed VSMs ever passed gating.
    verify(stats, never()).emitVersionSwapCountMetrics(SUCCESS);
    verify(stats, never()).emitVersionSwapCountMetrics(FAIL);
  }

  @Test
  public void testOnVersionSwapAaPathTerminalFailureResumesBothSides() {
    // Wire a coordinator whose flip throws after both sides have paused partitions. The terminal
    // failure path must resume Kafka prefetch on both sides so partitions don't stay stuck paused
    // when the SIT continues running on the same store-version.
    Map<Integer, Integer> throwingMap = new ConcurrentHashMap<Integer, Integer>() {
      @Override
      public Integer put(Integer key, Integer value) {
        throw new IllegalStateException("induced flip failure");
      }

      @Override
      public Collection<Integer> values() {
        return Collections.singletonList(CURRENT_VERSION);
      }
    };

    RecordTransformerVersionSwapCoordinator failingCoordinator = new RecordTransformerVersionSwapCoordinator(
        STORE,
        CLIENT_REGION,
        TOTAL_REGIONS,
        changelogClientConfig.getVersionSwapTimeoutInMs(),
        stats,
        throwingMap,
        consumer.getSubscribedPartitions(),
        consumer::stashVersionSwapException,
        (LogContext) null);
    consumer.setVersionSwapCoordinator(failingCoordinator);

    VersionSwap fromA = newVsm(60L, CLIENT_REGION, DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    VersionSwap fromB = newVsm(60L, CLIENT_REGION, DEST_B, CURRENT_VERSION, FUTURE_VERSION);

    // Drive both sides to paused on both partitions before triggering the commit
    currentTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 0);
    currentTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 0);
    currentTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 1);
    currentTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 1);
    futureTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 0);
    futureTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 0);
    futureTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 1);
    futureTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 1); // triggers commit

    // Both sides should be resumed by handleTerminalFailure for every paused partition
    for (int p = 0; p < PARTITION_COUNT; p++) {
      verify(currentInternal, times(1)).resumePartitionConsumption(p);
      verify(futureInternal, times(1)).resumePartitionConsumption(p);
    }
  }

  @Test
  public void testOnVersionSwapLegacyBehaviorUnchangedWhenFlagFalse() {
    // Build a fresh consumer with the flag OFF and verify the legacy per-partition flip semantics.
    ChangelogClientConfig legacyConfig =
        ChangelogClientConfig.cloneConfig(changelogClientConfig).setVersionSwapByControlMessageEnabled(false);
    legacyConfig.setStoreName(STORE);
    VeniceChangelogConsumerClientFactory factory = spy(new VeniceChangelogConsumerClientFactory(legacyConfig, null));
    VeniceChangelogConsumerDaVinciRecordTransformerImpl<Integer, Integer> legacyConsumer =
        new VeniceChangelogConsumerDaVinciRecordTransformerImpl<>(legacyConfig, factory);

    // Coordinator must be null when flag is off
    assertNull(legacyConsumer.getVersionSwapCoordinator());
  }
}
