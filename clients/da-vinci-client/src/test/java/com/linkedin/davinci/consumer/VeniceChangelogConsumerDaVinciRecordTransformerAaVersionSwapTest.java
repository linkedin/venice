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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.InternalDaVinciRecordTransformer;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaReader;
import java.lang.reflect.Field;
import java.util.Properties;
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

  private ChangelogClientConfig changelogClientConfig;
  private VeniceChangelogConsumerDaVinciRecordTransformerImpl<Integer, Integer> consumer;
  private VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer currentTransformer;
  private VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer futureTransformer;
  private InternalDaVinciRecordTransformer<Integer, Integer, Integer> currentInternal;
  private InternalDaVinciRecordTransformer<Integer, Integer, Integer> futureInternal;
  private BasicConsumerStats stats;

  @BeforeMethod
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    SchemaReader schemaReader = mock(SchemaReader.class);
    Schema keySchema = Schema.create(Schema.Type.INT);
    Schema valueSchema = Schema.create(Schema.Type.INT);
    org.mockito.Mockito.when(schemaReader.getKeySchema()).thenReturn(keySchema);
    org.mockito.Mockito.when(schemaReader.getValueSchema(1)).thenReturn(valueSchema);

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
    Field statsField = VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("changeCaptureStats");
    statsField.setAccessible(true);
    stats = spy((BasicConsumerStats) statsField.get(consumer));
    statsField.set(consumer, stats);

    // Coordinator was initialized with the original (pre-spy) stats reference; rebuild it so its
    // metric emissions are routed through the spy.
    Field coordinatorField =
        VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("versionSwapCoordinator");
    coordinatorField.setAccessible(true);
    Field p2vField =
        VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("partitionToVersionToServe");
    p2vField.setAccessible(true);
    Field subscribedField =
        VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("subscribedPartitions");
    subscribedField.setAccessible(true);
    Field excField =
        VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("versionSwapThreadException");
    excField.setAccessible(true);
    java.util.concurrent.atomic.AtomicReference<Exception> exc =
        (java.util.concurrent.atomic.AtomicReference<Exception>) excField.get(consumer);
    coordinatorField.set(
        consumer,
        new RecordTransformerVersionSwapCoordinator(
            STORE,
            CLIENT_REGION,
            TOTAL_REGIONS,
            changelogClientConfig.getVersionSwapTimeoutInMs(),
            stats,
            (java.util.Map<Integer, Integer>) p2vField.get(consumer),
            (java.util.Set<Integer>) subscribedField.get(consumer),
            exc::set));

    currentTransformer = consumer.new DaVinciRecordTransformerChangelogConsumer(STORE, CURRENT_VERSION, keySchema,
        valueSchema, valueSchema, mock(com.linkedin.davinci.client.DaVinciRecordTransformerConfig.class));
    futureTransformer = consumer.new DaVinciRecordTransformerChangelogConsumer(STORE, FUTURE_VERSION, keySchema,
        valueSchema, valueSchema, mock(com.linkedin.davinci.client.DaVinciRecordTransformerConfig.class));

    currentInternal =
        (InternalDaVinciRecordTransformer<Integer, Integer, Integer>) mock(InternalDaVinciRecordTransformer.class);
    futureInternal =
        (InternalDaVinciRecordTransformer<Integer, Integer, Integer>) mock(InternalDaVinciRecordTransformer.class);
    currentTransformer.setInternalRecordTransformer(currentInternal);
    futureTransformer.setInternalRecordTransformer(futureInternal);

    // Both partitions assigned and starting on CURRENT_VERSION
    java.util.Map<Integer, Integer> partitionToVersionToServe =
        (java.util.Map<Integer, Integer>) p2vField.get(consumer);
    java.util.Set<Integer> subscribed = (java.util.Set<Integer>) subscribedField.get(consumer);
    for (int p = 0; p < TOTAL_REGIONS; p++) {
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
  public void testOnVersionSwapAaPathDropsStaleRollbackVsm() throws IllegalAccessException, NoSuchFieldException {
    Field p2vField =
        VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("partitionToVersionToServe");
    p2vField.setAccessible(true);
    java.util.Map<Integer, Integer> partitionToVersionToServe =
        (java.util.Map<Integer, Integer>) p2vField.get(consumer);
    // Already serving v6 — rollback re-emitting v3->v4 must be ignored
    partitionToVersionToServe.put(0, 6);
    partitionToVersionToServe.put(1, 6);

    VersionSwap rollback = newVsm(50L, CLIENT_REGION, DEST_A, 3, 4);
    currentTransformer.onVersionSwap(rollback, 3, 4, 0);
    verify(currentInternal, never()).pausePartitionConsumption(0);
  }

  @Test
  public void testOnVersionSwapAaPathFullSwapEmitsSingleSuccessAndFlipsAllPartitions()
      throws NoSuchFieldException, IllegalAccessException {
    VersionSwap fromA = newVsm(20L, CLIENT_REGION, DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    VersionSwap fromB = newVsm(20L, CLIENT_REGION, DEST_B, CURRENT_VERSION, FUTURE_VERSION);

    for (int p = 0; p < TOTAL_REGIONS; p++) {
      currentTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, p);
      currentTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, p);
      futureTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, p);
      futureTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, p);
    }

    Field p2vField =
        VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("partitionToVersionToServe");
    p2vField.setAccessible(true);
    java.util.Map<Integer, Integer> partitionToVersionToServe =
        (java.util.Map<Integer, Integer>) p2vField.get(consumer);
    for (int p = 0; p < TOTAL_REGIONS; p++) {
      assertNotNull(partitionToVersionToServe.get(p));
      org.testng.Assert.assertEquals(partitionToVersionToServe.get(p).intValue(), FUTURE_VERSION);
    }
    // Single SUCCESS metric for the entire swap
    verify(stats, times(1)).emitVersionSwapCountMetrics(SUCCESS);
    verify(stats, never()).emitVersionSwapCountMetrics(FAIL);
    // Future side resumed for every partition
    for (int p = 0; p < TOTAL_REGIONS; p++) {
      verify(futureInternal, atLeastOnce()).resumePartitionConsumption(p);
    }
  }

  @Test
  public void testOnVersionSwapAaPathFailureSurfacesViaPoll() {
    // Wrap partitionToVersionToServe in a map that throws on the second put — simulates an
    // unexpected failure mid-commit. The coordinator catches via failSwap and stashes the exception
    // so the next poll() throws.
    java.util.concurrent.atomic.AtomicReference<Exception> exc;
    java.util.Set<Integer> subscribed;
    try {
      Field excField =
          VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("versionSwapThreadException");
      excField.setAccessible(true);
      exc = (java.util.concurrent.atomic.AtomicReference<Exception>) excField.get(consumer);

      Field subscribedField =
          VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("subscribedPartitions");
      subscribedField.setAccessible(true);
      subscribed = (java.util.Set<Integer>) subscribedField.get(consumer);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    java.util.Map<Integer, Integer> throwingMap = new java.util.concurrent.ConcurrentHashMap<Integer, Integer>() {
      @Override
      public Integer put(Integer key, Integer value) {
        throw new IllegalStateException("induced failure during commit");
      }

      @Override
      public java.util.Collection<Integer> values() {
        return java.util.Collections.singletonList(CURRENT_VERSION);
      }
    };

    RecordTransformerVersionSwapCoordinator failingCoordinator = new RecordTransformerVersionSwapCoordinator(
        STORE,
        CLIENT_REGION,
        TOTAL_REGIONS,
        changelogClientConfig.getVersionSwapTimeoutInMs(),
        stats,
        throwingMap,
        subscribed,
        exc::set);
    try {
      Field coordinatorField =
          VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("versionSwapCoordinator");
      coordinatorField.setAccessible(true);
      coordinatorField.set(consumer, failingCoordinator);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    VersionSwap fromA = newVsm(30L, CLIENT_REGION, DEST_A, CURRENT_VERSION, FUTURE_VERSION);
    VersionSwap fromB = newVsm(30L, CLIENT_REGION, DEST_B, CURRENT_VERSION, FUTURE_VERSION);

    currentTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 0);
    currentTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 0);
    currentTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 1);
    currentTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 1);
    futureTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 0);
    futureTransformer.onVersionSwap(fromA, CURRENT_VERSION, FUTURE_VERSION, 1);
    futureTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 1);
    // Last call completes the barrier and triggers commitSwap which throws via the throwing map
    assertThrows(Exception.class, () -> futureTransformer.onVersionSwap(fromB, CURRENT_VERSION, FUTURE_VERSION, 0));

    verify(stats, times(1)).emitVersionSwapCountMetrics(FAIL);
    // Next poll() observes the stashed exception
    assertThrows(VeniceException.class, () -> consumer.poll(1L));
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
