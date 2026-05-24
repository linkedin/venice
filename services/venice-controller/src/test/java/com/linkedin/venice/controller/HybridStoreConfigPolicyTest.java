package com.linkedin.venice.controller;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.ACTIVE_ACTIVE_REPLICATION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BUFFER_REPLAY_POLICY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DATA_REPLICATION_POLICY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INCREMENTAL_PUSH_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OFFSET_LAG_TO_GO_ONLINE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REAL_TIME_TOPIC_NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REWIND_TIME_IN_SECONDS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SEPARATE_REAL_TIME_TOPIC_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TIME_LAG_TO_GO_ONLINE;
import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_HYBRID_OFFSET_LAG_THRESHOLD;
import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD;
import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_REAL_TIME_TOPIC_NAME;
import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_REWIND_TIME_IN_SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.kafka.protocol.admin.HybridStoreConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Store;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


public class HybridStoreConfigPolicyTest {
  private static final Logger LOGGER = LogManager.getLogger(HybridStoreConfigPolicyTest.class);
  private static final String STORE_NAME = "test_store";

  // ---------- isHybrid ----------

  @Test
  public void isHybridReturnsFalseForNullConfig() {
    assertFalse(HybridStoreConfigPolicy.isHybrid((HybridStoreConfig) null));
  }

  @Test
  public void isHybridReturnsFalseWhenAllThresholdsNegative() {
    HybridStoreConfig batchSentinel = new HybridStoreConfigImpl(
        -1L,
        -1L,
        -1L,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP,
        DEFAULT_REAL_TIME_TOPIC_NAME);
    assertFalse(HybridStoreConfigPolicy.isHybrid(batchSentinel));
  }

  @Test
  public void isHybridReturnsTrueWhenRewindNonNegative() {
    HybridStoreConfig hybrid = new HybridStoreConfigImpl(
        0L,
        -1L,
        -1L,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP,
        DEFAULT_REAL_TIME_TOPIC_NAME);
    assertTrue(HybridStoreConfigPolicy.isHybrid(hybrid));
  }

  @Test
  public void isHybridReturnsFalseForNullRecord() {
    assertFalse(HybridStoreConfigPolicy.isHybrid((HybridStoreConfigRecord) null));
  }

  @Test
  public void isHybridReturnsTrueForHybridRecord() {
    HybridStoreConfigRecord record = new HybridStoreConfigRecord();
    record.rewindTimeInSeconds = 100L;
    record.offsetLagThresholdToGoOnline = 1000L;
    record.producerTimestampLagThresholdToGoOnlineInSeconds = -1L;
    record.dataReplicationPolicy = DataReplicationPolicy.NON_AGGREGATE.getValue();
    record.bufferReplayPolicy = BufferReplayPolicy.REWIND_FROM_EOP.getValue();
    record.realTimeTopicName = DEFAULT_REAL_TIME_TOPIC_NAME;
    assertTrue(HybridStoreConfigPolicy.isHybrid(record));
  }

  @Test
  public void isHybridRecordWithNullRealTimeTopicNameUsesThresholdsWithoutThrowing() {
    HybridStoreConfigRecord record = new HybridStoreConfigRecord();
    record.rewindTimeInSeconds = 100L;
    record.offsetLagThresholdToGoOnline = -1L;
    record.producerTimestampLagThresholdToGoOnlineInSeconds = -1L;
    record.dataReplicationPolicy = DataReplicationPolicy.NON_AGGREGATE.getValue();
    record.bufferReplayPolicy = BufferReplayPolicy.REWIND_FROM_EOP.getValue();
    record.realTimeTopicName = null;

    assertTrue(HybridStoreConfigPolicy.isHybrid(record), "Hybrid classification ");
  }

  // ---------- mergeNewSettingsIntoOldHybridStoreConfig: edge cases ----------

  @Test
  public void mergeThrowsWhenConvertingBatchToHybridWithoutLagThreshold() {
    Store batchStore = newBatchStore();
    // Only rewind specified; need at least one lag threshold.
    VeniceException ex = expectThrows(
        VeniceException.class,
        () -> HybridStoreConfigPolicy.mergeNewSettingsIntoOldHybridStoreConfig(
            batchStore,
            Optional.of(100L),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()));
    assertTrue(ex.getMessage().contains("was not a hybrid store"), "Unexpected message: " + ex.getMessage());
  }

  @Test
  public void mergePreservesExistingHybridValuesWhenNothingSpecified() {
    HybridStoreConfig existing = new HybridStoreConfigImpl(
        500L,
        2000L,
        300L,
        DataReplicationPolicy.AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_SOP,
        "custom_rt");
    Store hybridStore = newHybridStore(existing);

    HybridStoreConfig merged = HybridStoreConfigPolicy.mergeNewSettingsIntoOldHybridStoreConfig(
        hybridStore,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

    assertNotNull(merged);
    assertEquals(merged.getRewindTimeInSeconds(), 500L);
    assertEquals(merged.getOffsetLagThresholdToGoOnline(), 2000L);
    assertEquals(merged.getProducerTimestampLagThresholdToGoOnlineInSeconds(), 300L);
    assertEquals(merged.getDataReplicationPolicy(), DataReplicationPolicy.AGGREGATE);
    assertEquals(merged.getBufferReplayPolicy(), BufferReplayPolicy.REWIND_FROM_SOP);
    assertEquals(merged.getRealTimeTopicName(), "custom_rt");
  }

  @Test
  public void mergeThrowsWhenRewindPositiveButBothLagsNegative() {
    HybridStoreConfig existing = new HybridStoreConfigImpl(
        100L,
        1000L,
        -1L,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP,
        DEFAULT_REAL_TIME_TOPIC_NAME);
    Store hybridStore = newHybridStore(existing);

    // Drive offsetLag to -1 while keeping rewind > 0 and timeLag -1 — should trip the safety check.
    VeniceException ex = expectThrows(
        VeniceException.class,
        () -> HybridStoreConfigPolicy.mergeNewSettingsIntoOldHybridStoreConfig(
            hybridStore,
            Optional.empty(),
            Optional.of(-1L),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()));
    assertTrue(
        ex.getMessage().contains("Both offset lag threshold and time lag threshold are negative"),
        "Unexpected message: " + ex.getMessage());
  }

  // ---------- applyHybridAndReplicationConfigUpdates: batch -> hybrid ----------

  @Test
  public void applyAutoEnablesActiveActiveOnBatchToHybridConversion() {
    Store currStore = newBatchStore();
    UpdateStore setStore = new UpdateStore();
    List<CharSequence> updatedConfigs = new ArrayList<>();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    when(controllerConfig.isActiveActiveReplicationEnabledAsDefaultForHybrid()).thenReturn(true);
    // Keep other auto-enables off so this test pins only the A/A flip.
    when(controllerConfig.enabledIncrementalPushForHybridActiveActiveUserStores()).thenReturn(false);
    when(controllerConfig.enabledSeparateRealTimeTopicForStoreWithIncrementalPush()).thenReturn(false);

    boolean converted = HybridStoreConfigPolicy.applyHybridAndReplicationConfigUpdates(
        STORE_NAME,
        currStore,
        setStore,
        controllerConfig,
        Optional.of(100L),
        Optional.of(1000L),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        updatedConfigs,
        LOGGER);

    assertTrue(converted, "store should be flagged as batch -> hybrid conversion");
    assertTrue(setStore.activeActiveReplicationEnabled, "A/A should be auto-enabled on conversion");
    assertFalse(setStore.incrementalPushEnabled, "incremental push must remain off without explicit/auto trigger");
    assertNotNull(setStore.hybridStoreConfig);
    assertEquals(setStore.hybridStoreConfig.rewindTimeInSeconds, 100L);
    assertEquals(setStore.hybridStoreConfig.offsetLagThresholdToGoOnline, 1000L);
    assertTrue(updatedConfigs.contains(REWIND_TIME_IN_SECONDS));
    assertTrue(updatedConfigs.contains(OFFSET_LAG_TO_GO_ONLINE));
    assertTrue(updatedConfigs.contains(ACTIVE_ACTIVE_REPLICATION_ENABLED));
    assertFalse(updatedConfigs.contains(INCREMENTAL_PUSH_ENABLED));
  }

  @Test
  public void applyDoesNotAutoEnableActiveActiveForSystemStore() {
    Store currStore = newBatchStore();
    when(currStore.isSystemStore()).thenReturn(true);
    UpdateStore setStore = new UpdateStore();
    List<CharSequence> updatedConfigs = new ArrayList<>();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    when(controllerConfig.isActiveActiveReplicationEnabledAsDefaultForHybrid()).thenReturn(true);

    boolean converted = HybridStoreConfigPolicy.applyHybridAndReplicationConfigUpdates(
        STORE_NAME,
        currStore,
        setStore,
        controllerConfig,
        Optional.of(100L),
        Optional.of(1000L),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        updatedConfigs,
        LOGGER);

    assertTrue(converted);
    assertFalse(setStore.activeActiveReplicationEnabled, "system store must not get auto-A/A");
    assertFalse(updatedConfigs.contains(ACTIVE_ACTIVE_REPLICATION_ENABLED));
  }

  @Test
  public void applyAutoEnablesIncrementalPushOnBatchToHybridActiveActiveWhenClusterConfigAllows() {
    Store currStore = newBatchStore();
    UpdateStore setStore = new UpdateStore();
    List<CharSequence> updatedConfigs = new ArrayList<>();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    when(controllerConfig.isActiveActiveReplicationEnabledAsDefaultForHybrid()).thenReturn(true);
    when(controllerConfig.enabledIncrementalPushForHybridActiveActiveUserStores()).thenReturn(true);
    when(controllerConfig.enabledSeparateRealTimeTopicForStoreWithIncrementalPush()).thenReturn(false);

    HybridStoreConfigPolicy.applyHybridAndReplicationConfigUpdates(
        STORE_NAME,
        currStore,
        setStore,
        controllerConfig,
        Optional.of(100L),
        Optional.of(1000L),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        updatedConfigs,
        LOGGER);

    assertTrue(setStore.activeActiveReplicationEnabled);
    assertTrue(setStore.incrementalPushEnabled, "incremental push should be auto-enabled on hybrid+A/A user store");
    assertFalse(setStore.separateRealTimeTopicEnabled);
    assertTrue(updatedConfigs.contains(INCREMENTAL_PUSH_ENABLED));
    assertFalse(updatedConfigs.contains(SEPARATE_REAL_TIME_TOPIC_ENABLED));
  }

  @Test
  public void applyAutoEnablesSeparateRealTimeTopicWhenIncrementalPushAndClusterConfigAllow() {
    Store currStore = newBatchStore();
    UpdateStore setStore = new UpdateStore();
    List<CharSequence> updatedConfigs = new ArrayList<>();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    when(controllerConfig.enabledSeparateRealTimeTopicForStoreWithIncrementalPush()).thenReturn(true);

    HybridStoreConfigPolicy.applyHybridAndReplicationConfigUpdates(
        STORE_NAME,
        currStore,
        setStore,
        controllerConfig,
        Optional.of(100L),
        Optional.of(1000L),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(true),
        updatedConfigs,
        LOGGER);

    assertTrue(setStore.incrementalPushEnabled);
    assertTrue(setStore.separateRealTimeTopicEnabled);
    assertTrue(updatedConfigs.contains(INCREMENTAL_PUSH_ENABLED));
    assertTrue(updatedConfigs.contains(SEPARATE_REAL_TIME_TOPIC_ENABLED));
  }

  // ---------- applyHybridAndReplicationConfigUpdates: hybrid -> batch ----------

  @Test
  public void applyRejectsHybridToBatchConversionCombinedWithActiveActive() {
    Store currStore = newHybridStore(newDefaultHybridConfig());
    UpdateStore setStore = new UpdateStore();
    List<CharSequence> updatedConfigs = new ArrayList<>();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);

    VeniceHttpException ex = expectThrows(
        VeniceHttpException.class,
        () -> HybridStoreConfigPolicy.applyHybridAndReplicationConfigUpdates(
            STORE_NAME,
            currStore,
            setStore,
            controllerConfig,
            Optional.of(-1L),
            Optional.of(-1L),
            Optional.of(-1L),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(true), // explicit A/A=true alongside hybrid->batch
            Optional.empty(),
            updatedConfigs,
            LOGGER));
    assertEquals(ex.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertTrue(ex.getMessage().contains("Active/Active"), "Unexpected message: " + ex.getMessage());
  }

  @Test
  public void applyRejectsHybridToBatchConversionCombinedWithIncrementalPush() {
    Store currStore = newHybridStore(newDefaultHybridConfig());
    UpdateStore setStore = new UpdateStore();
    List<CharSequence> updatedConfigs = new ArrayList<>();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);

    VeniceHttpException ex = expectThrows(
        VeniceHttpException.class,
        () -> HybridStoreConfigPolicy.applyHybridAndReplicationConfigUpdates(
            STORE_NAME,
            currStore,
            setStore,
            controllerConfig,
            Optional.of(-1L),
            Optional.of(-1L),
            Optional.of(-1L),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(true), // explicit incremental push=true alongside hybrid->batch
            updatedConfigs,
            LOGGER));
    assertEquals(ex.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertTrue(ex.getMessage().contains("incremental push"), "Unexpected message: " + ex.getMessage());
  }

  @Test
  public void applyAutoDisablesActiveActiveAndIncrementalPushOnHybridToBatchConversion() {
    Store currStore = newHybridStore(newDefaultHybridConfig());
    when(currStore.isActiveActiveReplicationEnabled()).thenReturn(true);
    when(currStore.isIncrementalPushEnabled()).thenReturn(true);
    UpdateStore setStore = new UpdateStore();
    List<CharSequence> updatedConfigs = new ArrayList<>();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);

    boolean converted = HybridStoreConfigPolicy.applyHybridAndReplicationConfigUpdates(
        STORE_NAME,
        currStore,
        setStore,
        controllerConfig,
        Optional.of(-1L),
        Optional.of(-1L),
        Optional.of(-1L),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        updatedConfigs,
        LOGGER);

    assertFalse(converted, "store is being converted to batch, not to hybrid");
    assertFalse(setStore.activeActiveReplicationEnabled, "A/A must be auto-disabled on hybrid->batch");
    assertFalse(setStore.incrementalPushEnabled, "incremental push must be auto-disabled on hybrid->batch");
    assertTrue(updatedConfigs.contains(ACTIVE_ACTIVE_REPLICATION_ENABLED));
    assertTrue(updatedConfigs.contains(INCREMENTAL_PUSH_ENABLED));
  }

  // ---------- applyHybridAndReplicationConfigUpdates: implicit batch -> A/A hybrid via incremental push ----------

  @Test
  public void applyConvertsBatchStoreToActiveActiveHybridWhenEnablingIncrementalPush() {
    Store currStore = newBatchStore();
    UpdateStore setStore = new UpdateStore();
    List<CharSequence> updatedConfigs = new ArrayList<>();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    when(controllerConfig.isActiveActiveReplicationEnabledAsDefaultForHybrid()).thenReturn(true);

    boolean converted = HybridStoreConfigPolicy.applyHybridAndReplicationConfigUpdates(
        STORE_NAME,
        currStore,
        setStore,
        controllerConfig,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(true),
        updatedConfigs,
        LOGGER);

    assertTrue(
        converted,
        "implicit batch->hybrid via incremental push must surface as storeBeingConvertedToHybrid=true so partial-update auto-enable runs");
    assertTrue(setStore.incrementalPushEnabled);
    assertNotNull(setStore.hybridStoreConfig, "implicit hybrid record should be synthesized");
    assertEquals(setStore.hybridStoreConfig.rewindTimeInSeconds, DEFAULT_REWIND_TIME_IN_SECONDS);
    assertEquals(setStore.hybridStoreConfig.offsetLagThresholdToGoOnline, DEFAULT_HYBRID_OFFSET_LAG_THRESHOLD);
    assertEquals(
        setStore.hybridStoreConfig.producerTimestampLagThresholdToGoOnlineInSeconds,
        DEFAULT_HYBRID_TIME_LAG_THRESHOLD);
    assertEquals(setStore.hybridStoreConfig.dataReplicationPolicy, DataReplicationPolicy.NON_AGGREGATE.getValue());
    assertEquals(setStore.hybridStoreConfig.bufferReplayPolicy, BufferReplayPolicy.REWIND_FROM_EOP.getValue());
    assertEquals(setStore.hybridStoreConfig.realTimeTopicName, DEFAULT_REAL_TIME_TOPIC_NAME);
    assertTrue(setStore.activeActiveReplicationEnabled, "A/A is auto-enabled alongside the implicit hybrid conversion");

    assertTrue(updatedConfigs.contains(INCREMENTAL_PUSH_ENABLED));
    assertTrue(updatedConfigs.contains(REWIND_TIME_IN_SECONDS));
    assertTrue(updatedConfigs.contains(OFFSET_LAG_TO_GO_ONLINE));
    assertTrue(updatedConfigs.contains(TIME_LAG_TO_GO_ONLINE));
    assertTrue(updatedConfigs.contains(DATA_REPLICATION_POLICY));
    assertTrue(updatedConfigs.contains(BUFFER_REPLAY_POLICY));
    assertTrue(updatedConfigs.contains(ACTIVE_ACTIVE_REPLICATION_ENABLED));
  }

  @Test
  public void applyDoesNotImplicitlyConvertSystemStoreToActiveActiveHybrid() {
    Store currStore = newBatchStore();
    when(currStore.isSystemStore()).thenReturn(true);
    UpdateStore setStore = new UpdateStore();
    List<CharSequence> updatedConfigs = new ArrayList<>();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    when(controllerConfig.isActiveActiveReplicationEnabledAsDefaultForHybrid()).thenReturn(true);

    HybridStoreConfigPolicy.applyHybridAndReplicationConfigUpdates(
        STORE_NAME,
        currStore,
        setStore,
        controllerConfig,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(true),
        updatedConfigs,
        LOGGER);

    // The implicit hybrid record is still synthesized, but A/A is NOT auto-enabled for system stores.
    assertNotNull(setStore.hybridStoreConfig);
    assertFalse(setStore.activeActiveReplicationEnabled);
    assertFalse(updatedConfigs.contains(ACTIVE_ACTIVE_REPLICATION_ENABLED));
  }

  // ---------- applyHybridAndReplicationConfigUpdates: mid-life no-op ----------

  @Test
  public void applyOnHybridStoreWithoutChangesIsANoOp() {
    HybridStoreConfig existing = newDefaultHybridConfig();
    Store currStore = newHybridStore(existing);
    when(currStore.isActiveActiveReplicationEnabled()).thenReturn(true);
    UpdateStore setStore = new UpdateStore();
    List<CharSequence> updatedConfigs = new ArrayList<>();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    // Even if cluster-level toggles are on, they only fire on transitions.
    when(controllerConfig.isActiveActiveReplicationEnabledAsDefaultForHybrid()).thenReturn(true);
    when(controllerConfig.enabledIncrementalPushForHybridActiveActiveUserStores()).thenReturn(true);
    when(controllerConfig.enabledSeparateRealTimeTopicForStoreWithIncrementalPush()).thenReturn(true);

    boolean converted = HybridStoreConfigPolicy.applyHybridAndReplicationConfigUpdates(
        STORE_NAME,
        currStore,
        setStore,
        controllerConfig,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        updatedConfigs,
        LOGGER);

    assertFalse(converted);
    assertTrue(setStore.activeActiveReplicationEnabled, "A/A flag tracks currStore when not explicitly overridden");
    assertFalse(setStore.incrementalPushEnabled, "incremental push tracks currStore when not explicitly overridden");
    assertNotNull(setStore.hybridStoreConfig, "existing hybrid record is preserved");
    assertEquals(setStore.hybridStoreConfig.rewindTimeInSeconds, existing.getRewindTimeInSeconds());
    assertTrue(
        updatedConfigs.isEmpty(),
        "no config keys should be recorded for a no-op update, got: " + updatedConfigs);
  }

  @Test
  public void applyOnBatchStoreWithoutChangesIsANoOp() {
    Store currStore = newBatchStore();
    UpdateStore setStore = new UpdateStore();
    List<CharSequence> updatedConfigs = new ArrayList<>();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);

    boolean converted = HybridStoreConfigPolicy.applyHybridAndReplicationConfigUpdates(
        STORE_NAME,
        currStore,
        setStore,
        controllerConfig,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        updatedConfigs,
        LOGGER);

    assertFalse(converted);
    assertNull(setStore.hybridStoreConfig);
    assertFalse(setStore.activeActiveReplicationEnabled);
    assertFalse(setStore.incrementalPushEnabled);
    assertTrue(updatedConfigs.isEmpty(), "no config keys should be recorded, got: " + updatedConfigs);
  }

  @Test
  public void applyPassesRealTimeTopicNameThroughToHybridRecord() {
    Store currStore = newBatchStore();
    UpdateStore setStore = new UpdateStore();
    List<CharSequence> updatedConfigs = new ArrayList<>();
    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);

    HybridStoreConfigPolicy.applyHybridAndReplicationConfigUpdates(
        STORE_NAME,
        currStore,
        setStore,
        controllerConfig,
        Optional.of(100L),
        Optional.of(1000L),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of("custom_rt_v2"),
        Optional.empty(),
        Optional.empty(),
        updatedConfigs,
        LOGGER);

    assertNotNull(setStore.hybridStoreConfig);
    assertEquals(setStore.hybridStoreConfig.realTimeTopicName, "custom_rt_v2");
    assertTrue(updatedConfigs.contains(REAL_TIME_TOPIC_NAME));
  }

  // ---------- helpers ----------

  private static Store newBatchStore() {
    Store store = mock(Store.class);
    when(store.getName()).thenReturn(STORE_NAME);
    when(store.isHybrid()).thenReturn(false);
    when(store.getHybridStoreConfig()).thenReturn(null);
    when(store.isActiveActiveReplicationEnabled()).thenReturn(false);
    when(store.isIncrementalPushEnabled()).thenReturn(false);
    when(store.isSystemStore()).thenReturn(false);
    when(store.getLargestUsedRTVersionNumber()).thenReturn(0);
    return store;
  }

  private static Store newHybridStore(HybridStoreConfig hybridConfig) {
    Store store = mock(Store.class);
    when(store.getName()).thenReturn(STORE_NAME);
    when(store.isHybrid()).thenReturn(true);
    when(store.getHybridStoreConfig()).thenReturn(hybridConfig);
    when(store.isActiveActiveReplicationEnabled()).thenReturn(false);
    when(store.isIncrementalPushEnabled()).thenReturn(false);
    when(store.isSystemStore()).thenReturn(false);
    when(store.getLargestUsedRTVersionNumber()).thenReturn(0);
    return store;
  }

  private static HybridStoreConfig newDefaultHybridConfig() {
    return new HybridStoreConfigImpl(
        100L,
        1000L,
        -1L,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP,
        DEFAULT_REAL_TIME_TOPIC_NAME);
  }

}
