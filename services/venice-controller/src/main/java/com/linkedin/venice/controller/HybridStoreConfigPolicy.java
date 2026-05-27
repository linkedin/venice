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
import static com.linkedin.venice.meta.Version.DEFAULT_RT_VERSION_NUMBER;

import com.linkedin.venice.controller.kafka.protocol.admin.HybridStoreConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.Logger;


/**
 * Encodes Venice's policy for hybrid + replication configuration on a store: when a store
 * counts as hybrid, how user-supplied update options merge onto the existing config, and the
 * auto-enable / auto-disable rules at hybrid <-> batch transitions. Pure computation against
 * in-memory state; no I/O.
 */
final class HybridStoreConfigPolicy {
  private HybridStoreConfigPolicy() {
  }

  /**
   * A store is hybrid iff its {@link HybridStoreConfig} is non-null and at least one of the
   * rewind / offset-lag / time-lag thresholds is non-negative. A negative threshold sentinel is
   * how a store is set back to batch-only.
   */
  static boolean isHybrid(HybridStoreConfig hybridStoreConfig) {
    return hybridStoreConfig != null
        && (hybridStoreConfig.getRewindTimeInSeconds() >= 0 || hybridStoreConfig.getOffsetLagThresholdToGoOnline() >= 0
            || hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds() >= 0);
  }

  /** @see #isHybrid(HybridStoreConfig) */
  static boolean isHybrid(HybridStoreConfigRecord hybridStoreConfigRecord) {
    HybridStoreConfig hybridStoreConfig = null;
    if (hybridStoreConfigRecord != null) {
      // realTimeTopicName may be null on records produced before the field was added (PR #1345);
      // HybridStoreConfigImpl normalizes a null name to its default, so just guard the .toString().
      hybridStoreConfig = new HybridStoreConfigImpl(
          hybridStoreConfigRecord.rewindTimeInSeconds,
          hybridStoreConfigRecord.offsetLagThresholdToGoOnline,
          hybridStoreConfigRecord.producerTimestampLagThresholdToGoOnlineInSeconds,
          DataReplicationPolicy.valueOf(hybridStoreConfigRecord.dataReplicationPolicy),
          BufferReplayPolicy.valueOf(hybridStoreConfigRecord.bufferReplayPolicy),
          hybridStoreConfigRecord.realTimeTopicName == null
              ? null
              : hybridStoreConfigRecord.realTimeTopicName.toString());
    }
    return isHybrid(hybridStoreConfig);
  }

  /**
   * Merge user-supplied hybrid options on top of {@code oldStore}'s current hybrid config.
   *
   * <p>Returns null when {@code oldStore} is currently batch-only and no hybrid option has been
   * specified (i.e. the caller is not converting the store to hybrid). When converting a
   * batch-only store to hybrid, the rewind time plus at least one of the lag thresholds must be
   * specified, otherwise {@link VeniceException} is thrown.
   */
  static HybridStoreConfig mergeNewSettingsIntoOldHybridStoreConfig(
      Store oldStore,
      Optional<Long> hybridRewindSeconds,
      Optional<Long> hybridOffsetLagThreshold,
      Optional<Long> hybridTimeLagThreshold,
      Optional<DataReplicationPolicy> hybridDataReplicationPolicy,
      Optional<BufferReplayPolicy> bufferReplayPolicy,
      Optional<String> realTimeTopicName) {
    if (!hybridRewindSeconds.isPresent() && !hybridOffsetLagThreshold.isPresent() && !oldStore.isHybrid()) {
      return null; // For the nullable union in the avro record
    }
    HybridStoreConfig mergedHybridStoreConfig;
    if (oldStore.isHybrid()) { // for an existing hybrid store, just replace any specified values
      HybridStoreConfig oldHybridConfig = oldStore.getHybridStoreConfig().clone();
      mergedHybridStoreConfig = new HybridStoreConfigImpl(
          hybridRewindSeconds.isPresent() ? hybridRewindSeconds.get() : oldHybridConfig.getRewindTimeInSeconds(),
          hybridOffsetLagThreshold.isPresent()
              ? hybridOffsetLagThreshold.get()
              : oldHybridConfig.getOffsetLagThresholdToGoOnline(),
          hybridTimeLagThreshold.isPresent()
              ? hybridTimeLagThreshold.get()
              : oldHybridConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds(),
          hybridDataReplicationPolicy.isPresent()
              ? hybridDataReplicationPolicy.get()
              : oldHybridConfig.getDataReplicationPolicy(),
          bufferReplayPolicy.isPresent() ? bufferReplayPolicy.get() : oldHybridConfig.getBufferReplayPolicy(),
          realTimeTopicName.orElseGet(oldHybridConfig::getRealTimeTopicName));
    } else {
      // switching a non-hybrid store to hybrid; must specify:
      // 1. rewind time
      // 2. either offset lag threshold or time lag threshold, or both
      if (!(hybridRewindSeconds.isPresent()
          && (hybridOffsetLagThreshold.isPresent() || hybridTimeLagThreshold.isPresent()))) {
        throw new VeniceException(
            oldStore.getName() + " was not a hybrid store.  In order to make it a hybrid store both "
                + " rewind time in seconds and offset or time lag threshold must be specified");
      }

      String newRealTimeTopicName = oldStore.getLargestUsedRTVersionNumber() > DEFAULT_RT_VERSION_NUMBER
          && Utils.isRTVersioningApplicable(oldStore.getName())
              ? Utils.composeRealTimeTopic(oldStore.getName(), oldStore.getLargestUsedRTVersionNumber())
              : DEFAULT_REAL_TIME_TOPIC_NAME;

      mergedHybridStoreConfig = new HybridStoreConfigImpl(
          hybridRewindSeconds.get(),
          // If not specified, offset/time lag threshold will be -1 and will not be used to determine whether
          // a partition is ready to serve
          hybridOffsetLagThreshold.orElse(DEFAULT_HYBRID_OFFSET_LAG_THRESHOLD),
          hybridTimeLagThreshold.orElse(DEFAULT_HYBRID_TIME_LAG_THRESHOLD),
          hybridDataReplicationPolicy.orElse(DataReplicationPolicy.NON_AGGREGATE),
          bufferReplayPolicy.orElse(BufferReplayPolicy.REWIND_FROM_EOP),
          realTimeTopicName.orElse(newRealTimeTopicName));
    }
    if (mergedHybridStoreConfig.getRewindTimeInSeconds() > 0
        && mergedHybridStoreConfig.getOffsetLagThresholdToGoOnline() < 0
        && mergedHybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds() < 0) {
      throw new VeniceException(
          "Both offset lag threshold and time lag threshold are negative when setting hybrid" + " configs for store "
              + oldStore.getName());
    }
    return mergedHybridStoreConfig;
  }

  /**
   * Apply user-supplied hybrid + replication updates to {@code setStore}, return whether the
   * store is being converted from batch to hybrid. Mutates {@code setStore} and
   * {@code updatedConfigsList}; performs no I/O.
   */
  static boolean applyHybridAndReplicationConfigUpdates(
      String storeName,
      Store currStore,
      UpdateStore setStore,
      VeniceControllerClusterConfig controllerConfig,
      Optional<Long> hybridRewindSeconds,
      Optional<Long> hybridOffsetLagThreshold,
      Optional<Long> hybridTimeLagThreshold,
      Optional<DataReplicationPolicy> hybridDataReplicationPolicy,
      Optional<BufferReplayPolicy> hybridBufferReplayPolicy,
      Optional<String> realTimeTopicName,
      Optional<Boolean> activeActiveReplicationEnabled,
      Optional<Boolean> incrementalPushEnabled,
      List<CharSequence> updatedConfigsList,
      Logger logger) {
    HybridStoreConfig updatedHybridStoreConfig = resolveUpdatedHybridConfig(
        currStore,
        hybridRewindSeconds,
        hybridOffsetLagThreshold,
        hybridTimeLagThreshold,
        hybridDataReplicationPolicy,
        hybridBufferReplayPolicy,
        realTimeTopicName,
        updatedConfigsList);

    resolveReplicationPolicyToggles(
        currStore,
        setStore,
        controllerConfig,
        updatedHybridStoreConfig,
        activeActiveReplicationEnabled,
        incrementalPushEnabled,
        updatedConfigsList);

    if (updatedHybridStoreConfig == null) {
      setStore.hybridStoreConfig = null;
    } else {
      setStore.hybridStoreConfig = toRecord(updatedHybridStoreConfig);
    }

    maybeConvertBatchStoreToActiveActiveHybridForIncrementalPush(
        storeName,
        currStore,
        setStore,
        controllerConfig,
        updatedHybridStoreConfig,
        incrementalPushEnabled,
        updatedConfigsList,
        logger);

    // Derive from the final setStore so the implicit batch->hybrid conversion above is reflected.
    // Computing this earlier (before maybeConvertBatchStoreToActiveActiveHybridForIncrementalPush) would
    // return false when enabling incremental push on a batch store, even though setStore.hybridStoreConfig
    // has been synthesized into a hybrid record - causing the downstream partial-update auto-enable gate in
    // ParentControllerConfigUpdateUtils.checkAndMaybeApplyPartialUpdateConfig to be skipped incorrectly.
    return !currStore.isHybrid() && isHybrid(setStore.hybridStoreConfig);
  }

  /**
   * Merge user-supplied hybrid options on top of the current store's hybrid config, track which
   * config keys were touched in {@code updatedConfigsList}.
   */
  private static HybridStoreConfig resolveUpdatedHybridConfig(
      Store currStore,
      Optional<Long> hybridRewindSeconds,
      Optional<Long> hybridOffsetLagThreshold,
      Optional<Long> hybridTimeLagThreshold,
      Optional<DataReplicationPolicy> hybridDataReplicationPolicy,
      Optional<BufferReplayPolicy> hybridBufferReplayPolicy,
      Optional<String> realTimeTopicName,
      List<CharSequence> updatedConfigsList) {
    hybridRewindSeconds.map(addToUpdatedConfigList(updatedConfigsList, REWIND_TIME_IN_SECONDS));
    hybridOffsetLagThreshold.map(addToUpdatedConfigList(updatedConfigsList, OFFSET_LAG_TO_GO_ONLINE));
    hybridTimeLagThreshold.map(addToUpdatedConfigList(updatedConfigsList, TIME_LAG_TO_GO_ONLINE));
    hybridDataReplicationPolicy.map(addToUpdatedConfigList(updatedConfigsList, DATA_REPLICATION_POLICY));
    hybridBufferReplayPolicy.map(addToUpdatedConfigList(updatedConfigsList, BUFFER_REPLAY_POLICY));
    realTimeTopicName.map(addToUpdatedConfigList(updatedConfigsList, REAL_TIME_TOPIC_NAME));

    return mergeNewSettingsIntoOldHybridStoreConfig(
        currStore,
        hybridRewindSeconds,
        hybridOffsetLagThreshold,
        hybridTimeLagThreshold,
        hybridDataReplicationPolicy,
        hybridBufferReplayPolicy,
        realTimeTopicName);
  }

  /**
   * Resolve the A/A, incremental-push, and separate-RT toggles on {@code setStore} based on
   * user input plus auto-enable/auto-disable rules at hybrid/batch transitions.
   */
  private static void resolveReplicationPolicyToggles(
      Store currStore,
      UpdateStore setStore,
      VeniceControllerClusterConfig controllerConfig,
      HybridStoreConfig updatedHybridStoreConfig,
      Optional<Boolean> activeActiveReplicationEnabled,
      Optional<Boolean> incrementalPushEnabled,
      List<CharSequence> updatedConfigsList) {
    ReplicationToggleDecision decision = computeReplicationToggleDecision(
        currStore,
        controllerConfig,
        updatedHybridStoreConfig,
        activeActiveReplicationEnabled,
        incrementalPushEnabled);
    setStore.activeActiveReplicationEnabled = decision.activeActiveReplicationEnabled;
    setStore.incrementalPushEnabled = decision.incrementalPushEnabled;
    if (decision.enableSeparateRealTimeTopic) {
      setStore.separateRealTimeTopicEnabled = true;
    }
    updatedConfigsList.addAll(decision.updatedConfigs);
  }

  /**
   * Resolved A/A and incremental-push toggles plus the should-enable signal for separate-RT, paired
   * with the config keys that were touched while computing them. Returned by
   * {@link #computeReplicationToggleDecision} so the impure apply step can stay trivial.
   */
  static final class ReplicationToggleDecision {
    final boolean activeActiveReplicationEnabled;
    final boolean incrementalPushEnabled;
    final boolean enableSeparateRealTimeTopic;
    final List<CharSequence> updatedConfigs;

    ReplicationToggleDecision(
        boolean activeActiveReplicationEnabled,
        boolean incrementalPushEnabled,
        boolean enableSeparateRealTimeTopic,
        List<CharSequence> updatedConfigs) {
      this.activeActiveReplicationEnabled = activeActiveReplicationEnabled;
      this.incrementalPushEnabled = incrementalPushEnabled;
      this.enableSeparateRealTimeTopic = enableSeparateRealTimeTopic;
      this.updatedConfigs = updatedConfigs;
    }
  }

  static ReplicationToggleDecision computeReplicationToggleDecision(
      Store currStore,
      VeniceControllerClusterConfig controllerConfig,
      HybridStoreConfig updatedHybridStoreConfig,
      Optional<Boolean> activeActiveReplicationEnabled,
      Optional<Boolean> incrementalPushEnabled) {
    List<CharSequence> updatedConfigs = new ArrayList<>();
    boolean storeBeingConvertedToHybrid =
        !currStore.isHybrid() && updatedHybridStoreConfig != null && isHybrid(updatedHybridStoreConfig);
    boolean storeBeingConvertedToBatch = currStore.isHybrid() && !isHybrid(updatedHybridStoreConfig);
    if (storeBeingConvertedToBatch && activeActiveReplicationEnabled.orElse(false)) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Cannot convert store to batch-only and enable Active/Active together.",
          ErrorType.BAD_REQUEST);
    }
    if (storeBeingConvertedToBatch && incrementalPushEnabled.orElse(false)) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Cannot convert store to batch-only and enable incremental push together.",
          ErrorType.BAD_REQUEST);
    }

    boolean resolvedActiveActiveReplicationEnabled =
        activeActiveReplicationEnabled.map(addToUpdatedConfigList(updatedConfigs, ACTIVE_ACTIVE_REPLICATION_ENABLED))
            .orElseGet(currStore::isActiveActiveReplicationEnabled);
    if (storeBeingConvertedToHybrid && !resolvedActiveActiveReplicationEnabled && !currStore.isSystemStore()
        && controllerConfig.isActiveActiveReplicationEnabledAsDefaultForHybrid()) {
      resolvedActiveActiveReplicationEnabled = true;
      updatedConfigs.add(ACTIVE_ACTIVE_REPLICATION_ENABLED);
    }
    if (storeBeingConvertedToBatch && resolvedActiveActiveReplicationEnabled) {
      resolvedActiveActiveReplicationEnabled = false;
      updatedConfigs.add(ACTIVE_ACTIVE_REPLICATION_ENABLED);
    }

    boolean resolvedIncrementalPushEnabled =
        incrementalPushEnabled.map(addToUpdatedConfigList(updatedConfigs, INCREMENTAL_PUSH_ENABLED))
            .orElseGet(currStore::isIncrementalPushEnabled);
    // Note: reads the resolved (post-auto-enable) A/A value, not the user input, so an
    // auto-enabled A/A on batch->hybrid can also unlock the auto-enable of incremental push.
    if (!resolvedIncrementalPushEnabled && !currStore.isSystemStore() && storeBeingConvertedToHybrid
        && resolvedActiveActiveReplicationEnabled
        && controllerConfig.enabledIncrementalPushForHybridActiveActiveUserStores()) {
      resolvedIncrementalPushEnabled = true;
      updatedConfigs.add(INCREMENTAL_PUSH_ENABLED);
    }

    boolean enableSeparateRealTimeTopic = false;
    if (resolvedIncrementalPushEnabled && controllerConfig.enabledSeparateRealTimeTopicForStoreWithIncrementalPush()) {
      enableSeparateRealTimeTopic = true;
      updatedConfigs.add(SEPARATE_REAL_TIME_TOPIC_ENABLED);
    }

    if (storeBeingConvertedToBatch && resolvedIncrementalPushEnabled) {
      resolvedIncrementalPushEnabled = false;
      updatedConfigs.add(INCREMENTAL_PUSH_ENABLED);
    }

    return new ReplicationToggleDecision(
        resolvedActiveActiveReplicationEnabled,
        resolvedIncrementalPushEnabled,
        enableSeparateRealTimeTopic,
        updatedConfigs);
  }

  /**
   * Enabling incremental push on a non-hybrid batch store implicitly converts it into an
   * Active/Active hybrid store using default hybrid settings.
   */
  private static void maybeConvertBatchStoreToActiveActiveHybridForIncrementalPush(
      String storeName,
      Store currStore,
      UpdateStore setStore,
      VeniceControllerClusterConfig controllerConfig,
      HybridStoreConfig updatedHybridStoreConfig,
      Optional<Boolean> incrementalPushEnabled,
      List<CharSequence> updatedConfigsList,
      Logger logger) {
    if (!incrementalPushEnabled.orElseGet(currStore::isIncrementalPushEnabled)
        || isHybrid(currStore.getHybridStoreConfig()) || isHybrid(updatedHybridStoreConfig)) {
      return;
    }
    logger.info(
        "Enabling incremental push for a batch store:{}. Converting it to Active/Active hybrid store with default configs.",
        storeName);
    HybridStoreConfigRecord hybridStoreConfigRecord = new HybridStoreConfigRecord();
    hybridStoreConfigRecord.rewindTimeInSeconds = DEFAULT_REWIND_TIME_IN_SECONDS;
    updatedConfigsList.add(REWIND_TIME_IN_SECONDS);
    hybridStoreConfigRecord.offsetLagThresholdToGoOnline = DEFAULT_HYBRID_OFFSET_LAG_THRESHOLD;
    updatedConfigsList.add(OFFSET_LAG_TO_GO_ONLINE);
    hybridStoreConfigRecord.producerTimestampLagThresholdToGoOnlineInSeconds = DEFAULT_HYBRID_TIME_LAG_THRESHOLD;
    updatedConfigsList.add(TIME_LAG_TO_GO_ONLINE);
    hybridStoreConfigRecord.dataReplicationPolicy = DataReplicationPolicy.NON_AGGREGATE.getValue();
    updatedConfigsList.add(DATA_REPLICATION_POLICY);
    hybridStoreConfigRecord.bufferReplayPolicy = BufferReplayPolicy.REWIND_FROM_EOP.getValue();
    updatedConfigsList.add(BUFFER_REPLAY_POLICY);
    hybridStoreConfigRecord.realTimeTopicName = DEFAULT_REAL_TIME_TOPIC_NAME;
    setStore.hybridStoreConfig = hybridStoreConfigRecord;
    if (!currStore.isSystemStore() && controllerConfig.isActiveActiveReplicationEnabledAsDefaultForHybrid()) {
      setStore.activeActiveReplicationEnabled = true;
      updatedConfigsList.add(ACTIVE_ACTIVE_REPLICATION_ENABLED);
    }
  }

  private static HybridStoreConfigRecord toRecord(HybridStoreConfig hybridStoreConfig) {
    HybridStoreConfigRecord hybridStoreConfigRecord = new HybridStoreConfigRecord();
    hybridStoreConfigRecord.offsetLagThresholdToGoOnline = hybridStoreConfig.getOffsetLagThresholdToGoOnline();
    hybridStoreConfigRecord.rewindTimeInSeconds = hybridStoreConfig.getRewindTimeInSeconds();
    hybridStoreConfigRecord.producerTimestampLagThresholdToGoOnlineInSeconds =
        hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds();
    hybridStoreConfigRecord.dataReplicationPolicy = hybridStoreConfig.getDataReplicationPolicy().getValue();
    hybridStoreConfigRecord.bufferReplayPolicy = hybridStoreConfig.getBufferReplayPolicy().getValue();
    hybridStoreConfigRecord.realTimeTopicName = hybridStoreConfig.getRealTimeTopicName();
    return hybridStoreConfigRecord;
  }

  private static <T> Function<T, T> addToUpdatedConfigList(List<CharSequence> updatedConfigList, String config) {
    return configValue -> {
      updatedConfigList.add(config);
      return configValue;
    };
  }
}
