package com.linkedin.venice.controller.storeconfig;

import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.Store;
import java.util.Map;
import java.util.Optional;


/**
 * Encodes how user-supplied partitioner options merge onto a store's existing
 * {@link PartitionerConfig}. Pure computation against in-memory state; no I/O.
 */
public final class PartitionerConfigPolicy {
  private PartitionerConfigPolicy() {
  }

  /**
   * Merge any user-supplied partitioner fields on top of {@code oldStore}'s current partitioner
   * config, falling back to the existing value for unspecified fields. A store without a
   * partitioner config is treated as if it had a default {@link PartitionerConfigImpl}.
   */
  public static PartitionerConfig mergeNewSettingsIntoOldPartitionerConfig(
      Store oldStore,
      Optional<String> partitionerClass,
      Optional<Map<String, String>> partitionerParams,
      Optional<Integer> amplificationFactor) {
    PartitionerConfig originalPartitionerConfig;
    if (oldStore.getPartitionerConfig() == null) {
      originalPartitionerConfig = new PartitionerConfigImpl();
    } else {
      originalPartitionerConfig = oldStore.getPartitionerConfig();
    }
    return new PartitionerConfigImpl(
        partitionerClass.orElseGet(originalPartitionerConfig::getPartitionerClass),
        partitionerParams.orElseGet(originalPartitionerConfig::getPartitionerParams),
        amplificationFactor.orElseGet(originalPartitionerConfig::getAmplificationFactor));
  }
}
