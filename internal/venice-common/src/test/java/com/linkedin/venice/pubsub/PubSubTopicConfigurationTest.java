package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;

import java.util.Optional;
import org.testng.annotations.Test;


public class PubSubTopicConfigurationTest {
  @Test
  public void testPubSubTopicConfiguration() throws CloneNotSupportedException {
    // Case 1: Verify construction and getters
    Optional<Long> retentionInMs = Optional.of(3600000L);
    boolean isLogCompacted = true;
    Optional<Integer> minInSyncReplicas = Optional.of(2);
    Long minLogCompactionLagMs = 60000L;
    Optional<Long> maxLogCompactionLagMs = Optional.of(120000L);

    PubSubTopicConfiguration config = new PubSubTopicConfiguration(
        retentionInMs,
        isLogCompacted,
        minInSyncReplicas,
        minLogCompactionLagMs,
        maxLogCompactionLagMs);

    assertEquals(config.retentionInMs(), retentionInMs, "Retention in ms should match the provided value.");
    assertEquals(config.isLogCompacted(), isLogCompacted, "Log compaction flag should match the provided value.");
    assertEquals(
        config.minInSyncReplicas(),
        minInSyncReplicas,
        "Min in-sync replicas should match the provided value.");
    assertEquals(
        config.minLogCompactionLagMs(),
        minLogCompactionLagMs,
        "Min log compaction lag ms should match the provided value.");
    assertEquals(
        config.getMaxLogCompactionLagMs(),
        maxLogCompactionLagMs,
        "Max log compaction lag ms should match the provided value.");

    // Case 2: Verify setters
    config.setLogCompacted(false);
    config.setRetentionInMs(Optional.of(7200000L));
    config.setMinInSyncReplicas(Optional.of(3));
    config.setMinLogCompactionLagMs(120000L);
    config.setMaxLogCompactionLagMs(Optional.of(180000L));

    assertFalse(config.isLogCompacted(), "Log compaction flag should be updated.");
    assertEquals(config.retentionInMs(), Optional.of(7200000L), "Retention in ms should be updated.");
    assertEquals(config.minInSyncReplicas(), Optional.of(3), "Min in-sync replicas should be updated.");
    assertEquals(config.minLogCompactionLagMs(), Long.valueOf(120000L), "Min log compaction lag ms should be updated.");
    assertEquals(
        config.getMaxLogCompactionLagMs(),
        Optional.of(180000L),
        "Max log compaction lag ms should be updated.");

    // Case 3: Verify cloning
    PubSubTopicConfiguration clonedConfig = config.clone();

    assertNotSame(clonedConfig, config, "Cloned object should not be the same as the original.");
    assertEquals(
        clonedConfig.retentionInMs(),
        config.retentionInMs(),
        "Retention in ms should be identical in the cloned object.");
    assertEquals(
        clonedConfig.isLogCompacted(),
        config.isLogCompacted(),
        "Log compaction flag should be identical in the cloned object.");
    assertEquals(
        clonedConfig.minInSyncReplicas(),
        config.minInSyncReplicas(),
        "Min in-sync replicas should be identical in the cloned object.");
    assertEquals(
        clonedConfig.minLogCompactionLagMs(),
        config.minLogCompactionLagMs(),
        "Min log compaction lag ms should be identical in the cloned object.");
    assertEquals(
        clonedConfig.getMaxLogCompactionLagMs(),
        config.getMaxLogCompactionLagMs(),
        "Max log compaction lag ms should be identical in the cloned object.");

    clonedConfig.setLogCompacted(true);
    clonedConfig.setRetentionInMs(Optional.of(14400000L));
    clonedConfig.setMinInSyncReplicas(Optional.of(4));
    clonedConfig.setMinLogCompactionLagMs(180000L);

    assertNotEquals(
        config.isLogCompacted(),
        clonedConfig.isLogCompacted(),
        "Log compaction flag should be different in the cloned object.");
    assertNotEquals(
        config.retentionInMs(),
        clonedConfig.retentionInMs(),
        "Retention in ms should be different in the cloned object.");
    assertNotEquals(
        config.minInSyncReplicas(),
        clonedConfig.minInSyncReplicas(),
        "Min in-sync replicas should be different in the cloned object.");
    assertNotEquals(
        config.minLogCompactionLagMs(),
        clonedConfig.minLogCompactionLagMs(),
        "Min log compaction lag ms should be different in the cloned object.");
    assertEquals(
        config.getMaxLogCompactionLagMs(),
        clonedConfig.getMaxLogCompactionLagMs(),
        "Max log compaction lag ms should be identical in the cloned object.");

    // Case 4: Verify edge cases
    PubSubTopicConfiguration emptyConfig =
        new PubSubTopicConfiguration(Optional.empty(), false, Optional.empty(), null, Optional.empty());

    assertFalse(emptyConfig.retentionInMs().isPresent(), "Retention in ms should be empty.");
    assertFalse(emptyConfig.minInSyncReplicas().isPresent(), "Min in-sync replicas should be empty.");
    assertNull(emptyConfig.minLogCompactionLagMs(), "Min log compaction lag ms should be null.");
    assertFalse(emptyConfig.getMaxLogCompactionLagMs().isPresent(), "Max log compaction lag ms should be empty.");
  }
}
