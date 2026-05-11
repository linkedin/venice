package com.linkedin.davinci.store;

import com.linkedin.davinci.callback.BytesStreamingCallback;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link AbstractStorageEngine#restoreStoragePartitions(boolean, boolean, boolean)} covering the
 * drop-bad-partition-on-restore behavior.
 */
public class AbstractStorageEngineRestoreTest {
  private static final String STORE_VERSION_NAME = "test_store_v1";

  /**
   * Test stub that exposes {@link #restoreStoragePartitions(boolean, boolean, boolean)} and lets tests configure
   * which partitions are persisted on disk and which partition ids should fail to be created.
   */
  private static class TestStorageEngine extends AbstractStorageEngine<TestStoragePartition> {
    private final Set<Integer> persistedPartitionIds;
    private final Set<Integer> failingPartitionIds;
    private final java.util.List<Integer> dropOrder = new java.util.ArrayList<>();
    private final java.util.List<Integer> clearOffsetOrder = new java.util.ArrayList<>();
    private final Set<Integer> partitionsWithClearOffsetFailure = new HashSet<>();
    private boolean shouldDropOverride = true;

    TestStorageEngine(Set<Integer> persistedPartitionIds, Set<Integer> failingPartitionIds) {
      super(
          STORE_VERSION_NAME,
          AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
          AvroProtocolDefinition.PARTITION_STATE.getSerializer());
      this.persistedPartitionIds = persistedPartitionIds;
      this.failingPartitionIds = failingPartitionIds;
    }

    @Override
    public PersistenceType getType() {
      return PersistenceType.IN_MEMORY;
    }

    @Override
    public Set<Integer> getPersistedPartitionIds() {
      return new HashSet<>(persistedPartitionIds);
    }

    @Override
    public TestStoragePartition createStoragePartition(StoragePartitionConfig storagePartitionConfig) {
      int partitionId = storagePartitionConfig.getPartitionId();
      if (failingPartitionIds.contains(partitionId)) {
        throw new VeniceException("Simulated failure to open partition: " + partitionId);
      }
      return new TestStoragePartition(partitionId);
    }

    @Override
    public void clearPartitionOffset(int partitionId) {
      clearOffsetOrder.add(partitionId);
      if (partitionsWithClearOffsetFailure.contains(partitionId)) {
        throw new VeniceException("Simulated failure to clear offset for partition: " + partitionId);
      }
    }

    @Override
    protected void dropPartitionDirectory(int partitionId) {
      dropOrder.add(partitionId);
    }

    @Override
    protected boolean shouldDropPartitionOnRestoreFailure(int partitionId, Throwable cause) {
      return shouldDropOverride;
    }

    void setShouldDropOverride(boolean shouldDropOverride) {
      this.shouldDropOverride = shouldDropOverride;
    }

    void failClearOffsetFor(int partitionId) {
      partitionsWithClearOffsetFailure.add(partitionId);
    }

    void invokeRestoreStoragePartitions(boolean dropBadPartitionEnabled) {
      restoreStoragePartitions(false, true, dropBadPartitionEnabled);
    }

    void invokeRestoreStoragePartitions(
        boolean restoreMetadataPartition,
        boolean restoreDataPartitions,
        boolean dropBadPartitionEnabled) {
      restoreStoragePartitions(restoreMetadataPartition, restoreDataPartitions, dropBadPartitionEnabled);
    }

    Set<Integer> getDroppedPartitionDirectories() {
      return new HashSet<>(dropOrder);
    }

    java.util.List<Integer> getDropOrder() {
      return dropOrder;
    }

    java.util.List<Integer> getClearOffsetOrder() {
      return clearOffsetOrder;
    }
  }

  private static class TestStoragePartition extends AbstractStoragePartition {
    TestStoragePartition(int partitionId) {
      super(partitionId);
    }

    @Override
    public void put(byte[] key, byte[] value) {
    }

    @Override
    public void put(byte[] key, ByteBuffer value) {
    }

    @Override
    public <K, V> void put(K key, V value) {
    }

    @Override
    public byte[] get(byte[] key) {
      return null;
    }

    @Override
    public <K, V> V get(K key) {
      return null;
    }

    @Override
    public byte[] get(ByteBuffer key) {
      return null;
    }

    @Override
    public void getByKeyPrefix(byte[] keyPrefix, BytesStreamingCallback callback) {
    }

    @Override
    public void delete(byte[] key) {
    }

    @Override
    public Map<String, String> sync() {
      return Collections.emptyMap();
    }

    @Override
    public void drop() {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean verifyConfig(StoragePartitionConfig storagePartitionConfig) {
      return true;
    }

    @Override
    public void createSnapshot() {
    }

    @Override
    public void cleanupSnapshot() {
    }

    @Override
    public long getPartitionSizeInBytes() {
      return 0;
    }
  }

  @Test
  public void testRestoreFailsFastWhenDropBadPartitionDisabled() {
    Set<Integer> persisted = new HashSet<>();
    persisted.add(0);
    persisted.add(1);
    persisted.add(2);
    Set<Integer> failing = new HashSet<>();
    failing.add(1);
    TestStorageEngine engine = new TestStorageEngine(persisted, failing);

    Assert.assertThrows(VeniceException.class, () -> engine.invokeRestoreStoragePartitions(false));
    Assert.assertTrue(
        engine.getDroppedPartitionDirectories().isEmpty(),
        "No partition directory should be dropped when the flag is disabled.");
  }

  @Test
  public void testRestoreDropsBadPartitionWhenEnabled() {
    Set<Integer> persisted = new HashSet<>();
    persisted.add(0);
    persisted.add(1);
    persisted.add(2);
    Set<Integer> failing = new HashSet<>();
    failing.add(1);
    TestStorageEngine engine = new TestStorageEngine(persisted, failing);

    engine.invokeRestoreStoragePartitions(true);

    Assert.assertTrue(engine.containsPartition(0), "Partition 0 should be restored.");
    Assert.assertFalse(engine.containsPartition(1), "Failed partition 1 should be skipped.");
    Assert.assertTrue(engine.containsPartition(2), "Partition 2 should be restored.");
    Assert.assertEquals(
        engine.getDroppedPartitionDirectories(),
        Collections.singleton(1),
        "Only the failed partition's directory should be dropped.");
  }

  @Test
  public void testMetadataPartitionFailureAlwaysPropagates() {
    Set<Integer> persisted = new HashSet<>();
    persisted.add(0);
    persisted.add(1);
    Set<Integer> failing = new HashSet<>();
    failing.add(AbstractStorageEngine.METADATA_PARTITION_ID);
    TestStorageEngine engine = new TestStorageEngine(persisted, failing);

    Assert.assertThrows(VeniceException.class, () -> engine.invokeRestoreStoragePartitions(true, true, true));
    Assert.assertTrue(
        engine.getDroppedPartitionDirectories().isEmpty(),
        "Metadata partition failure must propagate without dropping anything, even when the flag is on.");
  }

  @Test
  public void testRestoreRethrowsWhenShouldDropPredicateReturnsFalse() {
    Set<Integer> persisted = new HashSet<>();
    persisted.add(0);
    persisted.add(1);
    Set<Integer> failing = new HashSet<>();
    failing.add(1);
    TestStorageEngine engine = new TestStorageEngine(persisted, failing);
    engine.setShouldDropOverride(false);

    Assert.assertThrows(VeniceException.class, () -> engine.invokeRestoreStoragePartitions(true));
    Assert.assertTrue(
        engine.getDroppedPartitionDirectories().isEmpty(),
        "When the subclass classifies the cause as not-drop-eligible, the failure must propagate.");
  }

  @Test
  public void testRestoreClearsOffsetBeforeDroppingDirectory() {
    Set<Integer> persisted = new HashSet<>();
    persisted.add(0);
    persisted.add(1);
    Set<Integer> failing = new HashSet<>();
    failing.add(1);
    TestStorageEngine engine = new TestStorageEngine(persisted, failing);

    engine.invokeRestoreStoragePartitions(true);

    Assert.assertEquals(
        engine.getClearOffsetOrder(),
        Collections.singletonList(1),
        "Stale offset record must be cleared for the dropped partition.");
    Assert.assertEquals(
        engine.getDropOrder(),
        Collections.singletonList(1),
        "On-disk directory must be dropped after clearing the offset.");
  }

  @Test
  public void testRestoreAbortsDropWhenClearOffsetFails() {
    Set<Integer> persisted = new HashSet<>();
    persisted.add(0);
    persisted.add(1);
    Set<Integer> failing = new HashSet<>();
    failing.add(1);
    TestStorageEngine engine = new TestStorageEngine(persisted, failing);
    engine.failClearOffsetFor(1);

    Assert.assertThrows(VeniceException.class, () -> engine.invokeRestoreStoragePartitions(true));
    Assert.assertEquals(
        engine.getClearOffsetOrder(),
        Collections.singletonList(1),
        "clearPartitionOffset must be attempted before the drop.");
    Assert.assertTrue(
        engine.getDropOrder().isEmpty(),
        "On-disk directory must NOT be dropped when clearing the offset fails - otherwise re-bootstrap would "
            + "resume from a stale checkpoint.");
  }

  @Test
  public void testRestoreDropsMultipleBadPartitionsWhenEnabled() {
    Set<Integer> persisted = new HashSet<>();
    persisted.add(0);
    persisted.add(1);
    persisted.add(2);
    persisted.add(3);
    Set<Integer> failing = new HashSet<>();
    failing.add(1);
    failing.add(3);
    TestStorageEngine engine = new TestStorageEngine(persisted, failing);

    engine.invokeRestoreStoragePartitions(true);

    Assert.assertTrue(engine.containsPartition(0));
    Assert.assertFalse(engine.containsPartition(1));
    Assert.assertTrue(engine.containsPartition(2));
    Assert.assertFalse(engine.containsPartition(3));
    Set<Integer> expected = new HashSet<>();
    expected.add(1);
    expected.add(3);
    Assert.assertEquals(engine.getDroppedPartitionDirectories(), expected);
  }
}
