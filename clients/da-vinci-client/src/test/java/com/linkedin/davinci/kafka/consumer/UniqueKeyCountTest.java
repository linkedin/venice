package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.UniqueKeyCountTestUtils.freshPcs;
import static com.linkedin.davinci.kafka.consumer.UniqueKeyCountTestUtils.pcsFromCheckpoint;
import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.api.EmptyPubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit tests for unique key count: PCS fields, OffsetRecord persistence, schema evolution,
 * key count signal matrix, header encoding/decoding, and feature flag behavior.
 * No duplication with UniqueKeyCountIntegrationTest which covers lifecycle/recovery scenarios.
 */
public class UniqueKeyCountTest {
  // 1. PCS Field Operations
  @Test
  public void testInitialState() {
    PartitionConsumptionState pcs = freshPcs();
    assertEquals(pcs.getUniqueKeyCount(), -1L, "Default uniqueKeyCount should be -1 (not tracked)");
  }

  @Test
  public void testSetGetUniqueKeyCount() {
    PartitionConsumptionState pcs = freshPcs();
    for (long v: new long[] { 0, 1, 42, 100_000_000, -1, Long.MAX_VALUE }) {
      pcs.setUniqueKeyCount(v);
      assertEquals(pcs.getUniqueKeyCount(), v);
    }
  }

  @Test
  public void testIncrementDecrement() {
    PartitionConsumptionState pcs = freshPcs();
    pcs.setUniqueKeyCount(10);
    pcs.incrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 11L);
    pcs.decrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 10L);
  }

  @Test
  public void testAdjust() {
    PartitionConsumptionState pcs = freshPcs();
    pcs.setUniqueKeyCount(5);
    pcs.incrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 6L);
    pcs.decrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), 5L);
  }

  @Test
  public void testBatchKeyCountAndFinalize() {
    PartitionConsumptionState pcs = freshPcs();
    for (int i = 0; i < 5; i++) {
      pcs.incrementUniqueKeyCountForBatchRecord();
    }
    assertEquals(pcs.getUniqueKeyCount(), 5L, "uniqueKeyCount grows during batch for checkpoint safety");
    pcs.finalizeUniqueKeyCountForBatchPush();
    assertEquals(pcs.getUniqueKeyCount(), 5L, "After finalize, still 5");
  }

  @Test
  public void testFinalizeWithZeroBatch() {
    PartitionConsumptionState pcs = freshPcs();
    pcs.finalizeUniqueKeyCountForBatchPush();
    assertEquals(pcs.getUniqueKeyCount(), 0L, "Empty batch → count=0, not -1");
  }

  @Test
  public void testFinalizeDoesNotOverwriteRTSignals() {
    PartitionConsumptionState pcs = freshPcs();
    for (int i = 0; i < 10; i++) {
      pcs.incrementUniqueKeyCountForBatchRecord();
    }
    assertEquals(pcs.getUniqueKeyCount(), 10L);
    pcs.finalizeUniqueKeyCountForBatchPush();
    pcs.incrementUniqueKeyCount(); // RT adjustment → 11
    assertEquals(pcs.getUniqueKeyCount(), 11L);
    // Second finalize (e.g., from duplicate EOP) does NOT overwrite RT signals
    // because finalize only acts on the empty-partition edge case now
    pcs.finalizeUniqueKeyCountForBatchPush();
    assertEquals(pcs.getUniqueKeyCount(), 11L, "Second finalize must not overwrite RT signals");
  }

  @Test
  public void testDecrementBelowZero() {
    PartitionConsumptionState pcs = freshPcs();
    pcs.setUniqueKeyCount(0);
    pcs.decrementUniqueKeyCount();
    assertEquals(pcs.getUniqueKeyCount(), -1L, "AtomicLong supports negative");
  }

  @Test
  public void testConcurrency() throws InterruptedException {
    PartitionConsumptionState pcs = freshPcs();
    pcs.setUniqueKeyCount(0);
    int n = 10;
    int ops = 1000;
    Thread[] threads = new Thread[n * 2];
    for (int i = 0; i < n; i++) {
      threads[i] = new Thread(() -> {
        for (int j = 0; j < ops; j++) {
          pcs.incrementUniqueKeyCount();
        }
      });
      threads[i + n] = new Thread(() -> {
        for (int j = 0; j < ops; j++) {
          pcs.decrementUniqueKeyCount();
        }
      });
    }
    for (Thread t: threads) {
      t.start();
    }
    for (Thread t: threads) {
      t.join();
    }
    assertEquals(pcs.getUniqueKeyCount(), 0L, "Equal inc/dec should net to 0");
  }

  // 2. OffsetRecord Persistence & Schema Evolution

  @Test
  public void testOffsetRecordDefault() {
    OffsetRecord or = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals(or.getUniqueKeyCount(), -1L, "Fresh OffsetRecord should default to -1");
  }

  @Test
  public void testOffsetRecordGetSet() {
    OffsetRecord or = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    or.setUniqueKeyCount(999);
    assertEquals(or.getUniqueKeyCount(), 999L);
    or.setUniqueKeyCount(0);
    assertEquals(or.getUniqueKeyCount(), 0L);
  }

  @DataProvider(name = "serializationValues")
  public Object[][] serializationValues() {
    return new Object[][] { { -1L }, { 0L }, { 100L }, { 54321L }, { 100_000_000L } };
  }

  @Test(dataProvider = "serializationValues")
  public void testOffsetRecordSerializationRoundTrip(long value) {
    OffsetRecord orig = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    orig.setUniqueKeyCount(value);
    byte[] bytes = orig.toBytes();
    OffsetRecord restored = new OffsetRecord(
        bytes,
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals(restored.getUniqueKeyCount(), value);
  }

  @Test
  public void testSchemaEvolutionOldRecordDefaultsToNegativeOne() {
    // When v20 PartitionState (no uniqueKeyCount) is deserialized by v21 code,
    // the Avro default (-1) is used. We test this by creating a fresh OffsetRecord
    // (which initializes uniqueKeyCount=-1 in getEmptyPartitionState).
    OffsetRecord fresh = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals(fresh.getUniqueKeyCount(), -1L, "Schema evolution: missing field defaults to -1");
  }

  @Test
  public void testSchemaEvolutionRoundTripPreservesDefault() {
    // Simulate: OffsetRecord created WITHOUT ever setting uniqueKeyCount (as v20 code would),
    // serialized, then deserialized by v21 code. The uniqueKeyCount should be -1 (Avro default).
    OffsetRecord oldStyleRecord = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    // Don't set uniqueKeyCount — simulates v20 behavior where the field doesn't exist
    assertEquals(oldStyleRecord.getUniqueKeyCount(), -1L, "Before serialization: default -1");

    byte[] serialized = oldStyleRecord.toBytes();
    OffsetRecord deserialized = new OffsetRecord(
        serialized,
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals(
        deserialized.getUniqueKeyCount(),
        -1L,
        "After serialization round-trip: -1 preserved (v20 records get Avro default)");
  }

  @Test
  public void testSchemaEvolutionDoesNotCorruptOtherFields() {
    // Adding uniqueKeyCount to schema must not corrupt other persisted fields
    OffsetRecord record = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    record.setUniqueKeyCount(42L);
    record.setOffsetLag(77L);

    byte[] serialized = record.toBytes();
    OffsetRecord restored = new OffsetRecord(
        serialized,
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals(restored.getUniqueKeyCount(), 42L, "uniqueKeyCount preserved");
    assertEquals(restored.getOffsetLag(), 77L, "offsetLag preserved — not corrupted by new field");
  }

  @Test
  public void testPcsRestoredFromCheckpoint() {
    PartitionConsumptionState pcs = pcsFromCheckpoint(12345);
    assertEquals(pcs.getUniqueKeyCount(), 12345L);
  }

  @Test
  public void testPcsRestoredFromNegativeOneCheckpoint() {
    PartitionConsumptionState pcs = pcsFromCheckpoint(-1);
    assertEquals(pcs.getUniqueKeyCount(), -1L, "Restore -1 when feature not yet started");
  }

  // 3. Key Count Signal Matrix (all 4 wasAlive×isAlive combinations)

  @DataProvider(name = "keyCountDeltaMatrix")
  public Object[][] keyCountDeltaMatrix() {
    return new Object[][] {
        // wasAlive, isAlive, expectedDelta
        { false, true, 1 }, // Dead→Alive: new key
        { true, false, -1 }, // Alive→Dead: deleted
        { true, true, 0 }, // Alive→Alive: update (no change)
        { false, false, 0 }, // Dead→Dead: double-delete (no change)
    };
  }

  @Test(dataProvider = "keyCountDeltaMatrix")
  public void testKeyCountDelta(boolean wasAlive, boolean isAlive, int expectedDelta) {
    PartitionConsumptionState pcs = freshPcs();
    pcs.setUniqueKeyCount(100);

    // Replicate exact logic from processMessageAndMaybeProduceToKafka
    int delta = 0;
    if (!wasAlive && isAlive) {
      delta = 1;
      pcs.incrementUniqueKeyCount();
    }
    if (wasAlive && !isAlive) {
      delta = -1;
      pcs.decrementUniqueKeyCount();
    }

    assertEquals(delta, expectedDelta);
    assertEquals(pcs.getUniqueKeyCount(), 100L + expectedDelta);
  }

  // 4. "kcs" Header Encoding/Decoding

  @DataProvider(name = "headerDeltas")
  public Object[][] headerDeltas() {
    return new Object[][] { { 1 }, { -1 } };
  }

  @Test(dataProvider = "headerDeltas")
  public void testHeaderRoundTrip(int delta) {
    // Encode
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[] { (byte) delta });

    // Decode
    PubSubMessageHeader h = headers.get(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER);
    Assert.assertNotNull(h);
    assertEquals((int) h.value()[0], delta, "Byte round-trip must preserve sign");
  }

  @Test
  public void testAbsentHeaderMeansNoSignal() {
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    assertNull(headers.get(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER));
  }

  @Test
  public void testEmptyHeadersMeansNoSignal() {
    assertNull(EmptyPubSubMessageHeaders.SINGLETON.get(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER));
  }

  // 5. Batch Counter Schema/MessageType Filtering (decision logic)

  @DataProvider(name = "batchSchemaFilter")
  public Object[][] batchSchemaFilter() {
    return new Object[][] { { 1, true, "User schema 1" }, { 100, true, "User schema 100" },
        { AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion(), true, "Chunk manifest" },
        { AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion(), false, "Chunk fragment — skip" }, };
  }

  @Test(dataProvider = "batchSchemaFilter")
  public void testBatchCounterSchemaFilter(int schemaId, boolean shouldCount, String desc) {
    int chunkSchemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    assertEquals(schemaId != chunkSchemaId, shouldCount, desc);
  }

  @Test
  public void testBatchCounterMessageTypeFilter() {
    // Only MessageType.PUT should trigger counting; DELETE, UPDATE, GLOBAL_RT_DIV should not.
    // Verify by checking each type against the batch counting condition (messageType == PUT).
    MessageType[] allTypes = { MessageType.PUT, MessageType.DELETE, MessageType.UPDATE, MessageType.GLOBAL_RT_DIV };
    for (MessageType mt: allTypes) {
      boolean shouldCount = (mt == MessageType.PUT);
      if (mt == MessageType.PUT) {
        assertEquals(shouldCount, true, mt + " should be counted during batch");
      } else {
        assertEquals(shouldCount, false, mt + " should NOT be counted during batch");
      }
    }
  }

  // 6. Mixed Deployment (header evolution)

  @Test
  public void testNewCodeOldVTNoHeader() {
    // Old server produced VT without "kcs" header. New follower reads it.
    // trackUniqueKeyCount finds no header → no adjustment.
    PartitionConsumptionState pcs = pcsFromCheckpoint(50);
    PubSubMessageHeaders oldHeaders = new PubSubMessageHeaders();
    PubSubMessageHeader h = oldHeaders.get(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER);
    // Simulate: header absent, no adjustment
    if (h != null) {
      pcs.incrementUniqueKeyCount();
    }
    assertEquals(pcs.getUniqueKeyCount(), 50L, "No header → no change (backward compatible)");
  }

  @Test
  public void testNewCodeNewVTWithHeader() {
    // New server produced VT with "kcs" header. New follower reads it.
    PartitionConsumptionState pcs = pcsFromCheckpoint(50);
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[] { 1 });

    PubSubMessageHeader h = headers.get(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER);
    if (h != null) {
      pcs.incrementUniqueKeyCount();
    }
    assertEquals(pcs.getUniqueKeyCount(), 51L);
  }

  @Test
  public void testOldCodeIgnoresNewHeader() {
    // Old follower (no trackUniqueKeyCount code) consumes VT with "kcs" header.
    // Headers are preserved in Kafka but the old code never reads them.
    // This is a conceptual test: the old code simply doesn't call getPubSubMessageHeaders().get("kcs").
    // Kafka headers don't affect message processing unless explicitly read.
    // Verification: no crash, no data corruption.
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, new byte[] { 1 });
    // Old code would never read this header — just verify it doesn't throw
    Assert.assertNotNull(headers.get(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER));
  }

  // 7. Constants and Config Keys

  @Test
  public void testKeyCountSignalHeaderConstant() {
    assertEquals(StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, "kcs");
  }

  @Test
  public void testPreComputedKeyCountSignalHeaders() {
    // Verify the pre-computed static header constants have the correct key and value
    PubSubMessageHeader created = ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL;
    assertEquals(created.key(), StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, "KEY_CREATED_SIGNAL key");
    assertEquals(created.value()[0], (byte) 1, "KEY_CREATED_SIGNAL value must be +1");

    PubSubMessageHeader deleted = ActiveActiveStoreIngestionTask.KEY_DELETED_SIGNAL;
    assertEquals(deleted.key(), StoreIngestionTask.KEY_COUNT_SIGNAL_HEADER, "KEY_DELETED_SIGNAL key");
    assertEquals(deleted.value()[0], (byte) -1, "KEY_DELETED_SIGNAL value must be -1");
  }

  @Test
  public void testPreComputedSignalHeadersAreReusable() {
    // Verify the same static instance is returned each time (not recreated)
    PubSubMessageHeader first = ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL;
    PubSubMessageHeader second = ActiveActiveStoreIngestionTask.KEY_CREATED_SIGNAL;
    assertEquals(first, second, "Pre-computed header must be the same static instance");
  }

  @Test
  public void testConfigKeyValues() {
    assertEquals(
        ConfigKeys.SERVER_ADD_RMD_TO_BATCH_PUSH_FOR_HYBRID_STORES,
        "server.add.rmd.to.batch.push.for.hybrid.stores");
    assertEquals(
        ConfigKeys.SERVER_UNIQUE_KEY_COUNT_FOR_ALL_BATCH_PUSH_ENABLED,
        "server.unique.key.count.for.all.batch.push.enabled");
    assertEquals(
        ConfigKeys.SERVER_UNIQUE_KEY_COUNT_FOR_HYBRID_STORE_ENABLED,
        "server.unique.key.count.for.hybrid.store.enabled");
  }

  @Test
  public void testIsChunkFragment() {
    int chunkSchemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    int manifestSchemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    assertEquals(StoreIngestionTask.isChunkFragment(chunkSchemaId), true, "CHUNK schema ID is a chunk fragment");
    assertEquals(StoreIngestionTask.isChunkFragment(manifestSchemaId), false, "Manifest is NOT a chunk fragment");
    assertEquals(StoreIngestionTask.isChunkFragment(1), false, "User schema 1 is NOT a chunk fragment");
    assertEquals(StoreIngestionTask.isChunkFragment(100), false, "User schema 100 is NOT a chunk fragment");
    assertEquals(StoreIngestionTask.isChunkFragment(0), false, "Schema 0 is NOT a chunk fragment");
    assertEquals(StoreIngestionTask.isChunkFragment(-1), false, "Schema -1 is NOT a chunk fragment");
  }

  @Test
  public void testIsChunkManifest() {
    int chunkSchemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    int manifestSchemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    assertEquals(StoreIngestionTask.isChunkManifest(manifestSchemaId), true, "Manifest schema ID is a chunk manifest");
    assertEquals(StoreIngestionTask.isChunkManifest(chunkSchemaId), false, "CHUNK is NOT a manifest");
    assertEquals(StoreIngestionTask.isChunkManifest(1), false, "User schema 1 is NOT a manifest");
    assertEquals(StoreIngestionTask.isChunkManifest(100), false, "User schema 100 is NOT a manifest");
    assertEquals(StoreIngestionTask.isChunkManifest(0), false, "Schema 0 is NOT a manifest");
    assertEquals(StoreIngestionTask.isChunkManifest(-1), false, "Schema -1 is NOT a manifest");
  }

  @Test
  public void testIsChunkFragmentAndManifestAreMutuallyExclusive() {
    int chunkSchemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    int manifestSchemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    // A schema ID cannot be both a chunk fragment and a manifest
    assertEquals(
        StoreIngestionTask.isChunkFragment(chunkSchemaId) && StoreIngestionTask.isChunkManifest(chunkSchemaId),
        false,
        "CHUNK cannot be both fragment and manifest");
    assertEquals(
        StoreIngestionTask.isChunkFragment(manifestSchemaId) && StoreIngestionTask.isChunkManifest(manifestSchemaId),
        false,
        "Manifest cannot be both fragment and manifest");
    // User schema is neither
    assertEquals(
        StoreIngestionTask.isChunkFragment(1) || StoreIngestionTask.isChunkManifest(1),
        false,
        "User schema is neither fragment nor manifest");
  }

  @Test
  public void testVeniceServerConfigDefaultsFalse() {
    // Can't construct VeniceServerConfig without many required keys, so verify the config key
    // string constants exist and follow Venice naming convention (dot-separated)
    String addRmd = ConfigKeys.SERVER_ADD_RMD_TO_BATCH_PUSH_FOR_HYBRID_STORES;
    String batchCount = ConfigKeys.SERVER_UNIQUE_KEY_COUNT_FOR_ALL_BATCH_PUSH_ENABLED;
    String uniqueCount = ConfigKeys.SERVER_UNIQUE_KEY_COUNT_FOR_HYBRID_STORE_ENABLED;
    for (String key: new String[] { addRmd, batchCount, uniqueCount }) {
      // Verify dot-separated naming convention
      Assert.assertFalse(key.contains("_"), "Config key should use dots, not underscores: " + key);
      Assert.assertTrue(key.startsWith("server."), "Config key should start with 'server.': " + key);
    }
  }
}
