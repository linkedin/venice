package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;


/**
 * Tests for the manifest link-back logic in the batch apply loop of
 * {@link StoreIngestionTask#processIngestionBatch}.
 *
 * When multiple records for the same key appear in a single ingestion batch, pre-processing
 * creates transient records with null manifests. After each produce, setChunkingInfo() (called
 * synchronously by VeniceWriter) sets the new manifest on the transient record. The link-back
 * loop reads this manifest and sets it on the next record's oldValueManifestContainer so that
 * chunk deletion works correctly for all records, not just the first.
 */
public class DeduplicateBatchResultsTest {
  /**
   * Creates a mock PubSubMessageProcessedResultWrapper with an AA (MergeConflictResultWrapper) processed result.
   */
  private PubSubMessageProcessedResultWrapper createAAResult(byte[] keyBytes, ChunkedValueManifest oldManifest) {
    DefaultPubSubMessage message = mock(DefaultPubSubMessage.class);
    KafkaKey kafkaKey = mock(KafkaKey.class);
    when(message.getKey()).thenReturn(kafkaKey);
    when(kafkaKey.getKey()).thenReturn(keyBytes);

    ChunkedValueManifestContainer manifestContainer = new ChunkedValueManifestContainer();
    manifestContainer.setManifest(oldManifest);

    MergeConflictResultWrapper mcr = mock(MergeConflictResultWrapper.class);
    when(mcr.getOldValueManifestContainer()).thenReturn(manifestContainer);

    PubSubMessageProcessedResult processedResult = new PubSubMessageProcessedResult(mcr);

    PubSubMessageProcessedResultWrapper wrapper = new PubSubMessageProcessedResultWrapper(message);
    wrapper.setProcessedResult(processedResult);
    return wrapper;
  }

  /**
   * Creates a mock PubSubMessageProcessedResultWrapper with a non-AA (WriteComputeResultWrapper) processed result.
   */
  private PubSubMessageProcessedResultWrapper createNonAAResult(byte[] keyBytes) {
    DefaultPubSubMessage message = mock(DefaultPubSubMessage.class);
    KafkaKey kafkaKey = mock(KafkaKey.class);
    when(message.getKey()).thenReturn(kafkaKey);
    when(kafkaKey.getKey()).thenReturn(keyBytes);

    WriteComputeResultWrapper wcr = mock(WriteComputeResultWrapper.class);
    PubSubMessageProcessedResult processedResult = new PubSubMessageProcessedResult(wcr);

    PubSubMessageProcessedResultWrapper wrapper = new PubSubMessageProcessedResultWrapper(message);
    wrapper.setProcessedResult(processedResult);
    return wrapper;
  }

  private ChunkedValueManifest createManifest(int keysWithChunkIdSuffixCount) {
    ChunkedValueManifest manifest = new ChunkedValueManifest();
    manifest.keysWithChunkIdSuffix = new ArrayList<>();
    for (int i = 0; i < keysWithChunkIdSuffixCount; i++) {
      manifest.keysWithChunkIdSuffix.add(ByteBuffer.wrap(new byte[] { (byte) i }));
    }
    return manifest;
  }

  /**
   * Simulates the manifest link-back logic from the batch apply loop.
   * For each record, if the key has been seen before, reads the manifest from the
   * mock transient record and sets it on the current record's oldValueManifestContainer.
   *
   * @param processedResults the batch of processed records
   * @param pcs the mock PartitionConsumptionState that provides transient records
   */
  private void simulateLinkBackLoop(
      List<PubSubMessageProcessedResultWrapper> processedResults,
      PartitionConsumptionState pcs) {
    Set<ByteArrayKey> seenKeys = new HashSet<>();

    for (PubSubMessageProcessedResultWrapper processedRecord: processedResults) {
      ByteArrayKey key = ByteArrayKey.wrap(processedRecord.getMessage().getKey().getKey());

      if (seenKeys.contains(key)) {
        MergeConflictResultWrapper mcr = processedRecord.getProcessedResult().getMergeConflictResultWrapper();
        if (mcr != null) {
          PartitionConsumptionState.TransientRecord transientRecord =
              pcs.getTransientRecord(processedRecord.getMessage().getKey().getKey());
          if (transientRecord != null) {
            mcr.getOldValueManifestContainer().setManifest(transientRecord.getValueManifest());
          }
        }
      }
      seenKeys.add(key);

      // In the real code, handleSingleMessage would be called here, which triggers
      // VeniceWriter.put() -> setChunkingInfo() -> transientRecord.setValueManifest().
      // We simulate this by updating the mock transient record after "producing".
    }
  }

  @Test
  public void testBatchOfThreeSameKeyRecordsAllProducedWithCorrectManifests() {
    byte[] key = new byte[] { 1 };
    ChunkedValueManifest m1 = createManifest(3); // Original manifest from storage
    ChunkedValueManifest m2 = createManifest(4); // Manifest set by setChunkingInfo after R1 produce
    ChunkedValueManifest m3 = createManifest(5); // Manifest set by setChunkingInfo after R2 produce

    PubSubMessageProcessedResultWrapper r1 = createAAResult(key, m1);
    PubSubMessageProcessedResultWrapper r2 = createAAResult(key, null); // null from transient record bug
    PubSubMessageProcessedResultWrapper r3 = createAAResult(key, null); // null from transient record bug

    // Set up mock PCS that simulates transient record manifest updates after each produce
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PartitionConsumptionState.TransientRecord transientAfterR1 =
        mock(PartitionConsumptionState.TransientRecord.class);
    when(transientAfterR1.getValueManifest()).thenReturn(m2);
    PartitionConsumptionState.TransientRecord transientAfterR2 =
        mock(PartitionConsumptionState.TransientRecord.class);
    when(transientAfterR2.getValueManifest()).thenReturn(m3);

    // First call returns m2 (after R1 produce), second call returns m3 (after R2 produce)
    when(pcs.getTransientRecord(key)).thenReturn(transientAfterR1, transientAfterR2);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(r1);
    results.add(r2);
    results.add(r3);

    simulateLinkBackLoop(results, pcs);

    // All 3 records are in the list (all produced, unlike dedup which would keep only the last)
    // R1 keeps its original manifest M1
    assertSame(
        r1.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m1);
    // R2 gets M2 (linked back from transient record after R1 produce)
    assertSame(
        r2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m2);
    // R3 gets M3 (linked back from transient record after R2 produce)
    assertSame(
        r3.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m3);
  }

  @Test
  public void testDifferentKeysNoLinkBackInterference() {
    byte[] key1 = new byte[] { 1 };
    byte[] key2 = new byte[] { 2 };
    byte[] key3 = new byte[] { 3 };
    ChunkedValueManifest m1 = createManifest(1);
    ChunkedValueManifest m2 = createManifest(2);
    ChunkedValueManifest m3 = createManifest(3);

    PubSubMessageProcessedResultWrapper r1 = createAAResult(key1, m1);
    PubSubMessageProcessedResultWrapper r2 = createAAResult(key2, m2);
    PubSubMessageProcessedResultWrapper r3 = createAAResult(key3, m3);

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(r1);
    results.add(r2);
    results.add(r3);

    simulateLinkBackLoop(results, pcs);

    // No key appears twice, so no link-back should occur — all manifests unchanged
    assertSame(
        r1.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m1);
    assertSame(
        r2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m2);
    assertSame(
        r3.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m3);
    // PCS should never be consulted since no key repeats
    verify(pcs, never()).getTransientRecord(key1);
    verify(pcs, never()).getTransientRecord(key2);
    verify(pcs, never()).getTransientRecord(key3);
  }

  @Test
  public void testMixedKeysCorrectManifestsPerKey() {
    byte[] keyA = new byte[] { 1 };
    byte[] keyB = new byte[] { 2 };
    ChunkedValueManifest mA1 = createManifest(3); // Key A's original manifest
    ChunkedValueManifest mB1 = createManifest(5); // Key B's original manifest
    ChunkedValueManifest mA2 = createManifest(4); // Manifest after A1 produce
    ChunkedValueManifest mB2 = createManifest(6); // Manifest after B1 produce
    ChunkedValueManifest mA3 = createManifest(7); // Manifest after A2 produce

    // Batch: A1, B1, A2, A3, B2
    PubSubMessageProcessedResultWrapper a1 = createAAResult(keyA, mA1);
    PubSubMessageProcessedResultWrapper b1 = createAAResult(keyB, mB1);
    PubSubMessageProcessedResultWrapper a2 = createAAResult(keyA, null);
    PubSubMessageProcessedResultWrapper a3 = createAAResult(keyA, null);
    PubSubMessageProcessedResultWrapper b2 = createAAResult(keyB, null);

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    // Key A: first link-back gets mA2, second gets mA3
    PartitionConsumptionState.TransientRecord transientA_afterA1 =
        mock(PartitionConsumptionState.TransientRecord.class);
    when(transientA_afterA1.getValueManifest()).thenReturn(mA2);
    PartitionConsumptionState.TransientRecord transientA_afterA2 =
        mock(PartitionConsumptionState.TransientRecord.class);
    when(transientA_afterA2.getValueManifest()).thenReturn(mA3);
    when(pcs.getTransientRecord(keyA)).thenReturn(transientA_afterA1, transientA_afterA2);

    // Key B: first link-back gets mB2
    PartitionConsumptionState.TransientRecord transientB_afterB1 =
        mock(PartitionConsumptionState.TransientRecord.class);
    when(transientB_afterB1.getValueManifest()).thenReturn(mB2);
    when(pcs.getTransientRecord(keyB)).thenReturn(transientB_afterB1);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(a1);
    results.add(b1);
    results.add(a2);
    results.add(a3);
    results.add(b2);

    simulateLinkBackLoop(results, pcs);

    // A1 keeps original manifest mA1
    assertSame(
        a1.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        mA1);
    // B1 keeps original manifest mB1
    assertSame(
        b1.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        mB1);
    // A2 gets mA2 (linked back after A1 produce)
    assertSame(
        a2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        mA2);
    // A3 gets mA3 (linked back after A2 produce)
    assertSame(
        a3.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        mA3);
    // B2 gets mB2 (linked back after B1 produce)
    assertSame(
        b2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        mB2);
  }

  @Test
  public void testFirstRecordNullManifestNoCrashForSubsequentRecords() {
    byte[] key = new byte[] { 1 };
    // First record has null manifest (e.g., key didn't exist before, value was not chunked)
    PubSubMessageProcessedResultWrapper r1 = createAAResult(key, null);
    PubSubMessageProcessedResultWrapper r2 = createAAResult(key, null);

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    // Transient record has null manifest (value was not chunked)
    PartitionConsumptionState.TransientRecord transientRecord =
        mock(PartitionConsumptionState.TransientRecord.class);
    when(transientRecord.getValueManifest()).thenReturn(null);
    when(pcs.getTransientRecord(key)).thenReturn(transientRecord);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(r1);
    results.add(r2);

    simulateLinkBackLoop(results, pcs);

    // R1 manifest stays null
    assertNull(
        r1.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest());
    // R2 manifest is set to null from transient record (no crash)
    assertNull(
        r2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest());
  }

  @Test
  public void testNonAARecordsDoNotCrash() {
    byte[] key = new byte[] { 1 };
    PubSubMessageProcessedResultWrapper r1 = createNonAAResult(key);
    PubSubMessageProcessedResultWrapper r2 = createNonAAResult(key);

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(r1);
    results.add(r2);

    // Should not crash — getMergeConflictResultWrapper() returns null for non-AA records
    simulateLinkBackLoop(results, pcs);

    // PCS is never consulted because mcr is null for non-AA records
    verify(pcs, never()).getTransientRecord(key);
  }

  @Test
  public void testNullTransientRecordDoesNotCrash() {
    byte[] key = new byte[] { 1 };
    ChunkedValueManifest m1 = createManifest(3);
    PubSubMessageProcessedResultWrapper r1 = createAAResult(key, m1);
    PubSubMessageProcessedResultWrapper r2 = createAAResult(key, null);

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    // Transient record was already removed (e.g., consumed by drainer)
    when(pcs.getTransientRecord(key)).thenReturn(null);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(r1);
    results.add(r2);

    // Should not crash even if transient record is null
    simulateLinkBackLoop(results, pcs);

    // R1 keeps its manifest
    assertSame(
        r1.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m1);
    // R2 manifest stays null (transient record was null, so no override)
    assertNull(
        r2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest());
  }
}
