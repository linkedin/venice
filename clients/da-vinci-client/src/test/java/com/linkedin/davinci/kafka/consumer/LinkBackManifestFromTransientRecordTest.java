package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import com.linkedin.davinci.replication.RmdWithValueSchemaId;
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
 * method reads this manifest and sets it on the next record's oldValueManifestContainer so that
 * chunk deletion works correctly for all records, not just the first.
 */
public class LinkBackManifestFromTransientRecordTest {
  private PubSubMessageProcessedResultWrapper createAAResult(byte[] keyBytes, ChunkedValueManifest oldManifest) {
    DefaultPubSubMessage message = mock(DefaultPubSubMessage.class);
    KafkaKey kafkaKey = mock(KafkaKey.class);
    when(message.getKey()).thenReturn(kafkaKey);
    when(kafkaKey.getKey()).thenReturn(keyBytes);

    ChunkedValueManifestContainer manifestContainer = new ChunkedValueManifestContainer();
    manifestContainer.setManifest(oldManifest);

    RmdWithValueSchemaId oldRmd = new RmdWithValueSchemaId();

    MergeConflictResultWrapper mcr = mock(MergeConflictResultWrapper.class);
    when(mcr.getOldValueManifestContainer()).thenReturn(manifestContainer);
    when(mcr.getOldRmdWithValueSchemaId()).thenReturn(oldRmd);

    PubSubMessageProcessedResult processedResult = new PubSubMessageProcessedResult(mcr);

    PubSubMessageProcessedResultWrapper wrapper = new PubSubMessageProcessedResultWrapper(message);
    wrapper.setProcessedResult(processedResult);
    return wrapper;
  }

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
   * Simulates the batch apply loop: for each record, if the key has been seen before,
   * calls the actual {@link StoreIngestionTask#linkBackManifestFromTransientRecord} method.
   * In real code, handleSingleMessage (which updates the transient record) runs between
   * link-back calls; here we simulate that via mock return value sequencing.
   */
  private void simulateLinkBackLoop(
      List<PubSubMessageProcessedResultWrapper> processedResults,
      PartitionConsumptionState pcs) {
    Set<ByteArrayKey> seenKeys = new HashSet<>();
    for (PubSubMessageProcessedResultWrapper processedRecord: processedResults) {
      ByteArrayKey key = ByteArrayKey.wrap(processedRecord.getMessage().getKey().getKey());
      if (seenKeys.contains(key)) {
        StoreIngestionTask.linkBackManifestFromTransientRecord(processedRecord, pcs);
      }
      seenKeys.add(key);
    }
  }

  @Test
  public void testBatchOfThreeSameKeyRecordsAllProducedWithCorrectManifests() {
    byte[] key = new byte[] { 1 };
    ChunkedValueManifest m1 = createManifest(3);
    ChunkedValueManifest m2 = createManifest(4);
    ChunkedValueManifest m3 = createManifest(5);
    ChunkedValueManifest rmdM2 = createManifest(10);
    ChunkedValueManifest rmdM3 = createManifest(11);

    PubSubMessageProcessedResultWrapper r1 = createAAResult(key, m1);
    PubSubMessageProcessedResultWrapper r2 = createAAResult(key, null);
    PubSubMessageProcessedResultWrapper r3 = createAAResult(key, null);

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PartitionConsumptionState.TransientRecord transientAfterR1 = mock(PartitionConsumptionState.TransientRecord.class);
    when(transientAfterR1.getValueManifest()).thenReturn(m2);
    when(transientAfterR1.getRmdManifest()).thenReturn(rmdM2);
    PartitionConsumptionState.TransientRecord transientAfterR2 = mock(PartitionConsumptionState.TransientRecord.class);
    when(transientAfterR2.getValueManifest()).thenReturn(m3);
    when(transientAfterR2.getRmdManifest()).thenReturn(rmdM3);
    when(pcs.getTransientRecord(key)).thenReturn(transientAfterR1, transientAfterR2);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(r1);
    results.add(r2);
    results.add(r3);

    simulateLinkBackLoop(results, pcs);

    assertSame(
        r1.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m1);
    assertSame(
        r2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m2);
    assertSame(
        r3.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m3);
    // Verify RMD manifests are also linked back
    assertSame(
        r2.getProcessedResult().getMergeConflictResultWrapper().getOldRmdWithValueSchemaId().getRmdManifest(),
        rmdM2);
    assertSame(
        r3.getProcessedResult().getMergeConflictResultWrapper().getOldRmdWithValueSchemaId().getRmdManifest(),
        rmdM3);
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

    assertSame(
        r1.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m1);
    assertSame(
        r2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m2);
    assertSame(
        r3.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m3);
    verify(pcs, never()).getTransientRecord(key1);
    verify(pcs, never()).getTransientRecord(key2);
    verify(pcs, never()).getTransientRecord(key3);
  }

  @Test
  public void testMixedKeysCorrectManifestsPerKey() {
    byte[] keyA = new byte[] { 1 };
    byte[] keyB = new byte[] { 2 };
    ChunkedValueManifest mA1 = createManifest(3);
    ChunkedValueManifest mB1 = createManifest(5);
    ChunkedValueManifest mA2 = createManifest(4);
    ChunkedValueManifest mB2 = createManifest(6);
    ChunkedValueManifest mA3 = createManifest(7);
    ChunkedValueManifest rmdA2 = createManifest(20);
    ChunkedValueManifest rmdA3 = createManifest(21);
    ChunkedValueManifest rmdB2 = createManifest(22);

    PubSubMessageProcessedResultWrapper a1 = createAAResult(keyA, mA1);
    PubSubMessageProcessedResultWrapper b1 = createAAResult(keyB, mB1);
    PubSubMessageProcessedResultWrapper a2 = createAAResult(keyA, null);
    PubSubMessageProcessedResultWrapper a3 = createAAResult(keyA, null);
    PubSubMessageProcessedResultWrapper b2 = createAAResult(keyB, null);

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PartitionConsumptionState.TransientRecord transientA_afterA1 =
        mock(PartitionConsumptionState.TransientRecord.class);
    when(transientA_afterA1.getValueManifest()).thenReturn(mA2);
    when(transientA_afterA1.getRmdManifest()).thenReturn(rmdA2);
    PartitionConsumptionState.TransientRecord transientA_afterA2 =
        mock(PartitionConsumptionState.TransientRecord.class);
    when(transientA_afterA2.getValueManifest()).thenReturn(mA3);
    when(transientA_afterA2.getRmdManifest()).thenReturn(rmdA3);
    when(pcs.getTransientRecord(keyA)).thenReturn(transientA_afterA1, transientA_afterA2);

    PartitionConsumptionState.TransientRecord transientB_afterB1 =
        mock(PartitionConsumptionState.TransientRecord.class);
    when(transientB_afterB1.getValueManifest()).thenReturn(mB2);
    when(transientB_afterB1.getRmdManifest()).thenReturn(rmdB2);
    when(pcs.getTransientRecord(keyB)).thenReturn(transientB_afterB1);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(a1);
    results.add(b1);
    results.add(a2);
    results.add(a3);
    results.add(b2);

    simulateLinkBackLoop(results, pcs);

    assertSame(
        a1.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        mA1);
    assertSame(
        b1.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        mB1);
    assertSame(
        a2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        mA2);
    assertSame(
        a3.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        mA3);
    assertSame(
        b2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        mB2);
    // Verify RMD manifests are also linked back per key
    assertSame(
        a2.getProcessedResult().getMergeConflictResultWrapper().getOldRmdWithValueSchemaId().getRmdManifest(),
        rmdA2);
    assertSame(
        a3.getProcessedResult().getMergeConflictResultWrapper().getOldRmdWithValueSchemaId().getRmdManifest(),
        rmdA3);
    assertSame(
        b2.getProcessedResult().getMergeConflictResultWrapper().getOldRmdWithValueSchemaId().getRmdManifest(),
        rmdB2);
  }

  @Test
  public void testFirstRecordNullManifestNoCrashForSubsequentRecords() {
    byte[] key = new byte[] { 1 };
    PubSubMessageProcessedResultWrapper r1 = createAAResult(key, null);
    PubSubMessageProcessedResultWrapper r2 = createAAResult(key, null);

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PartitionConsumptionState.TransientRecord transientRecord = mock(PartitionConsumptionState.TransientRecord.class);
    when(transientRecord.getValueManifest()).thenReturn(null);
    when(transientRecord.getRmdManifest()).thenReturn(null);
    when(pcs.getTransientRecord(key)).thenReturn(transientRecord);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(r1);
    results.add(r2);

    simulateLinkBackLoop(results, pcs);

    assertNull(r1.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest());
    assertNull(r2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest());
    assertNull(r2.getProcessedResult().getMergeConflictResultWrapper().getOldRmdWithValueSchemaId().getRmdManifest());
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

    simulateLinkBackLoop(results, pcs);

    verify(pcs, never()).getTransientRecord(key);
  }

  @Test
  public void testNullTransientRecordDoesNotCrash() {
    byte[] key = new byte[] { 1 };
    ChunkedValueManifest m1 = createManifest(3);
    PubSubMessageProcessedResultWrapper r1 = createAAResult(key, m1);
    PubSubMessageProcessedResultWrapper r2 = createAAResult(key, null);

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getTransientRecord(key)).thenReturn(null);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(r1);
    results.add(r2);

    simulateLinkBackLoop(results, pcs);

    assertSame(
        r1.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m1);
    assertNull(r2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest());
  }

  @Test
  public void testNullOldRmdDoesNotCrash() {
    byte[] key = new byte[] { 1 };
    ChunkedValueManifest m1 = createManifest(3);
    ChunkedValueManifest m2 = createManifest(4);
    ChunkedValueManifest rmdM2 = createManifest(10);

    // r1 has a value manifest; r2 starts with null (will be linked back)
    PubSubMessageProcessedResultWrapper r1 = createAAResult(key, m1);
    PubSubMessageProcessedResultWrapper r2 = createAAResult(key, null);

    // Override r2's MCR to return null for getOldRmdWithValueSchemaId (no RMD chunking)
    MergeConflictResultWrapper mcr2 = r2.getProcessedResult().getMergeConflictResultWrapper();
    when(mcr2.getOldRmdWithValueSchemaId()).thenReturn(null);

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PartitionConsumptionState.TransientRecord transientRecord = mock(PartitionConsumptionState.TransientRecord.class);
    when(transientRecord.getValueManifest()).thenReturn(m2);
    when(transientRecord.getRmdManifest()).thenReturn(rmdM2);
    when(pcs.getTransientRecord(key)).thenReturn(transientRecord);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(r1);
    results.add(r2);

    // Should not throw NPE even though getOldRmdWithValueSchemaId() is null
    simulateLinkBackLoop(results, pcs);

    // Value manifest should still be linked back correctly
    assertSame(
        r2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m2);
    // RMD is null, so no RMD manifest to verify
    assertNull(r2.getProcessedResult().getMergeConflictResultWrapper().getOldRmdWithValueSchemaId());
  }

  @Test
  public void testNullProcessedResultDoesNotCrash() {
    byte[] key = new byte[] { 1 };

    // r1 is a normal AA result; r2 has a null processedResult (not yet pre-processed)
    PubSubMessageProcessedResultWrapper r1 = createAAResult(key, createManifest(3));

    DefaultPubSubMessage message2 = mock(DefaultPubSubMessage.class);
    KafkaKey kafkaKey2 = mock(KafkaKey.class);
    when(message2.getKey()).thenReturn(kafkaKey2);
    when(kafkaKey2.getKey()).thenReturn(key);
    PubSubMessageProcessedResultWrapper r2 = new PubSubMessageProcessedResultWrapper(message2);
    // Do NOT call r2.setProcessedResult() — getProcessedResult() returns null

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(r1);
    results.add(r2);

    // Should not throw NPE when getProcessedResult() is null
    simulateLinkBackLoop(results, pcs);

    // r2 has null processedResult, so no transient record lookup should happen
    verify(pcs, never()).getTransientRecord(key);
  }
}
