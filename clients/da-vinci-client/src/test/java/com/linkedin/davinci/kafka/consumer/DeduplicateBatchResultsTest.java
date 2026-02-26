package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;


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
      manifest.keysWithChunkIdSuffix.add(java.nio.ByteBuffer.wrap(new byte[] { (byte) i }));
    }
    return manifest;
  }

  @Test
  public void testSingleRecordReturnsAsIs() {
    byte[] key = new byte[] { 1 };
    ChunkedValueManifest manifest = createManifest(3);
    PubSubMessageProcessedResultWrapper record = createAAResult(key, manifest);

    List<PubSubMessageProcessedResultWrapper> results = Collections.singletonList(record);
    List<PubSubMessageProcessedResultWrapper> deduplicated = StoreIngestionTask.deduplicateBatchResults(results);

    assertEquals(deduplicated.size(), 1);
    assertSame(deduplicated.get(0), record);
  }

  @Test
  public void testEmptyListReturnsAsIs() {
    List<PubSubMessageProcessedResultWrapper> results = Collections.emptyList();
    List<PubSubMessageProcessedResultWrapper> deduplicated = StoreIngestionTask.deduplicateBatchResults(results);
    assertEquals(deduplicated.size(), 0);
  }

  @Test
  public void testMultipleRecordsSameKeyOnlyProducesLast() {
    byte[] key = new byte[] { 1 };
    ChunkedValueManifest m1 = createManifest(3); // First record's manifest (from storage)
    PubSubMessageProcessedResultWrapper r1 = createAAResult(key, m1);
    PubSubMessageProcessedResultWrapper r2 = createAAResult(key, null); // null manifest (transient record bug)
    PubSubMessageProcessedResultWrapper r3 = createAAResult(key, null); // null manifest (transient record bug)

    List<PubSubMessageProcessedResultWrapper> results = Arrays.asList(r1, r2, r3);
    List<PubSubMessageProcessedResultWrapper> deduplicated = StoreIngestionTask.deduplicateBatchResults(results);

    // Only the last record should be produced
    assertEquals(deduplicated.size(), 1);
    assertSame(deduplicated.get(0), r3);

    // The last record's manifest should be overridden with the first record's manifest
    MergeConflictResultWrapper mcr = r3.getProcessedResult().getMergeConflictResultWrapper();
    assertSame(mcr.getOldValueManifestContainer().getManifest(), m1);
  }

  @Test
  public void testDifferentKeysAllProduced() {
    byte[] key1 = new byte[] { 1 };
    byte[] key2 = new byte[] { 2 };
    byte[] key3 = new byte[] { 3 };
    PubSubMessageProcessedResultWrapper r1 = createAAResult(key1, createManifest(1));
    PubSubMessageProcessedResultWrapper r2 = createAAResult(key2, createManifest(2));
    PubSubMessageProcessedResultWrapper r3 = createAAResult(key3, createManifest(3));

    List<PubSubMessageProcessedResultWrapper> results = Arrays.asList(r1, r2, r3);
    List<PubSubMessageProcessedResultWrapper> deduplicated = StoreIngestionTask.deduplicateBatchResults(results);

    // All records have different keys, so all should be produced
    assertEquals(deduplicated.size(), 3);
    assertSame(deduplicated.get(0), r1);
    assertSame(deduplicated.get(1), r2);
    assertSame(deduplicated.get(2), r3);
  }

  @Test
  public void testMixedKeysDeduplicatedCorrectly() {
    byte[] keyA = new byte[] { 1 };
    byte[] keyB = new byte[] { 2 };
    ChunkedValueManifest mA1 = createManifest(3);
    ChunkedValueManifest mB1 = createManifest(5);

    // Batch: A1, B1, A2, A3, B2
    PubSubMessageProcessedResultWrapper a1 = createAAResult(keyA, mA1);
    PubSubMessageProcessedResultWrapper b1 = createAAResult(keyB, mB1);
    PubSubMessageProcessedResultWrapper a2 = createAAResult(keyA, null);
    PubSubMessageProcessedResultWrapper a3 = createAAResult(keyA, null);
    PubSubMessageProcessedResultWrapper b2 = createAAResult(keyB, null);

    List<PubSubMessageProcessedResultWrapper> results = Arrays.asList(a1, b1, a2, a3, b2);
    List<PubSubMessageProcessedResultWrapper> deduplicated = StoreIngestionTask.deduplicateBatchResults(results);

    // Only A3 and B2 (last for each key) should be produced
    assertEquals(deduplicated.size(), 2);
    assertSame(deduplicated.get(0), a3);
    assertSame(deduplicated.get(1), b2);

    // A3's manifest should be overridden with A1's manifest
    assertSame(
        a3.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        mA1);

    // B2's manifest should be overridden with B1's manifest
    assertSame(
        b2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        mB1);
  }

  @Test
  public void testFirstRecordManifestNullDoesNotOverride() {
    byte[] key = new byte[] { 1 };
    // First record has null manifest (e.g., key didn't exist before)
    PubSubMessageProcessedResultWrapper r1 = createAAResult(key, null);
    PubSubMessageProcessedResultWrapper r2 = createAAResult(key, null);

    List<PubSubMessageProcessedResultWrapper> results = Arrays.asList(r1, r2);
    List<PubSubMessageProcessedResultWrapper> deduplicated = StoreIngestionTask.deduplicateBatchResults(results);

    assertEquals(deduplicated.size(), 1);
    assertSame(deduplicated.get(0), r2);
    // The manifest should remain null since the first record also had null
    assertNull(r2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest());
  }

  @Test
  public void testNonAARecordsDeduplicatedWithoutManifestOverride() {
    byte[] key = new byte[] { 1 };
    PubSubMessageProcessedResultWrapper r1 = createNonAAResult(key);
    PubSubMessageProcessedResultWrapper r2 = createNonAAResult(key);
    PubSubMessageProcessedResultWrapper r3 = createNonAAResult(key);

    List<PubSubMessageProcessedResultWrapper> results = Arrays.asList(r1, r2, r3);
    List<PubSubMessageProcessedResultWrapper> deduplicated = StoreIngestionTask.deduplicateBatchResults(results);

    // Only the last record should be produced
    assertEquals(deduplicated.size(), 1);
    assertSame(deduplicated.get(0), r3);
  }

  @Test
  public void testMixedAAAndNonAARecords() {
    byte[] keyAA = new byte[] { 1 };
    byte[] keyNonAA = new byte[] { 2 };
    ChunkedValueManifest m1 = createManifest(3);

    PubSubMessageProcessedResultWrapper aa1 = createAAResult(keyAA, m1);
    PubSubMessageProcessedResultWrapper nonAA1 = createNonAAResult(keyNonAA);
    PubSubMessageProcessedResultWrapper aa2 = createAAResult(keyAA, null);
    PubSubMessageProcessedResultWrapper nonAA2 = createNonAAResult(keyNonAA);

    List<PubSubMessageProcessedResultWrapper> results = Arrays.asList(aa1, nonAA1, aa2, nonAA2);
    List<PubSubMessageProcessedResultWrapper> deduplicated = StoreIngestionTask.deduplicateBatchResults(results);

    assertEquals(deduplicated.size(), 2);
    assertSame(deduplicated.get(0), aa2);
    assertSame(deduplicated.get(1), nonAA2);

    // AA record's manifest should be overridden
    assertSame(
        aa2.getProcessedResult().getMergeConflictResultWrapper().getOldValueManifestContainer().getManifest(),
        m1);
  }
}
