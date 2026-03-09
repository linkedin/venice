package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


/**
 * Tests for VT produce coalescing logic that marks intermediate same-key records
 * within a batch so their version topic produce can be skipped.
 */
public class VtProduceCoalescingTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();

  /**
   * Verifies that the coalescing index computation correctly identifies the last occurrence
   * of each key in a batch, and only intermediate (non-last) records get the coalesced flag.
   */
  @Test
  public void testCoalescingFlagSetOnIntermediateRecords() {
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic("store_rt");
    PubSubTopicPartition rtTp = new PubSubTopicPartitionImpl(rtTopic, 0);
    byte[] keyA = "keyA".getBytes();
    byte[] keyB = "keyB".getBytes();

    // Batch: A, A, B, A, B — last A is index 3, last B is index 4
    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(wrapWithResult(rtTp, keyA)); // 0: A (intermediate)
    results.add(wrapWithResult(rtTp, keyA)); // 1: A (intermediate)
    results.add(wrapWithResult(rtTp, keyB)); // 2: B (intermediate)
    results.add(wrapWithResult(rtTp, keyA)); // 3: A (last)
    results.add(wrapWithResult(rtTp, keyB)); // 4: B (last)

    applyCoalescingFlags(results);

    assertTrue(results.get(0).isVtProduceCoalesced(), "First A should be coalesced");
    assertTrue(results.get(1).isVtProduceCoalesced(), "Second A should be coalesced");
    assertTrue(results.get(2).isVtProduceCoalesced(), "First B should be coalesced");
    assertFalse(results.get(3).isVtProduceCoalesced(), "Last A should NOT be coalesced");
    assertFalse(results.get(4).isVtProduceCoalesced(), "Last B should NOT be coalesced");
  }

  /**
   * When all records have distinct keys, no coalescing should occur.
   */
  @Test
  public void testNoCoalescingForDistinctKeys() {
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic("store_rt");
    PubSubTopicPartition rtTp = new PubSubTopicPartitionImpl(rtTopic, 0);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(wrapWithResult(rtTp, "key1".getBytes()));
    results.add(wrapWithResult(rtTp, "key2".getBytes()));
    results.add(wrapWithResult(rtTp, "key3".getBytes()));

    applyCoalescingFlags(results);

    for (PubSubMessageProcessedResultWrapper r: results) {
      assertFalse(r.isVtProduceCoalesced(), "Distinct keys should not be coalesced");
    }
  }

  /**
   * A single record for a key should never be coalesced.
   */
  @Test
  public void testSingleRecordNotCoalesced() {
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic("store_rt");
    PubSubTopicPartition rtTp = new PubSubTopicPartitionImpl(rtTopic, 0);

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    results.add(wrapWithResult(rtTp, "onlyKey".getBytes()));

    applyCoalescingFlags(results);

    assertFalse(results.get(0).isVtProduceCoalesced());
  }

  /**
   * Control messages should never be coalesced, even if they share the same key bytes.
   */
  @Test
  public void testControlMessagesNeverCoalesced() {
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic("store_rt");
    PubSubTopicPartition rtTp = new PubSubTopicPartitionImpl(rtTopic, 0);
    byte[] keyBytes = "someKey".getBytes();

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    // Control message
    KafkaKey controlKey = new KafkaKey(MessageType.CONTROL_MESSAGE, keyBytes);
    DefaultPubSubMessage controlMsg = new ImmutablePubSubMessage(
        controlKey,
        mock(KafkaMessageEnvelope.class),
        rtTp,
        mock(PubSubPosition.class),
        100,
        50);
    PubSubMessageProcessedResultWrapper controlWrapper = new PubSubMessageProcessedResultWrapper(controlMsg);
    controlWrapper.setProcessedResult(mock(PubSubMessageProcessedResult.class));
    results.add(controlWrapper);

    // Data message with same key bytes
    results.add(wrapWithResult(rtTp, keyBytes));

    applyCoalescingFlags(results);

    assertFalse(results.get(0).isVtProduceCoalesced(), "Control message should never be coalesced");
    assertFalse(results.get(1).isVtProduceCoalesced(), "Only data record for this key, not coalesced");
  }

  /**
   * Records without a processed result (not pre-processed by Phase 1) should not be coalesced.
   */
  @Test
  public void testRecordWithoutProcessedResultNotCoalesced() {
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic("store_rt");
    PubSubTopicPartition rtTp = new PubSubTopicPartitionImpl(rtTopic, 0);
    byte[] keyBytes = "hotKey".getBytes();

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    // First record has no processed result
    KafkaKey dataKey = new KafkaKey(MessageType.PUT, keyBytes);
    DefaultPubSubMessage msg = new ImmutablePubSubMessage(
        dataKey,
        mock(KafkaMessageEnvelope.class),
        rtTp,
        mock(PubSubPosition.class),
        100,
        50);
    PubSubMessageProcessedResultWrapper noResultWrapper = new PubSubMessageProcessedResultWrapper(msg);
    // Intentionally NOT setting processedResult
    results.add(noResultWrapper);

    // Second record has processed result
    results.add(wrapWithResult(rtTp, keyBytes));

    applyCoalescingFlags(results);

    assertFalse(results.get(0).isVtProduceCoalesced(), "Record without processed result should not be coalesced");
    assertFalse(results.get(1).isVtProduceCoalesced(), "Last record for key should not be coalesced");
  }

  /**
   * All records for a hot key should be coalesced except the last one.
   */
  @Test
  public void testHotKeyCoalescing() {
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic("store_rt");
    PubSubTopicPartition rtTp = new PubSubTopicPartitionImpl(rtTopic, 0);
    byte[] hotKey = "hotKey".getBytes();
    int count = 50;

    List<PubSubMessageProcessedResultWrapper> results = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      results.add(wrapWithResult(rtTp, hotKey));
    }

    applyCoalescingFlags(results);

    int coalescedCount = 0;
    for (int i = 0; i < count; i++) {
      if (i < count - 1) {
        assertTrue(results.get(i).isVtProduceCoalesced(), "Record " + i + " should be coalesced");
        coalescedCount++;
      } else {
        assertFalse(results.get(i).isVtProduceCoalesced(), "Last record should NOT be coalesced");
      }
    }
    assertEquals(coalescedCount, count - 1);
  }

  /**
   * Applies the same coalescing flag logic as StoreIngestionTask.produceToStoreBufferServiceOrKafkaInBatch().
   * Extracted here to test the algorithm independently of the full ingestion pipeline.
   */
  private void applyCoalescingFlags(List<PubSubMessageProcessedResultWrapper> results) {
    Map<ByteArrayKey, Integer> lastIndexForKey = new HashMap<>();
    for (int i = 0; i < results.size(); i++) {
      DefaultPubSubMessage msg = results.get(i).getMessage();
      if (!msg.getKey().isControlMessage()) {
        lastIndexForKey.put(ByteArrayKey.wrap(msg.getKey().getKey()), i);
      }
    }

    for (int i = 0; i < results.size(); i++) {
      PubSubMessageProcessedResultWrapper record = results.get(i);
      if (!record.getMessage().getKey().isControlMessage()) {
        Integer lastIdx = lastIndexForKey.get(ByteArrayKey.wrap(record.getMessage().getKey().getKey()));
        if (lastIdx != null && i < lastIdx && record.getProcessedResult() != null) {
          record.setVtProduceCoalesced(true);
        }
      }
    }
  }

  private PubSubMessageProcessedResultWrapper wrapWithResult(PubSubTopicPartition tp, byte[] keyBytes) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keyBytes);
    DefaultPubSubMessage msg =
        new ImmutablePubSubMessage(kafkaKey, mock(KafkaMessageEnvelope.class), tp, mock(PubSubPosition.class), 100, 50);
    PubSubMessageProcessedResultWrapper wrapper = new PubSubMessageProcessedResultWrapper(msg);
    wrapper.setProcessedResult(mock(PubSubMessageProcessedResult.class));
    return wrapper;
  }
}
