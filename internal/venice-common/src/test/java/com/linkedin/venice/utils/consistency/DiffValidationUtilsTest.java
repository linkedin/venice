package com.linkedin.venice.utils.consistency;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DiffValidationUtilsTest {
  @Test
  public void testDoRecordsDiverge() {
    List<Long> firstValueOffsetRecord = new ArrayList<>();
    List<Long> secondValueOffsetRecord = new ArrayList<>();
    List<Long> firstPartitionHighWaterMark = new ArrayList<>();
    List<Long> secondPartitionHighWatermark = new ArrayList<>();

    // metadata isn't populated in both colo's
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST));

    // metadata isn't populated in first colo
    Collections.addAll(secondPartitionHighWatermark, 10L, 20L, 1500L);
    Collections.addAll(secondValueOffsetRecord, 3L, 0L);
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            Collections.EMPTY_LIST,
            secondPartitionHighWatermark,
            Collections.EMPTY_LIST,
            secondValueOffsetRecord));

    // metadata isn't populated in second colo
    Collections.addAll(firstPartitionHighWaterMark, 10L, 20L, 1500L);
    Collections.addAll(firstValueOffsetRecord, 3L, 0L);
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            firstPartitionHighWaterMark,
            Collections.EMPTY_LIST,
            firstValueOffsetRecord,
            Collections.EMPTY_LIST));

    // values are the same
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "foo",
            firstPartitionHighWaterMark,
            secondPartitionHighWatermark,
            firstValueOffsetRecord,
            secondValueOffsetRecord));

    // Clean up
    firstPartitionHighWaterMark.clear();
    firstValueOffsetRecord.clear();
    secondPartitionHighWatermark.clear();
    firstPartitionHighWaterMark.clear();

    // first colo is ahead completely
    Collections.addAll(firstPartitionHighWaterMark, 20L, 40L, 1600L);
    Collections.addAll(firstValueOffsetRecord, 20L, 40L);
    Collections.addAll(secondPartitionHighWatermark, 10L, 20L, 1500L);
    Collections.addAll(secondValueOffsetRecord, 3L, 0L);
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            firstPartitionHighWaterMark,
            secondPartitionHighWatermark,
            firstValueOffsetRecord,
            secondValueOffsetRecord));
    firstPartitionHighWaterMark.clear();
    firstValueOffsetRecord.clear();
    secondPartitionHighWatermark.clear();
    secondValueOffsetRecord.clear();

    // second colo is ahead completely
    Collections.addAll(firstPartitionHighWaterMark, 10L, 20L, 1500L);
    Collections.addAll(firstValueOffsetRecord, 3L, 0L);
    Collections.addAll(secondPartitionHighWatermark, 20L, 40L, 1600L);
    Collections.addAll(secondValueOffsetRecord, 20L, 39L);
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            firstPartitionHighWaterMark,
            secondPartitionHighWatermark,
            firstValueOffsetRecord,
            secondValueOffsetRecord));
    firstPartitionHighWaterMark.clear();
    firstValueOffsetRecord.clear();
    secondPartitionHighWatermark.clear();
    secondValueOffsetRecord.clear();

    // fist colo has a lagging colo
    Collections.addAll(firstPartitionHighWaterMark, 10L, 20L, 1500L);
    Collections.addAll(firstValueOffsetRecord, 3L, 0L);
    Collections.addAll(secondPartitionHighWatermark, 10L, 40L, 1500L);
    Collections.addAll(secondValueOffsetRecord, 10L, 25L);
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            firstPartitionHighWaterMark,
            secondPartitionHighWatermark,
            firstValueOffsetRecord,
            secondValueOffsetRecord));
    firstPartitionHighWaterMark.clear();
    firstValueOffsetRecord.clear();
    secondPartitionHighWatermark.clear();
    secondValueOffsetRecord.clear();

    // second colo has a lagging colo
    Collections.addAll(firstPartitionHighWaterMark, 10L, 40L, 1500L);
    Collections.addAll(firstValueOffsetRecord, 3L, 25L);
    Collections.addAll(secondPartitionHighWatermark, 10L, 20L, 1500L);
    Collections.addAll(secondValueOffsetRecord, 10L, 19L);
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            firstPartitionHighWaterMark,
            secondPartitionHighWatermark,
            firstValueOffsetRecord,
            secondValueOffsetRecord));
    firstPartitionHighWaterMark.clear();
    firstValueOffsetRecord.clear();
    secondPartitionHighWatermark.clear();
    secondValueOffsetRecord.clear();

    // records diverge
    Collections.addAll(firstPartitionHighWaterMark, 10L, 40L, 1505L);
    Collections.addAll(firstValueOffsetRecord, 3L, 25L);
    Collections.addAll(secondPartitionHighWatermark, 10L, 40L, 1500L);
    Collections.addAll(secondValueOffsetRecord, 10L, 19L);
    Assert.assertTrue(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            firstPartitionHighWaterMark,
            secondPartitionHighWatermark,
            firstValueOffsetRecord,
            secondValueOffsetRecord));

  }

  // ── PubSubPosition-aware hasOffsetAdvanced (Kafka positions) ─────────────

  @Test
  public void testHasOffsetAdvancedWithKafkaPositions_advancedCoversBase() {
    Assert.assertTrue(
        DiffValidationUtils.hasOffsetAdvanced(
            kafkaPos(5, 10),
            kafkaPos(50, 60),
            mockTopicManager(),
            mockTopicManager(),
            mockPartition()));
  }

  @Test
  public void testHasOffsetAdvancedWithKafkaPositions_advancedBehindInOneColo() {
    Assert.assertFalse(
        DiffValidationUtils.hasOffsetAdvanced(
            kafkaPos(5, 10),
            kafkaPos(50, 3),
            mockTopicManager(),
            mockTopicManager(),
            mockPartition()));
  }

  @Test
  public void testHasOffsetAdvancedWithKafkaPositions_exactlyEqual() {
    Assert.assertTrue(
        DiffValidationUtils.hasOffsetAdvanced(
            kafkaPos(10, 20),
            kafkaPos(10, 20),
            mockTopicManager(),
            mockTopicManager(),
            mockPartition()));
  }

  @Test
  public void testHasOffsetAdvancedWithKafkaPositions_baseHasMoreColos() {
    Assert.assertFalse(
        DiffValidationUtils.hasOffsetAdvanced(
            kafkaPos(5, 10, 15),
            kafkaPos(50, 60),
            mockTopicManager(),
            mockTopicManager(),
            mockPartition()));
  }

  // ── PubSubPosition-aware hasOffsetAdvanced (InMemory positions) ──────────

  @Test
  public void testHasOffsetAdvancedWithInMemoryPositions_advancedCoversBase() {
    Assert.assertTrue(
        DiffValidationUtils.hasOffsetAdvanced(
            inMemoryPos(5, 10),
            inMemoryPos(50, 60),
            mockTopicManager(),
            mockTopicManager(),
            mockPartition()));
  }

  @Test
  public void testHasOffsetAdvancedWithInMemoryPositions_advancedBehindInOneColo() {
    Assert.assertFalse(
        DiffValidationUtils.hasOffsetAdvanced(
            inMemoryPos(5, 10),
            inMemoryPos(50, 3),
            mockTopicManager(),
            mockTopicManager(),
            mockPartition()));
  }

  @Test
  public void testHasOffsetAdvancedWithInMemoryPositions_exactlyEqual() {
    Assert.assertTrue(
        DiffValidationUtils.hasOffsetAdvanced(
            inMemoryPos(10, 20),
            inMemoryPos(10, 20),
            mockTopicManager(),
            mockTopicManager(),
            mockPartition()));
  }

  // ── null handling ────────────────────────────────────────────────────────

  @Test
  public void testHasOffsetAdvanced_nullBaseIsTriviallyAdvanced() {
    List<PubSubPosition> base = Arrays.asList(null, ApacheKafkaOffsetPosition.of(10));
    Assert.assertTrue(
        DiffValidationUtils
            .hasOffsetAdvanced(base, kafkaPos(50, 60), mockTopicManager(), mockTopicManager(), mockPartition()));
  }

  @Test
  public void testHasOffsetAdvanced_nullAdvancedNotCovered() {
    List<PubSubPosition> advanced = Arrays.asList(null, ApacheKafkaOffsetPosition.of(60));
    Assert.assertFalse(
        DiffValidationUtils
            .hasOffsetAdvanced(kafkaPos(5, 10), advanced, mockTopicManager(), mockTopicManager(), mockPartition()));
  }

  @Test
  public void testHasOffsetAdvanced_bothNull() {
    List<PubSubPosition> base = Arrays.asList(null, null);
    List<PubSubPosition> advanced = Arrays.asList(null, null);
    Assert.assertTrue(
        DiffValidationUtils.hasOffsetAdvanced(base, advanced, mockTopicManager(), mockTopicManager(), mockPartition()));
  }

  // ── isRecordMissing ──────────────────────────────────────────────────────

  @Test
  public void testIsRecordMissing_hwCoversRecordKafka() {
    Assert.assertTrue(
        DiffValidationUtils.isRecordMissing(
            kafkaPos(5, 10),
            kafkaPos(50, 60),
            mockTopicManager(),
            mockTopicManager(),
            mockPartition()));
  }

  @Test
  public void testIsRecordMissing_hwDoesNotCoverRecordKafka() {
    Assert.assertFalse(
        DiffValidationUtils.isRecordMissing(
            kafkaPos(5, 10),
            kafkaPos(50, 3),
            mockTopicManager(),
            mockTopicManager(),
            mockPartition()));
  }

  @Test
  public void testIsRecordMissing_hwCoversRecordInMemory() {
    Assert.assertTrue(
        DiffValidationUtils.isRecordMissing(
            inMemoryPos(5, 10),
            inMemoryPos(50, 60),
            mockTopicManager(),
            mockTopicManager(),
            mockPartition()));
  }

  @Test
  public void testIsRecordMissing_nullInRecordOv() {
    List<PubSubPosition> recordOv = Arrays.asList(null, InMemoryPubSubPosition.of(10));
    Assert.assertTrue(
        DiffValidationUtils
            .isRecordMissing(recordOv, inMemoryPos(50, 60), mockTopicManager(), mockTopicManager(), mockPartition()));
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  private static List<PubSubPosition> kafkaPos(long... offsets) {
    List<PubSubPosition> list = new ArrayList<>();
    for (long o: offsets) {
      list.add(ApacheKafkaOffsetPosition.of(o));
    }
    return list;
  }

  private static List<PubSubPosition> inMemoryPos(long... offsets) {
    List<PubSubPosition> list = new ArrayList<>();
    for (long o: offsets) {
      list.add(InMemoryPubSubPosition.of(o));
    }
    return list;
  }

  private static TopicManager mockTopicManager() {
    TopicManager tm = mock(TopicManager.class);
    when(tm.comparePosition(any(), any(), any())).thenAnswer(inv -> {
      PubSubPosition p1 = inv.getArgument(1);
      PubSubPosition p2 = inv.getArgument(2);
      return p1.getNumericOffset() - p2.getNumericOffset();
    });
    return tm;
  }

  private static PubSubTopicPartition mockPartition() {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    return new PubSubTopicPartitionImpl(repo.getTopic("test_v1"), 0);
  }
}
