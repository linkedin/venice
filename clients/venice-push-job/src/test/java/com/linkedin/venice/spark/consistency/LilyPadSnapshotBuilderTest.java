package com.linkedin.venice.spark.consistency;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;


public class LilyPadSnapshotBuilderTest {
  /**
   * A split with zero records should return an empty snapshot without polling the broker.
   */
  @Test
  public void testBuildSnapshotEmptyPartitionReturnsEmptySnapshot() {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(repo.getTopic("store_v1"), 0);
    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);

    PubSubPosition pos = ApacheKafkaOffsetPosition.of(5);
    PubSubPartitionSplit split = new PubSubPartitionSplit(repo, tp, pos, pos, 0, 0, 0);

    TopicManager topicManager = mockTopicManager();
    PubSubPositionDeserializer deserializer =
        new PubSubPositionDeserializer(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY);

    try (PubSubSplitIterator iterator = new PubSubSplitIterator(consumer, split, false)) {
      LilyPadUtils.Snapshot<ComparablePubSubPosition> snapshot =
          LilyPadSnapshotBuilder.buildSnapshot(iterator, topicManager, deserializer, 2);

      assertTrue(snapshot.keyRecords.isEmpty(), "Empty partition must yield no key records");
      assertEquals(
          snapshot.partitionHighWatermark,
          Arrays.asList(null, null),
          "HW must be all null for empty partition");
      verify(consumer, never()).poll(anyLong());
    }
  }

  /**
   * A single PUT message with valid LeaderMetadata should produce exactly one KeyRecord with
   * the correct key hash, value hash, position vector, high-watermark, producer timestamp, and
   * VT position.
   */
  @Test
  public void testBuildSnapshotCapturesSinglePutMessage() {
    byte[] keyBytes = "wolf".getBytes();
    byte[] valBytes = "arctic-wolf".getBytes();
    DefaultPubSubMessage msg = putMsg(0L, keyBytes, valBytes, 0, 42L, 100L);

    LilyPadUtils.Snapshot<ComparablePubSubPosition> snapshot =
        buildSnapshotFromMessages(Collections.singletonList(msg));

    assertEquals(snapshot.keyRecords.size(), 1);
    List<LilyPadUtils.KeyRecord<ComparablePubSubPosition>> history =
        snapshot.keyRecords.get(ByteUtils.hash64(keyBytes));
    assertEquals(history.size(), 1);

    LilyPadUtils.KeyRecord<ComparablePubSubPosition> rec = history.get(0);
    assertEquals(rec.valueHash, Integer.valueOf(Arrays.hashCode(valBytes)));
    assertEquals(rec.upstreamRTPosition.size(), 2);
    assertEquals(rec.upstreamRTPosition.get(0).toString(), ApacheKafkaOffsetPosition.of(42).toString());
    assertNull(rec.upstreamRTPosition.get(1), "region 1 not yet seen");
    assertEquals(rec.highWatermark.size(), 2);
    assertEquals(rec.highWatermark.get(0).toString(), ApacheKafkaOffsetPosition.of(42).toString());
    assertNull(rec.highWatermark.get(1), "region 1 not yet seen");
    assertEquals(rec.logicalTimestamp, 100L);
    assertEquals(rec.vtPosition.toString(), ApacheKafkaOffsetPosition.of(0).toString());
  }

  /**
   * Control messages must be silently skipped; only PUT data messages should produce KeyRecords.
   */
  @Test
  public void testBuildSnapshotSkipsControlMessages() {
    byte[] keyBytes = "wolf".getBytes();
    byte[] valBytes = "arctic-wolf".getBytes();

    DefaultPubSubMessage ctrl = controlMsg(0L);
    DefaultPubSubMessage data = putMsg(1L, keyBytes, valBytes, 0, 10L, 50L);

    LilyPadUtils.Snapshot<ComparablePubSubPosition> snapshot = buildSnapshotFromMessages(Arrays.asList(ctrl, data));

    assertEquals(snapshot.keyRecords.size(), 1, "Control message must not produce a key record");
  }

  /**
   * A PUT message whose LeaderMetadata is null should be skipped — it has no upstream offset
   * information and cannot be part of the lily-pad comparison.
   */
  @Test
  public void testBuildSnapshotSkipsMessagesWithNullLeaderMetadata() {
    DefaultPubSubMessage msg = putMsgNullLeader(0L, "wolf".getBytes(), "arctic-wolf".getBytes());

    LilyPadUtils.Snapshot<ComparablePubSubPosition> snapshot =
        buildSnapshotFromMessages(Collections.singletonList(msg));

    assertTrue(snapshot.keyRecords.isEmpty(), "PUT with null LeaderMetadata must not produce a key record");
  }

  /**
   * The running high-watermark must advance as records arrive from different regions, and each
   * KeyRecord must capture a snapshot of the HW at the moment it was written.
   *
   * <p>msg0: key="hawk", regionId=0, upstreamOffset=10 → HW after = {0: 10}
   * <p>msg1: key="wolf", regionId=1, upstreamOffset=20 → HW after = {0: 10, 1: 20}
   *
   * <p>hawk's record must carry HW={0: 10}; wolf's record must carry HW={0: 10, 1: 20}.
   */
  @Test
  public void testBuildSnapshotRunningHighWatermarkUpdatesPerMessage() {
    byte[] hawkKey = "hawk".getBytes();
    byte[] wolfKey = "wolf".getBytes();

    DefaultPubSubMessage msg0 = putMsg(0L, hawkKey, "val-a".getBytes(), 0, 10L, 1L);
    DefaultPubSubMessage msg1 = putMsg(1L, wolfKey, "val-b".getBytes(), 1, 20L, 2L);

    LilyPadUtils.Snapshot<ComparablePubSubPosition> snapshot = buildSnapshotFromMessages(Arrays.asList(msg0, msg1));

    LilyPadUtils.KeyRecord<ComparablePubSubPosition> hawkRec =
        snapshot.keyRecords.get(ByteUtils.hash64(hawkKey)).get(0);
    LilyPadUtils.KeyRecord<ComparablePubSubPosition> wolfRec =
        snapshot.keyRecords.get(ByteUtils.hash64(wolfKey)).get(0);

    assertEquals(hawkRec.highWatermark.size(), 2);
    assertEquals(hawkRec.highWatermark.get(0).toString(), ApacheKafkaOffsetPosition.of(10).toString());
    assertNull(hawkRec.highWatermark.get(1), "hawk HW must reflect only region-0 seen so far");
    assertEquals(wolfRec.highWatermark.size(), 2, "wolf HW must reflect both regions seen");
    assertEquals(wolfRec.highWatermark.get(0).toString(), ApacheKafkaOffsetPosition.of(10).toString());
    assertEquals(wolfRec.highWatermark.get(1).toString(), ApacheKafkaOffsetPosition.of(20).toString());
    assertEquals(snapshot.partitionHighWatermark.size(), 2);
    assertEquals(snapshot.partitionHighWatermark.get(0).toString(), ApacheKafkaOffsetPosition.of(10).toString());
    assertEquals(snapshot.partitionHighWatermark.get(1).toString(), ApacheKafkaOffsetPosition.of(20).toString());
  }

  /**
   * A DELETE message with valid LeaderMetadata should produce a KeyRecord with null valueHash
   * (tombstone) and still advance the running high-watermark.
   *
   * <p>msg0: PUT  key="wolf", value="arctic-wolf", regionId=0, upstreamOffset=10
   * <p>msg1: DELETE key="wolf", regionId=1, upstreamOffset=20
   *
   * <p>After msg1, wolf should have two history entries: a PUT (valueHash != null) and a DELETE (valueHash == null).
   * The HW should reflect both regions.
   */
  @Test
  public void testBuildSnapshotCapturesDeleteMessage() {
    byte[] keyBytes = "wolf".getBytes();

    DefaultPubSubMessage putMsg = putMsg(0L, keyBytes, "arctic-wolf".getBytes(), 0, 10L, 100L);
    DefaultPubSubMessage delMsg = deleteMsg(1L, keyBytes, 1, 20L, 200L);

    LilyPadUtils.Snapshot<ComparablePubSubPosition> snapshot = buildSnapshotFromMessages(Arrays.asList(putMsg, delMsg));

    List<LilyPadUtils.KeyRecord<ComparablePubSubPosition>> history =
        snapshot.keyRecords.get(ByteUtils.hash64(keyBytes));
    assertEquals(history.size(), 2, "Expected PUT + DELETE = 2 records");

    LilyPadUtils.KeyRecord<ComparablePubSubPosition> putRec = history.get(0);
    assertEquals(putRec.valueHash, Integer.valueOf(Arrays.hashCode("arctic-wolf".getBytes())));

    LilyPadUtils.KeyRecord<ComparablePubSubPosition> delRec = history.get(1);
    assertEquals(delRec.valueHash, null, "DELETE must produce null valueHash (tombstone)");
    assertEquals(delRec.upstreamRTPosition.size(), 2, "DELETE must update the position vector");
    assertEquals(snapshot.partitionHighWatermark.size(), 2, "DELETE must advance the partition HW");
  }

  // ── helpers ────────────────────────────────────────────────────────────────

  /** Creates a mock TopicManager whose comparePosition delegates to numeric offset comparison. */
  private TopicManager mockTopicManager() {
    TopicManager topicManager = mock(TopicManager.class);
    when(topicManager.comparePosition(any(), any(), any())).thenAnswer(inv -> {
      PubSubPosition p1 = inv.getArgument(1);
      PubSubPosition p2 = inv.getArgument(2);
      return p1.getNumericOffset() - p2.getNumericOffset();
    });
    return topicManager;
  }

  /** Runs buildSnapshot against a fixed list of messages using a mock consumer. */
  private LilyPadUtils.Snapshot<ComparablePubSubPosition> buildSnapshotFromMessages(
      List<DefaultPubSubMessage> messages) {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(repo.getTopic("store_v1"), 0);

    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
    when(consumer.poll(anyLong())).thenReturn(Collections.singletonMap(tp, messages));
    when(consumer.positionDifference(any(), any(), any())).thenAnswer(inv -> {
      PubSubPosition p1 = inv.getArgument(1);
      PubSubPosition p2 = inv.getArgument(2);
      return p1.getNumericOffset() - p2.getNumericOffset();
    });

    PubSubPosition begin = ApacheKafkaOffsetPosition.of(0);
    PubSubPosition end = ApacheKafkaOffsetPosition.of(messages.size());
    PubSubPartitionSplit split = new PubSubPartitionSplit(repo, tp, begin, end, messages.size(), 0, 0);

    TopicManager topicManager = mockTopicManager();
    PubSubPositionDeserializer deserializer =
        new PubSubPositionDeserializer(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY);

    try (PubSubSplitIterator iterator = new PubSubSplitIterator(consumer, split, false)) {
      return LilyPadSnapshotBuilder.buildSnapshot(iterator, topicManager, deserializer, 2);
    }
  }

  /** Builds a mock PUT message with valid LeaderMetadata. */
  private DefaultPubSubMessage putMsg(
      long vtOffset,
      byte[] keyBytes,
      byte[] valueBytes,
      int coloId,
      long upstreamOffset,
      long logicalTs) {
    KafkaMessageEnvelope kme = new KafkaMessageEnvelope();
    kme.messageType = MessageType.PUT.getValue();

    ProducerMetadata pm = new ProducerMetadata();
    pm.logicalTimestamp = logicalTs;
    kme.producerMetadata = pm;

    LeaderMetadata lm = new LeaderMetadata();
    lm.upstreamKafkaClusterId = coloId;
    lm.upstreamOffset = upstreamOffset;
    lm.upstreamPubSubPosition = ApacheKafkaOffsetPosition.of(upstreamOffset).toWireFormatBuffer();
    kme.leaderMetadataFooter = lm;

    Put put = new Put();
    put.putValue = ByteBuffer.wrap(valueBytes);
    kme.payloadUnion = put;

    KafkaKey key = mock(KafkaKey.class);
    when(key.isControlMessage()).thenReturn(false);
    when(key.getKey()).thenReturn(keyBytes);

    DefaultPubSubMessage msg = mock(DefaultPubSubMessage.class);
    when(msg.getKey()).thenReturn(key);
    when(msg.getValue()).thenReturn(kme);
    when(msg.getPosition()).thenReturn(ApacheKafkaOffsetPosition.of(vtOffset));
    return msg;
  }

  /** Builds a mock PUT message with null LeaderMetadata (should be skipped by buildSnapshot). */
  private DefaultPubSubMessage putMsgNullLeader(long vtOffset, byte[] keyBytes, byte[] valueBytes) {
    KafkaMessageEnvelope kme = new KafkaMessageEnvelope();
    kme.messageType = MessageType.PUT.getValue();
    kme.producerMetadata = new ProducerMetadata();
    kme.leaderMetadataFooter = null;
    Put put = new Put();
    put.putValue = ByteBuffer.wrap(valueBytes);
    kme.payloadUnion = put;

    KafkaKey key = mock(KafkaKey.class);
    when(key.isControlMessage()).thenReturn(false);
    when(key.getKey()).thenReturn(keyBytes);

    DefaultPubSubMessage msg = mock(DefaultPubSubMessage.class);
    when(msg.getKey()).thenReturn(key);
    when(msg.getValue()).thenReturn(kme);
    when(msg.getPosition()).thenReturn(ApacheKafkaOffsetPosition.of(vtOffset));
    return msg;
  }

  /** Builds a mock DELETE message with valid LeaderMetadata. */
  private DefaultPubSubMessage deleteMsg(
      long vtOffset,
      byte[] keyBytes,
      int coloId,
      long upstreamOffset,
      long logicalTs) {
    KafkaMessageEnvelope kme = new KafkaMessageEnvelope();
    kme.messageType = MessageType.DELETE.getValue();

    ProducerMetadata pm = new ProducerMetadata();
    pm.logicalTimestamp = logicalTs;
    kme.producerMetadata = pm;

    LeaderMetadata lm = new LeaderMetadata();
    lm.upstreamKafkaClusterId = coloId;
    lm.upstreamOffset = upstreamOffset;
    lm.upstreamPubSubPosition = ApacheKafkaOffsetPosition.of(upstreamOffset).toWireFormatBuffer();
    kme.leaderMetadataFooter = lm;

    kme.payloadUnion = new Delete();

    KafkaKey key = mock(KafkaKey.class);
    when(key.isControlMessage()).thenReturn(false);
    when(key.getKey()).thenReturn(keyBytes);

    DefaultPubSubMessage msg = mock(DefaultPubSubMessage.class);
    when(msg.getKey()).thenReturn(key);
    when(msg.getValue()).thenReturn(kme);
    when(msg.getPosition()).thenReturn(ApacheKafkaOffsetPosition.of(vtOffset));
    return msg;
  }

  /** Builds a mock control message (should be skipped by buildSnapshot). */
  private DefaultPubSubMessage controlMsg(long vtOffset) {
    KafkaKey key = mock(KafkaKey.class);
    when(key.isControlMessage()).thenReturn(true);

    DefaultPubSubMessage msg = mock(DefaultPubSubMessage.class);
    when(msg.getKey()).thenReturn(key);
    when(msg.getPosition()).thenReturn(ApacheKafkaOffsetPosition.of(vtOffset));
    return msg;
  }
}
