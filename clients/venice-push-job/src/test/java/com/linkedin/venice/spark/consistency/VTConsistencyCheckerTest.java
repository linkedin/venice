package com.linkedin.venice.spark.consistency;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.testng.annotations.Test;


public class VTConsistencyCheckerTest {
  /**
   * The core scenario: two DCs both had full information (each DC's high-watermark covers
   * the other DC's per-key offset vector) yet ended up with different values.
   * This mirrors the hawk+wolf setup in the integration test.
   *
   * <p>DC-0: wolf=arctic-wolf, OV=[5,10], HW=[50,60], logicalTs=200
   * <p>DC-1: wolf=dire-wolf,   OV=[10,15], HW=[20,30], logicalTs=180
   *
   * <p>Comparability check:
   * <ul>
   *   <li>DC-0's HW[0]=50 >= DC-1's OV[0]=10 ✓</li>
   *   <li>DC-0's HW[1]=60 >= DC-1's OV[1]=15 ✓</li>
   *   <li>DC-1's HW[0]=20 >= DC-0's OV[0]=5  ✓</li>
   *   <li>DC-1's HW[1]=30 >= DC-0's OV[1]=10 ✓</li>
   * </ul>
   * Both records are comparable → value mismatch is a real inconsistency.
   */
  @Test
  public void testFindInconsistenciesDetectsValueMismatchWhenBothDCsHadFullInfo() {
    VTConsistencyChecker.KeyRecord dc0Wolf = new VTConsistencyChecker.KeyRecord(
        "arctic-wolf",
        Arrays.asList(ApacheKafkaOffsetPosition.of(5), ApacheKafkaOffsetPosition.of(10)),
        Arrays.asList(ApacheKafkaOffsetPosition.of(50), ApacheKafkaOffsetPosition.of(60)),
        200L,
        "pos:42");
    VTConsistencyChecker.KeyRecord dc1Wolf = new VTConsistencyChecker.KeyRecord(
        "dire-wolf",
        Arrays.asList(ApacheKafkaOffsetPosition.of(10), ApacheKafkaOffsetPosition.of(15)),
        Arrays.asList(ApacheKafkaOffsetPosition.of(20), ApacheKafkaOffsetPosition.of(30)),
        180L,
        "pos:17");

    Map<String, List<VTConsistencyChecker.KeyRecord>> dc0Map = new TreeMap<>();
    dc0Map.put("wolf", Collections.singletonList(dc0Wolf));
    Map<String, List<VTConsistencyChecker.KeyRecord>> dc1Map = new TreeMap<>();
    dc1Map.put("wolf", Collections.singletonList(dc1Wolf));

    VTConsistencyChecker.Snapshot dc0Snapshot = new VTConsistencyChecker.Snapshot(dc0Map, dc0Wolf.highWatermark);
    VTConsistencyChecker.Snapshot dc1Snapshot = new VTConsistencyChecker.Snapshot(dc1Map, dc1Wolf.highWatermark);

    TopicManager dc0TopicManager = mockTopicManager();
    TopicManager dc1TopicManager = mockTopicManager();
    PubSubTopicPartition partition = mockPartition();

    List<VTConsistencyChecker.Inconsistency> result =
        VTConsistencyChecker.findInconsistencies(dc0Snapshot, dc1Snapshot, dc0TopicManager, dc1TopicManager, partition);

    assertEquals(result.size(), 1, "Expected exactly one inconsistency");
    VTConsistencyChecker.Inconsistency inc = result.get(0);
    assertEquals(inc.type, VTConsistencyChecker.InconsistencyType.VALUE_MISMATCH);
    assertEquals(inc.key, "wolf");
    assertEquals(inc.dc0Record.valueHash, "arctic-wolf");
    assertEquals(inc.dc1Record.valueHash, "dire-wolf");
    assertTrue(
        inc.dc0Record.logicalTimestamp > inc.dc1Record.logicalTimestamp,
        "DC-0 should have the higher logicalTimestamp (the correct winner)");
  }

  /**
   * Replication lag should NOT be reported as an inconsistency.
   *
   * <p>DC-1's high-watermark for colo-1 is only 8, but DC-0's per-key offset for colo-1
   * is 10 — meaning DC-1 hadn't yet seen offset 10 from colo-1 when it wrote its record.
   * The two records are therefore not comparable; the difference is just lag, not a bug.
   *
   * <p>DC-0: wolf=arctic-wolf, OV=[5,10], HW=[50,60]
   * <p>DC-1: wolf=dire-wolf,   OV=[5, 3], HW=[20, 8]
   *
   * <p>Comparability check (fails):
   * <ul>
   *   <li>DC-1's HW[1]=8 < DC-0's OV[1]=10 → DC-1 hadn't seen DC-0's colo-1 offset yet</li>
   * </ul>
   * Records are not comparable → silently skipped, no inconsistency reported.
   */
  @Test
  public void testFindInconsistenciesSkipsRecordsWhenOneDCHadIncompleteInfo() {
    VTConsistencyChecker.KeyRecord dc0Wolf = new VTConsistencyChecker.KeyRecord(
        "arctic-wolf",
        Arrays.asList(ApacheKafkaOffsetPosition.of(5), ApacheKafkaOffsetPosition.of(10)),
        Arrays.asList(ApacheKafkaOffsetPosition.of(50), ApacheKafkaOffsetPosition.of(60)),
        200L,
        "pos:42");
    VTConsistencyChecker.KeyRecord dc1Wolf = new VTConsistencyChecker.KeyRecord(
        "dire-wolf",
        Arrays.asList(ApacheKafkaOffsetPosition.of(5), ApacheKafkaOffsetPosition.of(3)),
        Arrays.asList(ApacheKafkaOffsetPosition.of(20), ApacheKafkaOffsetPosition.of(8)),
        180L,
        "pos:17");

    Map<String, List<VTConsistencyChecker.KeyRecord>> dc0Map = new TreeMap<>();
    dc0Map.put("wolf", Collections.singletonList(dc0Wolf));
    Map<String, List<VTConsistencyChecker.KeyRecord>> dc1Map = new TreeMap<>();
    dc1Map.put("wolf", Collections.singletonList(dc1Wolf));

    VTConsistencyChecker.Snapshot dc0Snapshot = new VTConsistencyChecker.Snapshot(dc0Map, dc0Wolf.highWatermark);
    VTConsistencyChecker.Snapshot dc1Snapshot = new VTConsistencyChecker.Snapshot(dc1Map, dc1Wolf.highWatermark);

    TopicManager dc0TopicManager = mockTopicManager();
    TopicManager dc1TopicManager = mockTopicManager();
    PubSubTopicPartition partition = mockPartition();

    List<VTConsistencyChecker.Inconsistency> result =
        VTConsistencyChecker.findInconsistencies(dc0Snapshot, dc1Snapshot, dc0TopicManager, dc1TopicManager, partition);

    assertTrue(
        result.isEmpty(),
        "Replication lag (DC-1's HW didn't cover DC-0's offset) must not be reported as an inconsistency");
  }

  // ── buildSnapshot ──────────────────────────────────────────────────────────

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
      VTConsistencyChecker.Snapshot snapshot = VTConsistencyChecker.buildSnapshot(iterator, topicManager, deserializer);

      assertTrue(snapshot.keyRecords.isEmpty(), "Empty partition must yield no key records");
      assertEquals(
          snapshot.partitionHighWatermark,
          Arrays.asList(null, null),
          "HW must remain all null for empty partition");
      verify(consumer, never()).poll(anyLong());
    }
  }

  /**
   * A single PUT message with valid LeaderMetadata should produce exactly one KeyRecord with
   * the correct key hash, value hash, offset vector, high-watermark, logical timestamp, and
   * VT position.
   *
   * <p>Message: key="wolf", value="arctic-wolf", coloId=0, upstreamOffset=42, logicalTs=100, vtOffset=0
   *
   * <p>Expected KeyRecord:
   * <ul>
   *   <li>keyHash   = sha256("wolf")</li>
   *   <li>valueHash = sha256("arctic-wolf")</li>
   *   <li>OV        = [42, EARLIEST]   (colo-0 seen, colo-1 not yet)</li>
   *   <li>HW        = [42, EARLIEST]   (running max same as OV after first message)</li>
   *   <li>logicalTs = 100</li>
   *   <li>vtPosition = pos.toString() for offset 0</li>
   * </ul>
   */
  @Test
  public void testBuildSnapshotCapturesSinglePutMessage() {
    byte[] keyBytes = "wolf".getBytes();
    byte[] valBytes = "arctic-wolf".getBytes();
    DefaultPubSubMessage msg = putMsg(0L, keyBytes, valBytes, 0, 42L, 100L);

    VTConsistencyChecker.Snapshot snapshot = buildSnapshotFromMessages(Collections.singletonList(msg));

    assertEquals(snapshot.keyRecords.size(), 1);
    List<VTConsistencyChecker.KeyRecord> history = snapshot.keyRecords.get(DigestUtils.sha256Hex(keyBytes));
    assertEquals(history.size(), 1);

    VTConsistencyChecker.KeyRecord rec = history.get(0);
    assertEquals(rec.valueHash, DigestUtils.sha256Hex(valBytes));
    assertEquals(rec.upstreamRTOffset, Arrays.asList(ApacheKafkaOffsetPosition.of(42), null));
    assertEquals(rec.highWatermark, Arrays.asList(ApacheKafkaOffsetPosition.of(42), null));
    assertEquals(rec.logicalTimestamp, 100L);
    assertEquals(rec.vtPosition, ApacheKafkaOffsetPosition.of(0).toString());
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

    VTConsistencyChecker.Snapshot snapshot = buildSnapshotFromMessages(Arrays.asList(ctrl, data));

    assertEquals(snapshot.keyRecords.size(), 1, "Control message must not produce a key record");
  }

  /**
   * A PUT message whose LeaderMetadata is null should be skipped — it has no upstream offset
   * information and cannot be part of the lily-pad comparison.
   */
  @Test
  public void testBuildSnapshotSkipsMessagesWithNullLeaderMetadata() {
    DefaultPubSubMessage msg = putMsgNullLeader(0L, "wolf".getBytes(), "arctic-wolf".getBytes());

    VTConsistencyChecker.Snapshot snapshot = buildSnapshotFromMessages(Collections.singletonList(msg));

    assertTrue(snapshot.keyRecords.isEmpty(), "PUT with null LeaderMetadata must not produce a key record");
  }

  /**
   * The running high-watermark must advance as records arrive from different colos, and each
   * KeyRecord must capture a snapshot of the HW at the moment it was written.
   *
   * <p>msg0: key="hawk", coloId=0, upstreamOffset=10 → HW after = [10, EARLIEST]
   * <p>msg1: key="wolf", coloId=1, upstreamOffset=20 → HW after = [10, 20]
   *
   * <p>hawk's record must carry HW=[10,EARLIEST]; wolf's record must carry HW=[10,20].
   */
  @Test
  public void testBuildSnapshotRunningHighWatermarkUpdatesPerMessage() {
    byte[] hawkKey = "hawk".getBytes();
    byte[] wolfKey = "wolf".getBytes();

    DefaultPubSubMessage msg0 = putMsg(0L, hawkKey, "val-a".getBytes(), 0, 10L, 1L);
    DefaultPubSubMessage msg1 = putMsg(1L, wolfKey, "val-b".getBytes(), 1, 20L, 2L);

    VTConsistencyChecker.Snapshot snapshot = buildSnapshotFromMessages(Arrays.asList(msg0, msg1));

    VTConsistencyChecker.KeyRecord hawkRec = snapshot.keyRecords.get(DigestUtils.sha256Hex(hawkKey)).get(0);
    VTConsistencyChecker.KeyRecord wolfRec = snapshot.keyRecords.get(DigestUtils.sha256Hex(wolfKey)).get(0);

    assertEquals(
        hawkRec.highWatermark,
        Arrays.asList(ApacheKafkaOffsetPosition.of(10), null),
        "hawk HW must reflect only colo-0 seen so far");
    assertEquals(
        wolfRec.highWatermark,
        Arrays.asList(ApacheKafkaOffsetPosition.of(10), ApacheKafkaOffsetPosition.of(20)),
        "wolf HW must reflect both colos seen");
    assertEquals(
        snapshot.partitionHighWatermark,
        Arrays.asList(ApacheKafkaOffsetPosition.of(10), ApacheKafkaOffsetPosition.of(20)),
        "final partition HW must be max across all messages");
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

  /** Creates a mock PubSubTopicPartition for use in findInconsistencies calls. */
  private PubSubTopicPartition mockPartition() {
    return mock(PubSubTopicPartition.class);
  }

  /** Runs buildSnapshot against a fixed list of messages using a mock consumer. */
  private VTConsistencyChecker.Snapshot buildSnapshotFromMessages(List<DefaultPubSubMessage> messages) {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(repo.getTopic("store_v1"), 0);

    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
    when(consumer.poll(anyLong())).thenReturn(Collections.singletonMap(tp, messages));
    // positionDifference(tp, end, current) = end.offset - current.offset; used by hasNext()
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
      return VTConsistencyChecker.buildSnapshot(iterator, topicManager, deserializer);
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
