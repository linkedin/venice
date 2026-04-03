package com.linkedin.venice.spark.consistency;

import static com.linkedin.venice.spark.consistency.VTConsistencyCheckerJob.OUTPUT_SCHEMA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.util.LongAccumulator;
import org.testng.annotations.Test;


public class VTConsistencyCheckerJobTest {

  // ── toRow ──────────────────────────────────────────────────────────────────
  /**
   * For a VALUE_MISMATCH inconsistency both records are present, so every field in
   * the output row must be non-null and carry the correct value from its record.
   */
  @Test
  public void testToRowValueMismatchPopulatesAllFields() {
    TopicManager tm = mockTopicManager();
    PubSubTopicPartition tp = mockPartition();

    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc0 = new LilyPadUtils.KeyRecord<>(
        100,
        Arrays.asList(cmp(5, tm, tp), cmp(10, tm, tp)),
        Arrays.asList(cmp(50, tm, tp), cmp(60, tm, tp)),
        200L,
        cmp(42, tm, tp));
    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc1 = new LilyPadUtils.KeyRecord<>(
        200,
        Arrays.asList(cmp(10, tm, tp), cmp(15, tm, tp)),
        Arrays.asList(cmp(20, tm, tp), cmp(30, tm, tp)),
        180L,
        cmp(17, tm, tp));
    LilyPadUtils.Inconsistency<ComparablePubSubPosition> inc =
        new LilyPadUtils.Inconsistency<>(123L, LilyPadUtils.InconsistencyType.VALUE_MISMATCH, dc0, dc1);

    Row row = VTConsistencyCheckerJob.toRow(inc, "store_v3", 7);

    assertEquals(row.getString(0), "store_v3", "version_topic");
    assertEquals(row.getInt(1), 7, "vt_partition");
    assertEquals(row.getString(2), "VALUE_MISMATCH", "type");
    assertEquals(row.getLong(3), 123L, "key_hash");
    assertEquals(row.getInt(4), 100, "dc0_value_hash");
    assertEquals(row.getInt(5), 200, "dc1_value_hash");
    assertEquals(row.getLong(10), 200L, "dc0_logical_ts");
    assertEquals(row.getLong(11), 180L, "dc1_logical_ts");
  }

  /**
   * For a MISSING_IN_DC0 inconsistency dc0Record is null, so all dc0 columns must
   * be null. The dc1 columns must still carry the correct values.
   */
  @Test
  public void testToRowMissingInDc0NullsDc0Columns() {
    TopicManager tm = mockTopicManager();
    PubSubTopicPartition tp = mockPartition();

    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc1 = new LilyPadUtils.KeyRecord<>(
        200,
        Arrays.asList(cmp(10, tm, tp), cmp(15, tm, tp)),
        Arrays.asList(cmp(20, tm, tp), cmp(30, tm, tp)),
        180L,
        cmp(17, tm, tp));
    LilyPadUtils.Inconsistency<ComparablePubSubPosition> inc =
        new LilyPadUtils.Inconsistency<>(456L, LilyPadUtils.InconsistencyType.MISSING_IN_DC0, null, dc1);

    Row row = VTConsistencyCheckerJob.toRow(inc, "store_v3", 0);

    assertEquals(row.getString(2), "MISSING_IN_DC0", "type");
    assertNull(row.get(4), "dc0_value_hash must be null");
    assertNull(row.get(6), "dc0_position_vector must be null");
    assertNull(row.get(8), "dc0_high_watermark must be null");
    assertNull(row.get(10), "dc0_logical_ts must be null");
    assertNull(row.get(12), "dc0_vt_position must be null");
    assertEquals(row.getInt(5), 200, "dc1_value_hash");
    assertEquals(row.getLong(11), 180L, "dc1_logical_ts");
  }

  /**
   * For a MISSING_IN_DC1 inconsistency dc1Record is null, so all dc1 columns must be null.
   */
  @Test
  public void testToRowMissingInDc1NullsDc1Columns() {
    TopicManager tm = mockTopicManager();
    PubSubTopicPartition tp = mockPartition();

    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc0 = new LilyPadUtils.KeyRecord<>(
        100,
        Arrays.asList(cmp(5, tm, tp), cmp(10, tm, tp)),
        Arrays.asList(cmp(50, tm, tp), cmp(60, tm, tp)),
        200L,
        cmp(42, tm, tp));
    LilyPadUtils.Inconsistency<ComparablePubSubPosition> inc =
        new LilyPadUtils.Inconsistency<>(789L, LilyPadUtils.InconsistencyType.MISSING_IN_DC1, dc0, null);

    Row row = VTConsistencyCheckerJob.toRow(inc, "store_v3", 0);

    assertEquals(row.getString(2), "MISSING_IN_DC1", "type");
    assertEquals(row.getInt(4), 100, "dc0_value_hash");
    assertEquals(row.getLong(10), 200L, "dc0_logical_ts");
    assertNull(row.get(5), "dc1_value_hash must be null");
    assertNull(row.get(7), "dc1_position_vector must be null");
    assertNull(row.get(9), "dc1_high_watermark must be null");
    assertNull(row.get(11), "dc1_logical_ts must be null");
    assertNull(row.get(13), "dc1_vt_position must be null");
  }

  /**
   * DELETE tombstone (null valueHash) must not cause NPE in toRow.
   */
  @Test
  public void testToRowHandlesDeleteTombstone() {
    TopicManager tm = mockTopicManager();
    PubSubTopicPartition tp = mockPartition();

    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc0 = new LilyPadUtils.KeyRecord<>(
        100,
        Arrays.asList(cmp(5, tm, tp), cmp(10, tm, tp)),
        Arrays.asList(cmp(50, tm, tp), cmp(60, tm, tp)),
        200L,
        cmp(42, tm, tp));
    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc1 = new LilyPadUtils.KeyRecord<>(
        null,
        Arrays.asList(cmp(10, tm, tp), cmp(15, tm, tp)),
        Arrays.asList(cmp(20, tm, tp), cmp(30, tm, tp)),
        180L,
        cmp(17, tm, tp));
    LilyPadUtils.Inconsistency<ComparablePubSubPosition> inc =
        new LilyPadUtils.Inconsistency<>(111L, LilyPadUtils.InconsistencyType.VALUE_MISMATCH, dc0, dc1);

    Row row = VTConsistencyCheckerJob.toRow(inc, "store_v3", 5);

    assertEquals(row.getInt(4), 100, "dc0_value_hash");
    assertNull(row.get(5), "dc1_value_hash must be null for DELETE");
  }

  // ── batchFetchSplits ──────────────────────────────────────────────────────

  /**
   * batchFetchSplits should create one PubSubPartitionSplit per partition with correct
   * start/end positions and estimated record count.
   */
  @Test
  public void testBatchFetchSplitsCreatesOneSplitPerPartition() {
    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    PubSubTopic topic = topicRepository.getTopic("store_v1");
    PubSubTopicPartition tp0 = new PubSubTopicPartitionImpl(topic, 0);
    PubSubTopicPartition tp1 = new PubSubTopicPartitionImpl(topic, 1);

    PubSubPosition start0 = ApacheKafkaOffsetPosition.of(0);
    PubSubPosition end0 = ApacheKafkaOffsetPosition.of(100);
    PubSubPosition start1 = ApacheKafkaOffsetPosition.of(5);
    PubSubPosition end1 = ApacheKafkaOffsetPosition.of(50);

    Map<PubSubTopicPartition, PubSubPosition> startPositions = new HashMap<>();
    startPositions.put(tp0, start0);
    startPositions.put(tp1, start1);
    Map<PubSubTopicPartition, PubSubPosition> endPositions = new HashMap<>();
    endPositions.put(tp0, end0);
    endPositions.put(tp1, end1);

    TopicManager tm = mock(TopicManager.class);
    when(tm.getStartPositionsForTopicWithRetries(topic)).thenReturn(startPositions);
    when(tm.getEndPositionsForTopicWithRetries(topic)).thenReturn(endPositions);
    when(tm.diffPosition(any(), any(), any())).thenAnswer(inv -> {
      PubSubPosition p1 = inv.getArgument(1);
      PubSubPosition p2 = inv.getArgument(2);
      return p1.getNumericOffset() - p2.getNumericOffset();
    });

    Map<Integer, PubSubPartitionSplit> splits = VTConsistencyCheckerJob.batchFetchSplits(tm, topic, topicRepository, 2);

    assertEquals(splits.size(), 2, "should have one split per partition");
    assertEquals(splits.get(0).getPartitionNumber(), 0);
    assertEquals(splits.get(0).getNumberOfRecords(), 100L);
    assertEquals(splits.get(1).getPartitionNumber(), 1);
    assertEquals(splits.get(1).getNumberOfRecords(), 45L);
  }

  // ── OUTPUT_SCHEMA ──────────────────────────────────────────────────────────

  /**
   * The schema is the public contract between this job and downstream Parquet readers.
   * Verify field count, names, types, and nullability.
   */
  @Test
  public void testOutputSchemaContractIsStable() {
    assertEquals(OUTPUT_SCHEMA.length(), 14, "schema must have exactly 14 fields");

    assertField(0, "version_topic", DataTypes.StringType, false);
    assertField(1, "vt_partition", DataTypes.IntegerType, false);
    assertField(2, "type", DataTypes.StringType, false);
    assertField(3, "key_hash", DataTypes.LongType, false);
    assertField(4, "dc0_value_hash", DataTypes.IntegerType, true);
    assertField(5, "dc1_value_hash", DataTypes.IntegerType, true);
    assertField(6, "dc0_position_vector", DataTypes.StringType, true);
    assertField(7, "dc1_position_vector", DataTypes.StringType, true);
    assertField(8, "dc0_high_watermark", DataTypes.StringType, true);
    assertField(9, "dc1_high_watermark", DataTypes.StringType, true);
    assertField(10, "dc0_logical_ts", DataTypes.LongType, true);
    assertField(11, "dc1_logical_ts", DataTypes.LongType, true);
    assertField(12, "dc0_vt_position", DataTypes.StringType, true);
    assertField(13, "dc1_vt_position", DataTypes.StringType, true);
  }

  // ── findInconsistenciesForPartition error handling ─────────────────────────

  /**
   * When an exception is thrown while scanning a partition (e.g. broker unreachable),
   * the method must not propagate the exception. Instead it emits one sentinel ERROR row,
   * increments partitionsWithErrors, and leaves partitionsProcessed untouched.
   */
  @Test
  public void testFindInconsistenciesForPartitionReturnsErrorRowOnException() {
    LongAccumulator partitionsProcessed = mock(LongAccumulator.class);
    LongAccumulator partitionsWithErrors = mock(LongAccumulator.class);

    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(repo.getTopic("store_v1"), 3);
    PubSubPosition pos = ApacheKafkaOffsetPosition.of(0);
    PubSubPartitionSplit dc0Split = new PubSubPartitionSplit(repo, tp, pos, pos, 0, 0, 0);
    PubSubPartitionSplit dc1Split = new PubSubPartitionSplit(repo, tp, pos, pos, 0, 0, 0);

    // Empty jobProps means DC0_BROKER_URL resolves to null, causing NPE inside brokerProps
    Iterator<Row> result = VTConsistencyCheckerJob.findInconsistenciesForPartition(
        dc0Split,
        dc1Split,
        new Properties(),
        3,
        partitionsProcessed,
        partitionsWithErrors);

    List<Row> rows = collectRows(result);
    assertEquals(rows.size(), 1, "exactly one sentinel ERROR row");

    Row errorRow = rows.get(0);
    assertEquals(errorRow.getString(0), "store_v1", "version_topic");
    assertEquals(errorRow.getInt(1), 3, "vt_partition");
    assertEquals(errorRow.getString(2), "ERROR", "type");
    for (int i = 4; i < OUTPUT_SCHEMA.length(); i++) {
      assertNull(errorRow.get(i), "field[" + i + "] must be null in ERROR row");
    }

    verify(partitionsWithErrors).add(1L);
    verify(partitionsProcessed, never()).add(anyLong());
  }

  // ── helpers ────────────────────────────────────────────────────────────────

  private static ComparablePubSubPosition cmp(long offset, TopicManager tm, PubSubTopicPartition tp) {
    return new ComparablePubSubPosition(ApacheKafkaOffsetPosition.of(offset), tm, tp);
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
    return mock(PubSubTopicPartition.class);
  }

  private static List<Row> collectRows(Iterator<Row> it) {
    List<Row> rows = new ArrayList<>();
    it.forEachRemaining(rows::add);
    return rows;
  }

  private static void assertField(int idx, String name, Object type, boolean nullable) {
    assertEquals(OUTPUT_SCHEMA.fields()[idx].name(), name, "field[" + idx + "] name");
    assertEquals(OUTPUT_SCHEMA.fields()[idx].dataType(), type, "field[" + idx + "] dataType");
    assertEquals(OUTPUT_SCHEMA.fields()[idx].nullable(), nullable, "field[" + idx + "] nullable");
  }
}
