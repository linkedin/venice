package com.linkedin.venice.spark.consistency;

import static com.linkedin.venice.spark.consistency.VTConsistencyCheckerJob.DC0_BROKER_URL;
import static com.linkedin.venice.spark.consistency.VTConsistencyCheckerJob.DC1_BROKER_URL;
import static com.linkedin.venice.spark.consistency.VTConsistencyCheckerJob.OUTPUT_PATH;
import static com.linkedin.venice.spark.consistency.VTConsistencyCheckerJob.OUTPUT_SCHEMA;
import static com.linkedin.venice.spark.consistency.VTConsistencyCheckerJob.VERSION_TOPIC;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.spark.consistency.VTConsistencyChecker.Inconsistency;
import com.linkedin.venice.spark.consistency.VTConsistencyChecker.InconsistencyType;
import com.linkedin.venice.spark.consistency.VTConsistencyChecker.KeyRecord;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
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
  public void testToRowValueMismatchPopulatesAllFields() throws Exception {
    KeyRecord dc0 = new KeyRecord("arctic-wolf", pos(5, 10), pos(50, 60), 200L, "pos:42");
    KeyRecord dc1 = new KeyRecord("dire-wolf", pos(10, 15), pos(20, 30), 180L, "pos:17");
    Inconsistency inc = new Inconsistency("wolf", InconsistencyType.VALUE_MISMATCH, dc0, dc1);

    Row row = invokeToRow(inc, "store_v3", 7);

    assertEquals(row.getString(0), "store_v3", "version_topic");
    assertEquals(row.getInt(1), 7, "vt_partition");
    assertEquals(row.getString(2), "VALUE_MISMATCH", "type");
    assertEquals(row.getString(3), "wolf", "key");
    assertEquals(row.getString(4), "arctic-wolf", "dc0_value");
    assertEquals(row.getString(5), "dire-wolf", "dc1_value");
    assertEquals(row.get(6), toStringArr(pos(5, 10)), "dc0_offset_vector");
    assertEquals(row.get(7), toStringArr(pos(10, 15)), "dc1_offset_vector");
    assertEquals(row.get(8), toStringArr(pos(50, 60)), "dc0_high_watermark");
    assertEquals(row.get(9), toStringArr(pos(20, 30)), "dc1_high_watermark");
    assertEquals(row.getLong(10), 200L, "dc0_logical_ts");
    assertEquals(row.getLong(11), 180L, "dc1_logical_ts");
    assertEquals(row.getString(12), "pos:42", "dc0_vt_position");
    assertEquals(row.getString(13), "pos:17", "dc1_vt_position");
  }

  /**
   * For a MISSING_IN_DC0 inconsistency dc0Record is null, so all dc0 columns must
   * be null. The dc1 columns must still carry the correct values from dc1Record.
   */
  @Test
  public void testToRowMissingInDc0NullsDc0Columns() throws Exception {
    KeyRecord dc1 = new KeyRecord("dire-wolf", pos(10, 15), pos(20, 30), 180L, "pos:17");
    Inconsistency inc = new Inconsistency("wolf", InconsistencyType.MISSING_IN_DC0, null, dc1);

    Row row = invokeToRow(inc, "store_v3", 0);

    assertEquals(row.getString(2), "MISSING_IN_DC0", "type");
    assertNull(row.get(4), "dc0_value must be null when dc0Record is absent");
    assertNull(row.get(6), "dc0_offset_vector must be null when dc0Record is absent");
    assertNull(row.get(8), "dc0_high_watermark must be null when dc0Record is absent");
    assertNull(row.get(10), "dc0_logical_ts must be null when dc0Record is absent");
    // dc1 columns are populated
    assertEquals(row.getString(5), "dire-wolf", "dc1_value");
    assertEquals(row.getLong(11), 180L, "dc1_logical_ts");
  }

  // ── OUTPUT_SCHEMA ──────────────────────────────────────────────────────────

  /**
   * The schema is the public contract between this job and downstream Parquet readers.
   * Verify field count, names, types, and nullability so regressions are caught at
   * compile/test time rather than silently corrupting production output.
   */
  @Test
  public void testOutputSchemaContractIsStable() {
    assertEquals(OUTPUT_SCHEMA.length(), 14, "schema must have exactly 14 fields");

    // Non-nullable identity columns
    assertField(0, "version_topic", DataTypes.StringType, false);
    assertField(1, "vt_partition", DataTypes.IntegerType, false);
    assertField(2, "type", DataTypes.StringType, false);
    assertField(3, "key", DataTypes.StringType, false);

    // Nullable per-DC columns (null when the key is absent in that DC)
    assertField(4, "dc0_value", DataTypes.StringType, true);
    assertField(5, "dc1_value", DataTypes.StringType, true);
    assertField(6, "dc0_offset_vector", new ArrayType(DataTypes.StringType, false), true);
    assertField(7, "dc1_offset_vector", new ArrayType(DataTypes.StringType, false), true);
    assertField(8, "dc0_high_watermark", new ArrayType(DataTypes.StringType, false), true);
    assertField(9, "dc1_high_watermark", new ArrayType(DataTypes.StringType, false), true);
    assertField(10, "dc0_logical_ts", DataTypes.LongType, true);
    assertField(11, "dc1_logical_ts", DataTypes.LongType, true);
    assertField(12, "dc0_vt_position", DataTypes.StringType, true);
    assertField(13, "dc1_vt_position", DataTypes.StringType, true);
  }

  // ── parseArgs ──────────────────────────────────────────────────────────────

  @Test
  public void testParseArgsAllRequiredKeysPresent() {
    Properties props = VTConsistencyCheckerJob.parseArgs(
        new String[] { "dc0.broker.url=dc0:9092", "dc1.broker.url=dc1:9092", "version.topic=store_v3",
            "output.path=/tmp/out" });

    assertEquals(props.getProperty(DC0_BROKER_URL), "dc0:9092");
    assertEquals(props.getProperty(DC1_BROKER_URL), "dc1:9092");
    assertEquals(props.getProperty(VERSION_TOPIC), "store_v3");
    assertEquals(props.getProperty(OUTPUT_PATH), "/tmp/out");
  }

  @Test
  public void testParseArgsExtraPropsAreIncluded() {
    Properties props = VTConsistencyCheckerJob.parseArgs(
        new String[] { "dc0.broker.url=dc0:9092", "dc1.broker.url=dc1:9092", "version.topic=store_v3",
            "output.path=/tmp/out", "ssl.keystore.location=/path/to/ks.jks" });

    assertEquals(props.getProperty("ssl.keystore.location"), "/path/to/ks.jks");
  }

  @Test
  public void testParseArgsMissingRequiredKeyThrows() {
    // Missing output.path
    expectThrows(
        IllegalArgumentException.class,
        () -> VTConsistencyCheckerJob.parseArgs(
            new String[] { "dc0.broker.url=dc0:9092", "dc1.broker.url=dc1:9092", "version.topic=store_v3" }));
  }

  @Test
  public void testParseArgsMalformedArgThrows() {
    // No '=' in the argument
    expectThrows(
        IllegalArgumentException.class,
        () -> VTConsistencyCheckerJob.parseArgs(
            new String[] { "dc0.broker.url=dc0:9092", "dc1.broker.url=dc1:9092", "version.topic=store_v3",
                "output.path=/tmp/out", "ssl.keystore.location" }));
  }

  @Test
  public void testParseArgsValueWithEqualsSign() {
    // Value itself contains '=' — only the first '=' is the delimiter
    Properties props = VTConsistencyCheckerJob.parseArgs(
        new String[] { "dc0.broker.url=dc0:9092", "dc1.broker.url=dc1:9092", "version.topic=store_v3",
            "output.path=/tmp/a=b" });

    assertEquals(props.getProperty(OUTPUT_PATH), "/tmp/a=b");
  }

  // ── findInconsistenciesForPartition ────────────────────────────────────────

  /**
   * When an exception is thrown while scanning a partition (e.g. broker unreachable),
   * the method must not propagate the exception. Instead it emits one sentinel ERROR row
   * so the failure is visible in the Parquet output, increments partitionsWithErrors,
   * and leaves partitionsProcessed untouched.
   *
   * <p>Empty jobProps means DC0_BROKER_URL resolves to null, causing a NullPointerException
   * inside brokerProps — a cheap way to exercise the catch block without a real broker.
   */
  @Test
  public void testFindInconsistenciesForPartitionReturnsErrorRowOnException() throws Exception {
    LongAccumulator partitionsProcessed = mock(LongAccumulator.class);
    LongAccumulator partitionsWithErrors = mock(LongAccumulator.class);

    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(repo.getTopic("store_v1"), 3);
    PubSubPosition pos = ApacheKafkaOffsetPosition.of(0);
    PubSubPartitionSplit dc0Split = new PubSubPartitionSplit(repo, tp, pos, pos, 0, 0, 0);
    PubSubPartitionSplit dc1Split = new PubSubPartitionSplit(repo, tp, pos, pos, 0, 0, 0);

    Iterator<Row> result =
        invokeFindInconsistencies(dc0Split, dc1Split, new Properties(), partitionsProcessed, partitionsWithErrors);

    List<Row> rows = collectRows(result);
    assertEquals(rows.size(), 1, "exactly one sentinel ERROR row");

    Row errorRow = rows.get(0);
    assertEquals(errorRow.getString(0), "store_v1", "version_topic");
    assertEquals(errorRow.getInt(1), 3, "vt_partition");
    assertEquals(errorRow.getString(2), "ERROR", "type");
    // All per-DC columns must be null in an error row.
    for (int i = 4; i < OUTPUT_SCHEMA.length(); i++) {
      assertNull(errorRow.get(i), "field[" + i + "] must be null in ERROR row");
    }

    verify(partitionsWithErrors).add(1L);
    verify(partitionsProcessed, never()).add(anyLong());
  }

  // ── helpers ────────────────────────────────────────────────────────────────

  private static Row invokeToRow(Inconsistency inc, String versionTopic, int partition) throws Exception {
    Method m = VTConsistencyCheckerJob.class.getDeclaredMethod("toRow", Inconsistency.class, String.class, int.class);
    m.setAccessible(true);
    return (Row) m.invoke(null, inc, versionTopic, partition);
  }

  @SuppressWarnings("unchecked")
  private static Iterator<Row> invokeFindInconsistencies(
      PubSubPartitionSplit dc0Split,
      PubSubPartitionSplit dc1Split,
      Properties jobProps,
      LongAccumulator partitionsProcessed,
      LongAccumulator partitionsWithErrors) throws Exception {
    Method m = VTConsistencyCheckerJob.class.getDeclaredMethod(
        "findInconsistenciesForPartition",
        PubSubPartitionSplit.class,
        PubSubPartitionSplit.class,
        Properties.class,
        LongAccumulator.class,
        LongAccumulator.class);
    m.setAccessible(true);
    return (Iterator<Row>) m.invoke(null, dc0Split, dc1Split, jobProps, partitionsProcessed, partitionsWithErrors);
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

  /** Shorthand: create a List<PubSubPosition> from offset longs. */
  private static List<PubSubPosition> pos(long... offsets) {
    List<PubSubPosition> list = new ArrayList<>();
    for (long o: offsets) {
      list.add(ApacheKafkaOffsetPosition.of(o));
    }
    return list;
  }

  /** Convert a List<PubSubPosition> to a String[] matching the Parquet output format. */
  private static String[] toStringArr(List<PubSubPosition> positions) {
    return positions.stream().map(Object::toString).toArray(String[]::new);
  }
}
