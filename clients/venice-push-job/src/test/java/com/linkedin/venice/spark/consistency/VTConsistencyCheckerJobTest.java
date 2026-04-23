package com.linkedin.venice.spark.consistency;

import static com.linkedin.venice.spark.consistency.VTConsistencyCheckerJob.OUTPUT_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ENABLE_SSL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfPush;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
    PubSubConsumerAdapter consumer = mockConsumer();
    PubSubTopicPartition tp = mockPartition();

    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc0 = new LilyPadUtils.KeyRecord<>(
        100,
        Arrays.asList(cmp(5, consumer, tp), cmp(10, consumer, tp)),
        Arrays.asList(cmp(50, consumer, tp), cmp(60, consumer, tp)),
        200L,
        cmp(42, consumer, tp));
    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc1 = new LilyPadUtils.KeyRecord<>(
        200,
        Arrays.asList(cmp(10, consumer, tp), cmp(15, consumer, tp)),
        Arrays.asList(cmp(20, consumer, tp), cmp(30, consumer, tp)),
        180L,
        cmp(17, consumer, tp));
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
    PubSubConsumerAdapter consumer = mockConsumer();
    PubSubTopicPartition tp = mockPartition();

    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc1 = new LilyPadUtils.KeyRecord<>(
        200,
        Arrays.asList(cmp(10, consumer, tp), cmp(15, consumer, tp)),
        Arrays.asList(cmp(20, consumer, tp), cmp(30, consumer, tp)),
        180L,
        cmp(17, consumer, tp));
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
    PubSubConsumerAdapter consumer = mockConsumer();
    PubSubTopicPartition tp = mockPartition();

    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc0 = new LilyPadUtils.KeyRecord<>(
        100,
        Arrays.asList(cmp(5, consumer, tp), cmp(10, consumer, tp)),
        Arrays.asList(cmp(50, consumer, tp), cmp(60, consumer, tp)),
        200L,
        cmp(42, consumer, tp));
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
    PubSubConsumerAdapter consumer = mockConsumer();
    PubSubTopicPartition tp = mockPartition();

    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc0 = new LilyPadUtils.KeyRecord<>(
        100,
        Arrays.asList(cmp(5, consumer, tp), cmp(10, consumer, tp)),
        Arrays.asList(cmp(50, consumer, tp), cmp(60, consumer, tp)),
        200L,
        cmp(42, consumer, tp));
    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc1 = new LilyPadUtils.KeyRecord<>(
        null,
        Arrays.asList(cmp(10, consumer, tp), cmp(15, consumer, tp)),
        Arrays.asList(cmp(20, consumer, tp), cmp(30, consumer, tp)),
        180L,
        cmp(17, consumer, tp));
    LilyPadUtils.Inconsistency<ComparablePubSubPosition> inc =
        new LilyPadUtils.Inconsistency<>(111L, LilyPadUtils.InconsistencyType.VALUE_MISMATCH, dc0, dc1);

    Row row = VTConsistencyCheckerJob.toRow(inc, "store_v3", 5);

    assertEquals(row.getInt(4), 100, "dc0_value_hash");
    assertNull(row.get(5), "dc1_value_hash must be null for DELETE");
  }

  // ── batchFetchSplits ──────────────────────────────────────────────────────

  // ── findEndOfPushPosition ──────────────────────────────────────────────────

  /**
   * When the VT contains an EOP control message, findEndOfPushPosition should return its position.
   */
  @Test
  public void testFindEndOfPushPositionReturnsEOPPosition() {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(repo.getTopic("store_v1"), 0);

    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
    when(consumer.getAssignment()).thenReturn(Collections.emptySet());
    doNothing().when(consumer).batchUnsubscribe(any());
    doNothing().when(consumer).subscribe(any(), any(), anyBoolean());
    when(consumer.beginningPosition(tp)).thenReturn(ApacheKafkaOffsetPosition.of(0));

    // First poll returns a PUT, second poll returns EOP at offset 5
    DefaultPubSubMessage putMsg = mockPutMessage(ApacheKafkaOffsetPosition.of(0));
    DefaultPubSubMessage eopMsg = mockEOPMessage(ApacheKafkaOffsetPosition.of(5));
    when(consumer.poll(anyLong())).thenReturn(Collections.singletonMap(tp, Collections.singletonList(putMsg)))
        .thenReturn(Collections.singletonMap(tp, Collections.singletonList(eopMsg)));

    PubSubPosition result = VTConsistencyCheckerJob.findEndOfPushPosition(consumer, tp);
    assertEquals(result, ApacheKafkaOffsetPosition.of(5), "Should return the EOP position");
  }

  /**
   * When the VT has no EOP control message, findEndOfPushPosition should throw
   * IllegalStateException instead of looping forever.
   */
  @Test(expectedExceptions = IllegalStateException.class)
  public void testFindEndOfPushPositionThrowsWhenNoEOP() {
    PubSubTopicRepository repo = new PubSubTopicRepository();
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(repo.getTopic("store_v1"), 0);

    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
    when(consumer.getAssignment()).thenReturn(Collections.emptySet());
    doNothing().when(consumer).batchUnsubscribe(any());
    doNothing().when(consumer).subscribe(any(), any(), anyBoolean());
    when(consumer.beginningPosition(tp)).thenReturn(ApacheKafkaOffsetPosition.of(0));

    // Only PUT messages, then empty polls exceeding retry limit — no EOP found
    DefaultPubSubMessage putMsg = mockPutMessage(ApacheKafkaOffsetPosition.of(2));
    when(consumer.poll(anyLong())).thenReturn(Collections.singletonMap(tp, Collections.singletonList(putMsg)))
        .thenReturn(Collections.emptyMap())
        .thenReturn(Collections.emptyMap())
        .thenReturn(Collections.emptyMap());

    VTConsistencyCheckerJob.findEndOfPushPosition(consumer, tp);
  }

  private static DefaultPubSubMessage mockEOPMessage(PubSubPosition position) {
    KafkaMessageEnvelope kme = new KafkaMessageEnvelope();
    kme.messageType = MessageType.CONTROL_MESSAGE.getValue();
    kme.producerMetadata = new ProducerMetadata();
    ControlMessage cm = new ControlMessage();
    cm.controlMessageType = ControlMessageType.END_OF_PUSH.getValue();
    cm.controlMessageUnion = new EndOfPush();
    cm.debugInfo = Collections.emptyMap();
    kme.payloadUnion = cm;

    DefaultPubSubMessage msg = mock(DefaultPubSubMessage.class);
    when(msg.getValue()).thenReturn(kme);
    when(msg.getPosition()).thenReturn(position);
    return msg;
  }

  private static DefaultPubSubMessage mockPutMessage(PubSubPosition position) {
    KafkaMessageEnvelope kme = new KafkaMessageEnvelope();
    kme.messageType = MessageType.PUT.getValue();
    kme.producerMetadata = new ProducerMetadata();
    kme.payloadUnion = new Put();

    DefaultPubSubMessage msg = mock(DefaultPubSubMessage.class);
    when(msg.getValue()).thenReturn(kme);
    when(msg.getPosition()).thenReturn(position);
    return msg;
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

  private static ComparablePubSubPosition cmp(long offset, PubSubConsumerAdapter consumer, PubSubTopicPartition tp) {
    return new ComparablePubSubPosition(ApacheKafkaOffsetPosition.of(offset), consumer, tp);
  }

  private static PubSubConsumerAdapter mockConsumer() {
    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
    when(consumer.comparePositions(any(), any(), any())).thenAnswer(inv -> {
      PubSubPosition p1 = inv.getArgument(1);
      PubSubPosition p2 = inv.getArgument(2);
      return p1.getNumericOffset() - p2.getNumericOffset();
    });
    return consumer;
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

  // ── versionTopicFromStoreResponse ─────────────────────────────────────────

  /**
   * Happy path: a successful {@link StoreResponse} with a positive current version yields a
   * version topic composed as {@code storeName_v<currentVersion>}.
   */
  @Test
  public void testVersionTopicFromStoreResponseComposesTopicFromCurrentVersion() {
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setCurrentVersion(7);
    StoreResponse response = new StoreResponse();
    response.setStore(storeInfo);

    String versionTopic =
        VTConsistencyCheckerJob.versionTopicFromStoreResponse("my-store", "http://controller.example:1234", response);
    assertEquals(versionTopic, "my-store_v7");
  }

  /**
   * If the controller response is an error, the helper must throw a VeniceException that
   * includes the store name and the controller-reported error.
   */
  @Test
  public void testVersionTopicFromStoreResponseThrowsOnErrorResponse() {
    StoreResponse response = new StoreResponse();
    response.setError("store does not exist");

    VeniceException ex = expectThrows(
        VeniceException.class,
        () -> VTConsistencyCheckerJob
            .versionTopicFromStoreResponse("missing-store", "http://controller.example:1234", response));
    assertTrue(ex.getMessage().contains("missing-store"), "error message should contain store name");
    assertTrue(ex.getMessage().contains("store does not exist"), "error message should contain controller error");
  }

  /**
   * If the store exists but has no current version yet (e.g. never pushed), the helper must
   * fail fast rather than construct an invalid version topic like {@code store_v0}.
   */
  @Test
  public void testVersionTopicFromStoreResponseThrowsWhenNoCurrentVersion() {
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setCurrentVersion(0);
    StoreResponse response = new StoreResponse();
    response.setStore(storeInfo);

    VeniceException ex = expectThrows(
        VeniceException.class,
        () -> VTConsistencyCheckerJob
            .versionTopicFromStoreResponse("never-pushed", "http://controller.example:1234", response));
    assertTrue(ex.getMessage().contains("no current version"), "error message should mention missing version");
  }

  /**
   * If the controller returns a non-error response but with a null store payload, the helper must
   * fail with a clear message rather than NPE when dereferencing {@code getStore()}.
   */
  @Test
  public void testVersionTopicFromStoreResponseThrowsOnNullStorePayload() {
    StoreResponse response = new StoreResponse();
    // no setStore(...) call → getStore() returns null

    VeniceException ex = expectThrows(
        VeniceException.class,
        () -> VTConsistencyCheckerJob
            .versionTopicFromStoreResponse("null-payload-store", "http://controller.example:1234", response));
    assertTrue(ex.getMessage().contains("null-payload-store"), "error message should contain store name");
    assertTrue(ex.getMessage().contains("no store payload"), "error message should describe the missing payload");
  }

  /**
   * A negative {@code currentVersion} (e.g. a disabled or deleted store reported with a sentinel
   * value) must also trip the guard. Covers the negative branch of the {@code <= 0} check that
   * the zero-version test cannot exercise on its own.
   */
  @Test
  public void testVersionTopicFromStoreResponseThrowsWhenCurrentVersionIsNegative() {
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setCurrentVersion(-1);
    StoreResponse response = new StoreResponse();
    response.setStore(storeInfo);

    VeniceException ex = expectThrows(
        VeniceException.class,
        () -> VTConsistencyCheckerJob
            .versionTopicFromStoreResponse("disabled-store", "http://controller.example:1234", response));
    assertTrue(ex.getMessage().contains("no current version"), "error message should mention missing version");
    assertTrue(ex.getMessage().contains("-1"), "error message should include the negative sentinel value");
  }

  // ── buildControllerSSLFactory ────────────────────────────────────────────

  /**
   * SSL explicitly disabled: the factory must be absent so the controller lookup falls back to
   * plain HTTP. Guards against accidentally enabling SSL and reading Hadoop token files in
   * non-SSL deployments (integration test path).
   */
  @Test
  public void testBuildControllerSSLFactoryReturnsEmptyWhenSslDisabledExplicitly() {
    Properties props = new Properties();
    props.setProperty(ENABLE_SSL, "false");
    assertFalse(
        VTConsistencyCheckerJob.buildControllerSSLFactory(props).isPresent(),
        "factory should be empty when SSL is explicitly disabled");
  }

  /**
   * SSL key absent altogether: defaults must resolve to "disabled" so the helper doesn't attempt
   * to read Hadoop tokens that don't exist in most test and local setups.
   */
  @Test
  public void testBuildControllerSSLFactoryReturnsEmptyWhenSslEnableKeyAbsent() {
    Properties props = new Properties();
    // Intentionally no ENABLE_SSL key — must fall back to DEFAULT_SSL_ENABLED (false).
    assertFalse(
        VTConsistencyCheckerJob.buildControllerSSLFactory(props).isPresent(),
        "factory should be empty when the ENABLE_SSL key is absent");
  }
}
