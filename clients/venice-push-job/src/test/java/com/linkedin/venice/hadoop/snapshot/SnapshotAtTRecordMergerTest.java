package com.linkedin.venice.hadoop.snapshot;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.replication.merge.StringAnnotatedStoreSchemaCache;
import com.linkedin.davinci.serializer.avro.MapOrderPreservingSerDeFactory;
import com.linkedin.venice.hadoop.snapshot.SnapshotAtTRecordMerger.KeyMergeState;
import com.linkedin.venice.hadoop.snapshot.SnapshotAtTRecordMerger.MergedRecord;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link SnapshotAtTRecordMerger}: folding RT PUT/UPDATE/DELETE onto a batch base via the real
 * {@link com.linkedin.davinci.replication.merge.MergeConflictResolver} must produce the merged value AND the
 * correct RMD, for both value-level and (write-compute) field-level stores.
 */
public class SnapshotAtTRecordMergerTest {
  private static final String STORE = "snapshot_test_store";
  private static final int VALUE_SCHEMA_ID = 1;
  private static final int UPDATE_PROTOCOL_VERSION = 1;
  private static final String VALUE_SCHEMA_STR = "{\n" + "  \"type\": \"record\",\n"
      + "  \"name\": \"SnapshotTestValue\",\n" + "  \"namespace\": \"com.linkedin.venice.hadoop.snapshot\",\n"
      + "  \"fields\": [\n" + "    {\"name\": \"field1\", \"type\": \"string\", \"default\": \"\"},\n"
      + "    {\"name\": \"field2\", \"type\": \"string\", \"default\": \"\"}\n" + "  ]\n" + "}";

  private Schema valueSchema;
  private Schema rmdSchema;
  private Schema updateSchema;
  private int rmdVersion;
  private ReadOnlySchemaRepository schemaRepository;
  private RmdSerDe rmdSerDe;

  @BeforeMethod
  public void setUp() {
    valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_STR);
    rmdVersion = RmdSchemaGenerator.getLatestVersion();
    rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema, rmdVersion);
    updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

    schemaRepository = mock(ReadOnlySchemaRepository.class);
    when(schemaRepository.getValueSchema(eq(STORE), anyInt()))
        .thenReturn(new SchemaEntry(VALUE_SCHEMA_ID, valueSchema));
    when(schemaRepository.getSupersetOrLatestValueSchema(STORE))
        .thenReturn(new SchemaEntry(VALUE_SCHEMA_ID, valueSchema));
    when(schemaRepository.getSupersetSchema(STORE)).thenReturn(new SchemaEntry(VALUE_SCHEMA_ID, valueSchema));
    when(schemaRepository.getReplicationMetadataSchema(eq(STORE), anyInt(), anyInt()))
        .thenReturn(new RmdSchemaEntry(VALUE_SCHEMA_ID, rmdVersion, rmdSchema));
    when(schemaRepository.getDerivedSchema(eq(STORE), anyInt(), anyInt()))
        .thenReturn(new DerivedSchemaEntry(VALUE_SCHEMA_ID, UPDATE_PROTOCOL_VERSION, updateSchema));

    // A separate RmdSerDe (same schemas) to read back the merged RMD bytes for assertions.
    rmdSerDe = new RmdSerDe(new StringAnnotatedStoreSchemaCache(STORE, schemaRepository), rmdVersion);
  }

  @Test
  public void testFoldRtPutOntoBatchBaseProducesValueAndRmd() {
    SnapshotAtTRecordMerger merger = new SnapshotAtTRecordMerger(schemaRepository, STORE, rmdVersion, false);
    KeyMergeState state = merger.seedFromBatch(serializeValue("batch", "b"), VALUE_SCHEMA_ID);

    // The batch base carries the sentinel RMD (timestamp 0).
    assertEquals(field(merger, state, "field1"), "batch");
    assertEquals(valueLevelRmdTimestamp(merger.finalizeRecord(state)), 0L);

    // An RT PUT at ts=1000 wins over the sentinel, and the merged record carries RMD timestamp 1000.
    merger.applyPut(state, serializeValue("rt", "b"), VALUE_SCHEMA_ID, 1000L, 0);
    assertEquals(field(merger, state, "field1"), "rt");
    assertEquals(valueLevelRmdTimestamp(merger.finalizeRecord(state)), 1000L);

    // A stale RT PUT at ts=500 is ignored (only possible because the RMD tracks the winning timestamp 1000).
    merger.applyPut(state, serializeValue("stale", "b"), VALUE_SCHEMA_ID, 500L, 0);
    assertEquals(field(merger, state, "field1"), "rt");
    assertEquals(valueLevelRmdTimestamp(merger.finalizeRecord(state)), 1000L);
  }

  @Test
  public void testFoldRtDeleteProducesTombstoneWithRmd() {
    SnapshotAtTRecordMerger merger = new SnapshotAtTRecordMerger(schemaRepository, STORE, rmdVersion, false);
    KeyMergeState state = merger.seedFromBatch(serializeValue("batch", "b"), VALUE_SCHEMA_ID);
    merger.applyPut(state, serializeValue("rt", "b"), VALUE_SCHEMA_ID, 1000L, 0);

    merger.applyDelete(state, 2000L, 0);
    assertTrue(state.isDeleted());

    MergedRecord record = merger.finalizeRecord(state);
    assertTrue(record.isDelete());
    assertNull(record.getValue());
    // A delete still carries an RMD tombstone (timestamp 2000) so later writes resolve against it.
    assertNotNull(record.getRmd());
    assertEquals(valueLevelRmdTimestamp(record), 2000L);

    // A PUT older than the delete is ignored.
    merger.applyPut(state, serializeValue("resurrect-but-stale", "b"), VALUE_SCHEMA_ID, 1500L, 0);
    assertTrue(state.isDeleted());
  }

  @Test
  public void testFoldRtPartialUpdateProducesFieldLevelRmd() {
    SnapshotAtTRecordMerger merger = new SnapshotAtTRecordMerger(schemaRepository, STORE, rmdVersion, true);
    KeyMergeState state = merger.seedFromBatch(serializeValue("a", "b"), VALUE_SCHEMA_ID);

    // Partial update sets only field1.
    GenericRecord update = new UpdateBuilderImpl(updateSchema).setNewFieldValue("field1", "updated").build();
    merger.applyUpdate(state, serializeUpdate(update), VALUE_SCHEMA_ID, UPDATE_PROTOCOL_VERSION, 1500L, 0);

    assertEquals(field(merger, state, "field1"), "updated"); // updated field
    assertEquals(field(merger, state, "field2"), "b"); // untouched field preserved from the batch base

    // RMD is field-level: field1's per-field timestamp is the update timestamp; field2 stays at the batch sentinel.
    GenericRecord timestampRecord = fieldLevelRmdTimestampRecord(merger.finalizeRecord(state));
    assertEquals(((Number) timestampRecord.get("field1")).longValue(), 1500L);
    assertEquals(((Number) timestampRecord.get("field2")).longValue(), 0L);
  }

  // ---- helpers ----

  private ByteBuffer serializeValue(String field1, String field2) {
    GenericRecord record = new GenericData.Record(valueSchema);
    record.put("field1", field1);
    record.put("field2", field2);
    return ByteBuffer.wrap(MapOrderPreservingSerDeFactory.<GenericRecord>getSerializer(valueSchema).serialize(record));
  }

  private ByteBuffer serializeUpdate(GenericRecord updateRecord) {
    return ByteBuffer
        .wrap(MapOrderPreservingSerDeFactory.<GenericRecord>getSerializer(updateSchema).serialize(updateRecord));
  }

  private String field(SnapshotAtTRecordMerger merger, KeyMergeState state, String fieldName) {
    GenericRecord value = (GenericRecord) MapOrderPreservingSerDeFactory.getDeserializer(valueSchema, valueSchema)
        .deserialize(state.getValueBytes());
    return value.get(fieldName).toString();
  }

  private long valueLevelRmdTimestamp(MergedRecord record) {
    GenericRecord rmd = rmdSerDe.deserializeRmdBytes(VALUE_SCHEMA_ID, VALUE_SCHEMA_ID, record.getRmd());
    return ((Number) rmd.get(RmdConstants.TIMESTAMP_FIELD_NAME)).longValue();
  }

  private GenericRecord fieldLevelRmdTimestampRecord(MergedRecord record) {
    GenericRecord rmd = rmdSerDe.deserializeRmdBytes(VALUE_SCHEMA_ID, VALUE_SCHEMA_ID, record.getRmd());
    return (GenericRecord) rmd.get(RmdConstants.TIMESTAMP_FIELD_NAME);
  }
}
