package com.linkedin.venice.hadoop.snapshot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link SnapshotAtTPushExecutor}: per key, it seeds from the batch value, folds RT records via the
 * merger, and writes the merged value+RMD (or an RMD-carrying delete) to the writer.
 */
public class SnapshotAtTPushExecutorTest {
  private static final String STORE = "snapshot_exec_test";
  private static final String STRING_SCHEMA = "\"string\"";
  private static final int VALUE_SCHEMA_ID = 1;

  private VeniceAvroKafkaSerializer serializer;
  private SnapshotAtTRecordMerger merger;
  private VeniceCompressor noOpCompressor;

  @BeforeMethod
  public void setUp() {
    Schema valueSchema = new Schema.Parser().parse(STRING_SCHEMA);
    int rmdVersion = RmdSchemaGenerator.getLatestVersion();
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema, rmdVersion);
    ReadOnlySchemaRepository repository = mock(ReadOnlySchemaRepository.class);
    when(repository.getValueSchema(anyString(), anyInt())).thenReturn(new SchemaEntry(VALUE_SCHEMA_ID, valueSchema));
    when(repository.getSupersetOrLatestValueSchema(anyString()))
        .thenReturn(new SchemaEntry(VALUE_SCHEMA_ID, valueSchema));
    when(repository.getSupersetSchema(anyString())).thenReturn(new SchemaEntry(VALUE_SCHEMA_ID, valueSchema));
    when(repository.getReplicationMetadataSchema(anyString(), anyInt(), anyInt()))
        .thenReturn(new RmdSchemaEntry(VALUE_SCHEMA_ID, rmdVersion, rmdSchema));
    merger = new SnapshotAtTRecordMerger(repository, STORE, rmdVersion, false);
    serializer = new VeniceAvroKafkaSerializer(STRING_SCHEMA);
    noOpCompressor = new CompressorFactory().getCompressor(CompressionStrategy.NO_OP);
  }

  @Test
  public void testExecuteMergesBatchAndRtAndProduces() {
    Map<ByteBuffer, ByteBuffer> batch = new HashMap<>();
    batch.put(key("1"), value("batch_1"));
    batch.put(key("2"), value("batch_2"));
    List<SnapshotAtTRtRecord> rtRecords = new ArrayList<>();
    rtRecords.add(put("2", "rt_2", 1000L, 0));
    rtRecords.add(put("3", "rt_3", 1000L, 1));

    Map<String, byte[]> produced = new HashMap<>();
    SnapshotAtTPushExecutor.Stats stats = new SnapshotAtTPushExecutor()
        .execute(recordingWriter(produced), batch, VALUE_SCHEMA_ID, rtRecords, merger, noOpCompressor);

    assertEquals(stats.getPuts(), 3);
    assertEquals(stats.getDeletes(), 0);
    assertEquals(deserialize(produced.get("1")), "batch_1"); // batch only
    assertEquals(deserialize(produced.get("2")), "rt_2"); // RT wins over the batch base
    assertEquals(deserialize(produced.get("3")), "rt_3"); // RT only
  }

  @Test
  public void testExecuteProducesDeleteTombstone() {
    Map<ByteBuffer, ByteBuffer> batch = new HashMap<>();
    batch.put(key("1"), value("batch_1"));
    List<SnapshotAtTRtRecord> rtRecords = new ArrayList<>();
    rtRecords
        .add(new SnapshotAtTRtRecord(SnapshotAtTRtRecord.Op.DELETE, key("1"), null, VALUE_SCHEMA_ID, -1, 2000L, 0));

    Map<String, byte[]> produced = new HashMap<>();
    SnapshotAtTPushExecutor.Stats stats = new SnapshotAtTPushExecutor()
        .execute(recordingWriter(produced), batch, VALUE_SCHEMA_ID, rtRecords, merger, noOpCompressor);

    assertEquals(stats.getDeletes(), 1);
    assertEquals(stats.getPuts(), 0);
    assertNull(produced.get("1")); // delete tombstone -> null value
  }

  private ByteBuffer key(String value) {
    return ByteBuffer.wrap(serializer.serialize(null, value));
  }

  private ByteBuffer value(String value) {
    return ByteBuffer.wrap(serializer.serialize(null, value));
  }

  private SnapshotAtTRtRecord put(String key, String value, long timestamp, int coloId) {
    return new SnapshotAtTRtRecord(
        SnapshotAtTRtRecord.Op.PUT,
        key(key),
        value(value),
        VALUE_SCHEMA_ID,
        -1,
        timestamp,
        coloId);
  }

  private String deserialize(byte[] bytes) {
    return serializer.deserialize(bytes).toString();
  }

  @SuppressWarnings("unchecked")
  private AbstractVeniceWriter<byte[], byte[], byte[]> recordingWriter(Map<String, byte[]> produced) {
    VeniceWriter<byte[], byte[], byte[]> writer = mock(VeniceWriter.class);
    doAnswer(invocation -> {
      produced.put(deserialize((byte[]) invocation.getArgument(0)), (byte[]) invocation.getArgument(1));
      return null;
    }).when(writer).put(any(byte[].class), any(byte[].class), anyInt(), any(), any(PutMetadata.class));
    doAnswer(invocation -> {
      produced.put(deserialize((byte[]) invocation.getArgument(0)), null);
      return null;
    }).when(writer).delete(any(byte[].class), any(), any(DeleteMetadata.class));
    return writer;
  }
}
