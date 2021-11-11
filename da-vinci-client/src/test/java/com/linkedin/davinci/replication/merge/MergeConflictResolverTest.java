package com.linkedin.davinci.replication.merge;

import com.linkedin.avro.fastserde.primitive.PrimitiveLongArrayList;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.ReplicationMetadataSchemaAdapter;
import com.linkedin.venice.schema.ReplicationMetadataSchemaEntry;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.Lazy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class MergeConflictResolverTest {
  private static final String recordSchemaStr = "{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"User\",\n"
      + "  \"namespace\" : \"example.avro\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"id\",\n"
      + "    \"type\" : \"string\",\n"
      + "    \"default\" : \"id\"\n"
      + "  }, {\n"
      + "    \"name\" : \"name\",\n"
      + "    \"type\" : \"string\",\n"
      + "    \"default\" : \"name\"\n"
      + "  }, {\n"
      + "    \"name\" : \"age\",\n"
      + "    \"type\" : \"int\",\n"
      + "    \"default\" : -1\n"
      + "  } ]\n"
      + "}";

  private String storeName;
  private ReadOnlySchemaRepository schemaRepository;
  private Schema recordSchema;
  private Schema aaSchema;
  private RecordSerializer<GenericRecord> serializer;
  private RecordDeserializer<GenericRecord> deserializer;
  private final int replicationMetadataVersionId = 1;
  private RecordSerializer<GenericRecord> rmdSerializer;
  private RecordDeserializer<GenericRecord> rmdDeserializer;

  @BeforeClass
  public void setup() {
    this.storeName = "store";
    this.schemaRepository = mock(ReadOnlySchemaRepository.class);
    this.recordSchema = Schema.parse(recordSchemaStr);
    this.aaSchema = ReplicationMetadataSchemaAdapter.parse(recordSchema, replicationMetadataVersionId);
    this.serializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(recordSchema);
    this.deserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(recordSchema, recordSchema);
    this.rmdSerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(aaSchema);
    this.rmdDeserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(aaSchema, aaSchema);

    ReplicationMetadataSchemaEntry
        rmdSchemaEntry = new ReplicationMetadataSchemaEntry(1, replicationMetadataVersionId, aaSchema);
    doReturn(rmdSchemaEntry).when(schemaRepository).getReplicationMetadataSchema(anyString(), anyInt(), anyInt());

    SchemaEntry valueSchemaEntry = new SchemaEntry(1, recordSchema);
    doReturn(valueSchemaEntry).when(schemaRepository).getLatestValueSchema(anyString());
  }

  private ByteBuffer getByteBufferOfRecord(GenericRecord record) {
    return ByteBuffer.wrap(serializer.serialize(record));
  }

  private ByteBuffer getByteBufferOfReplicationMetadata(GenericRecord record) {
    return ByteBuffer.wrap(rmdSerializer.serialize(record));
  }

  private GenericRecord getReplicationMetadataFromByteBuffer(ByteBuffer replicationMetadataBytes) {
    return (GenericRecord) rmdDeserializer.deserialize(replicationMetadataBytes);
  }

  @Test
  public void testPut() {
    GenericRecord valueRecord = new GenericData.Record(recordSchema);
    valueRecord.put("id", "id1");
    valueRecord.put("name", "name1");
    valueRecord.put("age", 10);
    GenericRecord timestampRecord = new GenericData.Record(aaSchema);
    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 20L);

    timestampRecord.put(0, 20L);
    timestampRecord.put(1, new ArrayList<Long>());

    GenericRecord newRecord = new GenericData.Record(recordSchema);
    newRecord.put("id", "id10");
    newRecord.put("name", "name10");
    newRecord.put("age", 20);
    MergeConflictResolver mergeConflictResolver = new MergeConflictResolver(schemaRepository, storeName, replicationMetadataVersionId);
    ByteBuffer oldBB = getByteBufferOfRecord(valueRecord);
    ByteBuffer newBB = getByteBufferOfRecord(newRecord);
    MergeConflictResult mergeConflictResult  = mergeConflictResolver.put(Lazy.of(() -> oldBB),
        getByteBufferOfReplicationMetadata(timestampRecord), newBB, 30, 1, 1, 1, 0);

    // verify id and name fields are from new record
    GenericRecord result = deserializer.deserialize(mergeConflictResult.getValue());
    Assert.assertEquals(GenericData.get().compare(result, newRecord, recordSchema), 0);

    // verify update ignored.
    mergeConflictResult  = mergeConflictResolver.put(Lazy.of(() -> oldBB),
        getByteBufferOfReplicationMetadata(timestampRecord), newBB, 10, 1, 1, 1, 0);
    Assert.assertTrue(mergeConflictResult.isUpdateIgnored());

    // verify same timestamp case
    mergeConflictResult  = mergeConflictResolver.put(Lazy.of(() -> oldBB),
        getByteBufferOfReplicationMetadata(timestampRecord), newBB, 20, 1, 1,1, 0);

    if (Merge.compareAndReturn(oldBB, newBB) == oldBB) {
      Assert.assertNull(mergeConflictResult.getValue());
    } else {
      Assert.assertEquals(mergeConflictResult.getValue(), newBB);
    }

    // verify overwrite with new value case
    mergeConflictResult  = mergeConflictResolver.put(Lazy.of(() -> oldBB),
        getByteBufferOfReplicationMetadata(timestampRecord), newBB, 30, 1, 1,1, 0);
    Assert.assertEquals(mergeConflictResult.getValue(), newBB);

    // verify put with invalid schema id
    Assert.assertThrows(VeniceException.class, () -> mergeConflictResolver.put(Lazy.of(() -> oldBB),
        getByteBufferOfReplicationMetadata(timestampRecord), newBB, 30, -1, 1,1, 0));

    // validate null old value
    mergeConflictResult  = mergeConflictResolver.put(Lazy.of(() -> null),
        getByteBufferOfReplicationMetadata(timestampRecord), newBB, 30, 1, 1,1, 0);
    result = deserializer.deserialize(mergeConflictResult.getValue());
    Assert.assertEquals(GenericData.get().compare(result, newRecord, recordSchema), 0);

    // Validate null RMD for existing old value
    mergeConflictResult  = mergeConflictResolver.put(Lazy.of(() -> oldBB), null, newBB, 30, 1, 1,1, 0);
    Assert.assertEquals(mergeConflictResult.getValue(), newBB);

    // validate error on per field TS record
    timestampRecord.put(0,  ts);
    Assert.assertThrows(VeniceException.class,() -> mergeConflictResolver.put(Lazy.of(() -> oldBB),
        getByteBufferOfReplicationMetadata(timestampRecord), newBB, 10, 1, 1,1, 0));
  }

  @Test
  public void testDelete() {
    GenericRecord valueRecord = new GenericData.Record(recordSchema);
    valueRecord.put("id", "id1");
    valueRecord.put("name", "name1");
    valueRecord.put("age", 10);
    GenericRecord timestampRecord = new GenericData.Record(aaSchema);
    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 20L);

    timestampRecord.put(0, 20L);
    timestampRecord.put(1, new ArrayList<Long>());

    MergeConflictResolver mergeConflictResolver = new MergeConflictResolver(schemaRepository, storeName, replicationMetadataVersionId);
    MergeConflictResult mergeConflictResult  = mergeConflictResolver.delete(getByteBufferOfReplicationMetadata(timestampRecord), 1, 30,1, 0);

    // verify delete null value
    Assert.assertNull(mergeConflictResult.getValue());

    // verify update ignored.
    mergeConflictResult  = mergeConflictResolver.delete(getByteBufferOfReplicationMetadata(timestampRecord), 1, 10,1, 0);
    Assert.assertTrue(mergeConflictResult.isUpdateIgnored());

    // verify same timestamp case
    mergeConflictResult  = mergeConflictResolver.delete(getByteBufferOfReplicationMetadata(timestampRecord), 1, 30,1, 0);
    Assert.assertNull(mergeConflictResult.getValue());

    // Validate null RMD for existing old value
    mergeConflictResult  = mergeConflictResolver.delete(null, 1, 30,1, 0);
    Assert.assertFalse(mergeConflictResult.isUpdateIgnored());
    Assert.assertNull(mergeConflictResult.getValue());

    // Validate null RMD for invalid schema id
    mergeConflictResult  = mergeConflictResolver.delete(null, -1, 30,1, 0);
    Assert.assertFalse(mergeConflictResult.isUpdateIgnored());
    Assert.assertEquals(mergeConflictResult.getValueSchemaID(), 1);

    // verify invalid schema id for existing old value
    Assert.assertThrows(VeniceException.class, () -> mergeConflictResolver.delete(getByteBufferOfReplicationMetadata(timestampRecord), -1, 30,1, 0));

    // validate error on per field TS record
    timestampRecord.put(0,  ts);
    Assert.assertThrows(VeniceException.class, () -> mergeConflictResolver.delete(getByteBufferOfReplicationMetadata(timestampRecord), 1, 10,1, 0));
  }

  @Test
  public void testPermutation() {
    List<Long> writeTs = new ArrayList<>();
    List<GenericRecord> payload = new ArrayList<>();
    List<GenericRecord> tsRecord = new ArrayList<>();

    GenericRecord origRecord = new GenericData.Record(recordSchema);
    origRecord.put("id", "id0");
    origRecord.put("name", "name0");
    origRecord.put("age", 10);

    for (int i = 1; i <= 100; i++) {
      GenericRecord record = new GenericData.Record(recordSchema);
      record.put("id", "id" + i);
      record.put("name", "name" + i);
      record.put("age", 10 + i);
      payload.add(record);
      GenericRecord timeStampRecord = new GenericData.Record(aaSchema);
      timeStampRecord.put(0, (long) (i + 10));
      timeStampRecord.put(1, new ArrayList<Long>());
      tsRecord.add(timeStampRecord);
      writeTs.add((long) (i + 15));
    }

    MergeConflictResolver mergeConflictResolver = new MergeConflictResolver(schemaRepository, storeName, replicationMetadataVersionId);
    MergeConflictResult mergeConflictResult = null;
    ByteBuffer oldBB = getByteBufferOfRecord(origRecord);

    for (int i = 0; i < 100; i++) {
      ByteBuffer newBB = getByteBufferOfRecord(payload.get(i));
      for (int j = 0; j < 100; j++) {
        mergeConflictResult =  mergeConflictResolver.put(Lazy.of(() -> oldBB), getByteBufferOfReplicationMetadata(tsRecord.get(j)),  newBB,
            writeTs.get(j), 1, 1,1, 0);
      }
    }

    GenericRecord result1 = deserializer.deserialize(mergeConflictResult.getValue());

    Assert.assertEquals((long)getReplicationMetadataFromByteBuffer(mergeConflictResult.getReplicationMetadata()).get(0), 115L);

    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        ByteBuffer newBB = getByteBufferOfRecord(payload.get(j));
        mergeConflictResult =  mergeConflictResolver.put(Lazy.of(() -> oldBB), getByteBufferOfReplicationMetadata(tsRecord.get(i)),  newBB,
            writeTs.get(i), 1, 1,1, 0);
      }
    }
    GenericRecord result2 = deserializer.deserialize(mergeConflictResult.getValue());

    // validate order of operation change results in a same object
    Assert.assertEquals((long)getReplicationMetadataFromByteBuffer(mergeConflictResult.getReplicationMetadata()).get(0), 115L);
    Assert.assertEquals(GenericData.get().compare(result1, result2, recordSchema), 0);
  }

  @Test
  public void testOffsetVectorMergeAndSum() {
    List<Long> newVector = new PrimitiveLongArrayList(1);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 1L, 0);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 2L, 1);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 3L, 4);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 7L, 1);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 8L, 1);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 9L, 1);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 3L, 5);
    List<Long> expectedVector = Arrays.asList(1L, 9L, 0L, 0L, 3L, 3L);
    Assert.assertEquals(newVector, expectedVector);
    Assert.assertEquals(Merge.sumOffsetVector(newVector), 16L);

    // Reverse it
    newVector = new PrimitiveLongArrayList(2);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 3L, 5);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 9L, 1);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 1L, 0);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 2L, 1);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 3L, 4);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 7L, 1);
    newVector = (List<Long>) Merge.mergeOffsetVectors(newVector, 8L, 1);
    expectedVector = Arrays.asList(1L, 8L, 0L, 0L, 3L, 3L);
    Assert.assertEquals(newVector, expectedVector);
    Assert.assertEquals(Merge.sumOffsetVector(newVector), 15L);
  }
}