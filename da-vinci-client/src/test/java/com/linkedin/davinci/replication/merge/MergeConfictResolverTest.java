package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.TimestampMetadataSchemaAdapter;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.Lazy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MergeConfictResolverTest {
  private static String recordSchemaStr = "{\n"
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
  private Schema recordSchema;
  private Schema aaSchema;
  private  RecordSerializer<GenericRecord> serializer;
  private RecordDeserializer<GenericRecord> deserializer;

  @BeforeClass
  public void setup() {
     this.recordSchema = Schema.parse(recordSchemaStr);
     this.aaSchema = TimestampMetadataSchemaAdapter.parse(recordSchema, 1);
     this.serializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(recordSchema);
     this.deserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(recordSchema, recordSchema);
  }

  private ByteBuffer getByteBufferOfRecord(GenericRecord record) {
    return ByteBuffer.wrap(serializer.serialize(record));
  }

  @Test
  public void testPut() throws Exception {
    GenericRecord valueRecord = new GenericData.Record(recordSchema);
    valueRecord.put("id", "id1");
    valueRecord.put("name", "name1");
    valueRecord.put("age", 10);
    GenericRecord timeStampRecord = new GenericData.Record(aaSchema);
    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10l);
    ts.put("name", 10l);
    ts.put("age", 20l);
    timeStampRecord.put(0, 20L);

    GenericRecord newRecord = new GenericData.Record(recordSchema);
    newRecord.put("id", "id10");
    newRecord.put("name", "name10");
    newRecord.put("age", 20);
    MergeConfictResolver mergeConfictResolver = new MergeConfictResolver(null, "store");
    ByteBuffer oldBB = getByteBufferOfRecord(valueRecord);
    ByteBuffer newBB = getByteBufferOfRecord(newRecord);
    MergeConflictResult mergeConflictResult  = mergeConfictResolver.put(Lazy.of(() -> oldBB), timeStampRecord, newBB,
        30, -1, -1);

    // verify id and name fields are from new record
    GenericRecord result = deserializer.deserialize(mergeConflictResult.getResultByteBuffer());
    Assert.assertEquals(GenericData.get().compare(result, newRecord, recordSchema), 0);

    // verify update ignored.
    mergeConflictResult  = mergeConfictResolver.put(Lazy.of(() -> oldBB), timeStampRecord, newBB,
        10, -1, -1);
    Assert.assertTrue(mergeConflictResult.isUpdateIgnored());

    // verify same timestamp case
    mergeConflictResult  = mergeConfictResolver.put(Lazy.of(() -> oldBB), timeStampRecord, newBB,
        30, -1, -1);
    Assert.assertEquals(mergeConflictResult.getResultByteBuffer(), Merge.compareAndReturn(oldBB, newBB));

    // validate null old value
    mergeConflictResult  = mergeConfictResolver.put(Lazy.of(() -> null), timeStampRecord, newBB,
        30, -1, -1);
    result = deserializer.deserialize(mergeConflictResult.getResultByteBuffer());
    Assert.assertEquals(GenericData.get().compare(result, newRecord, recordSchema), 0);

    // validate error on per field TS record
    timeStampRecord.put(0,  ts);
    Assert.assertThrows(VeniceException.class,() -> mergeConfictResolver.put(Lazy.of(() -> oldBB), timeStampRecord, newBB,
        10, -1, -1));
  }

  @Test
  public void testDelete() throws Exception {
    GenericRecord valueRecord = new GenericData.Record(recordSchema);
    valueRecord.put("id", "id1");
    valueRecord.put("name", "name1");
    valueRecord.put("age", 10);
    GenericRecord timeStampRecord = new GenericData.Record(aaSchema);
    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10l);
    ts.put("name", 10l);
    ts.put("age", 20l);

    timeStampRecord.put(0, 20L);

    GenericRecord newRecord = new GenericData.Record(recordSchema);
    newRecord.put("id", "id10");
    newRecord.put("name", "name10");
    newRecord.put("age", 20);
    MergeConfictResolver mergeConfictResolver = new MergeConfictResolver(null, "store");
    ByteBuffer oldBB = getByteBufferOfRecord(valueRecord);
    ByteBuffer newBB = getByteBufferOfRecord(newRecord);
    MergeConflictResult mergeConflictResult  = mergeConfictResolver.delete(timeStampRecord, 30);

    // verify delete null value
    Assert.assertNull(mergeConflictResult.getResultByteBuffer());

    // verify update ignored.
    mergeConflictResult  = mergeConfictResolver.delete(timeStampRecord, 10);
    Assert.assertTrue(mergeConflictResult.isUpdateIgnored());

    // verify same timestamp case
    mergeConflictResult  = mergeConfictResolver.delete(timeStampRecord, 30);
    Assert.assertNull(mergeConflictResult.getResultByteBuffer());

    // validate error on per field TS record
    timeStampRecord.put(0,  ts);
    Assert.assertThrows(VeniceException.class,() -> mergeConfictResolver.delete(timeStampRecord, 10));
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
      tsRecord.add(timeStampRecord);
      writeTs.add((long) (i + 15));
    }

    MergeConfictResolver mergeConfictResolver = new MergeConfictResolver(null, "store");
    MergeConflictResult mergeConflictResult = null;
    ByteBuffer oldBB = getByteBufferOfRecord(origRecord);

    for (int i = 0; i < 100; i++) {
      ByteBuffer newBB = getByteBufferOfRecord(payload.get(i));
      for (int j = 0; j < 100; j++) {
        mergeConflictResult =  mergeConfictResolver.put(Lazy.of(() -> oldBB), tsRecord.get(j),  newBB,
            writeTs.get(j), -1, -1);
      }
    }

    GenericRecord result1 = deserializer.deserialize(mergeConflictResult.getResultByteBuffer());

    Assert.assertEquals((long)mergeConflictResult.getReplicationMetadata().get(0), 115L);

    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        ByteBuffer newBB = getByteBufferOfRecord(payload.get(j));
        mergeConflictResult =  mergeConfictResolver.put(Lazy.of(() -> oldBB), tsRecord.get(i),  newBB,
            writeTs.get(i), -1, -1);
      }
    }
    GenericRecord result2 = deserializer.deserialize(mergeConflictResult.getResultByteBuffer());

    // validate order of operation change results in a same object
    Assert.assertEquals((long)mergeConflictResult.getReplicationMetadata().get(0), 115L);
    Assert.assertEquals(GenericData.get().compare(result1, result2, recordSchema), 0);
  }
}
