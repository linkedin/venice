package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.ReplicationMetadataSchemaAdapter;
import com.linkedin.venice.schema.WriteComputeSchemaAdapter;
import com.linkedin.venice.utils.Lazy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;
import org.testng.Assert;

import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.schema.WriteComputeSchemaAdapter.*;


public class MergeGenericRecordTest {

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

  private String arrSchemaStr = "{\n" +
      "  \"type\" : \"record\",\n" +
      "  \"name\" : \"testRecord\",\n" +
      "  \"namespace\" : \"com.linkedin.avro\",\n" +
      "  \"fields\" : [ {\n" +
      "    \"name\" : \"hits\",\n" +
      "    \"type\" : {\n" +
      "      \"type\" : \"array\",\n" +
      "      \"items\" : {\n" +
      "        \"type\" : \"record\",\n" +
      "        \"name\" : \"JobAlertHit\",\n" +
      "        \"fields\" : [ {\n" +
      "          \"name\" : \"memberId\",\n" +
      "          \"type\" : \"long\"\n" +
      "        }, {\n" +
      "          \"name\" : \"searchId\",\n" +
      "          \"type\" : \"long\"\n" +
      "        } ]\n"
      + "      }\n" +
      "    },\n" +
      "    \"default\" : [ ]\n" +
      "  }, {\n" +
      "    \"name\" : \"hasNext\",\n" +
      "    \"type\" : \"boolean\",\n" +
      "    \"default\" : false\n" +
      "  } ]\n" +
      "}";
  @Test
  public void testDelete() throws Exception {
    Schema schema = Schema.parse(recordSchemaStr);
    Schema aaSchema = ReplicationMetadataSchemaAdapter.parse(schema, 1);
    GenericRecord valueRecord = new GenericData.Record(schema);
    valueRecord.put("id", "id1");
    valueRecord.put("name", "name1");
    valueRecord.put("age", 10);
    GenericRecord timeStampRecord = new GenericData.Record(aaSchema);
    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10l);
    ts.put("name", 10l);
    ts.put("age", 25l);
    timeStampRecord.put(0, ts);

    ValueAndTimestampMetadata<GenericRecord> valueAndTimestampMetadata = new ValueAndTimestampMetadata<>(valueRecord, timeStampRecord);

    Merge<GenericRecord> merge = MergeGenericRecord.getInstance();
    valueAndTimestampMetadata = merge.delete(valueAndTimestampMetadata, 20);
    // verify id and name fields are default (deleted)
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("id"), valueRecord.getSchema().getField("id").defaultValue());
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("name"), valueRecord.getSchema().getField("name").defaultValue());
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("age"), 10);
    ts = (GenericRecord) valueAndTimestampMetadata.getTimestampMetadata().get(TIMESTAMP_FIELD);
    Assert.assertEquals(ts.get("id"), 20L);
    Assert.assertEquals(ts.get("name"), 20L);
    Assert.assertEquals(ts.get("age"), 25L);


    // full delete. expect null value
    timeStampRecord.put(0, 20L);
    valueAndTimestampMetadata.setTimestampMetadata(timeStampRecord);
    valueAndTimestampMetadata = merge.delete(valueAndTimestampMetadata, 30);
    Assert.assertNull(valueAndTimestampMetadata.getValue());

    // full delete based on field timestamp values
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 20L);
    timeStampRecord.put(0, ts);
    valueAndTimestampMetadata.setTimestampMetadata(timeStampRecord);
    valueAndTimestampMetadata.setValue(valueRecord);
    valueAndTimestampMetadata = merge.delete(valueAndTimestampMetadata, 30);
    Assert.assertNull(valueAndTimestampMetadata.getValue());
    Assert.assertEquals(valueAndTimestampMetadata.getTimestampMetadata().get(TIMESTAMP_FIELD), 30L);


    // no delete, return same object
    timeStampRecord.put(0, ts);
    valueAndTimestampMetadata.setTimestampMetadata(timeStampRecord);
    valueAndTimestampMetadata.setValue(valueRecord);
    ValueAndTimestampMetadata record = merge.delete(valueAndTimestampMetadata, 5);
    Assert.assertEquals(record, valueAndTimestampMetadata);
  }

  @Test
  public void testPut() throws Exception {
    Schema schema = Schema.parse(recordSchemaStr);
    Schema aaSchema = ReplicationMetadataSchemaAdapter.parse(schema, 1);
    GenericRecord valueRecord = new GenericData.Record(schema);
    valueRecord.put("id", "id1");
    valueRecord.put("name", "name1");
    valueRecord.put("age", 10);
    GenericRecord timeStampRecord = new GenericData.Record(aaSchema);
    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10l);
    ts.put("name", 10l);
    ts.put("age", 20l);
    timeStampRecord.put(0, ts);

    ValueAndTimestampMetadata<GenericRecord> valueAndTimestampMetadata = new ValueAndTimestampMetadata<>(valueRecord, timeStampRecord);

    GenericRecord newRecord = new GenericData.Record(schema);
    newRecord.put("id", "id10");
    newRecord.put("name", "name10");
    newRecord.put("age", 20);
    Merge<GenericRecord> merge = MergeGenericRecord.getInstance();
    valueAndTimestampMetadata = merge.put(valueAndTimestampMetadata, newRecord, 30);

    // verify id and name fields are from new record
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("id"), newRecord.get(0));
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("name"), newRecord.get(1));
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("age"), newRecord.get(2));

    // verify we reuse the same instance when nothings changed.
    ValueAndTimestampMetadata<GenericRecord> valueAndTimestampMetadata1 = merge.put(valueAndTimestampMetadata, newRecord, 10);
    Assert.assertEquals(valueAndTimestampMetadata1.getValue(), valueAndTimestampMetadata.getValue());

    ValueAndTimestampMetadata<GenericRecord> finalValueAndTimestampMetadata = valueAndTimestampMetadata;
    Schema schema2 = Schema.parse("{"
        + "\"fields\": ["
        + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
        + "   {\"default\": -1, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}"
        + "   ],"
        + " \"name\": \"testObject\", \"type\": \"record\""
        +"}");
    newRecord = new GenericData.Record(schema2);
    GenericRecord finalNewRecord = newRecord;
    Assert.assertThrows(VeniceException.class, () -> merge.put(finalValueAndTimestampMetadata, finalNewRecord, 10));
  }

  @Test(enabled = false)
  public void testUpdate() throws Exception {
    Schema schema = Schema.parse(recordSchemaStr);
    Schema aaSchema = ReplicationMetadataSchemaAdapter.parse(schema, 1);
    GenericRecord valueRecord = new GenericData.Record(schema);
    valueRecord.put("id", "id1");
    valueRecord.put("name", "name1");
    valueRecord.put("age", 10);
    GenericRecord timeStampRecord = new GenericData.Record(aaSchema);
    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10l);
    ts.put("name", 10l);
    ts.put("age", 20l);
    timeStampRecord.put(0, ts);

    ValueAndTimestampMetadata<GenericRecord> valueAndTimestampMetadata = new ValueAndTimestampMetadata<>(valueRecord, timeStampRecord);

    Schema recordWriteComputeSchema = WriteComputeSchemaAdapter.parse(schema);

    //construct write compute operation record
    Schema noOpSchema = recordWriteComputeSchema.getTypes().get(0).getField("name").schema().getTypes().get(0);
    GenericData.Record noOpRecord = new GenericData.Record(noOpSchema);

    GenericRecord wcRecord = new GenericData.Record(recordWriteComputeSchema.getTypes().get(0));
    wcRecord.put("id", "id10");
    wcRecord.put("name", noOpRecord);
    wcRecord.put("age", 20);
    Merge<GenericRecord> merge = MergeGenericRecord.getInstance();
    valueAndTimestampMetadata = merge.update(valueAndTimestampMetadata, Lazy.of(() -> wcRecord), 30);

    // verify id and name fields are from new record
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("id"), wcRecord.get(0));
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("name"), valueRecord.get(1));
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("age"), wcRecord.get(2));
    ts = (GenericRecord) valueAndTimestampMetadata.getTimestampMetadata().get(TIMESTAMP_FIELD);
    Assert.assertEquals(ts.get("id"), 30L);
    Assert.assertEquals(ts.get("name"), 10L);
    Assert.assertEquals(ts.get("age"), 30L);

    // verify we reuse the same instance when nothings changed.
    ValueAndTimestampMetadata<GenericRecord> valueAndTimestampMetadata1 = merge.update(valueAndTimestampMetadata, Lazy.of(() -> wcRecord), 10);
    Assert.assertEquals(valueAndTimestampMetadata1.getValue(), valueAndTimestampMetadata.getValue());

    // validate ts record change from LONG to GenericRecord.
    timeStampRecord.put(0, 10L);
    valueAndTimestampMetadata = merge.update(valueAndTimestampMetadata, Lazy.of(() -> wcRecord), 30);
    ts = (GenericRecord) valueAndTimestampMetadata.getTimestampMetadata().get(TIMESTAMP_FIELD);
    Assert.assertEquals(ts.get("id"), 30L);
    Assert.assertEquals(ts.get("name"), 10L);
    Assert.assertEquals(ts.get("age"), 30L);

    // validate exception on list operations.
    schema = Schema.parse(arrSchemaStr);
    Schema innerArraySchema = schema.getField("hits").schema();
    GenericData.Record collectionUpdateRecord =
        new GenericData.Record(WriteComputeSchemaAdapter.parse(innerArraySchema).getTypes().get(0));
    collectionUpdateRecord.put(SET_UNION, Collections.singletonList(timeStampRecord));
    collectionUpdateRecord.put(SET_DIFF, Collections.emptyList());
    wcRecord.put("name", collectionUpdateRecord);
    ValueAndTimestampMetadata finalValueAndTimestampMetadata = valueAndTimestampMetadata;
    Assert.assertThrows(VeniceException.class, () -> merge.update(finalValueAndTimestampMetadata, Lazy.of(() ->wcRecord), 10));
  }

  @Test
  public void testPutPermutation() {
    Schema schema = Schema.parse(recordSchemaStr);
    List<GenericRecord> payload = new ArrayList<>();
    List<Long> writeTs = new ArrayList<>();
    GenericRecord origRecord = new GenericData.Record(schema);
    Schema aaSchema = ReplicationMetadataSchemaAdapter.parse(schema, 1);
    GenericRecord timeStampRecord = new GenericData.Record(aaSchema);

    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10l);
    ts.put("name", 10l);
    ts.put("age", 20l);
    timeStampRecord.put(0, ts);
    origRecord.put("id", "id0");
    origRecord.put("name", "name0");
    origRecord.put("age", 10);

    for (int i = 1; i <= 100; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("id", "id" + i);
      record.put("name", "name" + i);
      record.put("age", 10 + i);
      payload.add(record);
      writeTs.add((long) (i + 10));
    }
    ValueAndTimestampMetadata<GenericRecord> valueAndTimestampMetadata = new ValueAndTimestampMetadata<>(origRecord, timeStampRecord);
    Merge<GenericRecord> merge = MergeGenericRecord.getInstance();

    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        valueAndTimestampMetadata = merge.put(valueAndTimestampMetadata, GenericData.get().deepCopy(schema, payload.get(j)), writeTs.get(i));
      }
    }
    // timestamp record should always contain the latest value
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("id").toString(), "id99");
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("name").toString(), "name99");
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("age"), 109);
    Assert.assertEquals((long) valueAndTimestampMetadata.getTimestampMetadata().get(TIMESTAMP_FIELD), 110);


    valueAndTimestampMetadata = new ValueAndTimestampMetadata<>(origRecord, timeStampRecord);
    // swap timestamp and record order
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        valueAndTimestampMetadata = merge.put(valueAndTimestampMetadata, GenericData.get().deepCopy(schema, payload.get(i)), writeTs.get(j));
      }
    }
    // timestamp record should always contain the latest value
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("id").toString(), "id99");
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("name").toString(), "name99");
    Assert.assertEquals(valueAndTimestampMetadata.getValue().get("age"), 109);
    Assert.assertEquals((long) valueAndTimestampMetadata.getTimestampMetadata().get(TIMESTAMP_FIELD), 110);
  }
}