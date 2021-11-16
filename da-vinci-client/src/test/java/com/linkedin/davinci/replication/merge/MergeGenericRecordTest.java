package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.ReplicationMetadataSchemaGenerator;
import com.linkedin.venice.schema.WriteComputeSchemaConverter;
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
import static com.linkedin.venice.schema.WriteComputeSchemaConverter.*;


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
    Schema aaSchema = ReplicationMetadataSchemaGenerator.generateMetadataSchema(schema, 1);
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

    ValueAndReplicationMetadata<GenericRecord>
        valueAndReplicationMetadata = new ValueAndReplicationMetadata<>(valueRecord, timeStampRecord);

    Merge<GenericRecord> merge = MergeGenericRecord.getInstance();
    valueAndReplicationMetadata = merge.delete(valueAndReplicationMetadata, 20, 1, 0);
    // verify id and name fields are default (deleted)
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("id").toString(), "id");
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("name").toString(), "name");
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("age"), 10);
    ts = (GenericRecord) valueAndReplicationMetadata.getReplicationMetadata().get(TIMESTAMP_FIELD);
    Assert.assertEquals(ts.get("id"), 20L);
    Assert.assertEquals(ts.get("name"), 20L);
    Assert.assertEquals(ts.get("age"), 25L);


    // full delete. expect null value
    timeStampRecord.put(0, 20L);
    valueAndReplicationMetadata.setReplicationMetadata(timeStampRecord);
    valueAndReplicationMetadata = merge.delete(valueAndReplicationMetadata, 30, 1, 0);
    Assert.assertNull(valueAndReplicationMetadata.getValue());

    // full delete based on field timestamp values
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 20L);
    timeStampRecord.put(0, ts);
    valueAndReplicationMetadata.setReplicationMetadata(timeStampRecord);
    valueAndReplicationMetadata.setValue(valueRecord);
    valueAndReplicationMetadata = merge.delete(valueAndReplicationMetadata, 30, 1, 0);
    Assert.assertNull(valueAndReplicationMetadata.getValue());
    Assert.assertEquals(valueAndReplicationMetadata.getReplicationMetadata().get(TIMESTAMP_FIELD), 30L);


    // no delete, return same object
    timeStampRecord.put(0, ts);
    valueAndReplicationMetadata.setReplicationMetadata(timeStampRecord);
    valueAndReplicationMetadata.setValue(valueRecord);
    ValueAndReplicationMetadata record = merge.delete(valueAndReplicationMetadata, 5, 1, 0);
    Assert.assertEquals(record, valueAndReplicationMetadata);
  }

  @Test
  public void testPut() throws Exception {
    Schema schema = Schema.parse(recordSchemaStr);
    Schema aaSchema = ReplicationMetadataSchemaGenerator.generateMetadataSchema(schema, 1);
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

    ValueAndReplicationMetadata<GenericRecord>
        valueAndReplicationMetadata = new ValueAndReplicationMetadata<>(valueRecord, timeStampRecord);

    GenericRecord newRecord = new GenericData.Record(schema);
    newRecord.put("id", "id10");
    newRecord.put("name", "name10");
    newRecord.put("age", 20);
    Merge<GenericRecord> merge = MergeGenericRecord.getInstance();
    valueAndReplicationMetadata = merge.put(valueAndReplicationMetadata, newRecord, 30, 1, 0);

    // verify id and name fields are from new record
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("id"), newRecord.get(0));
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("name"), newRecord.get(1));
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("age"), newRecord.get(2));

    // verify we reuse the same instance when nothings changed.
    ValueAndReplicationMetadata<GenericRecord>
        valueAndReplicationMetadata1 = merge.put(valueAndReplicationMetadata, newRecord, 10, 1, 0);
    Assert.assertEquals(valueAndReplicationMetadata1.getValue(), valueAndReplicationMetadata.getValue());

    ValueAndReplicationMetadata<GenericRecord> finalValueAndReplicationMetadata = valueAndReplicationMetadata;
    Schema schema2 = Schema.parse("{"
        + "\"fields\": ["
        + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
        + "   {\"default\": -1, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}"
        + "   ],"
        + " \"name\": \"testObject\", \"type\": \"record\""
        +"}");
    newRecord = new GenericData.Record(schema2);
    GenericRecord finalNewRecord = newRecord;
    Assert.assertThrows(VeniceException.class, () -> merge.put(finalValueAndReplicationMetadata, finalNewRecord, 10, 1, 0));
  }

  @Test(enabled = false)
  public void testUpdate() throws Exception {
    Schema schema = Schema.parse(recordSchemaStr);
    Schema aaSchema = ReplicationMetadataSchemaGenerator.generateMetadataSchema(schema, 1);
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

    ValueAndReplicationMetadata<GenericRecord>
        valueAndReplicationMetadata = new ValueAndReplicationMetadata<>(valueRecord, timeStampRecord);

    Schema recordWriteComputeSchema = WriteComputeSchemaConverter.convert(schema);

    //construct write compute operation record
    Schema noOpSchema = recordWriteComputeSchema.getTypes().get(0).getField("name").schema().getTypes().get(0);
    GenericData.Record noOpRecord = new GenericData.Record(noOpSchema);

    GenericRecord wcRecord = new GenericData.Record(recordWriteComputeSchema.getTypes().get(0));
    wcRecord.put("id", "id10");
    wcRecord.put("name", noOpRecord);
    wcRecord.put("age", 20);
    Merge<GenericRecord> merge = MergeGenericRecord.getInstance();
    valueAndReplicationMetadata = merge.update(valueAndReplicationMetadata, Lazy.of(() -> wcRecord), 30, 1, 0);

    // verify id and name fields are from new record
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("id"), wcRecord.get(0));
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("name"), valueRecord.get(1));
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("age"), wcRecord.get(2));
    ts = (GenericRecord) valueAndReplicationMetadata.getReplicationMetadata().get(TIMESTAMP_FIELD);
    Assert.assertEquals(ts.get("id"), 30L);
    Assert.assertEquals(ts.get("name"), 10L);
    Assert.assertEquals(ts.get("age"), 30L);

    // verify we reuse the same instance when nothings changed.
    ValueAndReplicationMetadata<GenericRecord>
        valueAndReplicationMetadata1 = merge.update(valueAndReplicationMetadata, Lazy.of(() -> wcRecord), 10, 1, 0);
    Assert.assertEquals(valueAndReplicationMetadata1.getValue(), valueAndReplicationMetadata.getValue());

    // validate ts record change from LONG to GenericRecord.
    timeStampRecord.put(0, 10L);
    valueAndReplicationMetadata = merge.update(valueAndReplicationMetadata, Lazy.of(() -> wcRecord), 30, 1, 0);
    ts = (GenericRecord) valueAndReplicationMetadata.getReplicationMetadata().get(TIMESTAMP_FIELD);
    Assert.assertEquals(ts.get("id"), 30L);
    Assert.assertEquals(ts.get("name"), 10L);
    Assert.assertEquals(ts.get("age"), 30L);

    // validate exception on list operations.
    schema = Schema.parse(arrSchemaStr);
    Schema innerArraySchema = schema.getField("hits").schema();
    GenericData.Record collectionUpdateRecord =
        new GenericData.Record(WriteComputeSchemaConverter.convert(innerArraySchema).getTypes().get(0));
    collectionUpdateRecord.put(SET_UNION, Collections.singletonList(timeStampRecord));
    collectionUpdateRecord.put(SET_DIFF, Collections.emptyList());
    wcRecord.put("name", collectionUpdateRecord);
    ValueAndReplicationMetadata finalValueAndReplicationMetadata = valueAndReplicationMetadata;
    Assert.assertThrows(VeniceException.class, () -> merge.update(finalValueAndReplicationMetadata, Lazy.of(() ->wcRecord), 10, 1, 0));
  }

  @Test
  public void testPutPermutation() {
    Schema schema = Schema.parse(recordSchemaStr);
    List<GenericRecord> payload = new ArrayList<>();
    List<Long> writeTs = new ArrayList<>();
    GenericRecord origRecord = new GenericData.Record(schema);
    Schema aaSchema = ReplicationMetadataSchemaGenerator.generateMetadataSchema(schema, 1);
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
    ValueAndReplicationMetadata<GenericRecord>
        valueAndReplicationMetadata = new ValueAndReplicationMetadata<>(origRecord, timeStampRecord);
    Merge<GenericRecord> merge = MergeGenericRecord.getInstance();

    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        valueAndReplicationMetadata = merge.put(valueAndReplicationMetadata, GenericData.get().deepCopy(schema, payload.get(j)), writeTs.get(i), 1, 0);
      }
    }
    // timestamp record should always contain the latest value
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("id").toString(), "id99");
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("name").toString(), "name99");
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("age"), 109);
    Assert.assertEquals((long) valueAndReplicationMetadata.getReplicationMetadata().get(TIMESTAMP_FIELD), 110);


    valueAndReplicationMetadata = new ValueAndReplicationMetadata<>(origRecord, timeStampRecord);
    // swap timestamp and record order
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        valueAndReplicationMetadata = merge.put(valueAndReplicationMetadata, GenericData.get().deepCopy(schema, payload.get(i)), writeTs.get(j), 1, 0);
      }
    }
    // timestamp record should always contain the latest value
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("id").toString(), "id99");
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("name").toString(), "name99");
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("age"), 109);
    Assert.assertEquals((long) valueAndReplicationMetadata.getReplicationMetadata().get(TIMESTAMP_FIELD), 110);
  }
}