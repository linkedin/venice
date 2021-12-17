package com.linkedin.davinci.replication.merge;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.schema.merge.CollectionTimestampMergeRecordHelper;
import com.linkedin.venice.schema.merge.MergeRecordHelper;
import com.linkedin.venice.schema.merge.ValueAndReplicationMetadata;
import com.linkedin.venice.schema.writecompute.WriteComputeProcessor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaGenerator;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.Lazy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;
import org.testng.Assert;

import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.*;


public class MergeGenericRecordTest {

  private static final String RECORD_SCHEMA_STR = "{\n"
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

  private static final String ARRAY_SCHEMA_STR = "{\n" +
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
  public void testDelete() {
    Schema schema = AvroCompatibilityHelper.parse(RECORD_SCHEMA_STR);
    Schema aaSchema = ReplicationMetadataSchemaGenerator.generateMetadataSchema(schema, 1);
    GenericRecord valueRecord = new GenericData.Record(schema);
    valueRecord.put("id", "id1");
    valueRecord.put("name", "name1");
    valueRecord.put("age", 10);
    GenericRecord timeStampRecord = new GenericData.Record(aaSchema);
    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 25L);
    timeStampRecord.put(0, ts);

    ValueAndReplicationMetadata<GenericRecord>
        valueAndReplicationMetadata = new ValueAndReplicationMetadata<>(valueRecord, timeStampRecord);

    Merge<GenericRecord> genericRecordMerge = createMergeGenericRecord();
    ValueAndReplicationMetadata<GenericRecord> deletedValueAndReplicationMetadata1 = genericRecordMerge.delete(valueAndReplicationMetadata, 20, -1, 1, 0);
    // verify id and name fields are default (deleted)
    Assert.assertEquals(deletedValueAndReplicationMetadata1.getValue().get("id").toString(), "id");
    Assert.assertEquals(deletedValueAndReplicationMetadata1.getValue().get("name").toString(), "name");
    Assert.assertEquals(deletedValueAndReplicationMetadata1.getValue().get("age"), 10);
    ts = (GenericRecord) deletedValueAndReplicationMetadata1.getReplicationMetadata().get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(ts.get("id"), 20L);
    Assert.assertEquals(ts.get("name"), 20L);
    Assert.assertEquals(ts.get("age"), 25L);
    // Verify that the same object is returned
    Assert.assertTrue(deletedValueAndReplicationMetadata1 == valueAndReplicationMetadata);

    // full delete. expect null value
    timeStampRecord.put(0, 20L);
    valueAndReplicationMetadata.setReplicationMetadata(timeStampRecord);
    ValueAndReplicationMetadata<GenericRecord> deletedValueAndReplicationMetadata2 = genericRecordMerge.delete(valueAndReplicationMetadata, 30, -1, 1, 0);
    Assert.assertNull(deletedValueAndReplicationMetadata2.getValue());
    // Verify that the same object is returned
    Assert.assertTrue(deletedValueAndReplicationMetadata2 == valueAndReplicationMetadata);

    // full delete based on field timestamp values
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 20L);
    timeStampRecord.put(0, ts);
    valueAndReplicationMetadata.setReplicationMetadata(timeStampRecord);
    valueAndReplicationMetadata.setValue(valueRecord);
    valueAndReplicationMetadata = genericRecordMerge.delete(valueAndReplicationMetadata, 30, 1, -1, 0);
    Assert.assertNull(valueAndReplicationMetadata.getValue());
    Assert.assertEquals(valueAndReplicationMetadata.getReplicationMetadata().get(TIMESTAMP_FIELD_NAME), 30L);

    // no delete, return same object
    timeStampRecord.put(0, ts);
    valueAndReplicationMetadata.setReplicationMetadata(timeStampRecord);
    valueAndReplicationMetadata.setValue(valueRecord);
    ValueAndReplicationMetadata<GenericRecord> deletedValueAndReplicationMetadata3 = genericRecordMerge.delete(valueAndReplicationMetadata, 5, -1, 1, 0);

    Assert.assertEquals(deletedValueAndReplicationMetadata3.getValue(), valueAndReplicationMetadata.getValue());
    Assert.assertEquals((List<Long>) deletedValueAndReplicationMetadata3.getReplicationMetadata().get(REPLICATION_CHECKPOINT_VECTOR_FIELD), Arrays.asList(1L));
    // Verify that the same object is returned
    Assert.assertTrue(deletedValueAndReplicationMetadata3 == valueAndReplicationMetadata);
  }

  @Test
  public void testPut() {
    Schema schema = AvroCompatibilityHelper.parse(RECORD_SCHEMA_STR);
    Schema aaSchema = ReplicationMetadataSchemaGenerator.generateMetadataSchema(schema, 1);
    GenericRecord valueRecord = new GenericData.Record(schema);
    valueRecord.put("id", "id1");
    valueRecord.put("name", "name1");
    valueRecord.put("age", 10);
    GenericRecord timeStampRecord = new GenericData.Record(aaSchema);
    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 20L);
    timeStampRecord.put(0, ts);

    ValueAndReplicationMetadata<GenericRecord>
        valueAndReplicationMetadata = new ValueAndReplicationMetadata<>(valueRecord, timeStampRecord);

    GenericRecord newRecord = new GenericData.Record(schema);
    newRecord.put("id", "id10");
    newRecord.put("name", "name10");
    newRecord.put("age", 20);
    Merge<GenericRecord> genericRecordMerge = createMergeGenericRecord();
    ValueAndReplicationMetadata<GenericRecord> mergedValueAndReplicationMetadata1 = genericRecordMerge.put(valueAndReplicationMetadata, newRecord, 30, -1, 1, 0);

    // verify id and name fields are from new record
    Assert.assertEquals(mergedValueAndReplicationMetadata1.getValue().get("id"), newRecord.get(0));
    Assert.assertEquals(mergedValueAndReplicationMetadata1.getValue().get("name"), newRecord.get(1));
    Assert.assertEquals(mergedValueAndReplicationMetadata1.getValue().get("age"), newRecord.get(2));
    // Verify that the same object is returned
    Assert.assertTrue(mergedValueAndReplicationMetadata1 == valueAndReplicationMetadata);

    // verify we reuse the same instance when nothings changed.
    ValueAndReplicationMetadata<GenericRecord>
        mergedValueAndReplicationMetadata2 = genericRecordMerge.put(valueAndReplicationMetadata, newRecord, 10, -1, 1, 0);
    Assert.assertEquals(mergedValueAndReplicationMetadata2.getValue(), valueAndReplicationMetadata.getValue());
    // Verify that the same object is returned
    Assert.assertTrue(mergedValueAndReplicationMetadata2 == valueAndReplicationMetadata);

    Schema schema2 = AvroCompatibilityHelper.parse("{"
        + "\"fields\": ["
        + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
        + "   {\"default\": -1, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}"
        + "   ],"
        + " \"name\": \"testObject\", \"type\": \"record\""
        +"}");
    newRecord = new GenericData.Record(schema2);
    GenericRecord finalNewRecord = newRecord;
    Assert.assertThrows(VeniceException.class, () -> genericRecordMerge.put(valueAndReplicationMetadata, finalNewRecord, 10, -1, 1, 0));
  }

  @Test(enabled = false)
  public void testUpdate() {
    Schema schema = AvroCompatibilityHelper.parse(RECORD_SCHEMA_STR);
    Schema aaSchema = ReplicationMetadataSchemaGenerator.generateMetadataSchema(schema, 1);
    GenericRecord valueRecord = new GenericData.Record(schema);
    valueRecord.put("id", "id1");
    valueRecord.put("name", "name1");
    valueRecord.put("age", 10);
    GenericRecord timeStampRecord = new GenericData.Record(aaSchema);
    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 20L);
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
    Merge<GenericRecord> genericRecordMerge = createMergeGenericRecord();
    valueAndReplicationMetadata = genericRecordMerge.update(
        valueAndReplicationMetadata,
        Lazy.of(() -> wcRecord),
        wcRecord.getSchema(),
        recordWriteComputeSchema,
        30,
        -1,
        1,
        0
    );

    // verify id and name fields are from new record
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("id"), wcRecord.get(0));
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("name"), valueRecord.get(1));
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("age"), wcRecord.get(2));
    ts = (GenericRecord) valueAndReplicationMetadata.getReplicationMetadata().get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(ts.get("id"), 30L);
    Assert.assertEquals(ts.get("name"), 10L);
    Assert.assertEquals(ts.get("age"), 30L);

    // verify we reuse the same instance when nothings changed.
    ValueAndReplicationMetadata<GenericRecord>
        valueAndReplicationMetadata1 = genericRecordMerge.update(
            valueAndReplicationMetadata,
        Lazy.of(() -> wcRecord),
        wcRecord.getSchema(),
        recordWriteComputeSchema,
        10,
        -1,
        1,
        0
    );
    Assert.assertEquals(valueAndReplicationMetadata1.getValue(), valueAndReplicationMetadata.getValue());

    // validate ts record change from LONG to GenericRecord.
    timeStampRecord.put(0, 10L);
    valueAndReplicationMetadata = genericRecordMerge.update(
        valueAndReplicationMetadata, Lazy.of(() -> wcRecord),
        wcRecord.getSchema(),
        recordWriteComputeSchema,
        30,
        -1,
        1,
        0
    );
    ts = (GenericRecord) valueAndReplicationMetadata.getReplicationMetadata().get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(ts.get("id"), 30L);
    Assert.assertEquals(ts.get("name"), 10L);
    Assert.assertEquals(ts.get("age"), 30L);

    // validate exception on list operations.
    schema = AvroCompatibilityHelper.parse(ARRAY_SCHEMA_STR);
    Schema innerArraySchema = schema.getField("hits").schema();
    GenericData.Record collectionUpdateRecord =
        new GenericData.Record(WriteComputeSchemaConverter.convert(innerArraySchema).getTypes().get(0));
    collectionUpdateRecord.put(SET_UNION, Collections.singletonList(timeStampRecord));
    collectionUpdateRecord.put(SET_DIFF, Collections.emptyList());
    wcRecord.put("name", collectionUpdateRecord);
    ValueAndReplicationMetadata finalValueAndReplicationMetadata = valueAndReplicationMetadata;
    Assert.assertThrows(VeniceException.class, () -> genericRecordMerge.update(
        finalValueAndReplicationMetadata,
        Lazy.of(() ->wcRecord),
        wcRecord.getSchema(),
        recordWriteComputeSchema,
        10,
        -1,
        1,
        0
    ));
  }

  @Test
  public void testPutPermutation() {
    Schema schema = AvroCompatibilityHelper.parse(RECORD_SCHEMA_STR);
    List<GenericRecord> payload = new ArrayList<>();
    List<Long> writeTs = new ArrayList<>();
    GenericRecord origRecord = new GenericData.Record(schema);
    Schema aaSchema = ReplicationMetadataSchemaGenerator.generateMetadataSchema(schema, 1);
    GenericRecord timeStampRecord = new GenericData.Record(aaSchema);

    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 20L);
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
    Merge<GenericRecord> genericRecordMerge = createMergeGenericRecord();

    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        valueAndReplicationMetadata = genericRecordMerge.put(valueAndReplicationMetadata, GenericData.get().deepCopy(schema, payload.get(j)), writeTs.get(i), -1, 1, 0);
      }
    }
    // timestamp record should always contain the latest value
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("id").toString(), "id99");
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("name").toString(), "name99");
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("age"), 109);
    Assert.assertEquals((long) valueAndReplicationMetadata.getReplicationMetadata().get(TIMESTAMP_FIELD_NAME), 110);


    valueAndReplicationMetadata = new ValueAndReplicationMetadata<>(origRecord, timeStampRecord);
    // swap timestamp and record order
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        valueAndReplicationMetadata = genericRecordMerge.put(valueAndReplicationMetadata, GenericData.get().deepCopy(schema, payload.get(i)), writeTs.get(j), -1, 1, 0);
      }
    }
    // timestamp record should always contain the latest value
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("id").toString(), "id99");
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("name").toString(), "name99");
    Assert.assertEquals(valueAndReplicationMetadata.getValue().get("age"), 109);
    Assert.assertEquals((long) valueAndReplicationMetadata.getReplicationMetadata().get(TIMESTAMP_FIELD_NAME), 110);
  }

  private Merge<GenericRecord> createMergeGenericRecord() {
    MergeRecordHelper mergeRecordHelper = new CollectionTimestampMergeRecordHelper();
    WriteComputeProcessor writeComputeProcessor = new WriteComputeProcessor(mergeRecordHelper);
    return new MergeGenericRecord(writeComputeProcessor, mergeRecordHelper);
  }
}