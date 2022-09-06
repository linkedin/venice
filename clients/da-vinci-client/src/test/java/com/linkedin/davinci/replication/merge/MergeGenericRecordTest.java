package com.linkedin.davinci.replication.merge;

import static com.linkedin.venice.schema.rmd.RmdConstants.*;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.*;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.merge.CollectionTimestampMergeRecordHelper;
import com.linkedin.venice.schema.merge.MergeRecordHelper;
import com.linkedin.venice.schema.merge.ValueAndRmd;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.WriteComputeProcessor;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MergeGenericRecordTest {
  private static final String RECORD_SCHEMA_STR = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"User\",\n"
      + "  \"namespace\" : \"example.avro\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"id\",\n"
      + "    \"type\" : \"string\",\n" + "    \"default\" : \"id\"\n" + "  }, {\n" + "    \"name\" : \"name\",\n"
      + "    \"type\" : \"string\",\n" + "    \"default\" : \"name\"\n" + "  }, {\n" + "    \"name\" : \"age\",\n"
      + "    \"type\" : \"int\",\n" + "    \"default\" : -1\n" + "  } ]\n" + "}";

  private static final String ARRAY_SCHEMA_STR = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"testRecord\",\n"
      + "  \"namespace\" : \"com.linkedin.avro\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"hits\",\n"
      + "    \"type\" : {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : {\n"
      + "        \"type\" : \"record\",\n" + "        \"name\" : \"JobAlertHit\",\n" + "        \"fields\" : [ {\n"
      + "          \"name\" : \"memberId\",\n" + "          \"type\" : \"long\"\n" + "        }, {\n"
      + "          \"name\" : \"searchId\",\n" + "          \"type\" : \"long\"\n" + "        } ]\n" + "      }\n"
      + "    },\n" + "    \"default\" : [ ]\n" + "  }, {\n" + "    \"name\" : \"hasNext\",\n"
      + "    \"type\" : \"boolean\",\n" + "    \"default\" : false\n" + "  } ]\n" + "}";

  @Test
  public void testDelete() {
    Schema schema = AvroCompatibilityHelper.parse(RECORD_SCHEMA_STR);
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(schema, 1);
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

    ValueAndRmd<GenericRecord> valueAndRmd = new ValueAndRmd<>(Lazy.of(() -> valueRecord), timeStampRecord);

    Merge<GenericRecord> genericRecordMerge = createMergeGenericRecord();
    ValueAndRmd<GenericRecord> deletedValueAndRmd1 = genericRecordMerge.delete(valueAndRmd, 20, -1, 1, 0);
    // verify id and name fields are default (deleted)
    Assert.assertEquals(deletedValueAndRmd1.getValue().get("id").toString(), "id");
    Assert.assertEquals(deletedValueAndRmd1.getValue().get("name").toString(), "name");
    Assert.assertEquals(deletedValueAndRmd1.getValue().get("age"), 10);
    ts = (GenericRecord) deletedValueAndRmd1.getRmd().get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(ts.get("id"), 20L);
    Assert.assertEquals(ts.get("name"), 20L);
    Assert.assertEquals(ts.get("age"), 25L);
    // Verify that the same object is returned
    Assert.assertTrue(deletedValueAndRmd1 == valueAndRmd);

    // full delete. expect null value
    timeStampRecord.put(0, 20L);
    valueAndRmd.setRmd(timeStampRecord);
    ValueAndRmd<GenericRecord> deletedValueAndRmd2 = genericRecordMerge.delete(valueAndRmd, 30, -1, 1, 0);
    Assert.assertNull(deletedValueAndRmd2.getValue());
    // Verify that the same object is returned
    Assert.assertTrue(deletedValueAndRmd2 == valueAndRmd);

    // full delete based on field timestamp values
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 20L);
    timeStampRecord.put(0, ts);
    valueAndRmd.setRmd(timeStampRecord);
    valueAndRmd.setValue(valueRecord);
    valueAndRmd = genericRecordMerge.delete(valueAndRmd, 30, 1, -1, 0);
    Assert.assertNull(valueAndRmd.getValue());
    Assert.assertEquals(valueAndRmd.getRmd().get(TIMESTAMP_FIELD_NAME), 30L);

    // no delete, return same object
    timeStampRecord.put(0, ts);
    valueAndRmd.setRmd(timeStampRecord);
    valueAndRmd.setValue(valueRecord);
    ValueAndRmd<GenericRecord> deletedValueAndRmd3 = genericRecordMerge.delete(valueAndRmd, 5, -1, 1, 0);

    Assert.assertEquals(deletedValueAndRmd3.getValue(), valueAndRmd.getValue());
    Assert.assertEquals(
        (List<Long>) deletedValueAndRmd3.getRmd().get(REPLICATION_CHECKPOINT_VECTOR_FIELD),
        Collections.singletonList(1L));
    // Verify that the same object is returned
    Assert.assertSame(deletedValueAndRmd3, valueAndRmd);
  }

  @Test
  public void testPut() {
    Schema schema = AvroCompatibilityHelper.parse(RECORD_SCHEMA_STR);
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(schema, 1);
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

    ValueAndRmd<GenericRecord> valueAndRmd = new ValueAndRmd<>(Lazy.of(() -> valueRecord), timeStampRecord);

    GenericRecord newRecord = new GenericData.Record(schema);
    newRecord.put("id", "id10");
    newRecord.put("name", "name10");
    newRecord.put("age", 20);
    Merge<GenericRecord> genericRecordMerge = createMergeGenericRecord();
    ValueAndRmd<GenericRecord> mergedValueAndRmd1 = genericRecordMerge.put(valueAndRmd, newRecord, 30, -1, 1, 0);

    // verify id and name fields are from new record
    Assert.assertEquals(mergedValueAndRmd1.getValue().get("id"), newRecord.get(0));
    Assert.assertEquals(mergedValueAndRmd1.getValue().get("name"), newRecord.get(1));
    Assert.assertEquals(mergedValueAndRmd1.getValue().get("age"), newRecord.get(2));
    // Verify that the same object is returned
    Assert.assertTrue(mergedValueAndRmd1 == valueAndRmd);

    // verify we reuse the same instance when nothings changed.
    ValueAndRmd<GenericRecord> mergedValueAndRmd2 = genericRecordMerge.put(valueAndRmd, newRecord, 10, -1, 1, 0);
    Assert.assertEquals(mergedValueAndRmd2.getValue(), valueAndRmd.getValue());
    // Verify that the same object is returned
    Assert.assertTrue(mergedValueAndRmd2 == valueAndRmd);

    Schema schema2 = AvroCompatibilityHelper.parse(
        "{" + "\"fields\": ["
            + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
            + "   {\"default\": -1, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}"
            + "   ]," + " \"name\": \"testObject\", \"type\": \"record\"" + "}");
    newRecord = new GenericData.Record(schema2);
    GenericRecord finalNewRecord = newRecord;
    Assert.assertThrows(VeniceException.class, () -> genericRecordMerge.put(valueAndRmd, finalNewRecord, 10, -1, 1, 0));
  }

  @Test(enabled = false)
  public void testUpdate() {
    Schema schema = AvroCompatibilityHelper.parse(RECORD_SCHEMA_STR);
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(schema, 1);
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

    ValueAndRmd<GenericRecord> valueAndRmd = new ValueAndRmd<>(Lazy.of(() -> valueRecord), timeStampRecord);

    Schema recordWriteComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(schema);

    // construct write compute operation record
    Schema noOpSchema = recordWriteComputeSchema.getTypes().get(0).getField("name").schema().getTypes().get(0);
    GenericData.Record noOpRecord = new GenericData.Record(noOpSchema);

    GenericRecord wcRecord = new GenericData.Record(recordWriteComputeSchema.getTypes().get(0));
    wcRecord.put("id", "id10");
    wcRecord.put("name", noOpRecord);
    wcRecord.put("age", 20);
    Merge<GenericRecord> genericRecordMerge = createMergeGenericRecord();
    valueAndRmd = genericRecordMerge.update(valueAndRmd, Lazy.of(() -> wcRecord), wcRecord.getSchema(), 30, -1, 1, 0);

    // verify id and name fields are from new record
    Assert.assertEquals(valueAndRmd.getValue().get("id"), wcRecord.get(0));
    Assert.assertEquals(valueAndRmd.getValue().get("name"), valueRecord.get(1));
    Assert.assertEquals(valueAndRmd.getValue().get("age"), wcRecord.get(2));
    ts = (GenericRecord) valueAndRmd.getRmd().get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(ts.get("id"), 30L);
    Assert.assertEquals(ts.get("name"), 10L);
    Assert.assertEquals(ts.get("age"), 30L);

    // verify we reuse the same instance when nothings changed.
    ValueAndRmd<GenericRecord> valueAndRmd1 =
        genericRecordMerge.update(valueAndRmd, Lazy.of(() -> wcRecord), wcRecord.getSchema(), 10, -1, 1, 0);
    Assert.assertEquals(valueAndRmd1.getValue(), valueAndRmd.getValue());

    // validate ts record change from LONG to GenericRecord.
    timeStampRecord.put(0, 10L);
    valueAndRmd = genericRecordMerge.update(valueAndRmd, Lazy.of(() -> wcRecord), wcRecord.getSchema(), 30, -1, 1, 0);
    ts = (GenericRecord) valueAndRmd.getRmd().get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(ts.get("id"), 30L);
    Assert.assertEquals(ts.get("name"), 10L);
    Assert.assertEquals(ts.get("age"), 30L);

    // validate exception on list operations.
    schema = AvroCompatibilityHelper.parse(ARRAY_SCHEMA_STR);
    Schema innerArraySchema = schema.getField("hits").schema();
    GenericData.Record collectionUpdateRecord =
        new GenericData.Record(WriteComputeSchemaConverter.getInstance().convert(innerArraySchema).getTypes().get(0));
    collectionUpdateRecord.put(SET_UNION, Collections.singletonList(timeStampRecord));
    collectionUpdateRecord.put(SET_DIFF, Collections.emptyList());
    wcRecord.put("name", collectionUpdateRecord);
    ValueAndRmd finalValueAndRmd = valueAndRmd;
    Assert.assertThrows(
        VeniceException.class,
        () -> genericRecordMerge.update(finalValueAndRmd, Lazy.of(() -> wcRecord), wcRecord.getSchema(), 10, -1, 1, 0));
  }

  @Test
  public void testPutPermutation() {
    Schema schema = AvroCompatibilityHelper.parse(RECORD_SCHEMA_STR);
    List<GenericRecord> payload = new ArrayList<>();
    List<Long> writeTs = new ArrayList<>();
    GenericRecord origRecord = new GenericData.Record(schema);
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(schema, 1);
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
    ValueAndRmd<GenericRecord> valueAndRmd = new ValueAndRmd<>(Lazy.of(() -> origRecord), timeStampRecord);
    Merge<GenericRecord> genericRecordMerge = createMergeGenericRecord();

    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        valueAndRmd = genericRecordMerge
            .put(valueAndRmd, GenericData.get().deepCopy(schema, payload.get(j)), writeTs.get(i), -1, 1, 0);
      }
    }
    // timestamp record should always contain the latest value
    Assert.assertEquals(valueAndRmd.getValue().get("id").toString(), "id99");
    Assert.assertEquals(valueAndRmd.getValue().get("name").toString(), "name99");
    Assert.assertEquals(valueAndRmd.getValue().get("age"), 109);
    Assert.assertEquals((long) valueAndRmd.getRmd().get(TIMESTAMP_FIELD_NAME), 110);

    valueAndRmd = new ValueAndRmd<>(Lazy.of(() -> origRecord), timeStampRecord);
    // swap timestamp and record order
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        valueAndRmd = genericRecordMerge
            .put(valueAndRmd, GenericData.get().deepCopy(schema, payload.get(i)), writeTs.get(j), -1, 1, 0);
      }
    }
    // timestamp record should always contain the latest value
    Assert.assertEquals(valueAndRmd.getValue().get("id").toString(), "id99");
    Assert.assertEquals(valueAndRmd.getValue().get("name").toString(), "name99");
    Assert.assertEquals(valueAndRmd.getValue().get("age"), 109);
    Assert.assertEquals((long) valueAndRmd.getRmd().get(TIMESTAMP_FIELD_NAME), 110);
  }

  private Merge<GenericRecord> createMergeGenericRecord() {
    MergeRecordHelper mergeRecordHelper = new CollectionTimestampMergeRecordHelper();
    WriteComputeProcessor writeComputeProcessor = new WriteComputeProcessor(mergeRecordHelper);
    return new MergeGenericRecord(writeComputeProcessor, mergeRecordHelper);
  }
}
