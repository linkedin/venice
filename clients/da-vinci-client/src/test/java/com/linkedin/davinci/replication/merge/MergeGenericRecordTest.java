package com.linkedin.davinci.replication.merge;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_DIFF;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_UNION;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.schema.merge.CollectionTimestampMergeRecordHelper;
import com.linkedin.davinci.schema.merge.MergeRecordHelper;
import com.linkedin.davinci.schema.merge.ValueAndRmd;
import com.linkedin.davinci.schema.writecompute.WriteComputeProcessor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
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

  private static final PubSubPosition P1 = new ApacheKafkaOffsetPosition(1L);

  @Test
  public void testDelete() {
    Schema schema = AvroCompatibilityHelper.parse(RECORD_SCHEMA_STR);
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(schema, 1);
    GenericRecord valueRecord = new GenericData.Record(schema);
    valueRecord.put("id", "id1");
    valueRecord.put("name", "name1");
    valueRecord.put("age", 10);
    GenericRecord timestampRecord = new GenericData.Record(aaSchema);
    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 25L);
    timestampRecord.put(0, ts);

    ValueAndRmd<GenericRecord> valueAndRmd = new ValueAndRmd<>(Lazy.of(() -> valueRecord), timestampRecord);

    Merge<GenericRecord> genericRecordMerge = createMergeGenericRecord();
    ValueAndRmd<GenericRecord> deletedValueAndRmd1 = genericRecordMerge.delete(valueAndRmd, 20, -1, P1, 0);
    // verify id and name fields are default (deleted)
    Assert.assertEquals(deletedValueAndRmd1.getValue().get("id").toString(), "id");
    Assert.assertEquals(deletedValueAndRmd1.getValue().get("name").toString(), "name");
    Assert.assertEquals(deletedValueAndRmd1.getValue().get("age"), 10);
    ts = (GenericRecord) deletedValueAndRmd1.getRmd().get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(ts.get("id"), 20L);
    Assert.assertEquals(ts.get("name"), 20L);
    Assert.assertEquals(ts.get("age"), 25L);
    // Verify that the same object is returned
    Assert.assertSame(deletedValueAndRmd1, valueAndRmd);

    // full delete. expect null value
    timestampRecord.put(0, 20L);
    valueAndRmd.setRmd(timestampRecord);
    ValueAndRmd<GenericRecord> deletedValueAndRmd2 = genericRecordMerge.delete(valueAndRmd, 30, -1, P1, 0);
    Assert.assertNull(deletedValueAndRmd2.getValue());
    // Verify that the same object is returned
    Assert.assertSame(deletedValueAndRmd2, valueAndRmd);

    // full delete based on field timestamp values
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 20L);
    timestampRecord.put(0, ts);
    valueAndRmd.setRmd(timestampRecord);
    valueAndRmd.setValue(valueRecord);
    valueAndRmd = genericRecordMerge.delete(valueAndRmd, 30, 1, P1, 0);
    Assert.assertNull(valueAndRmd.getValue());
    Assert.assertTrue(valueAndRmd.getRmd().get(TIMESTAMP_FIELD_NAME) instanceof GenericRecord);
    GenericRecord newTimestampRecord = (GenericRecord) valueAndRmd.getRmd().get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(newTimestampRecord.get("id"), 30L);
    Assert.assertEquals(newTimestampRecord.get("name"), 30L);
    Assert.assertEquals(newTimestampRecord.get("age"), 30L);

    // no delete, return same object
    timestampRecord.put(0, ts);
    valueAndRmd.setRmd(timestampRecord);
    valueAndRmd.setValue(valueRecord);
    ValueAndRmd<GenericRecord> deletedValueAndRmd3 = genericRecordMerge.delete(valueAndRmd, 5, -1, P1, 0);

    Assert.assertEquals(deletedValueAndRmd3.getValue(), valueAndRmd.getValue());
    Assert.assertEquals(
        (List<Long>) deletedValueAndRmd3.getRmd().get(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME),
        Collections.emptyList());
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
    ValueAndRmd<GenericRecord> mergedValueAndRmd1 = genericRecordMerge.put(valueAndRmd, newRecord, 30, -1, P1, 0);

    // verify id and name fields are from new record
    Assert.assertEquals(mergedValueAndRmd1.getValue().get("id"), newRecord.get(0));
    Assert.assertEquals(mergedValueAndRmd1.getValue().get("name"), newRecord.get(1));
    Assert.assertEquals(mergedValueAndRmd1.getValue().get("age"), newRecord.get(2));
    // Verify that the same object is returned
    Assert.assertSame(mergedValueAndRmd1, valueAndRmd);

    // verify we reuse the same instance when nothings changed.
    ValueAndRmd<GenericRecord> mergedValueAndRmd2 = genericRecordMerge.put(valueAndRmd, newRecord, 10, -1, P1, 0);
    Assert.assertEquals(mergedValueAndRmd2.getValue(), valueAndRmd.getValue());
    // Verify that the same object is returned
    Assert.assertSame(mergedValueAndRmd2, valueAndRmd);

    Schema schema2 = AvroCompatibilityHelper.parse(
        "{" + "\"fields\": ["
            + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
            + "   {\"default\": -1, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}"
            + "   ]," + " \"name\": \"testObject\", \"type\": \"record\"" + "}");
    newRecord = new GenericData.Record(schema2);
    GenericRecord finalNewRecord = newRecord;
    Assert
        .assertThrows(VeniceException.class, () -> genericRecordMerge.put(valueAndRmd, finalNewRecord, 10, -1, P1, 0));
  }

  @Test
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
    Schema noOpSchema = recordWriteComputeSchema.getField("name").schema().getTypes().get(0);
    GenericData.Record noOpRecord = new GenericData.Record(noOpSchema);

    GenericRecord wcRecord = new GenericData.Record(recordWriteComputeSchema);
    wcRecord.put("id", "id10");
    wcRecord.put("name", noOpRecord);
    wcRecord.put("age", 20);
    Merge<GenericRecord> genericRecordMerge = createMergeGenericRecord();
    valueAndRmd = genericRecordMerge.update(valueAndRmd, Lazy.of(() -> wcRecord), wcRecord.getSchema(), 30, -1, P1, 0);

    // verify id and name fields are from new record
    Assert.assertEquals(valueAndRmd.getValue().get("id"), wcRecord.get(0));
    Assert.assertEquals(valueAndRmd.getValue().get("name"), valueRecord.get(1));
    Assert.assertEquals(valueAndRmd.getValue().get("age"), wcRecord.get(2));
    ts = (GenericRecord) valueAndRmd.getRmd().get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(ts.get("id"), 30L);
    Assert.assertEquals(ts.get("name"), 10L);
    Assert.assertEquals(ts.get("age"), 30L);

    // verify we reuse the same instance when nothing changed.
    ValueAndRmd<GenericRecord> valueAndRmd1 =
        genericRecordMerge.update(valueAndRmd, Lazy.of(() -> wcRecord), wcRecord.getSchema(), 10, -1, P1, 0);
    Assert.assertTrue(valueAndRmd1.getValue() == valueAndRmd.getValue());

    // validate ts record change from LONG to GenericRecord.
    // timeStampRecord.put(0, 10L); // N.B. The test fails when this is uncommented. TODO: Figure out if it's important?
    valueAndRmd = genericRecordMerge.update(valueAndRmd, Lazy.of(() -> wcRecord), wcRecord.getSchema(), 30, -1, P1, 0);
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
        IllegalStateException.class,
        () -> genericRecordMerge
            .update(finalValueAndRmd, Lazy.of(() -> wcRecord), wcRecord.getSchema(), 10, -1, P1, 0));
  }

  @Test
  public void testPutPermutation() {
    Schema schema = AvroCompatibilityHelper.parse(RECORD_SCHEMA_STR);
    List<GenericRecord> payload = new ArrayList<>();
    List<Long> writeTs = new ArrayList<>();
    GenericRecord origRecord = new GenericData.Record(schema);
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(schema, 1);
    GenericRecord timestampRecord = new GenericData.Record(aaSchema);

    GenericRecord ts = new GenericData.Record(aaSchema.getFields().get(0).schema().getTypes().get(1));
    ts.put("id", 10L);
    ts.put("name", 10L);
    ts.put("age", 20L);
    timestampRecord.put(0, ts);
    origRecord.put("id", "id0");
    origRecord.put("name", "name0");
    origRecord.put("age", 0);

    for (int i = 0; i < 100; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("id", "id" + i);
      record.put("name", "name" + i);
      record.put("age", i);
      payload.add(record);
      writeTs.add((long) (i));
    }
    ValueAndRmd<GenericRecord> valueAndRmd = new ValueAndRmd<>(Lazy.of(() -> origRecord), timestampRecord);
    Merge<GenericRecord> genericRecordMerge = createMergeGenericRecord();

    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        valueAndRmd = genericRecordMerge
            .put(valueAndRmd, GenericData.get().deepCopy(schema, payload.get(j)), writeTs.get(i), -1, P1, 0);
      }
    }
    // timestamp record should always contain the latest value
    Assert.assertEquals(valueAndRmd.getValue().get("id").toString(), "id99");
    Assert.assertEquals(valueAndRmd.getValue().get("name").toString(), "name99");
    Assert.assertEquals(valueAndRmd.getValue().get("age"), 99);
    Assert.assertTrue(valueAndRmd.getRmd().get(TIMESTAMP_FIELD_NAME) instanceof GenericRecord);
    GenericRecord newTimestampRecord = (GenericRecord) valueAndRmd.getRmd().get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(newTimestampRecord.get("id"), 99L);
    Assert.assertEquals(newTimestampRecord.get("name"), 99L);
    Assert.assertEquals(newTimestampRecord.get("age"), 99L);

    valueAndRmd = new ValueAndRmd<>(Lazy.of(() -> origRecord), timestampRecord);
    // swap timestamp and record order
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        valueAndRmd = genericRecordMerge
            .put(valueAndRmd, GenericData.get().deepCopy(schema, payload.get(i)), writeTs.get(j), -1, P1, 0);
      }
    }
    // timestamp record should always contain the latest value
    Assert.assertEquals(valueAndRmd.getValue().get("id").toString(), "id99");
    Assert.assertEquals(valueAndRmd.getValue().get("name").toString(), "name99");
    Assert.assertEquals(valueAndRmd.getValue().get("age"), 99);
    Assert.assertTrue(valueAndRmd.getRmd().get(TIMESTAMP_FIELD_NAME) instanceof GenericRecord);
    newTimestampRecord = (GenericRecord) valueAndRmd.getRmd().get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(newTimestampRecord.get("id"), 99L);
    Assert.assertEquals(newTimestampRecord.get("name"), 99L);
    Assert.assertEquals(newTimestampRecord.get("age"), 99L);

  }

  private Merge<GenericRecord> createMergeGenericRecord() {
    MergeRecordHelper mergeRecordHelper = new CollectionTimestampMergeRecordHelper();
    WriteComputeProcessor writeComputeProcessor = new WriteComputeProcessor(mergeRecordHelper);
    return new MergeGenericRecord(writeComputeProcessor, mergeRecordHelper);
  }
}
