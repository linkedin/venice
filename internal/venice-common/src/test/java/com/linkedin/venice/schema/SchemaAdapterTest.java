package com.linkedin.venice.schema;

import com.linkedin.alpini.io.IOUtils;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SchemaAdapterTest {
  private static final Logger LOGGER = LogManager.getLogger(SchemaAdapterTest.class);
  private static final Schema SIMPLE_RECORD_SCHEMA = AvroSchemaParseUtils
      .parseSchemaFromJSONStrictValidation(loadFileAsString("AvroRecordUtilsTest/SimpleRecordSchema.avsc"));
  private static final Schema OLD_RECORD_SCHEMA = AvroSchemaParseUtils
      .parseSchemaFromJSONStrictValidation(loadFileAsString("AvroRecordUtilsTest/OldRecordSchema.avsc"));
  private static final Schema EVOLVED_RECORD_SCHEMA = AvroSchemaParseUtils
      .parseSchemaFromJSONStrictValidation(loadFileAsString("AvroRecordUtilsTest/EvolvedRecordSchema.avsc"));

  private static String loadFileAsString(String fileName) {
    try {
      return IOUtils.toString(
          Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)),
          StandardCharsets.UTF_8);
    } catch (Exception e) {
      LOGGER.error(e);
      return null;
    }
  }

  @Test
  public void testAdaptRecordWithSameSchema() {
    GenericRecord record = new GenericData.Record(SIMPLE_RECORD_SCHEMA);
    record.put("field", 1);

    Object adaptedRecord = SchemaAdapter.adaptToSchema(SIMPLE_RECORD_SCHEMA, record);
    Assert.assertSame(adaptedRecord, record);
  }

  @Test
  public void testAdaptRecordFillMissingField() {
    GenericRecord inputRecord = new GenericData.Record(OLD_RECORD_SCHEMA);
    GenericRecord inputSubRecord = new GenericData.Record(OLD_RECORD_SCHEMA.getField("field1").schema());
    inputSubRecord.put("field1_1", 1);
    inputRecord.put("field1", inputSubRecord);

    Object adaptedRecord = SchemaAdapter.adaptToSchema(EVOLVED_RECORD_SCHEMA, inputRecord);
    Assert.assertTrue(adaptedRecord instanceof GenericRecord);
    GenericRecord adaptedGenericRecord = (GenericRecord) adaptedRecord;
    Assert.assertEquals(adaptedGenericRecord.getSchema(), EVOLVED_RECORD_SCHEMA);
    Assert.assertTrue(adaptedGenericRecord.get("field1") instanceof GenericRecord);
    Assert.assertEquals(((GenericRecord) adaptedGenericRecord.get("field1")).get("field1_1"), 1);
    Assert.assertEquals(((GenericRecord) adaptedGenericRecord.get("field1")).get("field1_2"), -5);
    Assert.assertEquals(adaptedGenericRecord.get("field2"), -10);
  }

  @Test
  public void testAdaptRecordRemoveExtraField() {
    GenericRecord inputRecord = new GenericData.Record(EVOLVED_RECORD_SCHEMA);
    GenericRecord inputSubRecord = new GenericData.Record(EVOLVED_RECORD_SCHEMA.getField("field1").schema());
    inputSubRecord.put("field1_1", 1);
    inputSubRecord.put("field1_2", 5);
    inputRecord.put("field1", inputSubRecord);
    inputRecord.put("field2", 3);

    Object adaptedRecord = SchemaAdapter.adaptToSchema(OLD_RECORD_SCHEMA, inputRecord);
    Assert.assertTrue(adaptedRecord instanceof GenericRecord);
    GenericRecord adaptedGenericRecord = (GenericRecord) adaptedRecord;
    Assert.assertEquals(adaptedGenericRecord.getSchema(), OLD_RECORD_SCHEMA);
    Assert.assertTrue(adaptedGenericRecord.get("field1") instanceof GenericRecord);
    Assert.assertEquals(((GenericRecord) adaptedGenericRecord.get("field1")).get("field1_1"), 1);
    Assert.assertNull(((GenericRecord) adaptedGenericRecord.get("field1")).get("field1_2"));
    Assert.assertNull(adaptedGenericRecord.get("field2"));
  }

  @Test
  public void testAdaptRecordInArraySchema() {
    Schema arraySchema = Schema.createArray(EVOLVED_RECORD_SCHEMA);

    GenericRecord inputRecord = new GenericData.Record(OLD_RECORD_SCHEMA);
    GenericRecord inputSubRecord = new GenericData.Record(OLD_RECORD_SCHEMA.getField("field1").schema());
    inputSubRecord.put("field1_1", 1);
    inputRecord.put("field1", inputSubRecord);

    GenericRecord inputRecord2 = new GenericData.Record(EVOLVED_RECORD_SCHEMA);
    GenericRecord inputSubRecord2 = new GenericData.Record(EVOLVED_RECORD_SCHEMA.getField("field1").schema());
    inputSubRecord2.put("field1_1", 1);
    inputSubRecord2.put("field1_2", 5);
    inputRecord2.put("field1", inputSubRecord2);
    inputRecord2.put("field2", 10);

    Object adaptedList = SchemaAdapter.adaptToSchema(arraySchema, Arrays.asList(inputRecord, inputRecord2));
    Assert.assertTrue(adaptedList instanceof List);

    Object adaptedRecord = ((List<?>) adaptedList).get(0);
    Assert.assertTrue(adaptedRecord instanceof GenericRecord);
    GenericRecord adaptedGenericRecord = (GenericRecord) adaptedRecord;
    Assert.assertEquals(adaptedGenericRecord.getSchema(), EVOLVED_RECORD_SCHEMA);
    Assert.assertTrue(adaptedGenericRecord.get("field1") instanceof GenericRecord);
    Assert.assertEquals(((GenericRecord) adaptedGenericRecord.get("field1")).get("field1_1"), 1);
    Assert.assertEquals(((GenericRecord) adaptedGenericRecord.get("field1")).get("field1_2"), -5);
    Assert.assertEquals(adaptedGenericRecord.get("field2"), -10);

    // If a record follows the same schema as the required schema, don't evolve it
    Object adaptedRecord2 = ((List<?>) adaptedList).get(1);
    Assert.assertTrue(adaptedRecord2 instanceof GenericRecord);
    Assert.assertSame(adaptedRecord2, inputRecord2);
  }

  @Test
  public void testAdaptRecordInMapSchema() {
    Schema mapSchema = Schema.createMap(EVOLVED_RECORD_SCHEMA);

    GenericRecord inputRecord = new GenericData.Record(OLD_RECORD_SCHEMA);
    GenericRecord inputSubRecord = new GenericData.Record(OLD_RECORD_SCHEMA.getField("field1").schema());
    inputSubRecord.put("field1_1", 1);
    inputRecord.put("field1", inputSubRecord);

    GenericRecord inputRecord2 = new GenericData.Record(EVOLVED_RECORD_SCHEMA);
    GenericRecord inputSubRecord2 = new GenericData.Record(EVOLVED_RECORD_SCHEMA.getField("field1").schema());
    inputSubRecord2.put("field1_1", 1);
    inputSubRecord2.put("field1_2", 5);
    inputRecord2.put("field1", inputSubRecord2);
    inputRecord2.put("field2", 10);

    Map<String, GenericRecord> recordMap = new LinkedHashMap<>();
    recordMap.put("testEvolveSchema", inputRecord);
    recordMap.put("testSameSchema", inputRecord2);

    Object adaptedMap = SchemaAdapter.adaptToSchema(mapSchema, recordMap);
    Assert.assertTrue(adaptedMap instanceof Map);

    Assert.assertEquals(((Map<String, ?>) adaptedMap).size(), 2);

    Object adaptedRecord = ((Map<String, ?>) adaptedMap).get("testEvolveSchema");
    Assert.assertTrue(adaptedRecord instanceof GenericRecord);
    GenericRecord adaptedGenericRecord = (GenericRecord) adaptedRecord;
    Assert.assertEquals(adaptedGenericRecord.getSchema(), EVOLVED_RECORD_SCHEMA);
    Assert.assertTrue(adaptedGenericRecord.get("field1") instanceof GenericRecord);
    Assert.assertEquals(((GenericRecord) adaptedGenericRecord.get("field1")).get("field1_1"), 1);
    Assert.assertEquals(((GenericRecord) adaptedGenericRecord.get("field1")).get("field1_2"), -5);
    Assert.assertEquals(adaptedGenericRecord.get("field2"), -10);

    // If a record follows the same schema as the required schema, don't evolve it
    Object adaptedRecord2 = ((Map<String, ?>) adaptedMap).get("testSameSchema");
    Assert.assertTrue(adaptedRecord2 instanceof GenericRecord);
    Assert.assertSame(adaptedRecord2, inputRecord2);
  }

  @Test
  public void testAdaptRecordInUnionSchema() {
    Schema unionSchema = Schema.createUnion(EVOLVED_RECORD_SCHEMA, Schema.create(Schema.Type.NULL));

    GenericRecord inputRecord = new GenericData.Record(OLD_RECORD_SCHEMA);
    GenericRecord inputSubRecord = new GenericData.Record(OLD_RECORD_SCHEMA.getField("field1").schema());
    inputSubRecord.put("field1_1", 1);
    inputRecord.put("field1", inputSubRecord);

    GenericRecord inputRecord2 = new GenericData.Record(EVOLVED_RECORD_SCHEMA);
    GenericRecord inputSubRecord2 = new GenericData.Record(EVOLVED_RECORD_SCHEMA.getField("field1").schema());
    inputSubRecord2.put("field1_1", 1);
    inputSubRecord2.put("field1_2", 5);
    inputRecord2.put("field1", inputSubRecord2);
    inputRecord2.put("field2", 10);

    Object adaptedRecord = SchemaAdapter.adaptToSchema(unionSchema, inputRecord);
    Assert.assertTrue(adaptedRecord instanceof GenericRecord);
    GenericRecord adaptedGenericRecord = (GenericRecord) adaptedRecord;
    Assert.assertEquals(adaptedGenericRecord.getSchema(), EVOLVED_RECORD_SCHEMA);
    Assert.assertTrue(adaptedGenericRecord.get("field1") instanceof GenericRecord);
    Assert.assertEquals(((GenericRecord) adaptedGenericRecord.get("field1")).get("field1_1"), 1);
    Assert.assertEquals(((GenericRecord) adaptedGenericRecord.get("field1")).get("field1_2"), -5);
    Assert.assertEquals(adaptedGenericRecord.get("field2"), -10);

    // If a record follows the same schema as the required schema, don't evolve it
    Object adaptedRecord2 = SchemaAdapter.adaptToSchema(unionSchema, inputRecord2);
    Assert.assertTrue(adaptedRecord2 instanceof GenericRecord);
    Assert.assertSame(adaptedRecord2, inputRecord2);
  }
}
