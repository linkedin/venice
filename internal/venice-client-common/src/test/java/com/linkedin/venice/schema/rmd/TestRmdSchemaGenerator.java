package com.linkedin.venice.schema.rmd;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.Schema.Type.UNION;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.AvroSchemaUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestRmdSchemaGenerator {
  private static final Logger LOGGER = LogManager.getLogger(TestRmdSchemaGenerator.class);
  private static final Schema EMPTY_RECORD_SCHEMA;
  static {
    EMPTY_RECORD_SCHEMA = Schema.createRecord("EmptyRecord", "", "", false);
    EMPTY_RECORD_SCHEMA.setFields(Collections.emptyList());
  }

  static String primitiveTypedSchemaStr = "{\"type\": \"string\"}";
  // Expected Replication metadata schema for above Schema
  static String aaSchemaPrimitive = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"string_MetadataRecord\",\n"
      + "  \"namespace\" : \"com.linkedin.venice\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"timestamp\",\n"
      + "    \"type\" : [ \"long\" ],\n" + "    \"doc\" : \"timestamp when the full record was last updated\",\n"
      + "    \"default\" : 0\n" + "  }, {\n" + "    \"name\" : \"replication_checkpoint_vector\",\n"
      + "    \"type\" : {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : \"long\"\n" + "    },\n"
      + "    \"doc\" : \"high watermark remote checkpoints which touched this record\",\n" + "    \"default\" : [ ]\n"
      + "  } ]\n" + "}";

  static String recordSchemaStr = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"User\",\n"
      + "  \"namespace\" : \"example.avro\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"id\",\n"
      + "    \"type\" : \"string\",\n" + "    \"default\" : \"id\"\n" + "  }, {\n" + "    \"name\" : \"name\",\n"
      + "    \"type\" : \"string\",\n" + "    \"default\" : \"id\"\n" + "  }, {\n" + "    \"name\" : \"age\",\n"
      + "    \"type\" : \"int\",\n" + "    \"default\" : -1\n" + "  } ]\n" + "}";
  // Expected Replication metadata schema for above Schema
  static String aaSchemaRecord = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"User_MetadataRecord\",\n"
      + "  \"namespace\" : \"example.avro\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"timestamp\",\n"
      + "    \"type\" : [ \"long\", {\n" + "      \"type\" : \"record\",\n" + "      \"name\" : \"User\",\n"
      + "      \"fields\" : [ {\n" + "        \"name\" : \"id\",\n" + "        \"type\" : \"long\",\n"
      + "        \"doc\" : \"timestamp when id of the record was last updated\",\n" + "        \"default\" : 0\n"
      + "      }, {\n" + "        \"name\" : \"name\",\n" + "        \"type\" : \"long\",\n"
      + "        \"doc\" : \"timestamp when name of the record was last updated\",\n" + "        \"default\" : 0\n"
      + "      }, {\n" + "        \"name\" : \"age\",\n" + "        \"type\" : \"long\",\n"
      + "        \"doc\" : \"timestamp when age of the record was last updated\",\n" + "        \"default\" : 0\n"
      + "      } ]\n" + "    } ],\n" + "    \"doc\" : \"timestamp when the full record was last updated\",\n"
      + "    \"default\" : 0\n" + "  }, {\n" + "    \"name\" : \"replication_checkpoint_vector\",\n"
      + "    \"type\" : {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : \"long\"\n" + "    },\n"
      + "    \"doc\" : \"high watermark remote checkpoints which touched this record\",\n" + "    \"default\" : [ ]\n"
      + "  } ]\n" + "}";

  static String arraySchemaStr = "{ \"type\": \"array\", \"items\": \"int\" }";
  // Expected Replication metadata schema for above Schema
  static String aaSchemaArray = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"array_MetadataRecord\",\n"
      + "  \"namespace\" : \"com.linkedin.venice\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"timestamp\",\n"
      + "    \"type\" : [ \"long\" ],\n" + "    \"doc\" : \"timestamp when the full record was last updated\",\n"
      + "    \"default\" : 0\n" + "  } , {\n" + "    \"name\" : \"replication_checkpoint_vector\",\n"
      + "    \"type\" : {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : \"long\"\n" + "    },\n"
      + "    \"doc\" : \"high watermark remote checkpoints which touched this record\",\n" + "    \"default\" : [ ]\n"
      + "  } ]\n" + "}";

  static String mapSchemaStr = "{ \"type\": \"map\", \"values\": \"int\" }";
  // Expected Replication metadata schema for above Schema
  static String aaSchemaMap = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"map_MetadataRecord\",\n"
      + "  \"namespace\" : \"com.linkedin.venice\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"timestamp\",\n"
      + "    \"type\" : [ \"long\" ],\n" + "    \"doc\" : \"timestamp when the full record was last updated\",\n"
      + "    \"default\" : 0\n" + "  } , {\n" + "    \"name\" : \"replication_checkpoint_vector\",\n"
      + "    \"type\" : {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : \"long\"\n" + "    },\n"
      + "    \"doc\" : \"high watermark remote checkpoints which touched this record\",\n" + "    \"default\" : [ ]\n"
      + "  } ]\n" + "}";

  static String unionSchemaStr = "[ {\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"namerecord\",\n"
      + "  \"namespace\" : \"example.avro\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"firstname\",\n"
      + "    \"type\" : \"string\",\n" + "    \"default\" : \"\"\n" + "  }, {\n" + "    \"name\" : \"lastname\",\n"
      + "    \"type\" : \"string\",\n" + "    \"default\" : \"\"\n" + "  } ]\n" + "}, {\n" + "  \"type\" : \"array\",\n"
      + "  \"items\" : \"int\"\n" + "}, {\n" + "  \"type\" : \"map\",\n" + "  \"values\" : \"int\"\n"
      + "}, \"string\" ]";
  // Expected Replication metadata schema for above Schema
  static String rmdSchemaUnion = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"union_MetadataRecord\",\n"
      + "  \"namespace\" : \"com.linkedin.venice\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"timestamp\",\n"
      + "    \"type\" : [ \"long\" ],\n" + "    \"doc\" : \"timestamp when the full record was last updated\",\n"
      + "    \"default\" : 0\n" + "  } , {\n" + "    \"name\" : \"replication_checkpoint_vector\",\n"
      + "    \"type\" : {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : \"long\"\n" + "    },\n"
      + "    \"doc\" : \"high watermark remote checkpoints which touched this record\",\n" + "    \"default\" : [ ]\n"
      + "  } ]\n" + "}";

  @Test
  public void testMetadataSchemaForPrimitive() {
    Schema origSchema = Schema.create(INT);
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(origSchema);
    String aaSchemaStr = aaSchema.toString(true);
    LOGGER.info(aaSchemaStr);

    verifyFullUpdateTsRecordPresent(aaSchema, true);
  }

  @Test
  public void testMetadataSchemaForPrimitiveTyped() {
    Schema origSchema = AvroCompatibilityHelper.parse(primitiveTypedSchemaStr);
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(origSchema);
    String aaSchemaStr = aaSchema.toString(true);
    LOGGER.info(aaSchemaStr);
    Assert.assertEquals(aaSchema, AvroCompatibilityHelper.parse(aaSchema.toString()));
    Assert.assertEquals(
        AvroCompatibilityHelper.parse(aaSchemaPrimitive),
        AvroCompatibilityHelper.parse(aaSchema.toString()));
    verifyFullUpdateTsRecordPresent(aaSchema, true);

  }

  @Test
  public void testMetadataSchemaForRecord() {
    Schema origSchema = AvroCompatibilityHelper.parse(recordSchemaStr);
    String OrigSchemaStr = origSchema.toString(true);
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(origSchema);
    String aaSchemaStr = aaSchema.toString(true);
    LOGGER.info(OrigSchemaStr);
    LOGGER.info(aaSchemaStr);

    Assert.assertEquals(aaSchema, AvroCompatibilityHelper.parse(aaSchema.toString()));
    Assert.assertEquals(
        AvroCompatibilityHelper.parse(aaSchemaRecord),
        AvroCompatibilityHelper.parse(aaSchema.toString()));

    verifyFullUpdateTsRecordPresent(aaSchema, false);
    Schema recordTsSchema = aaSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    Assert.assertEquals(recordTsSchema.getType(), RECORD);
    Assert.assertEquals(recordTsSchema.getFields().size(), 3);
    Assert.assertEquals(recordTsSchema.getField("id").schema().getType(), LONG);
    Assert.assertEquals(recordTsSchema.getField("name").schema().getType(), LONG);
    Assert.assertEquals(recordTsSchema.getField("age").schema().getType(), LONG);
    Assert.assertEquals(AvroSchemaUtils.getFieldDefault(recordTsSchema.getField("id")), 0L);
    Assert.assertEquals(AvroSchemaUtils.getFieldDefault(recordTsSchema.getField("name")), 0L);
    Assert.assertEquals(AvroSchemaUtils.getFieldDefault(recordTsSchema.getField("age")), 0L);
    Assert.assertEquals((long) AvroCompatibilityHelper.getGenericDefaultValue(recordTsSchema.getField("id")), 0);
    Assert.assertEquals((long) AvroCompatibilityHelper.getGenericDefaultValue(recordTsSchema.getField("name")), 0);
    Assert.assertEquals((long) AvroCompatibilityHelper.getGenericDefaultValue(recordTsSchema.getField("age")), 0);
  }

  @Test(dataProvider = "recordWithPrimitiveArraysOfSameElementType")
  public void testMetadataSchemaForRecordWithPrimitiveArraysOfSameElementType(Schema origSchema) {
    Assert.assertEquals(
        origSchema.getField("field1").schema().getType(),
        origSchema.getField("field2").schema().getType(),
        "Expect the original schema to have 2 fields with the same type");
    Assert.assertEquals(
        origSchema.getField("field1").schema().getElementType(),
        origSchema.getField("field2").schema().getElementType(),
        "Expect the original schema to have 2 fields with the same element type");

    final Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(origSchema);
    LOGGER.info(origSchema.toString(true));
    LOGGER.info(rmdSchema.toString(true));

    verifyFullUpdateTsRecordPresent(rmdSchema, false);
    Assert.assertEquals(rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getType(), UNION);
    Assert.assertEquals(rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(0).getType(), LONG);
    Assert.assertEquals(rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1).getType(), RECORD);

    Schema recordTsSchema = rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    Assert.assertEquals(recordTsSchema.getFields().size(), 2);

    Schema.Type originalElementSchemaType = origSchema.getField("field1").schema().getElementType().getType();
    validateCollectionFieldReplicationMetadata(recordTsSchema, "field1", originalElementSchemaType);
    validateCollectionFieldReplicationMetadata(recordTsSchema, "field2", originalElementSchemaType);

    Assert.assertSame(
        recordTsSchema.getField("field1").schema(),
        recordTsSchema.getField("field2").schema(),
        "Both fields should share the same schema object");
  }

  @Test(dataProvider = "recordWithArraysOfDifferentElementType")
  public void testMetadataSchemaForRecordWithPrimitiveArraysOfDifferentElementType(Schema origSchema) {
    Assert.assertEquals(
        origSchema.getField("field1").schema().getType(),
        origSchema.getField("field2").schema().getType(),
        "Expect the original schema to have 2 fields with the array type");
    Assert.assertNotEquals(
        origSchema.getField("field1").schema().getElementType(),
        origSchema.getField("field2").schema().getElementType(),
        "Expect the original schema to have 2 fields with different array element type");

    final Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(origSchema);
    LOGGER.info(origSchema.toString(true));
    LOGGER.info(rmdSchema.toString(true));

    verifyFullUpdateTsRecordPresent(rmdSchema, false);

    Assert.assertEquals(rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getType(), UNION);
    Assert.assertEquals(rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(0).getType(), LONG);
    Assert.assertEquals(rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1).getType(), RECORD);

    Schema recordTsSchema = rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    Assert.assertEquals(recordTsSchema.getFields().size(), 2);

    Schema.Type originalElementSchemaType = origSchema.getField("field1").schema().getElementType().getType();
    validateCollectionFieldReplicationMetadata(recordTsSchema, "field1", originalElementSchemaType);

    originalElementSchemaType = origSchema.getField("field2").schema().getElementType().getType();
    validateCollectionFieldReplicationMetadata(recordTsSchema, "field2", originalElementSchemaType);

    Assert.assertTrue(
        recordTsSchema.getField("field1").schema() != recordTsSchema.getField("field2").schema(),
        "Both fields should NOT share the same schema object");
  }

  @Test(dataProvider = "recordWithArraysOfHybridElementType")
  public void testMetadataSchemaForRecordWithArraysOfHybridElementType(Schema origSchema) {
    Assert.assertEquals(
        origSchema.getField("field1").schema().getType(),
        origSchema.getField("field2").schema().getType(),
        "Expect the original schema to have first 2 fields with the array type");
    Assert.assertEquals(
        origSchema.getField("field2").schema().getType(),
        origSchema.getField("field3").schema().getType(),
        "Expect the original schema to have last 2 fields with the array type");
    Assert.assertEquals(
        origSchema.getField("field1").schema().getElementType(),
        origSchema.getField("field2").schema().getElementType(),
        "Expect the original schema to have first 2 fields with same array element type");
    Assert.assertNotEquals(
        origSchema.getField("field2").schema().getElementType(),
        origSchema.getField("field3").schema().getElementType(),
        "Expect the original schema to have last 2 fields with different array element type");

    final Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(origSchema);
    LOGGER.info(origSchema.toString(true));
    LOGGER.info(rmdSchema.toString(true));

    verifyFullUpdateTsRecordPresent(rmdSchema, false);

    Assert.assertEquals(rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getType(), UNION);
    Assert.assertEquals(rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(0).getType(), LONG);
    Assert.assertEquals(rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1).getType(), RECORD);

    Schema recordTsSchema = rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    Assert.assertEquals(recordTsSchema.getFields().size(), 3); // We have 3 fields in the original schema now

    Schema.Type originalElementSchemaType = origSchema.getField("field1").schema().getElementType().getType();
    validateCollectionFieldReplicationMetadata(recordTsSchema, "field1", originalElementSchemaType);

    originalElementSchemaType = origSchema.getField("field2").schema().getElementType().getType();
    validateCollectionFieldReplicationMetadata(recordTsSchema, "field2", originalElementSchemaType);

    originalElementSchemaType = origSchema.getField("field3").schema().getElementType().getType();
    validateCollectionFieldReplicationMetadata(recordTsSchema, "field3", originalElementSchemaType);

    Assert.assertTrue(
        recordTsSchema.getField("field1").schema() == recordTsSchema.getField("field2").schema(),
        "First 2 fields should share the same schema object");
    Assert.assertTrue(
        recordTsSchema.getField("field2").schema() != recordTsSchema.getField("field3").schema(),
        "Last 2 fields should NOT share the same schema object");
  }

  private void validateCollectionFieldReplicationMetadata(
      Schema recordTsSchema,
      String fieldName,
      Schema.Type expectedElementSchemaType) {
    Schema field1TsSchema = recordTsSchema.getField(fieldName).schema();
    Assert.assertEquals(field1TsSchema.getType(), RECORD);
    Assert.assertEquals(field1TsSchema.getFields().size(), 6);
    Assert.assertEquals(field1TsSchema.getField("topLevelFieldTimestamp").schema().getType(), LONG);
    Assert.assertEquals(field1TsSchema.getField("topLevelColoID").schema().getType(), INT);
    Assert.assertEquals(field1TsSchema.getField("putOnlyPartLength").schema().getType(), INT);
    Assert.assertEquals(field1TsSchema.getField("activeElementsTimestamps").schema().getType(), ARRAY);
    Assert.assertEquals(field1TsSchema.getField("activeElementsTimestamps").schema().getElementType().getType(), LONG);
    Assert.assertEquals(field1TsSchema.getField("deletedElementsIdentities").schema().getType(), ARRAY);
    Assert.assertEquals(
        field1TsSchema.getField("deletedElementsIdentities").schema().getElementType().getType(),
        expectedElementSchemaType);
    Assert.assertEquals(field1TsSchema.getField("deletedElementsTimestamps").schema().getType(), ARRAY);
    Assert.assertEquals(field1TsSchema.getField("deletedElementsTimestamps").schema().getElementType().getType(), LONG);
  }

  @Test
  public void testMetadataSchemaForRecordOfMap() {
    Schema origSchema = createRecordOfMap(Schema.createMap(Schema.create(INT)));
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(origSchema);

    LOGGER.info(origSchema.toString(true));
    LOGGER.info(rmdSchema.toString(true));

    verifyFullUpdateTsRecordPresent(rmdSchema, false);
    Schema recordTsSchema = rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);

    Schema field1TsSchema = recordTsSchema.getField("field1").schema();
    Schema field2TsSchema = recordTsSchema.getField("field2").schema();
    Assert.assertSame(field1TsSchema, field2TsSchema, "Both fields should share the same field schema");
    validateCollectionFieldReplicationMetadata(recordTsSchema, "field1", STRING);
    validateCollectionFieldReplicationMetadata(recordTsSchema, "field2", STRING);
  }

  @Test
  public void testMetadataSchemaForArray() {
    Schema origSchema = AvroCompatibilityHelper.parse(arraySchemaStr);
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(origSchema);
    LOGGER.info(origSchema.toString(true));
    LOGGER.info(rmdSchema.toString(true));
    Assert.assertEquals(
        AvroCompatibilityHelper.parse(aaSchemaArray),
        AvroCompatibilityHelper.parse(rmdSchema.toString()));
    verifyFullUpdateTsRecordPresent(rmdSchema, true);
  }

  @Test
  public void testMetadataSchemaForMap() {
    Schema origSchema = AvroCompatibilityHelper.parse(mapSchemaStr);
    String OrigSchemaStr = origSchema.toString(true);
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(origSchema);
    String aaSchemaStr = aaSchema.toString(true);
    LOGGER.info(OrigSchemaStr);
    LOGGER.info(aaSchemaStr);
    Assert.assertEquals(AvroCompatibilityHelper.parse(aaSchemaMap), AvroCompatibilityHelper.parse(aaSchema.toString()));
    verifyFullUpdateTsRecordPresent(aaSchema, true);
  }

  @Test
  public void testMetadataSchemaForUnion() {
    Schema origSchema = AvroCompatibilityHelper.parse(unionSchemaStr);
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(origSchema);
    LOGGER.info(origSchema.toString(true));
    LOGGER.info(rmdSchema.toString(true));
    Assert.assertEquals(
        AvroCompatibilityHelper.parse(rmdSchemaUnion),
        AvroCompatibilityHelper.parse(rmdSchema.toString()));
    verifyFullUpdateTsRecordPresent(rmdSchema, true);
  }

  @Test
  public void testMetadataSchemaForUnionWithNullableEmptyRecord() {
    List<Schema> schemasInUnion = new ArrayList<>(2);
    schemasInUnion.add(Schema.create(NULL));
    schemasInUnion.add(EMPTY_RECORD_SCHEMA);
    Schema unionFieldSchema = Schema.createUnion(schemasInUnion);
    Schema origSchema = Schema.createRecord("originalSchemaRecord", "", "", false);
    origSchema.setFields(
        Collections.singletonList(
            AvroCompatibilityHelper.newField(null)
                .setName("unionField")
                .setSchema(unionFieldSchema)
                .setDefault(null)
                .setDoc("")
                .setOrder(Schema.Field.Order.ASCENDING)
                .build()));
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(origSchema);
    verifyFullUpdateTsRecordPresent(rmdSchema, false);
  }

  @Test
  public void testMetadataSchemaForUnionWithNullableCollectionRecord() {
    List<Schema> schemasInUnion = new ArrayList<>(2);
    schemasInUnion.add(Schema.create(NULL));
    schemasInUnion.add(Schema.createArray(Schema.create(INT)));
    Schema unionFieldSchema = Schema.createUnion(schemasInUnion);

    Schema origSchema = Schema.createRecord("originalSchemaRecord", "", "", false);
    origSchema.setFields(
        Collections.singletonList(
            AvroCompatibilityHelper.newField(null)
                .setName("field1")
                .setSchema(unionFieldSchema)
                .setDefault(null)
                .setDoc("")
                .setOrder(Schema.Field.Order.ASCENDING)
                .build()));

    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(origSchema);
    verifyFullUpdateTsRecordPresent(rmdSchema, false);

    Assert.assertEquals(rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getType(), UNION);
    Assert.assertEquals(rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(0).getType(), LONG);
    Assert.assertEquals(rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1).getType(), RECORD);

    Schema recordTsSchema = rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    Assert.assertEquals(recordTsSchema.getFields().size(), 1);
    validateCollectionFieldReplicationMetadata(recordTsSchema, "field1", INT);
  }

  private void verifyFullUpdateTsRecordPresent(Schema rmdSchema, boolean onlyRootTsPresent) {
    Assert.assertEquals(rmdSchema.getType(), RECORD);
    Assert.assertEquals(rmdSchema.getFields().size(), 2);
    Schema.Field tsField = rmdSchema.getField(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(tsField.schema().getType(), UNION);
    Assert.assertEquals(AvroSchemaUtils.getFieldDefault(tsField), 0L);

    Assert.assertEquals(rmdSchema.getType(), RECORD);
    Schema.Field vectorField = rmdSchema.getField(REPLICATION_CHECKPOINT_VECTOR_FIELD);
    Assert.assertEquals(vectorField.schema().getType(), ARRAY);
    Assert.assertEquals(AvroSchemaUtils.getFieldDefault(vectorField), Collections.EMPTY_LIST);

    if (onlyRootTsPresent) {
      Assert.assertEquals(tsField.schema().getTypes().size(), 1);
      Assert.assertEquals(tsField.schema().getTypes().get(0).getType(), LONG);
    } else {
      Assert.assertEquals(tsField.schema().getTypes().size(), 2);
      Assert.assertEquals(tsField.schema().getTypes().get(0).getType(), LONG);
      Assert.assertEquals(tsField.schema().getTypes().get(1).getType(), RECORD);
    }
  }

  @DataProvider(name = "recordWithPrimitiveArraysOfSameElementType")
  public static Object[] recordWithPrimitiveArraysOfSameElementType() {
    return new Object[] { createRecordWithPrimitiveArraysOfSameElementType(INT),
        createRecordWithPrimitiveArraysOfSameElementType(FLOAT), createRecordWithPrimitiveArraysOfSameElementType(LONG),
        createRecordWithPrimitiveArraysOfSameElementType(DOUBLE),
        createRecordWithPrimitiveArraysOfSameElementType(BOOLEAN),
        createRecordWithPrimitiveArraysOfSameElementType(STRING),
        createRecordWithPrimitiveArraysOfSameElementType(BYTES) };
  }

  @DataProvider(name = "recordWithArraysOfDifferentElementType")
  public static Object[] recordWithArraysOfDifferentElementType() {
    return new Object[] { createRecordWithArraysOfDifferentElementType(Schema.create(INT), Schema.create(FLOAT)),
        createRecordWithArraysOfDifferentElementType(Schema.create(FLOAT), Schema.create(LONG)),
        createRecordWithArraysOfDifferentElementType(Schema.create(LONG), Schema.create(DOUBLE)),
        createRecordWithArraysOfDifferentElementType(Schema.create(DOUBLE), EMPTY_RECORD_SCHEMA),
        createRecordWithArraysOfDifferentElementType(Schema.create(BOOLEAN), Schema.create(STRING)),
        createRecordWithArraysOfDifferentElementType(Schema.create(STRING), Schema.create(BYTES)),
        createRecordWithArraysOfDifferentElementType(Schema.create(BYTES), EMPTY_RECORD_SCHEMA) };
  }

  // "hybrid element type" means that each record have 3 arrays and only the 1st and 2nd arrays have the same element
  // type.
  @DataProvider(name = "recordWithArraysOfHybridElementType")
  public static Object[] recordWithArraysOfHybridElementType() {
    return new Object[] { createRecordWithArraysOfHybridElementType(Schema.create(INT), Schema.create(FLOAT)),
        createRecordWithArraysOfHybridElementType(EMPTY_RECORD_SCHEMA, Schema.create(LONG)),
        createRecordWithArraysOfHybridElementType(Schema.create(LONG), Schema.create(DOUBLE)),
        createRecordWithArraysOfHybridElementType(Schema.create(DOUBLE), EMPTY_RECORD_SCHEMA),
        createRecordWithArraysOfHybridElementType(Schema.create(BOOLEAN), Schema.create(STRING)),
        createRecordWithArraysOfHybridElementType(Schema.create(STRING), Schema.create(BYTES)),
        createRecordWithArraysOfHybridElementType(Schema.create(BYTES), EMPTY_RECORD_SCHEMA) };
  }

  private static Schema createRecordWithPrimitiveArraysOfSameElementType(Schema.Type elementType) {
    Map<String, Schema> fieldNameToArrayElementSchema = new HashMap<>(2);
    fieldNameToArrayElementSchema.put("field1", Schema.create(elementType));
    fieldNameToArrayElementSchema.put("field2", Schema.create(elementType));
    return createRecordWithArrays(fieldNameToArrayElementSchema);
  }

  private static Schema createRecordWithArraysOfDifferentElementType(Schema elementSchema1, Schema elementSchema2) {
    Map<String, Schema> fieldNameToArrayElementSchema = new HashMap<>(2);
    fieldNameToArrayElementSchema.put("field1", elementSchema1);
    fieldNameToArrayElementSchema.put("field2", elementSchema2);
    return createRecordWithArrays(fieldNameToArrayElementSchema);
  }

  private static Schema createRecordWithArraysOfHybridElementType(Schema elementSchema1, Schema elementSchema2) {
    Map<String, Schema> fieldNameToArrayElementSchema = new HashMap<>(2);
    // Only the 1st and 2nd arrays have the same element type
    fieldNameToArrayElementSchema.put("field1", elementSchema1);
    fieldNameToArrayElementSchema.put("field2", elementSchema1);
    fieldNameToArrayElementSchema.put("field3", elementSchema2);
    return createRecordWithArrays(fieldNameToArrayElementSchema);
  }

  private static Schema createRecordWithArrays(Map<String, Schema> fieldNameToArrayElementSchema) {
    Schema recordSchema = Schema.createRecord("RecordOfArray", "", "", false);
    List<Schema.Field> fields = new ArrayList<>(fieldNameToArrayElementSchema.size());

    fieldNameToArrayElementSchema.forEach((fieldName, arrayElementSchema) -> {
      fields.add(
          AvroCompatibilityHelper.newField(null)
              .setName(fieldName)
              .setSchema(Schema.createArray(arrayElementSchema))
              .setDoc("")
              .setDefault(null)
              .setOrder(Schema.Field.Order.ASCENDING)
              .build());
    });
    recordSchema.setFields(fields);
    return recordSchema;
  }

  private static Schema createRecordOfMap(Schema mapValueSchema) {
    Schema recordSchema = Schema.createRecord("RecordOfMap", "", "", false);
    Schema mapSchema = Schema.createMap(mapValueSchema);
    recordSchema.setFields(
        Arrays.asList(
            AvroCompatibilityHelper.newField(null)
                .setName("field1")
                .setSchema(mapSchema)
                .setDoc("")
                .setDefault(null)
                .setOrder(Schema.Field.Order.ASCENDING)
                .build(),
            AvroCompatibilityHelper.newField(null)
                .setName("field2")
                .setSchema(mapSchema)
                .setDoc("")
                .setDefault(null)
                .setOrder(Schema.Field.Order.ASCENDING)
                .build()));
    return recordSchema;
  }
}
