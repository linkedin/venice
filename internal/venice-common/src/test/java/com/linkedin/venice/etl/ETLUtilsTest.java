package com.linkedin.venice.etl;

import static com.linkedin.venice.utils.TestWriteUtils.ETL_UNION_VALUE_WITHOUT_NULL_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.ETL_UNION_VALUE_WITH_NULL_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.ETL_VALUE_SCHEMA;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ETLUtilsTest {
  @Test
  public void testTransformValueSchemaForETLForRecordSchema() {
    Schema schema = ETL_VALUE_SCHEMA;
    Schema etlValueSchema = ETLUtils.transformValueSchemaForETL(schema);

    Assert.assertEquals(Schema.Type.UNION, etlValueSchema.getType());
    List<Schema> types = etlValueSchema.getTypes();
    Assert.assertEquals(types.size(), 2);
    Assert.assertEquals(Schema.create(Schema.Type.NULL), types.get(0));
    Assert.assertEquals(schema, types.get(1));
  }

  @Test
  public void testTransformValueSchemaForETLForUnionSchemaWithoutNullField() {
    Schema schema = ETL_UNION_VALUE_WITHOUT_NULL_SCHEMA;
    Schema etlValueSchema = ETLUtils.transformValueSchemaForETL(schema);

    Assert.assertEquals(Schema.Type.UNION, etlValueSchema.getType());

    List<Schema> valueSchemaTypes = schema.getTypes();
    List<Schema> etlValueSchemaTypes = etlValueSchema.getTypes();

    List<Schema> expectedEtlValueSchemaTypes = new ArrayList<>();
    expectedEtlValueSchemaTypes.add(Schema.create(Schema.Type.NULL));
    expectedEtlValueSchemaTypes.addAll(valueSchemaTypes);

    Assert.assertEquals(etlValueSchemaTypes.size(), valueSchemaTypes.size() + 1);
    Assert.assertEquals(Schema.create(Schema.Type.NULL), etlValueSchemaTypes.get(0));
    Assert.assertEquals(etlValueSchemaTypes, expectedEtlValueSchemaTypes);
  }

  @Test
  public void testTransformValueSchemaForETLForUnionSchemaWithNullField() {
    Schema schema = ETL_UNION_VALUE_WITH_NULL_SCHEMA;
    Schema etlValueSchema = ETLUtils.transformValueSchemaForETL(schema);

    Assert.assertEquals(Schema.Type.UNION, etlValueSchema.getType());

    List<Schema> valueSchemaTypes = schema.getTypes();
    List<Schema> etlValueSchemaTypes = etlValueSchema.getTypes();

    Assert.assertEquals(etlValueSchemaTypes, valueSchemaTypes);
  }

  @Test
  public void testGetValueSchemaFromETLValueSchemaForRecordTypes() {
    Schema valueSchema = ETL_VALUE_SCHEMA;
    Schema etlValueSchema = ETLUtils.transformValueSchemaForETL(valueSchema);

    Schema inferredValueSchema =
        ETLUtils.getValueSchemaFromETLValueSchema(etlValueSchema, ETLValueSchemaTransformation.fromSchema(valueSchema));

    Assert.assertEquals(inferredValueSchema, valueSchema);
  }

  @Test
  public void testGetValueSchemaFromETLValueSchemaForUnionTypesWithoutNull() {
    Schema valueSchema = ETL_UNION_VALUE_WITHOUT_NULL_SCHEMA;
    Schema etlValueSchema = ETLUtils.transformValueSchemaForETL(valueSchema);

    Schema inferredValueSchema =
        ETLUtils.getValueSchemaFromETLValueSchema(etlValueSchema, ETLValueSchemaTransformation.fromSchema(valueSchema));

    Assert.assertEquals(inferredValueSchema, valueSchema);
  }

  @Test
  public void testGetValueSchemaFromETLValueSchemaForUnionTypesWithNull() {
    Schema valueSchema = ETL_UNION_VALUE_WITH_NULL_SCHEMA;
    Schema etlValueSchema = ETLUtils.transformValueSchemaForETL(valueSchema);

    Schema inferredValueSchema =
        ETLUtils.getValueSchemaFromETLValueSchema(etlValueSchema, ETLValueSchemaTransformation.fromSchema(valueSchema));

    Assert.assertEquals(inferredValueSchema, valueSchema);
  }
}
