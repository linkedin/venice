package com.linkedin.venice.etl;

import static com.linkedin.venice.utils.TestWriteUtils.ETL_UNION_VALUE_WITHOUT_NULL_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.ETL_UNION_VALUE_WITH_NULL_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.ETL_VALUE_SCHEMA;

import org.testng.Assert;
import org.testng.annotations.Test;


public class ETLValueSchemaTransformationTest {
  @Test
  public void testRecordSchemaBecomesUnionWithNull() {
    ETLValueSchemaTransformation transformation = ETLValueSchemaTransformation.fromSchema(ETL_VALUE_SCHEMA);
    Assert.assertEquals(transformation, ETLValueSchemaTransformation.UNIONIZE_WITH_NULL);
  }

  @Test
  public void testUnionSchemaWithoutNullAddsNull() {
    ETLValueSchemaTransformation transformation =
        ETLValueSchemaTransformation.fromSchema(ETL_UNION_VALUE_WITHOUT_NULL_SCHEMA);
    Assert.assertEquals(transformation, ETLValueSchemaTransformation.ADD_NULL_TO_UNION);
  }

  @Test
  public void testUnionSchemaWithNullStaysUnchanged() {
    ETLValueSchemaTransformation transformation =
        ETLValueSchemaTransformation.fromSchema(ETL_UNION_VALUE_WITH_NULL_SCHEMA);
    Assert.assertEquals(transformation, ETLValueSchemaTransformation.NONE);
  }
}
