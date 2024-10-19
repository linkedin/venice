package com.linkedin.venice.spark.utils;

import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;
import static org.apache.spark.sql.types.DataTypes.BinaryType;

import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SparkScalaUtilsTest {
  @Test
  public void testGetFieldIndex() {
    final StructType dataSchema = new StructType(
        new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
            new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()) });

    Assert.assertEquals(SparkScalaUtils.getFieldIndex(dataSchema, KEY_COLUMN_NAME), 0);
    Assert.assertEquals(SparkScalaUtils.getFieldIndex(dataSchema, VALUE_COLUMN_NAME), 1);
    Assert.assertEquals(SparkScalaUtils.getFieldIndex(dataSchema, "DUMMY_FILED"), -1);
  }
}
