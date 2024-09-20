package com.linkedin.venice.spark.utils;

import org.apache.spark.sql.types.StructType;
import scala.Int;
import scala.Option;


/**
 * Utility methods for restricting Scala-ism in Spark code in a single class.
 */
public final class SparkScalaUtils {
  private SparkScalaUtils() {
  }

  /**
   * Get the index of a field in a StructType. If the field does not exist, return -1.
   */
  public static int getFieldIndex(StructType dataSchema, String fieldName) {
    Option<Object> fieldIndexOption = dataSchema.getFieldIndex(fieldName);
    if (fieldIndexOption.isEmpty()) {
      return -1;
    }

    Object item = fieldIndexOption.get();
    if (item instanceof Int) {
      return ((Int) item).toInt();
    } else if (item instanceof Integer) {
      return (Integer) item;
    }

    return -1;
  }
}
