package com.linkedin.venice.hadoop.spark.utils;

import org.apache.spark.sql.types.StructType;
import scala.Int;
import scala.Option;
import scala.reflect.ClassTag;


public final class SparkScalaUtils {
  private SparkScalaUtils() {
  }

  public static <T> ClassTag<T> classTag(Class<T> clazz) {
    return scala.reflect.ClassManifestFactory.fromClass(clazz);
  }

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
