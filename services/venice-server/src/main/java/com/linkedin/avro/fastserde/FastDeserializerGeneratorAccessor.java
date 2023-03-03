package com.linkedin.avro.fastserde;

/**
 * Only for the sake of accessing a package-private method... can be removed once fast-avro is cleaned up.
 */
public class FastDeserializerGeneratorAccessor {
  public static void setFieldsPerPopulationMethod(int limit) {
    FastDeserializerGenerator.setFieldsPerPopulationMethod(limit);
  }
}
