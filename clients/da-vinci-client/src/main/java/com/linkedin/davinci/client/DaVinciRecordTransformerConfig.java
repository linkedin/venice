package com.linkedin.davinci.client;

import org.apache.avro.Schema;


/**
 * Configuration class for {@link DaVinciRecordTransformer}, which is passed into {@link DaVinciConfig}.
 */
public class DaVinciRecordTransformerConfig {
  private final DaVinciRecordTransformerFunctionalInterface recordTransformerFunction;
  private final Class outputValueClass;
  private final Schema outputValueSchema;

  /**
   * @param recordTransformerFunction the functional interface for creating a {@link DaVinciRecordTransformer}
   * @param outputValueClass the class of the output value
   * @param outputValueSchema the schema of the output value
   */
  public DaVinciRecordTransformerConfig(
      DaVinciRecordTransformerFunctionalInterface recordTransformerFunction,
      Class outputValueClass,
      Schema outputValueSchema) {
    this.recordTransformerFunction = recordTransformerFunction;
    this.outputValueClass = outputValueClass;
    this.outputValueSchema = outputValueSchema;
  }

  /**
   * @return {@link #recordTransformerFunction}
   */
  public DaVinciRecordTransformerFunctionalInterface getRecordTransformerFunction() {
    return recordTransformerFunction;
  }

  /**
   * @return {@link #outputValueClass}
   */
  public Class getOutputValueClass() {
    return outputValueClass;
  }

  /**
   * @return {@link #outputValueSchema}
   */
  public Schema getOutputValueSchema() {
    return outputValueSchema;
  }
}
