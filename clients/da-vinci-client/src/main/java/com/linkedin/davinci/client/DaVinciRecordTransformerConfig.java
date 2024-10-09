package com.linkedin.davinci.client;

import org.apache.avro.Schema;


public class DaVinciRecordTransformerConfig {
  private final DaVinciRecordTransformerFunctionalInterface recordTransformerFunction;
  private final Class outputValueClass;
  private final Schema outputValueSchema;

  public DaVinciRecordTransformerConfig(
      DaVinciRecordTransformerFunctionalInterface recordTransformerFunction,
      Class outputValueClass,
      Schema outputValueSchema) {
    this.recordTransformerFunction = recordTransformerFunction;
    this.outputValueClass = outputValueClass;
    this.outputValueSchema = outputValueSchema;
  }

  public DaVinciRecordTransformerFunctionalInterface getRecordTransformerFunction() {
    return recordTransformerFunction;
  }

  public DaVinciRecordTransformer getRecordTransformer(Integer storeVersion) {
    return recordTransformerFunction.apply(storeVersion);
  }

  public Class getOutputValueClass() {
    return outputValueClass;
  }

  public Schema getOutputValueSchema() {
    return outputValueSchema;
  }

}
