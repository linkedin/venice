package com.linkedin.davinci.client;

import org.apache.avro.Schema;


/**
 * This describes the implementation for the functional interface of {@link DaVinciRecordTransformer}
 */

@FunctionalInterface
public interface DaVinciRecordTransformerFunctionalInterface {
  DaVinciRecordTransformer apply(
      String storeName,
      Integer storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig);
}
