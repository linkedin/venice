package com.linkedin.davinci.client;

/**
 * This describes the implementation for the functional interface of {@link DaVinciRecordTransformer}
 */

@FunctionalInterface
public interface DaVinciRecordTransformerFunctionalInterface {
  DaVinciRecordTransformer apply(Integer storeVersion, DaVinciRecordTransformerConfig recordTransformerConfig);
}
