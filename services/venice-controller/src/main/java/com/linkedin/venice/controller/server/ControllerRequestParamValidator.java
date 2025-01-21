package com.linkedin.venice.controller.server;

import org.apache.commons.lang.StringUtils;


public class ControllerRequestParamValidator {
  public static void createStoreRequestValidator(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema) {
    if (StringUtils.isBlank(clusterName)) {
      throw new IllegalArgumentException("Cluster name is required for store creation");
    }
    if (StringUtils.isBlank(storeName)) {
      throw new IllegalArgumentException("Store name is required for store creation");
    }
    if (StringUtils.isBlank(keySchema)) {
      throw new IllegalArgumentException("Key schema is required for store creation");
    }
    if (StringUtils.isBlank(valueSchema)) {
      throw new IllegalArgumentException("Value schema is required for store creation");
    }
    if (owner == null) {
      throw new IllegalArgumentException("Owner is required for store creation");
    }
  }
}
