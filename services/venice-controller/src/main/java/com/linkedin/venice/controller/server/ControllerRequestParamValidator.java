package com.linkedin.venice.controller.server;

import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
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

  public static void validateClusterStoreInfo(ClusterStoreGrpcInfo rpcContext) {
    if (StringUtils.isBlank(rpcContext.getClusterName())) {
      throw new IllegalArgumentException("Cluster name is mandatory parameter");
    }
    if (StringUtils.isBlank(rpcContext.getStoreName())) {
      throw new IllegalArgumentException("Store name is mandatory parameter");
    }
  }

  public static void validateAdminCommandExecutionRequest(String clusterName, long executionId) {
    if (StringUtils.isBlank(clusterName)) {
      throw new IllegalArgumentException("Cluster name is required for getting admin command execution status");
    }
    if (executionId <= 0) {
      throw new IllegalArgumentException("Admin command execution id with positive value is required");
    }
  }

  public static void validateAdminTopicMetadataRequest(String clusterName) {
    if (StringUtils.isBlank(clusterName)) {
      throw new IllegalArgumentException("Cluster name is required for getting admin command execution status");
    }
  }
}
