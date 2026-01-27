package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.AdminCommandExecutionTracker;
import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controller.grpc.GrpcRequestResponseConverter;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcRequest;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcResponse;
import com.linkedin.venice.protocols.controller.AdminTopicGrpcMetadata;
import com.linkedin.venice.protocols.controller.AdminTopicMetadataGrpcRequest;
import com.linkedin.venice.protocols.controller.AdminTopicMetadataGrpcResponse;
import com.linkedin.venice.protocols.controller.IsStoreMigrationAllowedGrpcRequest;
import com.linkedin.venice.protocols.controller.IsStoreMigrationAllowedGrpcResponse;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcRequest;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcResponse;
import com.linkedin.venice.protocols.controller.UpdateAdminOperationProtocolVersionGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAdminTopicMetadataGrpcRequest;
import com.linkedin.venice.utils.Pair;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ClusterAdminOpsRequestHandler {
  Logger LOGGER = LogManager.getLogger(ClusterAdminOpsRequestHandler.class);

  private final Admin admin;

  public ClusterAdminOpsRequestHandler(ControllerRequestHandlerDependencies dependencies) {
    this.admin = dependencies.getAdmin();
  }

  public AdminCommandExecutionStatusGrpcResponse getAdminCommandExecutionStatus(
      AdminCommandExecutionStatusGrpcRequest request) {
    String clusterName = request.getClusterName();
    long executionId = request.getAdminCommandExecutionId();
    ControllerRequestParamValidator.validateAdminCommandExecutionRequest(clusterName, executionId);
    LOGGER
        .info("Getting admin command execution status for cluster: {} and execution id: {}", clusterName, executionId);
    Optional<AdminCommandExecutionTracker> adminCommandExecutionTracker =
        admin.getAdminCommandExecutionTracker(clusterName);
    if (!adminCommandExecutionTracker.isPresent()) {
      throw new VeniceException(
          "Could not track execution in this controller for the cluster: " + clusterName
              + ". Make sure you send the command to a correct parent controller.");
    }
    AdminCommandExecutionTracker tracker = adminCommandExecutionTracker.get();
    AdminCommandExecution execution = tracker.checkExecutionStatus(executionId);
    if (execution == null) {
      throw new VeniceException(
          "Could not find the execution by given id: " + executionId + " in cluster: " + clusterName);
    }

    AdminCommandExecutionStatusGrpcResponse.Builder responseBuilder =
        AdminCommandExecutionStatusGrpcResponse.newBuilder();
    responseBuilder.setClusterName(clusterName).setAdminCommandExecutionId(execution.getExecutionId());
    if (execution.getOperation() != null) {
      responseBuilder.setOperation(execution.getOperation());
    }
    if (execution.getStartTime() != null) {
      responseBuilder.setStartTime(execution.getStartTime());
    }
    Map<String, String> fabricToExecutionStatusMap =
        GrpcRequestResponseConverter.toGrpcExecutionStatusMap(execution.getFabricToExecutionStatusMap());
    responseBuilder.putAllFabricToExecutionStatusMap(fabricToExecutionStatusMap);
    return responseBuilder.build();
  }

  public LastSuccessfulAdminCommandExecutionGrpcResponse getLastSucceedExecutionId(
      LastSuccessfulAdminCommandExecutionGrpcRequest request) {
    String clusterName = request.getClusterName();
    if (StringUtils.isBlank(clusterName)) {
      throw new IllegalArgumentException("Cluster name is required for getting last succeeded execution id");
    }
    LOGGER.info("Getting last succeeded execution id in cluster: {}", clusterName);
    long lastSucceedExecutionId = admin.getLastSucceedExecutionId(clusterName);
    return LastSuccessfulAdminCommandExecutionGrpcResponse.newBuilder()
        .setClusterName(clusterName)
        .setLastSuccessfulAdminCommandExecutionId(lastSucceedExecutionId)
        .build();
  }

  public AdminTopicMetadataGrpcResponse getAdminTopicMetadata(AdminTopicMetadataGrpcRequest request) {
    String clusterName = request.getClusterName();
    if (StringUtils.isBlank(clusterName)) {
      throw new IllegalArgumentException("Cluster name is required for getting admin topic metadata");
    }
    String storeName = request.hasStoreName() ? request.getStoreName() : null;
    LOGGER.info(
        "Getting admin topic metadata for cluster: {}{}",
        clusterName,
        storeName != null ? " and store: " + storeName : "");

    AdminTopicGrpcMetadata.Builder adminMetadataBuilder =
        AdminTopicGrpcMetadata.newBuilder().setClusterName(clusterName);
    Map<String, Long> metadata = admin.getAdminTopicMetadata(clusterName, Optional.ofNullable(storeName));
    adminMetadataBuilder.setExecutionId(AdminTopicMetadataAccessor.getExecutionId(metadata));
    if (storeName == null) {
      Pair<Long, Long> offsets = AdminTopicMetadataAccessor.getOffsets(metadata);
      adminMetadataBuilder.setOffset(offsets.getFirst());
      adminMetadataBuilder.setUpstreamOffset(offsets.getSecond());
      adminMetadataBuilder
          .setAdminOperationProtocolVersion(AdminTopicMetadataAccessor.getAdminOperationProtocolVersion(metadata));
    } else {
      adminMetadataBuilder.setStoreName(storeName);
    }
    return AdminTopicMetadataGrpcResponse.newBuilder().setMetadata(adminMetadataBuilder.build()).build();
  }

  public AdminTopicMetadataGrpcResponse updateAdminTopicMetadata(UpdateAdminTopicMetadataGrpcRequest request) {
    AdminTopicGrpcMetadata metadata = request.getMetadata();
    String clusterName = metadata.getClusterName();
    long executionId = metadata.getExecutionId();
    ControllerRequestParamValidator.validateAdminCommandExecutionRequest(clusterName, executionId);
    String storeName = metadata.hasStoreName() ? metadata.getStoreName() : null;
    Long offset = metadata.hasOffset() ? metadata.getOffset() : null;
    Long upstreamOffset = metadata.hasUpstreamOffset() ? metadata.getUpstreamOffset() : null;
    LOGGER.info(
        "Updating admin topic metadata for cluster: {}{} with execution id: {}",
        clusterName,
        storeName != null ? " and store: " + storeName : "",
        executionId);
    if (storeName != null && (offset != null || upstreamOffset != null)) {
      throw new VeniceException("Updating offsets is not allowed for store-level admin topic metadata");
    } else if (storeName == null && (offset == null || upstreamOffset == null)) {
      throw new VeniceException("Offsets must be provided to update cluster-level admin topic metadata");
    }
    admin.updateAdminTopicMetadata(
        clusterName,
        executionId,
        Optional.ofNullable(storeName),
        Optional.ofNullable(offset),
        Optional.ofNullable(upstreamOffset));

    AdminTopicGrpcMetadata.Builder adminTopicGrpcMetadataBuilder =
        AdminTopicGrpcMetadata.newBuilder().setClusterName(clusterName).setExecutionId(executionId);

    if (storeName != null)
      adminTopicGrpcMetadataBuilder.setStoreName(storeName);
    if (offset != null)
      adminTopicGrpcMetadataBuilder.setOffset(offset);
    if (upstreamOffset != null)
      adminTopicGrpcMetadataBuilder.setUpstreamOffset(upstreamOffset);

    AdminTopicMetadataGrpcResponse.Builder responseBuilder =
        AdminTopicMetadataGrpcResponse.newBuilder().setMetadata(adminTopicGrpcMetadataBuilder.build());
    return responseBuilder.build();
  }

  public AdminTopicMetadataGrpcResponse updateAdminOperationProtocolVersion(
      UpdateAdminOperationProtocolVersionGrpcRequest request) {
    String clusterName = request.getClusterName();
    long adminOperationProtocolVersion = request.getAdminOperationProtocolVersion();
    ControllerRequestParamValidator
        .validateAdminOperationProtocolVersionRequest(clusterName, adminOperationProtocolVersion);

    LOGGER.info(
        "Updating admin operation protocol version for cluster: {} to version: {}",
        clusterName,
        adminOperationProtocolVersion);

    admin.updateAdminOperationProtocolVersion(clusterName, adminOperationProtocolVersion);

    AdminTopicGrpcMetadata.Builder adminMetadataBuilder = AdminTopicGrpcMetadata.newBuilder()
        .setClusterName(clusterName)
        .setAdminOperationProtocolVersion(adminOperationProtocolVersion);
    return AdminTopicMetadataGrpcResponse.newBuilder().setMetadata(adminMetadataBuilder.build()).build();
  }

  public IsStoreMigrationAllowedGrpcResponse isStoreMigrationAllowed(IsStoreMigrationAllowedGrpcRequest request) {
    String clusterName = request.getClusterName();
    if (StringUtils.isBlank(clusterName)) {
      throw new IllegalArgumentException("Cluster name is required for checking store migration allowed");
    }
    LOGGER.info("Checking if store migration is allowed for cluster: {}", clusterName);
    boolean storeMigrationAllowed = admin.isStoreMigrationAllowed(clusterName);
    return IsStoreMigrationAllowedGrpcResponse.newBuilder()
        .setClusterName(clusterName)
        .setStoreMigrationAllowed(storeMigrationAllowed)
        .build();
  }
}
