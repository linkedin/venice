package com.linkedin.venice.controller.server;

import static com.linkedin.venice.pubsub.PubSubUtil.getPubSubPositionGrpcWireFormat;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.AdminCommandExecutionTracker;
import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controller.grpc.GrpcRequestResponseConverter;
import com.linkedin.venice.controller.kafka.consumer.AdminMetadata;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcRequest;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcResponse;
import com.linkedin.venice.protocols.controller.AdminTopicGrpcMetadata;
import com.linkedin.venice.protocols.controller.AdminTopicMetadataGrpcRequest;
import com.linkedin.venice.protocols.controller.AdminTopicMetadataGrpcResponse;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcRequest;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcResponse;
import com.linkedin.venice.protocols.controller.PubSubPositionGrpcWireFormat;
import com.linkedin.venice.protocols.controller.StoreMigrationCheckGrpcRequest;
import com.linkedin.venice.protocols.controller.StoreMigrationCheckGrpcResponse;
import com.linkedin.venice.protocols.controller.UpdateAdminOperationProtocolVersionGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAdminTopicMetadataGrpcRequest;
import com.linkedin.venice.pubsub.api.PubSubPosition;
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
    AdminMetadata metadata = admin.getAdminTopicMetadata(clusterName, Optional.ofNullable(storeName));
    adminMetadataBuilder.setExecutionId(AdminTopicMetadataAccessor.getExecutionId(metadata));
    if (storeName == null) {
      Pair<PubSubPosition, PubSubPosition> positions = AdminTopicMetadataAccessor.getPositions(metadata);
      PubSubPositionGrpcWireFormat positionGrpcWireFormat = getPubSubPositionGrpcWireFormat(positions.getFirst());
      PubSubPositionGrpcWireFormat upstreamPositionGrpcWireFormat =
          getPubSubPositionGrpcWireFormat(positions.getSecond());

      adminMetadataBuilder.setPosition(positionGrpcWireFormat);
      adminMetadataBuilder.setUpstreamPosition(upstreamPositionGrpcWireFormat);
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
    PubSubPositionGrpcWireFormat position = metadata.hasPosition() ? metadata.getPosition() : null;
    PubSubPositionGrpcWireFormat upstreamPosition =
        metadata.hasUpstreamPosition() ? metadata.getUpstreamPosition() : null;
    LOGGER.info(
        "Updating admin topic metadata for cluster: {}{} with execution id: {}",
        clusterName,
        storeName != null ? " and store: " + storeName : "",
        executionId);
    if (storeName != null && (position != null || upstreamPosition != null)) {
      throw new VeniceException("Updating positions is not allowed for store-level admin topic metadata");
    } else if (storeName == null && (position == null || upstreamPosition == null)) {
      throw new VeniceException("Positions must be provided to update cluster-level admin topic metadata");
    }
    admin.updateAdminTopicMetadata(
        clusterName,
        executionId,
        Optional.ofNullable(storeName),
        Optional.ofNullable(position),
        Optional.ofNullable(upstreamPosition));

    AdminTopicGrpcMetadata.Builder adminTopicGrpcMetadataBuilder =
        AdminTopicGrpcMetadata.newBuilder().setClusterName(clusterName).setExecutionId(executionId);

    if (storeName != null)
      adminTopicGrpcMetadataBuilder.setStoreName(storeName);
    if (position != null)
      adminTopicGrpcMetadataBuilder.setPosition(position);
    if (upstreamPosition != null)
      adminTopicGrpcMetadataBuilder.setUpstreamPosition(upstreamPosition);

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

  public StoreMigrationCheckGrpcResponse isStoreMigrationAllowed(StoreMigrationCheckGrpcRequest request) {
    String clusterName = request.getClusterName();
    if (StringUtils.isBlank(clusterName)) {
      throw new IllegalArgumentException("Cluster name is required for checking if store migration is allowed");
    }
    LOGGER.info("Checking if store migration is allowed for cluster: {}", clusterName);
    boolean isAllowed = admin.isStoreMigrationAllowed(clusterName);
    return StoreMigrationCheckGrpcResponse.newBuilder()
        .setClusterName(clusterName)
        .setStoreMigrationAllowed(isAllowed)
        .build();
  }
}
