package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.transport.GrpcRequestResponseConverter.getClusterStoreGrpcInfo;
import static com.linkedin.venice.controllerapi.transport.GrpcRequestResponseConverter.getControllerRequest;

import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.controllerapi.AclResponse;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.AdminCommandExecutionStatus;
import com.linkedin.venice.controllerapi.AdminTopicMetadataResponse;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.MultiVersionStatusResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.AdminCommandExecutionStatusRequest;
import com.linkedin.venice.controllerapi.request.AdminTopicMetadataRequest;
import com.linkedin.venice.controllerapi.request.ClusterDiscoveryRequest;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.controllerapi.request.UpdateAclForStoreRequest;
import com.linkedin.venice.controllerapi.request.UpdateAdminTopicMetadataRequest;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.protocols.AdminCommandExecutionStatusGrpcRequest;
import com.linkedin.venice.protocols.AdminCommandExecutionStatusGrpcResponse;
import com.linkedin.venice.protocols.AdminTopicMetadataGrpcRequest;
import com.linkedin.venice.protocols.AdminTopicMetadataGrpcResponse;
import com.linkedin.venice.protocols.BootstrappingVersion;
import com.linkedin.venice.protocols.CheckResourceCleanupForStoreCreationGrpcRequest;
import com.linkedin.venice.protocols.CheckResourceCleanupForStoreCreationGrpcResponse;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.LastSuccessfulAdminCommandExecutionGrpcRequest;
import com.linkedin.venice.protocols.LastSuccessfulAdminCommandExecutionGrpcResponse;
import com.linkedin.venice.protocols.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.ListBootstrappingVersionsGrpcRequest;
import com.linkedin.venice.protocols.ListBootstrappingVersionsGrpcResponse;
import com.linkedin.venice.protocols.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.UpdateAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.UpdateAdminTopicMetadataGrpcRequest;
import com.linkedin.venice.protocols.UpdateAdminTopicMetadataGrpcResponse;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceImplBase;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a gRPC service implementation for the VeniceController public API.
 */
public class VeniceControllerGrpcServiceImpl extends VeniceControllerGrpcServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerGrpcServiceImpl.class);

  private final VeniceControllerRequestHandler requestHandler;

  public VeniceControllerGrpcServiceImpl(VeniceControllerRequestHandler requestHandler) {
    this.requestHandler = requestHandler;
  }

  @Override
  public void getLeaderController(
      LeaderControllerGrpcRequest request,
      StreamObserver<LeaderControllerGrpcResponse> responseObserver) {
    String clusterName = request.getClusterName();
    LOGGER.info("Received gRPC request to get leader controller for cluster: {}", clusterName);
    try {
      LeaderControllerResponse response = new LeaderControllerResponse();
      requestHandler.getLeaderController(request.getClusterName(), response);
      responseObserver.onNext(
          LeaderControllerGrpcResponse.newBuilder()
              .setClusterName(response.getCluster())
              .setHttpUrl(response.getUrl())
              .setHttpsUrl(response.getSecureUrl())
              .setGrpcUrl(response.getGrpcUrl())
              .setSecureGrpcUrl(response.getSecureGrpcUrl())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error("Error while getting leader controller for cluster: {}", request.getClusterName(), e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while getting leader controller")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void createStore(
      CreateStoreGrpcRequest grpcRequest,
      StreamObserver<CreateStoreGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterStoreInfo().getClusterName();
    String storeName = grpcRequest.getClusterStoreInfo().getStoreName();
    LOGGER.debug("Received gRPC request to create store: {} in cluster: {}", storeName, clusterName);
    try {
      // TODO (sushantmane) : Add the ACL check for allowlist users here

      // Convert the gRPC request to the internal request object
      NewStoreRequest request = new NewStoreRequest(
          grpcRequest.getClusterStoreInfo().getClusterName(),
          grpcRequest.getClusterStoreInfo().getStoreName(),
          grpcRequest.hasOwner() ? grpcRequest.getOwner() : null,
          grpcRequest.getKeySchema(),
          grpcRequest.getValueSchema(),
          grpcRequest.hasAccessPermission() ? grpcRequest.getAccessPermission() : null,
          grpcRequest.getIsSystemStore());

      // Create the store using the internal request object
      NewStoreResponse response = new NewStoreResponse();
      requestHandler.createStore(request, response);

      // Convert the internal response object to the gRPC response object and send the gRPC response
      responseObserver.onNext(
          CreateStoreGrpcResponse.newBuilder()
              .setClusterStoreInfo(getClusterStoreGrpcInfo(response))
              .setOwner(response.getOwner())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      // Log the error with structured details
      LOGGER.error("Error while creating store: {} in cluster: {}", storeName, clusterName, e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while creating store: ")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void updateAclForStore(
      UpdateAclForStoreGrpcRequest grpcRequest,
      StreamObserver<UpdateAclForStoreGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterStoreInfo().getClusterName();
    String storeName = grpcRequest.getClusterStoreInfo().getStoreName();
    LOGGER.debug("Received gRPC request to update ACL for store: {} in cluster: {}", storeName, clusterName);
    try {
      // Convert the gRPC request to the internal request object
      UpdateAclForStoreRequest request = new UpdateAclForStoreRequest(
          grpcRequest.getClusterStoreInfo().getClusterName(),
          grpcRequest.getClusterStoreInfo().getStoreName(),
          grpcRequest.getAccessPermissions());

      // Create the store using the internal request object
      AclResponse response = new AclResponse();
      requestHandler.updateAclForStore(request, response);

      // Convert the internal response object to the gRPC response object and send the gRPC response
      responseObserver.onNext(
          UpdateAclForStoreGrpcResponse.newBuilder().setClusterStoreInfo(getClusterStoreGrpcInfo(response)).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error("Error while updating ACL for store: {} in cluster: {}", storeName, clusterName, e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while updating ACL for store: ")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void getAclForStore(
      GetAclForStoreGrpcRequest grpcRequest,
      StreamObserver<GetAclForStoreGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterStoreInfo().getClusterName();
    String storeName = grpcRequest.getClusterStoreInfo().getStoreName();
    LOGGER.debug("Received gRPC request to get ACL for store: {} in cluster: {}", storeName, clusterName);
    try {
      // Convert the gRPC request to the internal request object
      ControllerRequest request = getControllerRequest(grpcRequest.getClusterStoreInfo());

      // Create the store using the internal request object
      AclResponse response = new AclResponse();
      requestHandler.getAclForStore(request, response);

      // Convert the internal response object to the gRPC response object and send the gRPC response
      responseObserver.onNext(
          GetAclForStoreGrpcResponse.newBuilder()
              .setClusterStoreInfo(getClusterStoreGrpcInfo(response))
              .setAccessPermissions(response.getAccessPermissions())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error("Error while getting ACL for store: {} in cluster: {}", storeName, clusterName, e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while getting ACL for store: ")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void deleteAclForStore(
      DeleteAclForStoreGrpcRequest grpcRequest,
      StreamObserver<DeleteAclForStoreGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterStoreInfo().getClusterName();
    String storeName = grpcRequest.getClusterStoreInfo().getStoreName();
    LOGGER.debug("Received gRPC request to delete ACL for store: {} in cluster: {}", storeName, clusterName);
    try {
      // Convert the gRPC request to the internal request object
      ControllerRequest request = getControllerRequest(grpcRequest.getClusterStoreInfo());

      // Create the store using the internal request object
      AclResponse response = new AclResponse();
      requestHandler.deleteAclForStore(request, response);

      // Convert the internal response object to the gRPC response object and send the gRPC response
      responseObserver.onNext(
          DeleteAclForStoreGrpcResponse.newBuilder().setClusterStoreInfo(getClusterStoreGrpcInfo(response)).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error("Error while deleting ACL for store: {} in cluster: {}", storeName, clusterName, e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while deleting ACL for store: ")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void checkResourceCleanupForStoreCreation(
      CheckResourceCleanupForStoreCreationGrpcRequest grpcRequest,
      StreamObserver<CheckResourceCleanupForStoreCreationGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterStoreInfo().getClusterName();
    String storeName = grpcRequest.getClusterStoreInfo().getStoreName();
    LOGGER.debug(
        "Received gRPC request to check resource cleanup before creating store: {} in cluster: {}",
        storeName,
        clusterName);
    try {
      // Convert the gRPC request to the internal request object
      ControllerRequest request = getControllerRequest(grpcRequest.getClusterStoreInfo());

      // Create the store using the internal request object
      ControllerResponse response = new ControllerResponse();
      requestHandler.checkResourceCleanupBeforeStoreCreation(request, response);

      // Convert the internal response object to the gRPC response object and send the gRPC response
      responseObserver.onNext(
          CheckResourceCleanupForStoreCreationGrpcResponse.newBuilder()
              .setClusterStoreInfo(getClusterStoreGrpcInfo(response))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error(
          "Error while checking resource cleanup before creating store: {} in cluster: {}",
          storeName,
          clusterName,
          e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while checking resource cleanup before creating store: ")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void getAdminCommandExecutionStatus(
      AdminCommandExecutionStatusGrpcRequest request,
      StreamObserver<AdminCommandExecutionStatusGrpcResponse> responseObserver) {
    String clusterName = request.getClusterName();
    long executionId = request.getAdminCommandExecutionId();
    LOGGER.debug(
        "Received gRPC request to get admin command execution status for executionId: {} in cluster: {}",
        executionId,
        clusterName);
    try {
      AdminCommandExecutionResponse response = new AdminCommandExecutionResponse();
      requestHandler
          .getAdminCommandExecutionStatus(new AdminCommandExecutionStatusRequest(clusterName, executionId), response);
      AdminCommandExecutionStatusGrpcResponse.Builder responseBuilder =
          AdminCommandExecutionStatusGrpcResponse.newBuilder();
      responseBuilder.setClusterName(response.getCluster());
      AdminCommandExecution adminCommandExecution = response.getExecution();
      responseBuilder.setOperation(adminCommandExecution.getOperation());
      responseBuilder.setAdminCommandExecutionId(adminCommandExecution.getExecutionId());
      responseBuilder.setStartTime(adminCommandExecution.getStartTime());
      for (Map.Entry<String, AdminCommandExecutionStatus> entry: adminCommandExecution.getFabricToExecutionStatusMap()
          .entrySet()) {
        responseBuilder.putFabricToExecutionStatusMap(entry.getKey(), entry.getValue().name());
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error(
          "Error while getting admin command execution status for executionId: {} in cluster: {}",
          executionId,
          clusterName,
          e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while getting admin command execution status")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void getLastSuccessfulAdminCommandExecutionId(
      LastSuccessfulAdminCommandExecutionGrpcRequest request,
      StreamObserver<LastSuccessfulAdminCommandExecutionGrpcResponse> responseObserver) {
    String clusterName = request.getClusterName();
    LOGGER
        .debug("Received gRPC request to get last successful admin command execution id for cluster: {}", clusterName);
    try {
      LastSucceedExecutionIdResponse response = new LastSucceedExecutionIdResponse();
      requestHandler.getLastSucceedExecutionId(new ControllerRequest(clusterName), response);
      responseObserver.onNext(
          LastSuccessfulAdminCommandExecutionGrpcResponse.newBuilder()
              .setClusterName(response.getCluster())
              .setLastSuccessfulAdminCommandExecutionId(response.getLastSucceedExecutionId())
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error("Error while getting last successful admin command execution id for cluster: {}", clusterName, e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while getting last successful admin command execution id")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void getAdminTopicMetadata(
      AdminTopicMetadataGrpcRequest grpcRequest,
      StreamObserver<AdminTopicMetadataGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterName();
    String storeName = null;
    if (grpcRequest.hasStoreName()) {
      storeName = grpcRequest.getStoreName();
    }
    LOGGER.debug(
        "Received gRPC request to get admin topic metadata for cluster: {}{}",
        clusterName,
        storeName != null ? " and store: " + storeName : "");
    try {
      AdminTopicMetadataResponse response = new AdminTopicMetadataResponse();
      requestHandler.getAdminTopicMetadata(new AdminTopicMetadataRequest(clusterName, storeName), response);
      AdminTopicMetadataGrpcResponse.Builder responseBuilder = AdminTopicMetadataGrpcResponse.newBuilder()
          .setClusterName(response.getCluster())
          .setExecutionId(response.getExecutionId());
      if (response.getName() != null) {
        responseBuilder.setStoreName(response.getName());
      } else {
        responseBuilder.setOffset(response.getOffset()).setUpstreamOffset(response.getUpstreamOffset());
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error(
          "Error while getting admin topic metadata for cluster: {}{}",
          clusterName,
          storeName != null ? " and store: " + storeName : "",
          e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while getting admin topic metadata")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void updateAdminTopicMetadata(
      UpdateAdminTopicMetadataGrpcRequest grpcRequest,
      StreamObserver<UpdateAdminTopicMetadataGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterName();
    String storeName = null;
    if (grpcRequest.hasStoreName()) {
      storeName = grpcRequest.getStoreName();
    }
    Long executionId = grpcRequest.getExecutionId();
    Long offset = grpcRequest.hasOffset() ? grpcRequest.getOffset() : null;
    Long upstreamOffset = grpcRequest.hasUpstreamOffset() ? grpcRequest.getUpstreamOffset() : null;
    LOGGER.debug(
        "Received gRPC request to update admin topic metadata for cluster: {}{}",
        clusterName,
        storeName != null ? " and store: " + storeName : "");
    try {
      ControllerResponse response = new ControllerResponse();
      requestHandler.updateAdminTopicMetadata(
          new UpdateAdminTopicMetadataRequest(clusterName, storeName, executionId, offset, upstreamOffset),
          response);

      UpdateAdminTopicMetadataGrpcResponse.Builder responseBuilder =
          UpdateAdminTopicMetadataGrpcResponse.newBuilder().setClusterName(response.getCluster());
      if (response.getName() != null) {
        responseBuilder.setStoreName(response.getName());
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error(
          "Error while updating admin topic metadata for cluster: {}{}",
          clusterName,
          storeName != null ? " and store: " + storeName : "",
          e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while updating admin topic metadata")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void discoverClusterForStore(
      DiscoverClusterGrpcRequest grpcRequest,
      StreamObserver<DiscoverClusterGrpcResponse> responseObserver) {
    String storeName = grpcRequest.getStoreName();
    LOGGER.debug("Received gRPC request to discover cluster for store: {}", storeName);
    try {
      D2ServiceDiscoveryResponse response = new D2ServiceDiscoveryResponse();
      requestHandler.getClusterDiscovery(new ClusterDiscoveryRequest(grpcRequest.getStoreName()), response);
      DiscoverClusterGrpcResponse.Builder responseBuilder =
          DiscoverClusterGrpcResponse.newBuilder().setStoreName(response.getName());
      if (response.getCluster() != null) {
        responseBuilder.setClusterName(response.getCluster());
      }
      if (response.getD2Service() != null) {
        responseBuilder.setD2Service(response.getD2Service());
      }
      if (response.getServerD2Service() != null) {
        responseBuilder.setServerD2Service(response.getServerD2Service());
      }
      if (response.getZkAddress() != null) {
        responseBuilder.setZkAddress(response.getZkAddress());
      }
      if (response.getKafkaBootstrapServers() != null) {
        responseBuilder.setPubSubBootstrapServers(response.getKafkaBootstrapServers());
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error("Error while discovering cluster for store: {}", storeName, e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while discovering cluster for store")
              .withCause(e)
              .asRuntimeException());
    }
  }

  @Override
  public void listBootstrappingVersions(
      ListBootstrappingVersionsGrpcRequest grpcRequest,
      StreamObserver<ListBootstrappingVersionsGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterName();
    LOGGER.debug("Received gRPC request to list bootstrapping versions for cluster: {}", clusterName);
    try {
      MultiVersionStatusResponse response = new MultiVersionStatusResponse();
      requestHandler.listBootstrappingVersions(new ControllerRequest(clusterName), response);
      ListBootstrappingVersionsGrpcResponse.Builder responseBuilder =
          ListBootstrappingVersionsGrpcResponse.newBuilder().setClusterName(response.getCluster());
      for (Map.Entry<String, String> entry: response.getVersionStatusMap().entrySet()) {
        BootstrappingVersion.Builder bootstrappingVersionBuilder =
            BootstrappingVersion.newBuilder().setStoreVersionName(entry.getKey()).setVersionStatus(entry.getValue());
        responseBuilder.addBootstrappingVersions(bootstrappingVersionBuilder);
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error("Error while listing bootstrapping versions for cluster: {}", clusterName, e);
      responseObserver.onError(
          Status.fromCode(Code.INTERNAL)
              .withDescription("Error while listing bootstrapping versions")
              .withCause(e)
              .asRuntimeException());
    }
  }
}
