package com.linkedin.venice.grpc;

import static com.linkedin.venice.grpc.GrpcRequestContext.GrpcRequestType.COMPUTE;
import static com.linkedin.venice.grpc.GrpcRequestContext.GrpcRequestType.LEGACY;
import static com.linkedin.venice.grpc.GrpcRequestContext.GrpcRequestType.MULTI_GET;
import static com.linkedin.venice.grpc.GrpcRequestContext.GrpcRequestType.SINGLE_GET;
import static com.linkedin.venice.listener.StorageReadRequestHandler.VENICE_STORAGE_NODE_HARDWARE_IS_NOT_HEALTHY_MSG;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.listener.StorageReadRequestHandler;
import com.linkedin.venice.listener.request.ComputeRouterRequestWrapper;
import com.linkedin.venice.listener.request.CurrentVersionRequest;
import com.linkedin.venice.listener.request.DictionaryFetchRequest;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.MetadataFetchRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.request.TopicPartitionIngestionContextRequest;
import com.linkedin.venice.listener.response.BinaryResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.protocols.AdminRequest;
import com.linkedin.venice.protocols.AdminResponse;
import com.linkedin.venice.protocols.CompressionDictionaryRequest;
import com.linkedin.venice.protocols.CompressionDictionaryResponse;
import com.linkedin.venice.protocols.ComputeRequest;
import com.linkedin.venice.protocols.CurrentVersionInfoRequest;
import com.linkedin.venice.protocols.CurrentVersionInfoResponse;
import com.linkedin.venice.protocols.HealthCheckRequest;
import com.linkedin.venice.protocols.HealthCheckResponse;
import com.linkedin.venice.protocols.IngestionContextRequest;
import com.linkedin.venice.protocols.IngestionContextResponse;
import com.linkedin.venice.protocols.MetadataRequest;
import com.linkedin.venice.protocols.MetadataResponse;
import com.linkedin.venice.protocols.MultiGetRequest;
import com.linkedin.venice.protocols.MultiKeyResponse;
import com.linkedin.venice.protocols.SingleGetRequest;
import com.linkedin.venice.protocols.SingleGetResponse;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.utils.ObjectMapperFactory;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class implements the Venice gRPC read service. It manages the processing of read requests
 * in a multi-threaded environment using Netty-based gRPC. To optimize performance and prevent deadlocks,
 * it handles IO and non-IO operations in different thread pools.
 *
 * ### Thread Pools in Netty-based gRPC:
 * 1. **Netty Boss Group**: Handles incoming connections.
 * 2. **Netty Worker Group**: Handles IO-related work (Netty IO threads).
 * 3. **gRPC Executor**: Executes gRPC service methods. The default gRPC executor is a caching thread pool,
 *    which creates a new thread for each incoming request. However, this is inefficient for handling
 *    a large number of requests, as it leads to thread creation overhead.
 *
 * Note: Netty threads and abstractions are not directly exposed to us.
 *
 * ### Custom gRPC Executor:
 * To avoid creating a new thread per request, we provide a fixed-size thread pool executor to the gRPC server builder.
 * The fixed thread pool ensures a controlled request processing concurrency.
 * However, to prevent deadlocks, we must avoid blocking the gRPC executor threads with long-running or blocking operations.
 *
 * ### Offloading IO Requests:
 * Since IO requests (like reading from storage) are blocking, they are offloaded to a separate thread pool.
 * The {@link StorageReadRequestHandler#executor} is responsible for processing these requests. Offloading IO requests
 * improves performance by ensuring that the gRPC executor is not tied up with slow operations, allowing it to process
 * new incoming requests efficiently.
 *
 * ### Non-IO Requests:
 * Non-IO requests, which are not expected to block, are processed directly in the gRPC executor threads.
 * For read requests, quota enforcement is performed in the gRPC executor thread. If the request passes quota enforcement,
 * it is then handed off to the IO thread pool for further processing.
 *
 * ### Rough Request Flow:
 * ```
 * +-------------------+    1. Incoming conn            +--------------------------+
 * | Netty Boss Group   | <---------------------------- | Client                   |
 * +-------------------+                                +--------------------------+
 *         |
 *         v
 * +-------------------+    2. Handles Req IO work      +--------------------------+
 * | Netty Worker Group | <---------------------------- | Storage / IO Operations  |
 * +-------------------+                                +--------------------------+
 *         |
 *         v
 * +-------------------+    3. Execute gRPC Method      +--------------------------+
 * | gRPC Executor      | <---------------------------- | gRPC Service Method      |
 * +-------------------+                                +--------------------------+
 *         |
 *         v
 * +-------------------+    4. Offload Blocking IO      +--------------------------+
 * | IO Thread Pool     | <---------------------------- | StorageReadRequestHandler|
 * +-------------------+                                +--------------------------+
 * ```
 */
public class VeniceGrpcReadServiceImpl extends VeniceReadServiceGrpc.VeniceReadServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceGrpcReadServiceImpl.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  private final DiskHealthCheckService diskHealthCheckService;
  private final StorageReadRequestHandler storageReadRequestHandler;
  private final GrpcServiceDependencies dependencies;
  private final GrpcIoRequestProcessor requestProcessor;

  public VeniceGrpcReadServiceImpl(GrpcServiceDependencies dependencies) {
    this(dependencies, new GrpcIoRequestProcessor(dependencies));
  }

  VeniceGrpcReadServiceImpl(GrpcServiceDependencies dependencies, GrpcIoRequestProcessor requestProcessor) {
    this.dependencies = dependencies;
    this.requestProcessor = requestProcessor;
    this.diskHealthCheckService = dependencies.getDiskHealthCheckService();
    this.storageReadRequestHandler = dependencies.getStorageReadRequestHandler();
  }

  /**
   * @deprecated This method is deprecated and will be removed in the future. Use the {@link #singleGet(SingleGetRequest, StreamObserver)} method instead.
   */
  @Deprecated
  @Override
  public void get(VeniceClientRequest singleGetRequest, StreamObserver<VeniceServerResponse> streamObserver) {
    GrpcRequestContext<VeniceServerResponse> clientRequestCtx =
        GrpcRequestContext.create(dependencies, streamObserver, LEGACY);
    try {
      RouterRequest routerRequest = GetRouterRequest.parseSingleGetGrpcRequest(singleGetRequest);
      clientRequestCtx.getRequestStatsRecorder()
          .setRequestInfo(routerRequest)
          .setRequestSize(singleGetRequest.getSerializedSize());
      clientRequestCtx.setRouterRequest(routerRequest);
      requestProcessor.processRequest(clientRequestCtx);
    } catch (Exception e) {
      LOGGER.debug("Error while processing single get request", e);
      VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
      builder.setErrorCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage(e.getMessage());
      }
      streamObserver.onNext(builder.build());
      streamObserver.onCompleted();
    }
  }

  /**
   * @deprecated This method is deprecated and will be removed in the future. Use the {@link #multiGet(MultiGetRequest, StreamObserver)} method instead.
   */
  @Deprecated
  @Override
  public void batchGet(VeniceClientRequest batchGetRequest, StreamObserver<VeniceServerResponse> streamObserver) {
    GrpcRequestContext<VeniceServerResponse> requestContext =
        GrpcRequestContext.create(dependencies, streamObserver, LEGACY);
    try {
      RouterRequest routerRequest = MultiGetRouterRequestWrapper.parseMultiGetGrpcRequest(batchGetRequest);
      requestContext.getRequestStatsRecorder()
          .setRequestInfo(routerRequest)
          .setRequestSize(batchGetRequest.getSerializedSize());
      requestContext.setRouterRequest(routerRequest);
      requestProcessor.processRequest(requestContext);
    } catch (Exception e) {
      LOGGER.debug("Error while processing batch-get request", e);
      VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
      builder.setErrorCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage(e.getMessage());
      }
      streamObserver.onNext(builder.build());
      streamObserver.onCompleted();
    }
  }

  @Override
  public void singleGet(SingleGetRequest singleGetRequest, StreamObserver<SingleGetResponse> streamObserver) {
    GrpcRequestContext<SingleGetResponse> requestContext =
        GrpcRequestContext.create(dependencies, streamObserver, SINGLE_GET);
    try {
      RouterRequest routerRequest = GetRouterRequest.parseSingleGetGrpcRequest(singleGetRequest);
      requestContext.getRequestStatsRecorder()
          .setRequestInfo(routerRequest)
          .setRequestSize(singleGetRequest.getSerializedSize());
      requestContext.setRouterRequest(routerRequest);
      requestProcessor.processRequest(requestContext);
    } catch (Exception e) {
      LOGGER.debug("Error while processing single get request", e);
      SingleGetResponse.Builder builder = SingleGetResponse.newBuilder();
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage(e.getMessage());
      }
      streamObserver.onNext(builder.build());
      streamObserver.onCompleted();
    }
  }

  @Override
  public void multiGet(MultiGetRequest multiGetRequest, StreamObserver<MultiKeyResponse> streamObserver) {
    GrpcRequestContext<MultiKeyResponse> requestContext =
        GrpcRequestContext.create(dependencies, streamObserver, MULTI_GET);
    try {
      RouterRequest routerRequest = MultiGetRouterRequestWrapper.parseMultiGetGrpcRequest(multiGetRequest);
      requestContext.getRequestStatsRecorder()
          .setRequestInfo(routerRequest)
          .setRequestSize(multiGetRequest.getSerializedSize());
      requestContext.setRouterRequest(routerRequest);
      requestProcessor.processRequest(requestContext);
    } catch (Exception e) {
      LOGGER.debug("Error while processing multi get request", e);
      MultiKeyResponse.Builder builder = MultiKeyResponse.newBuilder();
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage(e.getMessage());
      }
      streamObserver.onNext(builder.build());
      streamObserver.onCompleted();
    }
  }

  @Override
  public void compute(ComputeRequest computeRequest, StreamObserver<MultiKeyResponse> responseObserver) {
    GrpcRequestContext<MultiKeyResponse> requestContext =
        GrpcRequestContext.create(dependencies, responseObserver, COMPUTE);
    try {
      RouterRequest routerRequest = ComputeRouterRequestWrapper.parseComputeGrpcRequest(computeRequest);
      requestContext.getRequestStatsRecorder()
          .setRequestInfo(routerRequest)
          .setRequestSize(computeRequest.getSerializedSize());
      requestContext.setRouterRequest(routerRequest);
      requestProcessor.processRequest(requestContext);
    } catch (Exception e) {
      LOGGER.debug("Error while processing compute request", e);
      MultiKeyResponse.Builder builder = MultiKeyResponse.newBuilder();
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage(e.getMessage());
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void isServerHealthy(
      HealthCheckRequest healthCheckRequest,
      StreamObserver<HealthCheckResponse> responseObserver) {
    HealthCheckResponse.Builder builder = HealthCheckResponse.newBuilder();
    if (diskHealthCheckService.isDiskHealthy()) {
      builder.setStatusCode(VeniceReadResponseStatus.OK.getCode());
    } else {
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode())
          .setMessage(VENICE_STORAGE_NODE_HARDWARE_IS_NOT_HEALTHY_MSG);
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void getCompressionDictionary(
      CompressionDictionaryRequest dictionaryRequest,
      StreamObserver<CompressionDictionaryResponse> responseObserver) {
    CompressionDictionaryResponse.Builder builder = CompressionDictionaryResponse.newBuilder();
    try {
      String topicName =
          Version.composeKafkaTopic(dictionaryRequest.getStoreName(), dictionaryRequest.getStoreVersion());
      BinaryResponse binaryResponse = storageReadRequestHandler
          .handleDictionaryFetchRequest(DictionaryFetchRequest.create(dictionaryRequest.getStoreName(), topicName));
      builder.setStatusCode(binaryResponse.getStatus().getCode())
          .setContentType(HttpConstants.BINARY)
          .setContentLength(binaryResponse.getBody().readableBytes())
          .setValue(GrpcUtils.toByteString(binaryResponse.getBody()));
    } catch (Exception e) {
      LOGGER.error("Error while processing dictionary request", e);
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage("Error while processing dictionary request: " + e.getMessage());
      }
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void handleAdminRequest(AdminRequest request, StreamObserver<AdminResponse> responseObserver) {
    AdminResponse.Builder builder = AdminResponse.newBuilder();
    try {
      com.linkedin.davinci.listener.response.AdminResponse adminResponse = storageReadRequestHandler
          .handleServerAdminRequest(com.linkedin.venice.listener.request.AdminRequest.parseAdminGrpcRequest(request));
      if (!adminResponse.isError()) {
        builder.setSchemaId(com.linkedin.davinci.listener.response.AdminResponse.getResponseSchemaIdHeader())
            .setStatusCode(VeniceReadResponseStatus.OK.getCode())
            .setValue(GrpcUtils.toByteString(adminResponse.getResponseBody()))
            .setContentType(HttpConstants.AVRO_BINARY)
            .setContentLength(adminResponse.getResponseBody().readableBytes());
      } else {
        String errorMessage = adminResponse.getMessage() != null ? adminResponse.getMessage() : "Unknown error";
        builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode())
            .setErrorMessage(errorMessage)
            .setContentType(HttpConstants.TEXT_PLAIN);
      }
    } catch (Exception e) {
      LOGGER.error("Error while processing admin request", e);
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage("Error while processing admin request: " + e.getMessage());
      }
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void getMetadata(MetadataRequest request, StreamObserver<MetadataResponse> responseObserver) {
    MetadataResponse.Builder builder = MetadataResponse.newBuilder();
    try {
      MetadataFetchRequest metadataFetchRequest = MetadataFetchRequest.parseGetGrpcRequest(request);
      com.linkedin.davinci.listener.response.MetadataResponse metadataResponse =
          storageReadRequestHandler.handleMetadataFetchRequest(metadataFetchRequest);
      if (!metadataResponse.isError()) {
        builder.setStatusCode(VeniceReadResponseStatus.OK.getCode())
            .setValue(GrpcUtils.toByteString(metadataResponse.getResponseBody()))
            .setContentType(HttpConstants.AVRO_BINARY)
            .setContentLength(metadataResponse.getResponseBody().readableBytes())
            .setSchemaId(metadataResponse.getResponseSchemaIdHeader());
      } else {
        String errorMessage = metadataResponse.getMessage() != null ? metadataResponse.getMessage() : "Unknown error";
        builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode())
            .setErrorMessage(errorMessage)
            .setContentType(HttpConstants.TEXT_PLAIN);
      }
    } catch (Exception e) {
      LOGGER.error("Error while processing metadata request", e);
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage("Error while processing metadata request: " + e.getMessage());
      }
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void getCurrentVersionInfo(
      CurrentVersionInfoRequest request,
      StreamObserver<CurrentVersionInfoResponse> responseObserver) {
    CurrentVersionInfoResponse.Builder builder = CurrentVersionInfoResponse.newBuilder();
    try {
      CurrentVersionRequest currentVersionRequest = CurrentVersionRequest.parseGetGrpcRequest(request);
      ServerCurrentVersionResponse currentVersionResponse =
          storageReadRequestHandler.handleCurrentVersionRequest(currentVersionRequest);
      if (!currentVersionResponse.isError()) {
        builder.setStatusCode(VeniceReadResponseStatus.OK.getCode())
            .setCurrentVersion(currentVersionResponse.getCurrentVersion());
      } else {
        builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode())
            .setErrorMessage(
                currentVersionResponse.getMessage() != null ? currentVersionResponse.getMessage() : "Unknown error");
      }
    } catch (Exception e) {
      LOGGER.error("Error while processing current version info request", e);
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage("Error while processing current version info request: " + e.getMessage());
      }
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public void getIngestionContext(
      IngestionContextRequest request,
      StreamObserver<IngestionContextResponse> responseObserver) {
    IngestionContextResponse.Builder builder = IngestionContextResponse.newBuilder();
    try {
      TopicPartitionIngestionContextRequest ingestionContextRequest =
          TopicPartitionIngestionContextRequest.parseGetGrpcRequest(request);
      TopicPartitionIngestionContextResponse response =
          storageReadRequestHandler.handleTopicPartitionIngestionContextRequest(ingestionContextRequest);
      if (!response.isError()) {
        ByteString body = GrpcUtils.toByteString(response.getTopicPartitionIngestionContext());
        builder.setValue(body).setContentLength(body.size()).setStatusCode(VeniceReadResponseStatus.OK.getCode());
      } else {
        String errorMessage = response.getMessage() != null ? response.getMessage() : "Unknown error";
        builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode())
            .setErrorMessage(errorMessage)
            .setContentType(HttpConstants.TEXT_PLAIN);
      }
    } catch (Exception e) {
      LOGGER.error("Error while processing ingestion context request", e);
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage("Error while processing ingestion context request: " + e.getMessage());
      }
    }
    responseObserver.onNext(builder.build());
    responseObserver.onCompleted();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
