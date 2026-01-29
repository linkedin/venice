package com.linkedin.venice.controller.grpc.server;

import static com.linkedin.venice.controller.grpc.server.ControllerGrpcServerUtils.handleRequest;

import com.linkedin.venice.controller.server.JobRequestHandler;
import com.linkedin.venice.protocols.controller.GetJobStatusGrpcRequest;
import com.linkedin.venice.protocols.controller.GetJobStatusGrpcResponse;
import com.linkedin.venice.protocols.controller.JobGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.JobGrpcServiceGrpc.JobGrpcServiceImplBase;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * gRPC service implementation for job-related operations.
 * No ACL check is required for job status queries.
 */
public class JobGrpcServiceImpl extends JobGrpcServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(JobGrpcServiceImpl.class);
  private final JobRequestHandler jobRequestHandler;

  public JobGrpcServiceImpl(JobRequestHandler jobRequestHandler) {
    this.jobRequestHandler = jobRequestHandler;
  }

  @Override
  public void getJobStatus(
      GetJobStatusGrpcRequest grpcRequest,
      StreamObserver<GetJobStatusGrpcResponse> responseObserver) {
    LOGGER.debug("Received getJobStatus with args: {}", grpcRequest);
    String clusterName = grpcRequest.getStoreInfo().getClusterName();
    String storeName = grpcRequest.getStoreInfo().getStoreName();
    // No ACL check for getting job metadata
    handleRequest(
        JobGrpcServiceGrpc.getGetJobStatusMethod(),
        () -> jobRequestHandler.getJobStatus(grpcRequest),
        responseObserver,
        clusterName,
        storeName);
  }
}
