package com.linkedin.venice.listener.grpc;

import com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcRequestProcessor;
import com.linkedin.venice.protocols.CountByValueRequest;
import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceReadServiceImpl extends VeniceReadServiceGrpc.VeniceReadServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceReadServiceImpl.class);

  private final VeniceServerGrpcRequestProcessor requestProcessor;

  public VeniceReadServiceImpl(VeniceServerGrpcRequestProcessor requestProcessor) {
    this.requestProcessor = requestProcessor;
  }

  @Override
  public void get(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    GrpcRequestContext ctx = new GrpcRequestContext(request, VeniceServerResponse.newBuilder(), responseObserver);
    requestProcessor.processRequest(ctx);
  }

  @Override
  public void batchGet(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    GrpcRequestContext ctx = new GrpcRequestContext(request, VeniceServerResponse.newBuilder(), responseObserver);
    requestProcessor.processRequest(ctx);
  }

  @Override
  public void countByValue(CountByValueRequest request, StreamObserver<CountByValueResponse> responseObserver) {
    try {
      CountByValueResponse response = requestProcessor.processCountByValue(request);

      // Ensure response has a valid error code
      if (response.getErrorCode() == 0) {
        LOGGER.warn("Response has undefined error code 0, setting to INTERNAL_ERROR");
        response = response.toBuilder()
            .setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR)
            .setErrorMessage(response.getErrorMessage().isEmpty() ? "Undefined error code" : response.getErrorMessage())
            .build();
      }

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.error("Error processing countByValue request", e);
      CountByValueResponse errorResponse = CountByValueResponse.newBuilder()
          .setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR)
          .setErrorMessage("Internal error: " + e.getMessage())
          .build();
      responseObserver.onNext(errorResponse);
      responseObserver.onCompleted();
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
