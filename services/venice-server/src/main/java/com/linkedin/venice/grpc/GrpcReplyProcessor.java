package com.linkedin.venice.grpc;

import com.google.protobuf.ByteString;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.RequestStatsRecorder;
import com.linkedin.venice.listener.response.AbstractReadResponse;
import com.linkedin.venice.protocols.MultiKeyResponse;
import com.linkedin.venice.protocols.SingleGetResponse;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is responsible for sending responses to the client and recording request statistics.
 * Though it has methods which do not have any side effects, we have not made them static to allow
 * for easier testing of the callers.
 */
public class GrpcReplyProcessor {
  private static final Logger LOGGER = LogManager.getLogger(GrpcIoRequestProcessor.class);

  /**
   * Callers must ensure that all fields in the request context are properly set before invoking this method.
   * Callers must also use the appropriate {@link GrpcRequestContext#readResponseStatus} to comply with the API contract.
   *
   * @param requestContext The context of the request for which a response is being sent
   * @param <T> The type of the response observer
   */
  <T> void sendResponse(GrpcRequestContext<T> requestContext) {
    GrpcRequestContext.GrpcRequestType grpcRequestType = requestContext.getGrpcRequestType();
    switch (grpcRequestType) {
      case SINGLE_GET:
        sendSingleGetResponse((GrpcRequestContext<SingleGetResponse>) requestContext);
        break;
      case MULTI_GET:
      case COMPUTE:
        sendMultiKeyResponse((GrpcRequestContext<MultiKeyResponse>) requestContext);
        break;
      case LEGACY:
        sendVeniceServerResponse((GrpcRequestContext<VeniceServerResponse>) requestContext);
        break;
      default:
        VeniceException veniceException = new VeniceException("Unknown response type: " + grpcRequestType);
        LOGGER.error("Unknown response type: {}", grpcRequestType, veniceException);
        throw veniceException;
    }
  }

  /**
   * Sends a single get response to the client and records the request statistics via {@link #reportRequestStats}.
   * Since {@link io.grpc.stub.StreamObserver} is not thread-safe, synchronization is required before invoking
   * {@link io.grpc.stub.StreamObserver#onNext} and {@link io.grpc.stub.StreamObserver#onCompleted}.
   *
   * @param requestContext The context of the gRPC request, which includes the response and stats recorder to be updated.
   */
  void sendSingleGetResponse(GrpcRequestContext<SingleGetResponse> requestContext) {
    ReadResponse readResponse = requestContext.getReadResponse();
    SingleGetResponse.Builder builder = SingleGetResponse.newBuilder();
    VeniceReadResponseStatus responseStatus = requestContext.getReadResponseStatus();

    if (readResponse == null) {
      builder.setStatusCode(requestContext.getReadResponseStatus().getCode());
      builder.setErrorMessage(requestContext.getErrorMessage());
    } else if (readResponse.isFound()) {
      builder.setRcu(readResponse.getRCU())
          .setStatusCode(responseStatus.getCode())
          .setSchemaId(readResponse.getResponseSchemaIdHeader())
          .setCompressionStrategy(readResponse.getCompressionStrategy().getValue())
          .setContentLength(readResponse.getResponseBody().readableBytes())
          .setContentType(HttpConstants.AVRO_BINARY)
          .setValue(GrpcUtils.toByteString(readResponse.getResponseBody()));
    } else {
      builder.setStatusCode(responseStatus.getCode())
          .setRcu(readResponse.getRCU())
          .setErrorMessage("Key not found")
          .setContentLength(0);
    }

    StreamObserver<SingleGetResponse> responseObserver = requestContext.getResponseObserver();
    synchronized (responseObserver) {
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    }

    reportRequestStats(requestContext);
  }

  /**
   * Sends a multi key response (multiGet and compute requests) to the client and records the request statistics via {@link #reportRequestStats}.
   * Since {@link StreamObserver} is not thread-safe, synchronization is required before invoking
   * {@link StreamObserver#onNext} and {@link StreamObserver#onCompleted}.
   *
   * @param requestContext The context of the gRPC request, which includes the response and stats recorder to be updated.
   */
  void sendMultiKeyResponse(GrpcRequestContext<MultiKeyResponse> requestContext) {
    ReadResponse readResponse = requestContext.getReadResponse();
    MultiKeyResponse.Builder builder = MultiKeyResponse.newBuilder();
    VeniceReadResponseStatus responseStatus = requestContext.getReadResponseStatus();

    if (readResponse == null) {
      builder.setStatusCode(responseStatus.getCode());
      builder.setErrorMessage(requestContext.getErrorMessage());
    } else if (readResponse.isFound()) {
      builder.setStatusCode(responseStatus.getCode())
          .setRcu(readResponse.getRCU())
          .setCompressionStrategy(readResponse.getCompressionStrategy().getValue())
          .setContentLength(readResponse.getResponseBody().readableBytes())
          .setContentType(HttpConstants.AVRO_BINARY)
          .setValue(GrpcUtils.toByteString(readResponse.getResponseBody()));
    } else {
      builder.setStatusCode(responseStatus.getCode())
          .setRcu(readResponse.getRCU())
          .setErrorMessage("Key not found")
          .setContentLength(0);
    }

    StreamObserver<MultiKeyResponse> responseObserver = requestContext.getResponseObserver();
    synchronized (responseObserver) {
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    }
    reportRequestStats(requestContext);
  }

  /**
   * Sends response (for the legacy API) to the client and records the request statistics via {@link #reportRequestStats}.
   * Since {@link StreamObserver} is not thread-safe, synchronization is required before invoking
   * {@link StreamObserver#onNext} and {@link StreamObserver#onCompleted}.
   *
   * @param requestContext The context of the gRPC request, which includes the response and stats recorder to be updated.
   */
  void sendVeniceServerResponse(GrpcRequestContext<VeniceServerResponse> requestContext) {
    ReadResponse readResponse = requestContext.getReadResponse();
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
    VeniceReadResponseStatus readResponseStatus = requestContext.getReadResponseStatus();

    if (readResponse == null) {
      builder.setErrorCode(readResponseStatus.getCode());
      builder.setErrorMessage(requestContext.getErrorMessage());
    } else if (readResponse.isFound()) {
      builder.setErrorCode(readResponseStatus.getCode())
          .setResponseRCU(readResponse.getRCU())
          .setCompressionStrategy(readResponse.getCompressionStrategy().getValue())
          .setIsStreamingResponse(readResponse.isStreamingResponse())
          .setSchemaId(readResponse.getResponseSchemaIdHeader())
          .setData(GrpcUtils.toByteString(readResponse.getResponseBody()));
    } else {
      builder.setErrorCode(readResponseStatus.getCode()).setErrorMessage("Key not found").setData(ByteString.EMPTY);
    }

    StreamObserver<VeniceServerResponse> responseObserver = requestContext.getResponseObserver();
    synchronized (responseObserver) {
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    }

    reportRequestStats(requestContext);
  }

  /**
   * Records the request statistics based on the provided {@link GrpcRequestContext}.
   * This method updates the {@link RequestStatsRecorder} with statistics from the {@link GrpcRequestContext} and {@link ReadResponse}.
   * @param requestContext The context of the gRPC request, which contains the response and stats recorder to be updated.
   */
  void reportRequestStats(GrpcRequestContext requestContext) {
    ReadResponse readResponse = requestContext.getReadResponse();
    RequestStatsRecorder requestStatsRecorder = requestContext.getRequestStatsRecorder();
    AbstractReadResponse abstractReadResponse = (AbstractReadResponse) readResponse;
    if (readResponse == null) {
      requestStatsRecorder.setReadResponseStats(null).setResponseSize(0);
    } else if (readResponse.isFound()) {
      requestStatsRecorder.setReadResponseStats(abstractReadResponse.getReadResponseStatsRecorder())
          .setResponseSize(abstractReadResponse.getResponseBody().readableBytes());
    } else {
      requestStatsRecorder.setReadResponseStats(abstractReadResponse.getReadResponseStatsRecorder()).setResponseSize(0);
    }

    RequestStatsRecorder.recordRequestCompletionStats(requestContext.getRequestStatsRecorder(), true, -1);
  }
}
