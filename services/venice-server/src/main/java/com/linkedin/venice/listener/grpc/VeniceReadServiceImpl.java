package com.linkedin.venice.listener.grpc;

import com.google.protobuf.ByteString;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.storage.ReadMetadataRetriever;
import com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcRequestProcessor;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceMetadataRequest;
import com.linkedin.venice.protocols.VeniceMetadataResponse;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceReadServiceImpl extends VeniceReadServiceGrpc.VeniceReadServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceReadServiceImpl.class);
  private static final String GRPC_METADATA_ERROR_PREFIX = "GRPC_METADATA_ERROR:";
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  private final VeniceServerGrpcRequestProcessor requestProcessor;
  private final ReadMetadataRetriever readMetadataRetriever;

  public VeniceReadServiceImpl(
      VeniceServerGrpcRequestProcessor requestProcessor,
      ReadMetadataRetriever readMetadataRetriever) {
    this.requestProcessor = requestProcessor;
    this.readMetadataRetriever = readMetadataRetriever;
  }

  @Override
  public void get(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    handleRequest(request, responseObserver);
  }

  @Override
  public void batchGet(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    handleRequest(request, responseObserver);
  }

  @Override
  public void getMetadata(VeniceMetadataRequest request, StreamObserver<VeniceMetadataResponse> responseObserver) {
    VeniceMetadataResponse.Builder responseBuilder = VeniceMetadataResponse.newBuilder();
    try {
      String storeName = request.getStoreName();
      MetadataResponse metadataResponse = readMetadataRetriever.getMetadata(storeName);

      if (metadataResponse.isError()) {
        String errorMessage = metadataResponse.getMessage() != null ? metadataResponse.getMessage() : "Unknown error";
        responseBuilder.setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR).setErrorMessage(errorMessage);
      } else {
        ByteBuf body = metadataResponse.getResponseBody();
        // nioBuffer() is zero-copy for heap-backed ByteBufs (which MetadataResponse produces via
        // Unpooled.wrappedBuffer). copyFrom() performs a single copy into ByteString's internal array.
        // If this becomes a bottleneck, UnsafeByteOperations.unsafeWrap(body.nioBuffer()) can eliminate
        // that copy entirely, but it bypasses ByteString's immutability guarantee — the caller must
        // ensure the underlying buffer is not modified or released while the ByteString is in use.
        responseBuilder.setMetadata(ByteString.copyFrom(body.nioBuffer()))
            .setResponseSchemaId(metadataResponse.getResponseSchemaIdHeader())
            .setErrorCode(VeniceReadResponseStatus.OK);
      }
    } catch (UnsupportedOperationException e) {
      // This happens when storageNodeReadQuotaEnabled is false for the store. The metadata endpoint is
      // designed for the fast client, which requires read quota enforcement on storage nodes.
      responseBuilder.setErrorCode(VeniceReadResponseStatus.BAD_REQUEST).setErrorMessage(e.getMessage());
    } catch (Exception e) {
      String filterKey = GRPC_METADATA_ERROR_PREFIX + request.getStoreName();
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(filterKey)) {
        LOGGER.error("Error handling gRPC metadata request for store: {}", request.getStoreName(), e);
      }
      responseBuilder.setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR)
          .setErrorMessage("Internal error: " + e.getMessage());
    }

    responseObserver.onNext(responseBuilder.build());
    responseObserver.onCompleted();
  }

  private void handleRequest(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    VeniceServerResponse.Builder responseBuilder =
        VeniceServerResponse.newBuilder().setErrorCode(VeniceReadResponseStatus.OK);
    GrpcRequestContext ctx = new GrpcRequestContext(request, responseBuilder, responseObserver);
    requestProcessor.process(ctx);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
