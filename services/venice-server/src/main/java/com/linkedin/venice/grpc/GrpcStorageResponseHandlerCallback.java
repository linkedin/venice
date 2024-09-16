package com.linkedin.venice.grpc;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.listener.StorageResponseHandlerCallback;
import com.linkedin.venice.response.VeniceReadResponseStatus;


/**
 * This class is a callback that is used to handle the response from the storage layer.
 *
 * Except for the cases where the response was returned before the storage layer was invoked, this class is used to
 * handle the response from the storage layer and pass it to the next handler in the pipeline.
 */
public class GrpcStorageResponseHandlerCallback implements StorageResponseHandlerCallback {
  private final GrpcRequestContext requestContext;
  private final GrpcReplyProcessor grpcReplyProcessor;

  private GrpcStorageResponseHandlerCallback(GrpcRequestContext requestContext, GrpcReplyProcessor grpcReplyProcessor) {
    this.requestContext = requestContext;
    this.grpcReplyProcessor = grpcReplyProcessor;
  }

  // Factory method for creating an instance of this class.
  public static GrpcStorageResponseHandlerCallback create(
      GrpcRequestContext requestContext,
      GrpcReplyProcessor grpcReplyProcessor) {
    return new GrpcStorageResponseHandlerCallback(requestContext, grpcReplyProcessor);
  }

  @Override
  public void onReadResponse(ReadResponse readResponse) {
    if (readResponse == null) {
      onError(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR, "StorageHandler returned a unexpected null response");
      return;
    }

    if (readResponse.isFound()) {
      requestContext.setReadResponseStatus(VeniceReadResponseStatus.OK);
    } else {
      requestContext.setReadResponseStatus(VeniceReadResponseStatus.KEY_NOT_FOUND);
    }
    requestContext.setReadResponse(readResponse);
    grpcReplyProcessor.sendResponse(requestContext);
  }

  @Override
  public void onError(VeniceReadResponseStatus readResponseStatus, String message) {
    requestContext.setReadResponseStatus(readResponseStatus);
    requestContext.setErrorMessage(message);
    requestContext.setReadResponse(null);
    grpcReplyProcessor.sendResponse(requestContext);
  }
}
