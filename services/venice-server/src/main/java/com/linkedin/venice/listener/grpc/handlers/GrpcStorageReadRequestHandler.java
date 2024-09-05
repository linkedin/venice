package com.linkedin.venice.listener.grpc.handlers;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.listener.ReadQuotaEnforcementHandler;
import com.linkedin.venice.listener.StorageReadRequestHandler;
import com.linkedin.venice.listener.VeniceRequestEarlyTerminationException;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.utils.LatencyUtils;


public class GrpcStorageReadRequestHandler extends VeniceServerGrpcHandler {
  private final StorageReadRequestHandler storage;

  public GrpcStorageReadRequestHandler(StorageReadRequestHandler storage) {
    this.storage = storage;
  }

  @Override
  public void processRequest(GrpcRequestContext ctx) {
    RouterRequest request = ctx.getRouterRequest();
    final long preSubmissionTimeNs = System.nanoTime();
    ReadResponse response = null;
    double submissionWaitTime = -1;

    try {
      if (request.shouldRequestBeTerminatedEarly()) {
        throw new VeniceRequestEarlyTerminationException(request.getStoreName());
      }

      submissionWaitTime = LatencyUtils.getElapsedTimeFromNSToMS(preSubmissionTimeNs);
      switch (request.getRequestType()) {
        case SINGLE_GET:
          response = storage.handleSingleGetRequest((GetRouterRequest) request);
          break;
        case MULTI_GET:
          response = storage.handleMultiGetRequest((MultiGetRouterRequestWrapper) request);
          break;
        default:
          ctx.setError();
          ctx.getVeniceServerResponseBuilder()
              .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
              .setErrorMessage("Unknown request type: " + request.getRequestType());
      }
    } catch (VeniceNoStoreException e) {
      ctx.setError();
      ctx.getVeniceServerResponseBuilder()
          .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
          .setErrorMessage("No storage exists for: " + e.getStoreName());
    } catch (Exception e) {
      ctx.setError();
      ctx.getVeniceServerResponseBuilder()
          .setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR)
          .setErrorMessage(String.format("Internal Error: %s", e.getMessage()));
    }

    if (!ctx.hasError() && response != null) {
      response.setStorageExecutionSubmissionWaitTime(submissionWaitTime);
      response.setRCU(ReadQuotaEnforcementHandler.getRcu(request));
      if (request.isStreamingRequest()) {
        response.setStreamingResponse();
      }

      ctx.setReadResponse(response);
    }

    invokeNextHandler(ctx);
  }
}
