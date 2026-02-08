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
import io.netty.channel.ChannelHandlerContext;


/**
 * This class is an incomplete copypasta of the logic in {@link StorageReadRequestHandler#channelRead(ChannelHandlerContext, Object)}.
 *
 * Besides the maintenance issue of the repeated code, and the incomplete functionality support, another potentially big
 * issue is that the threading model seems to be significantly different. This class does all the work in-line, in a
 * blocking fashion. All of these disparities are likely to cause significant issues in terms of trying to ramp the gRPC
 * path.
 *
 * TODO: Refactor with better abstractions so that gRPC and legacy endpoints have better code reuse and behavior parity.
 */
public class GrpcStorageReadRequestHandler extends VeniceServerGrpcHandler {
  private final StorageReadRequestHandler storage;

  public GrpcStorageReadRequestHandler(StorageReadRequestHandler storage) {
    this.storage = storage;
  }

  @Override
  public void processRequest(GrpcRequestContext ctx) {
    RouterRequest request = ctx.getRouterRequest();
    ReadResponse response = null;

    try {
      if (request.shouldRequestBeTerminatedEarly()) {
        throw new VeniceRequestEarlyTerminationException(request.getStoreName());
      }

      switch (request.getRequestType()) {
        case SINGLE_GET:
          // TODO: get rid of blocking here
          response = storage.handleSingleGetRequest((GetRouterRequest) request).get();
          break;
        case MULTI_GET:
          // TODO: get rid of blocking here
          response = storage.handleMultiGetRequest((MultiGetRouterRequestWrapper) request).get();
          break;
        default:
          ctx.setError();
          ctx.getVeniceServerResponseBuilder()
              .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST.getCode())
              .setErrorMessage("Unknown request type: " + request.getRequestType());
      }
    } catch (VeniceNoStoreException e) {
      ctx.setError();
      ctx.getVeniceServerResponseBuilder()
          .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST.getCode())
          .setErrorMessage("No storage exists for: " + e.getStoreName());
    } catch (Exception e) {
      ctx.setError();
      ctx.getVeniceServerResponseBuilder()
          .setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR.getCode())
          .setErrorMessage(String.format("Internal Error: %s", e.getMessage()));
    }

    if (!ctx.hasError() && response != null) {
      response.setRCU(ReadQuotaEnforcementHandler.getRcu(request));
      if (request.isStreamingRequest()) {
        response.setStreamingResponse();
      }

      ctx.setReadResponse(response);
    }

    invokeNextHandler(ctx);
  }
}
