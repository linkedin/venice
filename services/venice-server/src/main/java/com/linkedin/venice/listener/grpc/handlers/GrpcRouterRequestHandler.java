package com.linkedin.venice.listener.grpc.handlers;

import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.listener.request.ComputeRouterRequestWrapper;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.protocols.VeniceClientRequest;


public class GrpcRouterRequestHandler extends VeniceServerGrpcHandler {
  public GrpcRouterRequestHandler() {
  }

  @Override
  public void processRequest(GrpcRequestContext ctx) {
    if (ctx.hasError()) {
      invokeNextHandler(ctx);
      return;
    }

    RouterRequest routerRequest;
    ServerStatsContext statsContext = ctx.getGrpcStatsContext();

    // Handle all requests through standard VeniceClientRequest
    VeniceClientRequest clientRequest = ctx.getVeniceClientRequest();

    // Determine request type based on method field or batch flag
    String method = clientRequest.getMethod();
    if (clientRequest.getIsBatchRequest()) {
      routerRequest = MultiGetRouterRequestWrapper.parseMultiGetGrpcRequest(clientRequest);
    } else if ("compute".equals(method)) {
      // Parse all compute requests as ComputeRouterRequestWrapper
      // The server will determine if it's CountByValue based on request content
      routerRequest = ComputeRouterRequestWrapper.parseGrpcComputeRequest(clientRequest);
    } else {
      // Handle single get requests
      routerRequest = GetRouterRequest.grpcGetRouterRequest(clientRequest);
    }

    statsContext.setRequestInfo(routerRequest);
    ctx.setRouterRequest(routerRequest);
    invokeNextHandler(ctx);
  }
}
